"""Wallet snapshot computation for trade-time analysis.

This module intentionally avoids "best-effort" fallbacks: if required inputs
cannot be computed, it raises a domain error.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Awaitable, Callable

from redis.asyncio import Redis

from polymarket_insider_tracker.profiler.chain import PolygonClient
from polymarket_insider_tracker.profiler.models import WalletSnapshot
from decimal import Decimal

logger = logging.getLogger(__name__)

# USDC contract address on Polygon (native USDC)
USDC_POLYGON_ADDRESS = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"

# Default configuration
DEFAULT_PROFILE_CACHE_TTL = 300  # 5 minutes


class WalletAnalyzerError(Exception):
    """Raised when a wallet snapshot cannot be computed strictly."""


FirstFundingAtProvider = Callable[[str, datetime], Awaitable[datetime]]


class WalletAnalyzer:
    """Computes strict, time-aware wallet snapshots."""

    def __init__(
        self,
        polygon_client: PolygonClient,
        *,
        redis: Redis | None = None,
        cache_ttl_seconds: int = DEFAULT_PROFILE_CACHE_TTL,
        usdc_addresses: tuple[str, ...] = (USDC_POLYGON_ADDRESS,),
        first_funding_at_provider: FirstFundingAtProvider | None = None,
    ) -> None:
        self._client = polygon_client
        self._redis = redis
        self._cache_ttl = cache_ttl_seconds
        self._usdc_addresses = tuple(a.lower() for a in usdc_addresses)
        self._first_funding_at_provider = first_funding_at_provider
        self._cache_prefix = "wallet_snapshot:"

    def _cache_key(self, address: str, *, as_of_block_number: int) -> str:
        return f"{self._cache_prefix}{address.lower()}:{as_of_block_number}"

    async def _get_cached_snapshot(
        self,
        address: str,
        *,
        as_of_block_number: int,
    ) -> WalletSnapshot | None:
        if not self._redis:
            return None
        try:
            cached = await self._redis.get(
                self._cache_key(address, as_of_block_number=as_of_block_number)
            )
            if cached is None:
                return None
            data = json.loads(cached if isinstance(cached, str) else cached.decode())
            return WalletSnapshot(
                address=data["address"],
                as_of=datetime.fromisoformat(data["as_of"]),
                as_of_block_number=int(data["as_of_block_number"]),
                nonce_as_of=int(data["nonce_as_of"]),
                matic_balance_wei_as_of=Decimal(data["matic_balance_wei_as_of"]),
                usdc_balance_units_as_of=Decimal(data["usdc_balance_units_as_of"]),
                first_funding_at=datetime.fromisoformat(data["first_funding_at"]),
                age_hours_as_of=float(data["age_hours_as_of"]),
                computed_at=datetime.fromisoformat(data["computed_at"]),
            )
        except Exception as e:
            logger.warning("Failed to parse cached wallet snapshot for %s: %s", address, e)
            return None

    async def _cache_snapshot(self, snapshot: WalletSnapshot) -> None:
        if not self._redis:
            return
        try:
            payload = {
                "address": snapshot.address,
                "as_of": snapshot.as_of.isoformat(),
                "as_of_block_number": snapshot.as_of_block_number,
                "nonce_as_of": snapshot.nonce_as_of,
                "matic_balance_wei_as_of": str(snapshot.matic_balance_wei_as_of),
                "usdc_balance_units_as_of": str(snapshot.usdc_balance_units_as_of),
                "first_funding_at": snapshot.first_funding_at.isoformat(),
                "age_hours_as_of": snapshot.age_hours_as_of,
                "computed_at": snapshot.computed_at.isoformat(),
            }
            await self._redis.set(
                self._cache_key(snapshot.address, as_of_block_number=snapshot.as_of_block_number),
                json.dumps(payload),
                ex=self._cache_ttl,
            )
        except Exception as e:
            logger.warning("Failed to cache wallet snapshot for %s: %s", snapshot.address, e)

    async def analyze(
        self,
        address: str,
        *,
        as_of: datetime,
        force_refresh: bool = False,
    ) -> WalletSnapshot:
        """Compute a strict wallet snapshot as-of a timestamp."""
        if as_of.tzinfo is None:
            raise WalletAnalyzerError("as_of must be timezone-aware")

        normalized_address = address.lower()
        as_of_block_number = await self._client.get_block_number_at_or_before(as_of)

        if not force_refresh:
            cached = await self._get_cached_snapshot(
                normalized_address,
                as_of_block_number=as_of_block_number,
            )
            if cached is not None:
                return cached

        if not self._first_funding_at_provider:
            raise WalletAnalyzerError(
                "first_funding_at_provider is required (wallet age is defined via first USDC funding)"
            )

        nonce_as_of = await self._client.get_transaction_count(
            normalized_address, block_number=as_of_block_number
        )
        matic_balance = await self._client.get_balance(
            normalized_address, block_number=as_of_block_number
        )
        usdc_balances = [
            await self._client.get_token_balance_at_block(
                normalized_address,
                token_address,
                block_number=as_of_block_number,
            )
            for token_address in self._usdc_addresses
        ]
        usdc_balance = sum(usdc_balances, Decimal(0))

        first_funding_at = await self._first_funding_at_provider(normalized_address, as_of)
        # NOTE: first_funding_at is defined as the earliest inbound USDC funding
        # within the configured lookback window, strictly as-of the trade time.
        # The provider must ensure it is <= as_of or raise.
        age_hours_as_of = (as_of - first_funding_at).total_seconds() / 3600.0
        if age_hours_as_of < 0:
            raise WalletAnalyzerError("first_funding_at is after as_of")

        snapshot = WalletSnapshot(
            address=normalized_address,
            as_of=as_of,
            as_of_block_number=as_of_block_number,
            nonce_as_of=nonce_as_of,
            matic_balance_wei_as_of=matic_balance,
            usdc_balance_units_as_of=usdc_balance,
            first_funding_at=first_funding_at,
            age_hours_as_of=age_hours_as_of,
        )
        await self._cache_snapshot(snapshot)
        return snapshot
