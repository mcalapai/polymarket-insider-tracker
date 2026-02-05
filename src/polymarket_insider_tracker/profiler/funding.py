"""Funding-chain indexing and tracing (bounded and strict).

This module implements:
- An on-demand inbound transfer index (`erc20_transfers`) for bounded lookback windows.
- A strict funding-chain tracer that consumes the index (no genesis scans).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from web3 import AsyncWeb3

from polymarket_insider_tracker.profiler.entities import EntityRegistry
from polymarket_insider_tracker.profiler.models import FundingChain, FundingTransfer
from polymarket_insider_tracker.storage.repos import ERC20TransferDTO, ERC20TransferRepository

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from polymarket_insider_tracker.profiler.chain import PolygonClient
    from polymarket_insider_tracker.storage.repos import RelationshipRepository

logger = logging.getLogger(__name__)

# ERC20 Transfer(address,address,uint256)
TRANSFER_EVENT_SIGNATURE = AsyncWeb3.keccak(text="Transfer(address,address,uint256)").hex()


class FundingTraceError(Exception):
    """Raised when a funding trace cannot be completed within configured bounds."""


def _pad_topic_address(address: str) -> str:
    return "0x" + address.lower().replace("0x", "").zfill(64)


def _topic_to_address(topic: Any) -> str:
    # topic may be HexBytes or bytes-like.
    hexed = topic.hex() if hasattr(topic, "hex") else str(topic)
    if hexed.startswith("0x"):
        hexed = hexed[2:]
    return ("0x" + hexed[-40:]).lower()


@dataclass(frozen=True)
class IndexWindow:
    start: datetime
    end: datetime


class ERC20TransferIndexer:
    """On-demand inbound transfer indexer for a bounded lookback window."""

    def __init__(
        self,
        polygon_client: PolygonClient,
        *,
        token_addresses: list[str],
        lookback_days: int,
        logs_chunk_size_blocks: int,
    ) -> None:
        self._polygon = polygon_client
        self._token_addresses = [t.lower() for t in token_addresses]
        self._lookback_days = lookback_days
        self._chunk = logs_chunk_size_blocks

    async def index_inbound_transfers(
        self,
        session: AsyncSession,
        *,
        to_address: str,
        window: IndexWindow | None = None,
    ) -> int:
        to_address = to_address.lower()
        effective_window = window or IndexWindow(
            start=datetime.now(UTC) - timedelta(days=self._lookback_days),
            end=datetime.now(UTC),
        )

        start_block = await self._polygon.get_block_number_at_or_before(effective_window.start)
        end_block = await self._polygon.get_block_number_at_or_before(effective_window.end)
        if end_block < start_block:
            raise FundingTraceError("Invalid index window (end before start)")

        repo = ERC20TransferRepository(session)
        inserted_total = 0
        for token_address in self._token_addresses:
            inserted_total += await self._index_token_inbound(
                repo,
                to_address=to_address,
                token_address=token_address,
                start_block=start_block,
                end_block=end_block,
            )
        return inserted_total

    async def _index_token_inbound(
        self,
        repo: ERC20TransferRepository,
        *,
        to_address: str,
        token_address: str,
        start_block: int,
        end_block: int,
    ) -> int:
        dtos: list[ERC20TransferDTO] = []
        for from_block in range(start_block, end_block + 1, self._chunk):
            to_block = min(end_block, from_block + self._chunk - 1)
            logs = await self._polygon.get_logs(
                {
                    "address": AsyncWeb3.to_checksum_address(token_address),
                    "topics": [
                        TRANSFER_EVENT_SIGNATURE,
                        None,
                        _pad_topic_address(to_address),
                    ],
                    "fromBlock": from_block,
                    "toBlock": to_block,
                }
            )
            if not logs:
                continue

            for log in logs:
                block_number = int(log["blockNumber"])
                block = await self._polygon.get_block(block_number)
                ts = datetime.fromtimestamp(int(block["timestamp"]), tz=UTC)
                tx_hash = (
                    log["transactionHash"].hex()
                    if hasattr(log["transactionHash"], "hex")
                    else str(log["transactionHash"])
                )
                dtos.append(
                    ERC20TransferDTO(
                        token_address=token_address,
                        from_address=_topic_to_address(log["topics"][1]),
                        to_address=_topic_to_address(log["topics"][2]),
                        amount_units=Decimal(int(log["data"].hex(), 16)),
                        tx_hash=tx_hash,
                        log_index=int(log.get("logIndex") or log.get("log_index") or 0),
                        block_number=block_number,
                        timestamp=ts,
                    )
                )

        if not dtos:
            return 0
        await repo.insert_many(dtos)
        return len(dtos)


class FundingTracer:
    """Strict funding-chain tracer backed by the transfer index."""

    def __init__(
        self,
        polygon_client: PolygonClient,
        *,
        token_addresses: list[str],
        lookback_days: int,
        logs_chunk_size_blocks: int,
        max_hops: int = 3,
        entity_registry: EntityRegistry | None = None,
    ) -> None:
        self._polygon = polygon_client
        self._token_addresses = [t.lower() for t in token_addresses]
        self._lookback_days = lookback_days
        self._max_hops = max_hops
        self._entity_registry = entity_registry or EntityRegistry()
        self._indexer = ERC20TransferIndexer(
            polygon_client,
            token_addresses=self._token_addresses,
            lookback_days=lookback_days,
            logs_chunk_size_blocks=logs_chunk_size_blocks,
        )

    async def ensure_first_inbound_transfer(
        self,
        session: AsyncSession,
        *,
        address: str,
        as_of: datetime,
    ) -> ERC20TransferDTO:
        if as_of.tzinfo is None:
            raise ValueError("as_of must be timezone-aware")

        since = as_of - timedelta(days=self._lookback_days)
        repo = ERC20TransferRepository(session)
        first = await repo.get_first_transfer_to(
            address,
            token_addresses=self._token_addresses,
            since=since,
        )
        if first is not None:
            return first

        await self._indexer.index_inbound_transfers(
            session,
            to_address=address,
            window=IndexWindow(start=since, end=as_of),
        )

        first = await repo.get_first_transfer_to(
            address,
            token_addresses=self._token_addresses,
            since=since,
        )
        if first is None:
            raise FundingTraceError(
                f"No inbound transfer indexed for {address} within {self._lookback_days}d lookback"
            )
        return first

    async def trace(
        self,
        session: AsyncSession,
        *,
        address: str,
        as_of: datetime,
        max_hops: int | None = None,
    ) -> FundingChain:
        normalized_address = address.lower()
        effective_max_hops = max_hops if max_hops is not None else self._max_hops

        chain: list[FundingTransfer] = []
        current = normalized_address
        origin_address = normalized_address
        origin_type = "unknown"

        for _hop in range(effective_max_hops):
            if self._entity_registry.is_terminal(current):
                origin_address = current
                origin_type = self._entity_registry.classify(current).value
                break

            first = await self.ensure_first_inbound_transfer(session, address=current, as_of=as_of)
            transfer = FundingTransfer(
                from_address=first.from_address,
                to_address=first.to_address,
                amount=first.amount_units,
                token="USDC",
                tx_hash=first.tx_hash,
                block_number=first.block_number,
                timestamp=first.timestamp,
            )
            chain.append(transfer)
            origin_address = transfer.from_address
            current = transfer.from_address

            if self._entity_registry.is_terminal(origin_address):
                origin_type = self._entity_registry.classify(origin_address).value
                break

        return FundingChain(
            target_address=normalized_address,
            chain=chain,
            origin_address=origin_address,
            origin_type=origin_type,
            hop_count=len(chain),
            traced_at=datetime.now(UTC),
        )

    async def persist_relationships(
        self,
        session: AsyncSession,
        chain: FundingChain,
        *,
        confidence: Decimal = Decimal("1.0"),
        shares_funder_time_band_minutes: int = 30,
        funding_burst_min_wallets: int = 3,
    ) -> None:
        """Persist graph edges derived from indexed transfers and trace results."""
        from polymarket_insider_tracker.storage.repos import RelationshipRepository, WalletRelationshipDTO

        repo = RelationshipRepository(session)
        if not chain.chain:
            return

        # funded_by edges for each hop
        for transfer in chain.chain:
            await repo.upsert(
                WalletRelationshipDTO(
                    wallet_a=transfer.to_address.lower(),
                    wallet_b=transfer.from_address.lower(),
                    relationship_type="funded_by",
                    confidence=confidence,
                )
            )

        # Derive shares_funder + funding_burst from the first hop.
        first = chain.chain[0]
        band = timedelta(minutes=shares_funder_time_band_minutes)
        start = first.timestamp - band
        end = first.timestamp + band

        transfer_repo = ERC20TransferRepository(session)
        candidates = await transfer_repo.list_transfers_from_in_window(
            first.from_address,
            token_addresses=self._token_addresses,
            start=start,
            end=end,
        )
        recipients = {t.to_address.lower() for t in candidates}
        if len(recipients) >= funding_burst_min_wallets:
            burst_conf = Decimal(str(min(1.0, len(recipients) / 10.0)))
            for wallet in recipients:
                await repo.upsert(
                    WalletRelationshipDTO(
                        wallet_a=wallet,
                        wallet_b=first.from_address.lower(),
                        relationship_type="funding_burst",
                        confidence=burst_conf,
                    )
                )

        recipients_sorted = sorted(recipients)
        for i in range(len(recipients_sorted)):
            for j in range(i + 1, len(recipients_sorted)):
                a, b = recipients_sorted[i], recipients_sorted[j]
                await repo.upsert(
                    WalletRelationshipDTO(
                        wallet_a=a,
                        wallet_b=b,
                        relationship_type="shares_funder",
                        confidence=Decimal("0.70"),
                    )
                )

    def get_suspiciousness_score(self, chain: FundingChain) -> float:
        """Calculate a suspiciousness score based on funding chain.

        Higher scores indicate more suspicious funding patterns:
        - CEX origin: Lower suspicion (0.0-0.2)
        - Bridge origin: Low suspicion (0.2-0.4)
        - Unknown origin with few hops: High suspicion (0.8-1.0)
        - Unknown origin with many hops: Medium suspicion (0.5-0.8)

        Args:
            chain: Funding chain to score.

        Returns:
            Suspiciousness score from 0.0 to 1.0.
        """
        if chain.is_cex_origin:
            # CEX origin is least suspicious
            return 0.1

        if chain.is_bridge_origin:
            # Bridge origin is slightly more suspicious
            return 0.3

        # Unknown/other terminal origins (contracts, EOA funders, etc.).
        if chain.hop_count == 0:
            # This can happen when the target itself is a terminal entity that's
            # not a CEX/bridge (or when tracing starts at a known contract).
            return 0.6

        if chain.hop_count >= self._max_hops:
            # Max hops reached without finding a CEX/bridge.
            return 0.75

        # Fewer hops without reaching a terminal entity is more suspicious.
        return 0.55 + (0.2 * (1 - chain.hop_count / self._max_hops))
