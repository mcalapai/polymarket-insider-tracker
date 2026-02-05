"""Tests for the wallet analyzer snapshot computation."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from polymarket_insider_tracker.profiler.analyzer import USDC_POLYGON_ADDRESS, WalletAnalyzer
from polymarket_insider_tracker.profiler.models import WalletSnapshot

VALID_ADDRESS = "0x742d35Cc6634C0532925a3b844Bc9e7595f5eaE2"


@pytest.fixture
def mock_client() -> AsyncMock:
    client = AsyncMock()
    client.get_block_number_at_or_before = AsyncMock(return_value=12_345_678)
    client.get_transaction_count = AsyncMock(return_value=3)
    client.get_balance = AsyncMock(return_value=Decimal("5000000000000000000"))
    client.get_token_balance_at_block = AsyncMock(return_value=Decimal("1000000"))
    return client


@pytest.fixture
def mock_redis() -> AsyncMock:
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.set = AsyncMock()
    return redis


async def first_funding_at_provider(_address: str, _as_of: datetime) -> datetime:
    return datetime(2026, 1, 1, tzinfo=UTC)


class TestWalletAnalyzerInit:
    def test_init_defaults(self, mock_client: AsyncMock) -> None:
        analyzer = WalletAnalyzer(mock_client)
        assert analyzer._client is mock_client
        assert analyzer._redis is None
        assert analyzer._usdc_addresses == (USDC_POLYGON_ADDRESS.lower(),)


class TestWalletAnalyzerAnalyze:
    @pytest.mark.asyncio
    async def test_analyze_computes_snapshot(self, mock_client: AsyncMock) -> None:
        analyzer = WalletAnalyzer(mock_client, first_funding_at_provider=first_funding_at_provider)
        as_of = datetime(2026, 1, 2, tzinfo=UTC)
        snapshot = await analyzer.analyze(VALID_ADDRESS, as_of=as_of)

        assert isinstance(snapshot, WalletSnapshot)
        assert snapshot.address == VALID_ADDRESS.lower()
        assert snapshot.as_of == as_of
        assert snapshot.as_of_block_number == 12_345_678
        assert snapshot.nonce_as_of == 3
        assert snapshot.matic_balance_wei_as_of == Decimal("5000000000000000000")
        assert snapshot.usdc_balance_units_as_of == Decimal("1000000")
        assert snapshot.first_funding_at == datetime(2026, 1, 1, tzinfo=UTC)
        assert snapshot.age_hours_as_of == 24.0

    @pytest.mark.asyncio
    async def test_analyze_uses_cache(self, mock_client: AsyncMock, mock_redis: AsyncMock) -> None:
        as_of = datetime(2026, 1, 2, tzinfo=UTC)
        mock_client.get_block_number_at_or_before.return_value = 99

        cached = {
            "address": VALID_ADDRESS.lower(),
            "as_of": as_of.isoformat(),
            "as_of_block_number": 99,
            "nonce_as_of": 2,
            "matic_balance_wei_as_of": "123",
            "usdc_balance_units_as_of": "456",
            "first_funding_at": datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
            "age_hours_as_of": 24.0,
            "computed_at": datetime(2026, 1, 2, tzinfo=UTC).isoformat(),
        }
        mock_redis.get.return_value = json.dumps(cached).encode()

        analyzer = WalletAnalyzer(
            mock_client,
            redis=mock_redis,
            first_funding_at_provider=first_funding_at_provider,
        )
        snapshot = await analyzer.analyze(VALID_ADDRESS, as_of=as_of)

        assert snapshot.nonce_as_of == 2
        mock_client.get_transaction_count.assert_not_called()

    @pytest.mark.asyncio
    async def test_analyze_force_refresh_bypasses_cache(
        self, mock_client: AsyncMock, mock_redis: AsyncMock
    ) -> None:
        as_of = datetime(2026, 1, 2, tzinfo=UTC)
        mock_client.get_block_number_at_or_before.return_value = 99
        mock_redis.get.return_value = b"{}"

        analyzer = WalletAnalyzer(
            mock_client,
            redis=mock_redis,
            first_funding_at_provider=first_funding_at_provider,
        )
        await analyzer.analyze(VALID_ADDRESS, as_of=as_of, force_refresh=True)
        mock_client.get_transaction_count.assert_called_once()
