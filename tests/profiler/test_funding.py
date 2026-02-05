"""Tests for bounded funding indexing and tracing."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from polymarket_insider_tracker.profiler.funding import FundingTracer, IndexWindow
from polymarket_insider_tracker.storage.models import Base, ERC20TransferModel

USDC = "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"
WALLET = "0x1234567890abcdef1234567890abcdef12345678"
FUNDER = "0xabcdef1234567890abcdef1234567890abcdef12"


@pytest.fixture
async def async_engine():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture
async def async_session(async_engine) -> AsyncSession:
    session_factory = async_sessionmaker(bind=async_engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session


class TestFundingTracer:
    @pytest.mark.asyncio
    async def test_ensure_first_inbound_uses_existing_index(self, async_session: AsyncSession) -> None:
        ts = datetime(2026, 2, 1, tzinfo=UTC)
        async_session.add(
            ERC20TransferModel(
                token_address=USDC,
                from_address=FUNDER,
                to_address=WALLET,
                amount_units=Decimal("1000000"),
                tx_hash="0x" + "a" * 64,
                log_index=1,
                block_number=123,
                timestamp=ts,
            )
        )
        await async_session.commit()

        polygon = MagicMock()
        tracer = FundingTracer(
            polygon,
            token_addresses=[USDC],
            lookback_days=180,
            logs_chunk_size_blocks=10_000,
            max_hops=3,
        )
        first = await tracer.ensure_first_inbound_transfer(
            async_session,
            address=WALLET,
            as_of=ts + timedelta(hours=1),
        )
        assert first.from_address == FUNDER
        assert first.to_address == WALLET

    @pytest.mark.asyncio
    async def test_indexes_when_missing(self, async_session: AsyncSession) -> None:
        # Configure mocked PolygonClient responses for indexer path.
        polygon = MagicMock()
        polygon.get_block_number_at_or_before = AsyncMock(side_effect=[100, 120])
        polygon.get_logs = AsyncMock(
            return_value=[
                {
                    "blockNumber": 110,
                    "transactionHash": bytes.fromhex("aa" * 32),
                    "logIndex": 0,
                    "topics": [
                        bytes.fromhex("00" * 32),
                        bytes.fromhex("00" * 12 + FUNDER[2:]),
                        bytes.fromhex("00" * 12 + WALLET[2:]),
                    ],
                    "data": bytes.fromhex("00" * 31 + "01"),
                }
            ]
        )
        polygon.get_block = AsyncMock(return_value={"timestamp": int(datetime(2026, 2, 1, tzinfo=UTC).timestamp())})

        tracer = FundingTracer(
            polygon,
            token_addresses=[USDC],
            lookback_days=180,
            logs_chunk_size_blocks=10_000,
            max_hops=1,
        )

        as_of = datetime(2026, 2, 2, tzinfo=UTC)
        # Force indexing by using a window and empty DB.
        await tracer._indexer.index_inbound_transfers(
            async_session,
            to_address=WALLET,
            window=IndexWindow(start=as_of - timedelta(days=1), end=as_of),
        )
        await async_session.commit()

        result = await tracer.ensure_first_inbound_transfer(async_session, address=WALLET, as_of=as_of)
        assert result.to_address == WALLET
