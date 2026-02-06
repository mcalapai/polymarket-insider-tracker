"""Tests for liquidity snapshots and coverage repositories."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from polymarket_insider_tracker.storage.models import Base, LiquiditySnapshotModel
from polymarket_insider_tracker.storage.repos import (
    LiquidityCoverageRepository,
    LiquiditySnapshotDTO,
    LiquiditySnapshotRepository,
)

pytest.importorskip("aiosqlite", exc_type=ImportError)


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


@pytest.mark.asyncio
async def test_liquidity_snapshot_upsert_is_idempotent(async_session: AsyncSession) -> None:
    repo = LiquiditySnapshotRepository(async_session)
    bucket = datetime(2026, 2, 6, 12, 0, tzinfo=UTC)

    await repo.upsert(
        LiquiditySnapshotDTO(
            condition_id="m1",
            asset_id="a1",
            computed_at=bucket,
            rolling_24h_volume_usdc=Decimal("1000"),
            visible_book_depth_usdc=Decimal("250"),
            mid_price=Decimal("0.5"),
        )
    )
    await repo.upsert(
        LiquiditySnapshotDTO(
            condition_id="m1",
            asset_id="a1",
            computed_at=bucket,
            rolling_24h_volume_usdc=Decimal("1200"),
            visible_book_depth_usdc=Decimal("300"),
            mid_price=Decimal("0.51"),
        )
    )

    result = await async_session.execute(select(func.count()).select_from(LiquiditySnapshotModel))
    assert int(result.scalar_one()) == 1

    latest = await repo.get_latest_before(condition_id="m1", asset_id="a1", as_of=bucket)
    assert latest is not None
    assert latest.visible_book_depth_usdc == Decimal("300")
    assert latest.rolling_24h_volume_usdc == Decimal("1200")


@pytest.mark.asyncio
async def test_liquidity_coverage_computation(async_session: AsyncSession) -> None:
    snapshot_repo = LiquiditySnapshotRepository(async_session)
    coverage_repo = LiquidityCoverageRepository(async_session)

    start = datetime(2026, 2, 6, 12, 0, tzinfo=UTC)
    cadence = 300
    for minutes in (0, 5, 10):
        ts = start + timedelta(minutes=minutes)
        await snapshot_repo.upsert(
            LiquiditySnapshotDTO(
                condition_id="m1",
                asset_id="a1",
                computed_at=ts,
                rolling_24h_volume_usdc=Decimal("1000"),
                visible_book_depth_usdc=Decimal("100"),
                mid_price=Decimal("0.5"),
            )
        )

    rows = await coverage_repo.compute_window_for_pairs(
        pairs=[("m1", "a1")],
        window_start=start,
        window_end=start + timedelta(minutes=15),
        cadence_seconds=cadence,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.expected_snapshots == 4
    assert row.observed_snapshots == 3
    assert float(row.coverage_ratio) == pytest.approx(0.75)
    assert row.availability_status == "measured"


@pytest.mark.asyncio
async def test_liquidity_coverage_marks_pre_collection_as_unavailable(
    async_session: AsyncSession,
) -> None:
    snapshot_repo = LiquiditySnapshotRepository(async_session)
    coverage_repo = LiquidityCoverageRepository(async_session)

    start = datetime(2026, 2, 6, 12, 0, tzinfo=UTC)
    await snapshot_repo.upsert(
        LiquiditySnapshotDTO(
            condition_id="m1",
            asset_id="a1",
            computed_at=start + timedelta(minutes=10),
            rolling_24h_volume_usdc=Decimal("1000"),
            visible_book_depth_usdc=Decimal("100"),
            mid_price=Decimal("0.5"),
        )
    )

    rows = await coverage_repo.compute_window_for_pairs(
        pairs=[("m1", "a1")],
        window_start=start,
        window_end=start + timedelta(minutes=15),
        cadence_seconds=300,
    )

    assert len(rows) == 1
    assert rows[0].availability_status == "unavailable"
