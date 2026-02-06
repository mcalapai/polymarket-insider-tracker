"""Operational backfill for liquidity coverage metadata.

This workflow is intentionally forward-only:
- It backfills coverage rows from already-collected liquidity snapshots.
- It does not attempt to reconstruct missing historical orderbook depth.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select

from polymarket_insider_tracker.config import Settings
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.models import LiquiditySnapshotModel
from polymarket_insider_tracker.storage.repos import LiquidityCoverageRepository


@dataclass(frozen=True)
class LiquidityCoverageBackfillResult:
    pairs_considered: int
    rows_upserted: int
    unavailable_pairs: int
    window_start: datetime
    window_end: datetime


async def backfill_liquidity_coverage(
    *,
    settings: Settings,
    window_start: datetime,
    window_end: datetime,
    cadence_seconds: int | None = None,
) -> LiquidityCoverageBackfillResult:
    if window_start.tzinfo is None or window_end.tzinfo is None:
        raise ValueError("window_start/window_end must be timezone-aware")
    if window_end < window_start:
        raise ValueError("window_end must be >= window_start")

    cadence = cadence_seconds or settings.liquidity.snapshot_cadence_seconds

    db = DatabaseManager(settings.database.url, async_mode=True)
    try:
        async with db.get_async_session() as session:
            pairs_result = await session.execute(
                select(LiquiditySnapshotModel.condition_id, LiquiditySnapshotModel.asset_id)
                .where(LiquiditySnapshotModel.computed_at <= window_end)
                .group_by(LiquiditySnapshotModel.condition_id, LiquiditySnapshotModel.asset_id)
            )
            pairs = [(str(row[0]), str(row[1])) for row in pairs_result.all()]
            if not pairs:
                return LiquidityCoverageBackfillResult(
                    pairs_considered=0,
                    rows_upserted=0,
                    unavailable_pairs=0,
                    window_start=window_start,
                    window_end=window_end,
                )

            coverage_repo = LiquidityCoverageRepository(session)
            rows = await coverage_repo.compute_and_upsert_window(
                pairs=pairs,
                window_start=window_start,
                window_end=window_end,
                cadence_seconds=cadence,
            )
            unavailable = sum(1 for row in rows if row.availability_status != "measured")
            await session.commit()
            return LiquidityCoverageBackfillResult(
                pairs_considered=len(pairs),
                rows_upserted=len(rows),
                unavailable_pairs=unavailable,
                window_start=window_start,
                window_end=window_end,
            )
    finally:
        await db.dispose_async()
