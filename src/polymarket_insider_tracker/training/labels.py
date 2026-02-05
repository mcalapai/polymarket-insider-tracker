"""Self-supervised label generation (from persisted pre-move signals)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

import sqlalchemy as sa

from polymarket_insider_tracker.storage.models import TradeSignalModel
from polymarket_insider_tracker.storage.repos import TradeLabelDTO, TradeLabelRepository


@dataclass(frozen=True)
class LabelConfig:
    label_type: str = "pre_move_z"
    horizon_seconds: int = 3600
    z_threshold: float = 2.0


class LabelGenerationError(RuntimeError):
    pass


async def generate_labels_from_pre_move(
    *,
    session: object,
    config: LabelConfig | None = None,
    batch_size: int = 1000,
) -> int:
    from sqlalchemy.ext.asyncio import AsyncSession

    if not isinstance(session, AsyncSession):
        raise TypeError("session must be an AsyncSession")
    cfg = config or LabelConfig()

    repo = TradeLabelRepository(session)
    count = 0

    # Stream through pre-move signal rows.
    offset = 0
    while True:
        result = await session.execute(
            sa.select(TradeSignalModel.trade_id, TradeSignalModel.payload_json, TradeSignalModel.computed_at)
            .where(TradeSignalModel.signal_type == "pre_move")
            .order_by(TradeSignalModel.trade_id.asc())
            .offset(offset)
            .limit(batch_size)
        )
        rows = result.all()
        if not rows:
            break
        offset += len(rows)

        for trade_id, payload_json, computed_at in rows:
            try:
                payload = json.loads(payload_json)
                z_scores = payload.get("z_scores_by_horizon") or {}
                # Keys may be ints or strings depending on serialization.
                z = z_scores.get(str(cfg.horizon_seconds))
                if z is None:
                    z = z_scores.get(int(cfg.horizon_seconds))
                if z is None:
                    continue
                z_f = float(z)
            except Exception:
                continue

            label = z_f >= cfg.z_threshold
            await repo.upsert(
                TradeLabelDTO(
                    trade_id=str(trade_id),
                    label_type=cfg.label_type,
                    horizon_seconds=cfg.horizon_seconds,
                    value=Decimal(str(z_f)),
                    label=label,
                    params_json=json.dumps({"z_threshold": cfg.z_threshold}),
                    computed_at=computed_at if isinstance(computed_at, datetime) else datetime.now(UTC),
                )
            )
            count += 1

    return count

