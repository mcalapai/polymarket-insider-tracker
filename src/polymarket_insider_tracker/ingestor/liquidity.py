"""Liquidity snapshot computation and caching helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from polymarket_insider_tracker.ingestor.models import Orderbook


class LiquidityError(Exception):
    """Raised when a liquidity snapshot cannot be computed."""


@dataclass(frozen=True)
class LiquiditySnapshot:
    """Liquidity snapshot for a specific market token (asset_id)."""

    condition_id: str
    asset_id: str
    rolling_24h_volume_usdc: Decimal | None
    visible_book_depth_usdc: Decimal
    mid_price: Decimal
    computed_at: datetime

    def to_json(self) -> str:
        return json.dumps(
            {
                "condition_id": self.condition_id,
                "asset_id": self.asset_id,
                "rolling_24h_volume_usdc": str(self.rolling_24h_volume_usdc)
                if self.rolling_24h_volume_usdc is not None
                else None,
                "visible_book_depth_usdc": str(self.visible_book_depth_usdc),
                "mid_price": str(self.mid_price),
                "computed_at": self.computed_at.isoformat(),
            }
        )

    @classmethod
    def from_json(cls, raw: str) -> "LiquiditySnapshot":
        data = json.loads(raw)
        return cls(
            condition_id=str(data["condition_id"]),
            asset_id=str(data["asset_id"]),
            rolling_24h_volume_usdc=Decimal(str(v))
            if (v := data.get("rolling_24h_volume_usdc")) is not None
            else None,
            visible_book_depth_usdc=Decimal(str(data["visible_book_depth_usdc"])),
            mid_price=Decimal(str(data["mid_price"])),
            computed_at=datetime.fromisoformat(str(data["computed_at"])),
        )


def compute_visible_book_depth_usdc(
    orderbook: Orderbook,
    *,
    max_slippage_bps: int,
) -> tuple[Decimal, Decimal]:
    """Compute visible depth and mid price from an orderbook.

    Visible depth is the sum of notional (price * size) of levels that are within
    `max_slippage_bps` of the mid price on both sides.
    """
    mid = orderbook.midpoint
    if mid is None:
        raise LiquidityError("Orderbook missing midpoint (best bid/ask required)")
    if max_slippage_bps <= 0:
        raise ValueError("max_slippage_bps must be > 0")

    band_low = mid * (Decimal(1) - Decimal(max_slippage_bps) / Decimal(10_000))
    band_high = mid * (Decimal(1) + Decimal(max_slippage_bps) / Decimal(10_000))

    depth = Decimal(0)
    for level in orderbook.bids:
        if level.price < band_low:
            break
        depth += level.price * level.size
    for level in orderbook.asks:
        if level.price > band_high:
            break
        depth += level.price * level.size

    return depth, mid


def now_utc() -> datetime:
    return datetime.now(UTC)

