"""Book-impact-without-fill detector.

Flags orders that:
- improve the best bid/ask (move the top of book),
- are canceled quickly, and
- have near-zero fill.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from polymarket_insider_tracker.detector.models import BookImpactWithoutFillSignal
from polymarket_insider_tracker.ingestor.baselines import RollingHistogramCache


class BookImpactWithoutFillDetectorError(Exception):
    pass


@dataclass(frozen=True)
class BookImpactWithoutFillConfig:
    rapid_percentile: float = 0.01
    impact_percentile: float = 0.99
    baseline_min_samples: int = 50
    max_fill_ratio: float = 0.01


class BookImpactWithoutFillDetector:
    def __init__(self, hist_cache: RollingHistogramCache, *, config: BookImpactWithoutFillConfig) -> None:
        self._hist = hist_cache
        self._cfg = config

    async def analyze_cancel(
        self,
        *,
        wallet_address: str,
        market_id: str,
        order_hash: str,
        canceled_at: datetime,
        moved_best: bool,
        impact_bps: float | None,
        cancel_latency_seconds: float,
        fill_ratio: float,
    ) -> BookImpactWithoutFillSignal | None:
        if not moved_best:
            return None
        if impact_bps is None or impact_bps <= 0:
            return None
        fill_ratio = max(0.0, min(1.0, fill_ratio))
        if fill_ratio > self._cfg.max_fill_ratio:
            return None
        if cancel_latency_seconds < 0:
            raise BookImpactWithoutFillDetectorError("cancel_latency_seconds must be non-negative")

        latency_series = f"{market_id}:cancel_latency_seconds"
        impact_series = f"{market_id}:book_impact_bps"

        latency_val = Decimal(str(max(cancel_latency_seconds, 1e-9)))
        impact_val = Decimal(str(max(impact_bps, 1e-9)))

        latency_p = await self._hist.estimate_percentile(
            market_id=latency_series, as_of=canceled_at, value=latency_val
        )
        impact_p = await self._hist.estimate_percentile(
            market_id=impact_series, as_of=canceled_at, value=impact_val
        )

        await self._hist.record(market_id=latency_series, ts=canceled_at, notional_usdc=latency_val)
        await self._hist.record(market_id=impact_series, ts=canceled_at, notional_usdc=impact_val)

        latency_total = await self._hist.total_samples(market_id=latency_series, as_of=canceled_at)
        impact_total = await self._hist.total_samples(market_id=impact_series, as_of=canceled_at)
        if (
            latency_total < self._cfg.baseline_min_samples
            or impact_total < self._cfg.baseline_min_samples
            or latency_p is None
            or impact_p is None
        ):
            return None

        if latency_p > self._cfg.rapid_percentile:
            return None
        if impact_p < self._cfg.impact_percentile:
            return None

        latency_score = max(0.0, min(1.0, (self._cfg.rapid_percentile - latency_p) / max(1e-9, self._cfg.rapid_percentile)))
        impact_score = max(
            0.0,
            min(1.0, (impact_p - self._cfg.impact_percentile) / max(1e-9, 1.0 - self._cfg.impact_percentile)),
        )
        confidence = latency_score * impact_score * (1.0 - fill_ratio)

        return BookImpactWithoutFillSignal(
            wallet_address=wallet_address.lower(),
            market_id=market_id,
            order_hash=order_hash,
            moved_best=True,
            impact_bps=impact_bps,
            impact_percentile=impact_p,
            cancel_latency_seconds=cancel_latency_seconds,
            latency_percentile=latency_p,
            fill_ratio=fill_ratio,
            confidence=confidence,
            timestamp=canceled_at,
        )

