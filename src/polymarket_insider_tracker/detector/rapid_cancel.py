"""Rapid cancel detector (baseline-derived)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from polymarket_insider_tracker.detector.models import RapidCancelSignal
from polymarket_insider_tracker.ingestor.baselines import RollingHistogramCache


class RapidCancelDetectorError(Exception):
    pass


@dataclass(frozen=True)
class RapidCancelConfig:
    rapid_percentile: float = 0.01
    baseline_min_samples: int = 50


class RapidCancelDetector:
    def __init__(self, hist_cache: RollingHistogramCache, *, config: RapidCancelConfig) -> None:
        self._hist = hist_cache
        self._cfg = config

    async def analyze_cancel(
        self,
        *,
        wallet_address: str,
        market_id: str,
        order_hash: str,
        canceled_at: datetime,
        cancel_latency_seconds: float,
        fill_ratio: float,
    ) -> RapidCancelSignal | None:
        if cancel_latency_seconds < 0:
            raise RapidCancelDetectorError("cancel_latency_seconds must be non-negative")
        series = f"{market_id}:cancel_latency_seconds"
        value = Decimal(str(max(cancel_latency_seconds, 1e-9)))

        p = await self._hist.estimate_percentile(market_id=series, as_of=canceled_at, value=value)
        await self._hist.record(market_id=series, ts=canceled_at, notional_usdc=value)

        total = await self._hist.total_samples(market_id=series, as_of=canceled_at)
        if total < self._cfg.baseline_min_samples or p is None:
            return None

        if p > self._cfg.rapid_percentile:
            return None

        fill_ratio = max(0.0, min(1.0, fill_ratio))
        fill_factor = 1.0 - fill_ratio
        denom = max(1e-9, self._cfg.rapid_percentile)
        confidence = min(1.0, max(0.0, (self._cfg.rapid_percentile - p) / denom)) * fill_factor

        return RapidCancelSignal(
            wallet_address=wallet_address.lower(),
            market_id=market_id,
            order_hash=order_hash,
            cancel_latency_seconds=cancel_latency_seconds,
            latency_percentile=p,
            fill_ratio=fill_ratio,
            confidence=confidence,
            timestamp=canceled_at,
        )
