"""Order-to-trade ratio detector (requires order lifecycle ingestion)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from polymarket_insider_tracker.detector.models import OrderToTradeRatioSignal
from polymarket_insider_tracker.ingestor.baselines import RollingHistogramCache


class OrderToTradeRatioDetectorError(Exception):
    pass


@dataclass(frozen=True)
class OrderToTradeRatioConfig:
    window_minutes: int
    alert_percentile: float = 0.99
    baseline_min_samples: int = 50


class OrderToTradeRatioDetector:
    def __init__(self, hist_cache: RollingHistogramCache, *, config: OrderToTradeRatioConfig) -> None:
        self._hist = hist_cache
        self._cfg = config

    async def analyze(
        self,
        *,
        wallet_address: str,
        market_id: str,
        as_of: datetime,
        orders_placed: int,
        trades_executed: int,
    ) -> OrderToTradeRatioSignal | None:
        if orders_placed < 0 or trades_executed < 0:
            raise OrderToTradeRatioDetectorError("counts must be non-negative")
        ratio = float(orders_placed) / float(max(1, trades_executed))
        value = Decimal(str(max(ratio, 1e-9)))
        series = f"{market_id}:order_to_trade_ratio"

        p = await self._hist.estimate_percentile(market_id=series, as_of=as_of, value=value)
        await self._hist.record(market_id=series, ts=as_of, notional_usdc=value)

        total = await self._hist.total_samples(market_id=series, as_of=as_of)
        if total < self._cfg.baseline_min_samples or p is None:
            return None

        if p < self._cfg.alert_percentile:
            return None

        denom = max(1e-9, 1.0 - self._cfg.alert_percentile)
        confidence = min(1.0, max(0.0, (p - self._cfg.alert_percentile) / denom))
        return OrderToTradeRatioSignal(
            wallet_address=wallet_address.lower(),
            market_id=market_id,
            window_minutes=self._cfg.window_minutes,
            orders_placed=orders_placed,
            trades_executed=trades_executed,
            ratio=ratio,
            ratio_percentile=p,
            confidence=confidence,
        )

