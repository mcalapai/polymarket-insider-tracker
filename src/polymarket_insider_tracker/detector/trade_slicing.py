"""Trade slicing / iceberg-style behavior detector."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from polymarket_insider_tracker.detector.models import TradeSlicingSignal
from polymarket_insider_tracker.ingestor.baselines import RollingHistogramCache
from polymarket_insider_tracker.ingestor.flow import RollingWalletMarketFlowCache
from polymarket_insider_tracker.ingestor.models import TradeEvent


class TradeSlicingDetectorError(Exception):
    pass


@dataclass(frozen=True)
class TradeSlicingConfig:
    window: timedelta = timedelta(minutes=30)
    min_trades: int = 3
    median_quantile: float = 0.50
    high_total_quantile: float = 0.995
    min_baseline_samples: int = 200


class TradeSlicingDetector:
    def __init__(
        self,
        flow: RollingWalletMarketFlowCache,
        hist: RollingHistogramCache,
        *,
        config: TradeSlicingConfig | None = None,
    ) -> None:
        self._flow = flow
        self._hist = hist
        self._cfg = config or TradeSlicingConfig()

    async def analyze(self, trade: TradeEvent) -> TradeSlicingSignal | None:
        if trade.timestamp.tzinfo is None:
            raise TradeSlicingDetectorError("trade.timestamp must be timezone-aware")

        window_sum, window_count = await self._flow.get_window(
            wallet=trade.wallet_address,
            market_id=trade.market_id,
            as_of=trade.timestamp,
        )
        if window_count < self._cfg.min_trades:
            return None

        counts = await self._hist.get_histogram_counts(market_id=trade.market_id, as_of=trade.timestamp)
        if sum(counts.values()) < self._cfg.min_baseline_samples:
            raise TradeSlicingDetectorError("Baseline not ready (insufficient trade samples)")

        median = await self._hist.estimate_quantile(
            market_id=trade.market_id,
            as_of=trade.timestamp,
            q=self._cfg.median_quantile,
        )
        high_total = await self._hist.estimate_quantile(
            market_id=trade.market_id,
            as_of=trade.timestamp,
            q=self._cfg.high_total_quantile,
        )
        if median is None or high_total is None or median <= 0 or high_total <= 0:
            raise TradeSlicingDetectorError("Failed to compute baseline quantiles")

        if window_sum <= high_total:
            return None

        avg = window_sum / Decimal(window_count)
        if avg >= median:
            return None

        total_multiple = float(window_sum / high_total)
        avg_ratio = float(median / avg) if avg > 0 else 10.0

        confidence = min(1.0, max(0.0, (total_multiple - 1.0) / 3.0)) * 0.6 + min(
            1.0, max(0.0, (avg_ratio - 1.0) / 3.0)
        ) * 0.4

        return TradeSlicingSignal(
            trade_event=trade,
            window_minutes=int(self._cfg.window.total_seconds() // 60),
            window_trade_count=window_count,
            window_notional_usdc=window_sum,
            window_avg_notional_usdc=avg,
            market_median_trade_usdc=median,
            confidence=round(float(confidence), 3),
            timestamp=datetime.now(UTC),
        )

