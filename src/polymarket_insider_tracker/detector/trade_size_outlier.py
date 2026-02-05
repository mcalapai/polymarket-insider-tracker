"""Trade-size outlier detector using rolling market baseline quantiles."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from polymarket_insider_tracker.detector.models import TradeSizeOutlierSignal
from polymarket_insider_tracker.ingestor.baselines import RollingHistogramCache
from polymarket_insider_tracker.ingestor.models import TradeEvent


class TradeSizeOutlierDetectorError(Exception):
    pass


@dataclass(frozen=True)
class TradeSizeOutlierConfig:
    quantile: float = 0.995
    min_samples: int = 100


class TradeSizeOutlierDetector:
    def __init__(self, hist: RollingHistogramCache, *, config: TradeSizeOutlierConfig | None = None) -> None:
        self._hist = hist
        self._cfg = config or TradeSizeOutlierConfig()

    async def analyze(self, trade: TradeEvent) -> TradeSizeOutlierSignal | None:
        if trade.timestamp.tzinfo is None:
            raise TradeSizeOutlierDetectorError("trade.timestamp must be timezone-aware")

        counts = await self._hist.get_histogram_counts(market_id=trade.market_id, as_of=trade.timestamp)
        total = sum(counts.values())
        if total < self._cfg.min_samples:
            raise TradeSizeOutlierDetectorError("Baseline not ready (insufficient samples)")

        q = await self._hist.estimate_quantile(
            market_id=trade.market_id,
            as_of=trade.timestamp,
            q=self._cfg.quantile,
        )
        if q is None or q <= 0:
            raise TradeSizeOutlierDetectorError("Failed to compute baseline quantile")

        multiple = float(trade.notional_value / q)
        if multiple <= 1.0:
            return None

        confidence = max(0.0, min(1.0, (multiple - 1.0) / 3.0))
        return TradeSizeOutlierSignal(
            trade_event=trade,
            baseline_quantile_usdc=q,
            multiple_of_quantile=round(multiple, 3),
            confidence=round(confidence, 3),
            timestamp=datetime.now(trade.timestamp.tzinfo),
        )

