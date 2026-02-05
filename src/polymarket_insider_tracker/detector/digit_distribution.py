"""Digit distribution detector (Benford-style, weak signal)."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime

from polymarket_insider_tracker.detector.models import DigitDistributionSignal
from polymarket_insider_tracker.ingestor.baselines import RollingDigitDistributionCache
from polymarket_insider_tracker.ingestor.models import TradeEvent


class DigitDistributionDetectorError(Exception):
    pass


def _benford_probs() -> dict[int, float]:
    return {d: math.log10(1 + 1 / d) for d in range(1, 10)}


@dataclass(frozen=True)
class DigitDistributionConfig:
    min_samples: int = 200
    emit_min_confidence: float = 0.6


class DigitDistributionDetector:
    def __init__(self, digits: RollingDigitDistributionCache, *, config: DigitDistributionConfig | None = None) -> None:
        self._digits = digits
        self._cfg = config or DigitDistributionConfig()
        self._expected = _benford_probs()

    async def analyze(self, trade: TradeEvent) -> DigitDistributionSignal | None:
        if trade.timestamp.tzinfo is None:
            raise DigitDistributionDetectorError("trade.timestamp must be timezone-aware")

        dist = await self._digits.get_distribution(market_id=trade.market_id, as_of=trade.timestamp)
        n = sum(dist.values())
        if n < self._cfg.min_samples:
            raise DigitDistributionDetectorError("Baseline not ready (insufficient digit samples)")

        obs = {d: dist[d] / n for d in range(1, 10)}
        l1 = sum(abs(obs[d] - self._expected[d]) for d in range(1, 10)) / 2.0
        # Scale by sample size to avoid brittle fixed thresholds.
        score = l1 * math.sqrt(n)
        confidence = max(0.0, min(1.0, (score - 3.0) / 6.0))
        if confidence < self._cfg.emit_min_confidence:
            return None
        return DigitDistributionSignal(
            trade_event=trade,
            l1_distance=round(l1, 4),
            sample_size=n,
            confidence=round(confidence, 3),
            timestamp=datetime.now(UTC),
        )
