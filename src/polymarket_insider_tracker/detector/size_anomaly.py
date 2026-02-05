"""Liquidity-aware position sizing anomaly detection.

Strict by design: required liquidity inputs must be provided by the caller.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal

from polymarket_insider_tracker.detector.models import SizeAnomalySignal
from polymarket_insider_tracker.ingestor.models import TradeEvent

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_VOLUME_THRESHOLD = 0.02  # 2% of rolling 24h volume
DEFAULT_BOOK_THRESHOLD = 0.05  # 5% of visible book depth


class SizeAnomalyError(Exception):
    """Raised when size anomaly inputs are invalid."""


@dataclass(frozen=True)
class LiquidityInputs:
    rolling_24h_volume_usdc: Decimal
    visible_book_depth_usdc: Decimal


class SizeAnomalyDetector:
    """Detector for unusually large trade sizes relative to measured liquidity."""

    def __init__(
        self,
        *,
        volume_threshold: float = DEFAULT_VOLUME_THRESHOLD,
        book_threshold: float = DEFAULT_BOOK_THRESHOLD,
    ) -> None:
        self._volume_threshold = volume_threshold
        self._book_threshold = book_threshold

    async def analyze(
        self,
        trade: TradeEvent,
        *,
        liquidity: LiquidityInputs,
    ) -> SizeAnomalySignal | None:
        trade_size = trade.notional_value
        if liquidity.rolling_24h_volume_usdc <= 0:
            raise SizeAnomalyError("rolling_24h_volume_usdc must be > 0")
        if liquidity.visible_book_depth_usdc <= 0:
            raise SizeAnomalyError("visible_book_depth_usdc must be > 0")

        volume_impact = float(trade_size / liquidity.rolling_24h_volume_usdc)
        book_impact = float(trade_size / liquidity.visible_book_depth_usdc)

        exceeds_volume = volume_impact > self._volume_threshold
        exceeds_book = book_impact > self._book_threshold
        if not exceeds_volume and not exceeds_book:
            return None

        confidence, factors = self.calculate_confidence(
            volume_impact=volume_impact,
            book_impact=book_impact,
        )
        if confidence <= 0:
            return None

        return SizeAnomalySignal(
            trade_event=trade,
            rolling_24h_volume_usdc=liquidity.rolling_24h_volume_usdc,
            visible_book_depth_usdc=liquidity.visible_book_depth_usdc,
            volume_impact=volume_impact,
            book_impact=book_impact,
            confidence=confidence,
            factors=factors,
        )

    def calculate_confidence(
        self,
        *,
        volume_impact: float,
        book_impact: float,
    ) -> tuple[float, dict[str, float]]:
        factors: dict[str, float] = {}
        confidence = 0.0

        if volume_impact > self._volume_threshold:
            ratio = min(volume_impact / self._volume_threshold, 3.0)
            volume_score = ratio / 3.0 * 0.6
            factors["volume_impact"] = volume_score
            confidence += volume_score

        if book_impact > self._book_threshold:
            ratio = min(book_impact / self._book_threshold, 3.0)
            book_score = ratio / 3.0 * 0.4
            factors["book_impact"] = book_score
            confidence += book_score

        confidence = max(0.0, min(1.0, confidence))
        return confidence, factors

