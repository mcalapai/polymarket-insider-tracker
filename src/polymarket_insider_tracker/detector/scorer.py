"""Composite risk scorer combining all detector signals.

This module provides the RiskScorer class that aggregates signals from
multiple detectors into a unified risk assessment with weighted scoring.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from redis.asyncio import Redis

from polymarket_insider_tracker.detector.models import (
    BookImpactWithoutFillSignal,
    CoEntryCorrelationSignal,
    FreshWalletSignal,
    FundingChainSignal,
    ModelScoreSignal,
    OrderToTradeRatioSignal,
    PreMoveSignal,
    RapidCancelSignal,
    RiskAssessment,
    SniperClusterSignal,
    TradeSizeOutlierSignal,
    DigitDistributionSignal,
    TradeSlicingSignal,
    SizeAnomalySignal,
)
from polymarket_insider_tracker.ingestor.models import TradeEvent

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_ALERT_THRESHOLD = 0.6
DEFAULT_DEDUP_WINDOW_SECONDS = 3600  # 1 hour
DEFAULT_REDIS_KEY_PREFIX = "polymarket:dedup:"

# Default weights for each signal type
DEFAULT_WEIGHTS = {
    "fresh_wallet": 0.30,
    "size_anomaly": 0.25,
    "sniper_cluster": 0.20,
    "coentry": 0.05,
    "funding": 0.20,
    "pre_move": 0.0,
    "trade_size_outlier": 0.0,
    "digit_distribution": 0.0,
    "trade_slicing": 0.0,
    "order_to_trade_ratio": 0.05,
    "rapid_cancel": 0.05,
    "book_impact_without_fill": 0.10,
    "model_score": 0.10,
}

# Multi-signal bonuses
MULTI_SIGNAL_BONUS_2 = 1.2  # 20% bonus for 2 signals
MULTI_SIGNAL_BONUS_3 = 1.3  # 30% bonus for 3+ signals


@dataclass
class SignalBundle:
    """Bundle of signals for a single trade.

    Collects all available signals for a trade event to pass to the scorer.
    """

    trade_event: TradeEvent
    fresh_wallet_signal: FreshWalletSignal | None = None
    size_anomaly_signal: SizeAnomalySignal | None = None
    sniper_cluster_signal: SniperClusterSignal | None = None
    coentry_signal: CoEntryCorrelationSignal | None = None
    funding_signal: FundingChainSignal | None = None
    pre_move_signal: PreMoveSignal | None = None
    trade_size_outlier_signal: TradeSizeOutlierSignal | None = None
    digit_distribution_signal: DigitDistributionSignal | None = None
    trade_slicing_signal: TradeSlicingSignal | None = None
    order_to_trade_ratio_signal: OrderToTradeRatioSignal | None = None
    rapid_cancel_signal: RapidCancelSignal | None = None
    book_impact_without_fill_signal: BookImpactWithoutFillSignal | None = None
    model_score_signal: ModelScoreSignal | None = None

    @property
    def wallet_address(self) -> str:
        """Return the wallet address from the trade event."""
        return self.trade_event.wallet_address

    @property
    def market_id(self) -> str:
        """Return the market ID from the trade event."""
        return self.trade_event.market_id


class RiskScorer:
    """Composite risk scorer combining signals into unified assessments.

    This scorer:
    - Aggregates signals from multiple detectors for the same trade
    - Applies configurable weights based on signal type
    - Calculates multi-signal bonuses for correlated signals
    - Enforces deduplication to prevent alert spam
    - Produces RiskAssessment objects for downstream alerting

    Scoring Formula:
        weighted_score = sum(signal.confidence * weight[type] for signal in signals)

        # Multi-signal bonus
        if signals >= 2: weighted_score *= 1.2
        if signals >= 3: weighted_score *= 1.3

        # Cap at 1.0
        final_score = min(weighted_score, 1.0)

        should_alert = final_score >= alert_threshold AND not deduplicated

    Example:
        ```python
        redis = Redis.from_url("redis://localhost:6379")
        scorer = RiskScorer(redis)

        bundle = SignalBundle(
            trade_event=trade,
            fresh_wallet_signal=fresh_signal,
            size_anomaly_signal=size_signal,
        )

        assessment = await scorer.assess(bundle)
        if assessment.should_alert:
            await send_alert(assessment)
        ```
    """

    def __init__(
        self,
        redis: Redis,
        *,
        weights: dict[str, float] | None = None,
        alert_threshold: float = DEFAULT_ALERT_THRESHOLD,
        dedup_window_seconds: int = DEFAULT_DEDUP_WINDOW_SECONDS,
        key_prefix: str = DEFAULT_REDIS_KEY_PREFIX,
    ) -> None:
        """Initialize the risk scorer.

        Args:
            redis: Redis async client for deduplication.
            weights: Custom weights for signal types. Defaults to DEFAULT_WEIGHTS.
            alert_threshold: Minimum score to trigger alert (default 0.6).
            dedup_window_seconds: Window for deduplication (default 3600 = 1 hour).
            key_prefix: Redis key prefix for dedup keys.
        """
        self._redis = redis
        self._weights = weights or DEFAULT_WEIGHTS.copy()
        self._alert_threshold = alert_threshold
        self._dedup_window = dedup_window_seconds
        self._key_prefix = key_prefix

    async def assess(self, bundle: SignalBundle) -> RiskAssessment:
        """Assess a trade's risk based on all available signals.

        This method:
        1. Counts triggered signals
        2. Calculates weighted score with bonuses
        3. Checks deduplication
        4. Creates RiskAssessment

        Args:
            bundle: SignalBundle containing trade and all signals.

        Returns:
            RiskAssessment with final scoring and alert decision.
        """
        # Calculate weighted score
        weighted_score, signals_triggered = self.calculate_weighted_score(bundle)

        # Determine if should alert (before dedup check)
        meets_threshold = weighted_score >= self._alert_threshold

        # Check deduplication
        is_duplicate = False
        if meets_threshold:
            is_duplicate = await self._check_and_set_dedup(
                bundle.wallet_address,
                bundle.market_id,
            )

        should_alert = meets_threshold and not is_duplicate

        # Log assessment
        if should_alert:
            logger.info(
                "Risk assessment triggered alert: wallet=%s, market=%s, score=%.2f, signals=%d",
                bundle.wallet_address[:10] + "...",
                bundle.market_id[:10] + "...",
                weighted_score,
                signals_triggered,
            )
        elif is_duplicate:
            logger.debug(
                "Risk assessment deduplicated: wallet=%s, market=%s",
                bundle.wallet_address[:10] + "...",
                bundle.market_id[:10] + "...",
            )

        return RiskAssessment(
            trade_event=bundle.trade_event,
            wallet_address=bundle.wallet_address,
            market_id=bundle.market_id,
            fresh_wallet_signal=bundle.fresh_wallet_signal,
            size_anomaly_signal=bundle.size_anomaly_signal,
            sniper_cluster_signal=bundle.sniper_cluster_signal,
            coentry_signal=bundle.coentry_signal,
            funding_signal=bundle.funding_signal,
            pre_move_signal=bundle.pre_move_signal,
            trade_size_outlier_signal=bundle.trade_size_outlier_signal,
            digit_distribution_signal=bundle.digit_distribution_signal,
            trade_slicing_signal=bundle.trade_slicing_signal,
            order_to_trade_ratio_signal=bundle.order_to_trade_ratio_signal,
            rapid_cancel_signal=bundle.rapid_cancel_signal,
            book_impact_without_fill_signal=bundle.book_impact_without_fill_signal,
            model_score_signal=bundle.model_score_signal,
            signals_triggered=signals_triggered,
            weighted_score=weighted_score,
            should_alert=should_alert,
        )

    def calculate_weighted_score(self, bundle: SignalBundle) -> tuple[float, int]:
        """Calculate weighted score from all signals.

        Applies per-signal weights and multi-signal bonuses.

        Args:
            bundle: SignalBundle with all available signals.

        Returns:
            Tuple of (weighted_score, signals_triggered_count).
        """
        score = 0.0
        signals_triggered = 0

        # Fresh wallet signal
        if bundle.fresh_wallet_signal is not None:
            weight = self._weights.get("fresh_wallet", 0.0)
            score += bundle.fresh_wallet_signal.confidence * weight
            signals_triggered += 1

        # Size anomaly signal
        if bundle.size_anomaly_signal is not None:
            weight = self._weights.get("size_anomaly", 0.0)
            score += bundle.size_anomaly_signal.confidence * weight
            signals_triggered += 1

        # Sniper clustering
        if bundle.sniper_cluster_signal is not None:
            weight = self._weights.get("sniper_cluster", 0.0)
            score += bundle.sniper_cluster_signal.confidence * weight
            signals_triggered += 1

        # Co-entry correlation (low-weight coordination signal)
        if bundle.coentry_signal is not None:
            weight = self._weights.get("coentry", 0.0)
            score += bundle.coentry_signal.confidence * weight
            signals_triggered += 1

        # Funding origin suspiciousness (on-demand)
        if bundle.funding_signal is not None:
            weight = self._weights.get("funding", 0.0)
            score += bundle.funding_signal.confidence * weight
            signals_triggered += 1

        # Pre-move (historical only by default)
        if bundle.pre_move_signal is not None:
            weight = self._weights.get("pre_move", 0.0)
            score += bundle.pre_move_signal.confidence * weight
            signals_triggered += 1

        if bundle.trade_size_outlier_signal is not None:
            weight = self._weights.get("trade_size_outlier", 0.0)
            score += bundle.trade_size_outlier_signal.confidence * weight
            signals_triggered += 1

        if bundle.digit_distribution_signal is not None:
            weight = self._weights.get("digit_distribution", 0.0)
            score += bundle.digit_distribution_signal.confidence * weight
            signals_triggered += 1

        if bundle.trade_slicing_signal is not None:
            weight = self._weights.get("trade_slicing", 0.0)
            score += bundle.trade_slicing_signal.confidence * weight
            signals_triggered += 1

        if bundle.order_to_trade_ratio_signal is not None:
            weight = self._weights.get("order_to_trade_ratio", 0.0)
            score += bundle.order_to_trade_ratio_signal.confidence * weight
            signals_triggered += 1

        if bundle.rapid_cancel_signal is not None:
            weight = self._weights.get("rapid_cancel", 0.0)
            score += bundle.rapid_cancel_signal.confidence * weight
            signals_triggered += 1

        if bundle.book_impact_without_fill_signal is not None:
            weight = self._weights.get("book_impact_without_fill", 0.0)
            score += bundle.book_impact_without_fill_signal.confidence * weight
            signals_triggered += 1

        if bundle.model_score_signal is not None:
            weight = self._weights.get("model_score", 0.0)
            score += bundle.model_score_signal.confidence * weight
            signals_triggered += 1

        # Apply multi-signal bonus
        if signals_triggered >= 3:
            score *= MULTI_SIGNAL_BONUS_3
        elif signals_triggered >= 2:
            score *= MULTI_SIGNAL_BONUS_2

        # Cap at 1.0
        score = min(score, 1.0)

        return score, signals_triggered

    async def _check_and_set_dedup(
        self,
        wallet_address: str,
        market_id: str,
    ) -> bool:
        """Check if this wallet/market combo was recently alerted.

        If not a duplicate, sets the dedup key with TTL.

        Args:
            wallet_address: The trader's wallet address.
            market_id: The market condition ID.

        Returns:
            True if this is a duplicate (already alerted), False otherwise.
        """
        key = f"{self._key_prefix}{wallet_address}:{market_id}"

        # Try to set with NX (only if not exists)
        was_set = await self._redis.set(
            key,
            datetime.now(UTC).isoformat(),
            nx=True,
            ex=self._dedup_window,
        )

        # If was_set is None/False, key already existed = duplicate
        return not was_set

    async def clear_dedup(
        self,
        wallet_address: str,
        market_id: str,
    ) -> bool:
        """Clear dedup key for a wallet/market combo.

        Useful for testing or manual override.

        Args:
            wallet_address: The trader's wallet address.
            market_id: The market condition ID.

        Returns:
            True if key was deleted, False if it didn't exist.
        """
        key = f"{self._key_prefix}{wallet_address}:{market_id}"
        deleted = await self._redis.delete(key)
        return int(deleted) > 0

    async def assess_batch(self, bundles: list[SignalBundle]) -> list[RiskAssessment]:
        """Assess multiple trade bundles.

        Args:
            bundles: List of SignalBundles to assess.

        Returns:
            List of RiskAssessments.
        """
        import asyncio

        tasks = [self.assess(bundle) for bundle in bundles]
        return await asyncio.gather(*tasks)

    def get_weights(self) -> dict[str, float]:
        """Get current signal weights.

        Returns:
            Copy of the weights dictionary.
        """
        return self._weights.copy()

    def set_weights(self, weights: dict[str, float]) -> None:
        """Update signal weights.

        Useful for A/B testing different weight configurations.

        Args:
            weights: New weights dictionary.
        """
        self._weights = weights.copy()
        logger.info("Updated risk scorer weights: %s", self._weights)
