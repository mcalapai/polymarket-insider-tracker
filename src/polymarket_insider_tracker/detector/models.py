"""Data models for the detector module."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.profiler.models import WalletSnapshot


@dataclass(frozen=True)
class FreshWalletSignal:
    """Signal emitted when a fresh wallet makes a suspicious trade.

    This signal combines trade event data with wallet profile analysis
    to produce a confidence score indicating the likelihood of suspicious
    activity.

    Attributes:
        trade_event: The original trade event that triggered this signal.
        wallet_snapshot: Time-aware snapshot of the trader's wallet.
        confidence: Overall confidence score (0.0 to 1.0).
        factors: Individual factor scores contributing to confidence.
        timestamp: When this signal was generated.
    """

    trade_event: TradeEvent
    wallet_snapshot: WalletSnapshot
    confidence: float
    factors: dict[str, float]
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def wallet_address(self) -> str:
        """Return the wallet address from the trade event."""
        return self.trade_event.wallet_address

    @property
    def market_id(self) -> str:
        """Return the market ID from the trade event."""
        return self.trade_event.market_id

    @property
    def trade_size_usdc(self) -> Decimal:
        """Return the trade size in USDC (notional value)."""
        return self.trade_event.notional_value

    @property
    def is_high_confidence(self) -> bool:
        """Return True if confidence exceeds 0.7."""
        return self.confidence >= 0.7

    @property
    def is_very_high_confidence(self) -> bool:
        """Return True if confidence exceeds 0.85."""
        return self.confidence >= 0.85

    def to_dict(self) -> dict[str, object]:
        """Serialize to dictionary for Redis stream publishing."""
        return {
            "wallet_address": self.wallet_address,
            "market_id": self.market_id,
            "trade_id": self.trade_event.trade_id,
            "trade_size": str(self.trade_size_usdc),
            "trade_side": self.trade_event.side,
            "trade_price": str(self.trade_event.price),
            "wallet_nonce_as_of": self.wallet_snapshot.nonce_as_of,
            "wallet_age_hours_as_of": self.wallet_snapshot.age_hours_as_of,
            "wallet_as_of_block": self.wallet_snapshot.as_of_block_number,
            "confidence": self.confidence,
            "factors": self.factors,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass(frozen=True)
class SizeAnomalySignal:
    """Signal emitted when a trade has unusually large position size.

    This signal is generated when a trade's size significantly impacts
    the market volume or order book depth, indicating potential informed
    trading activity.

    Attributes:
        trade_event: The original trade event that triggered this signal.
        rolling_24h_volume_usdc: Rolling 24h notional volume (USDC).
        visible_book_depth_usdc: Visible orderbook depth (USDC).
        volume_impact: Trade size as fraction of 24h volume (0.0 if unknown).
        book_impact: Trade size as fraction of order book depth (0.0 if unknown).
        confidence: Overall confidence score (0.0 to 1.0).
        factors: Individual factor scores contributing to confidence.
        timestamp: When this signal was generated.
    """

    trade_event: TradeEvent
    rolling_24h_volume_usdc: Decimal
    visible_book_depth_usdc: Decimal
    volume_impact: float
    book_impact: float
    confidence: float
    factors: dict[str, float]
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def wallet_address(self) -> str:
        """Return the wallet address from the trade event."""
        return self.trade_event.wallet_address

    @property
    def market_id(self) -> str:
        """Return the market ID from the trade event."""
        return self.trade_event.market_id

    @property
    def trade_size_usdc(self) -> Decimal:
        """Return the trade size in USDC (notional value)."""
        return self.trade_event.notional_value

    @property
    def is_high_confidence(self) -> bool:
        """Return True if confidence exceeds 0.7."""
        return self.confidence >= 0.7

    @property
    def is_very_high_confidence(self) -> bool:
        """Return True if confidence exceeds 0.85."""
        return self.confidence >= 0.85

    def to_dict(self) -> dict[str, object]:
        """Serialize to dictionary for Redis stream publishing."""
        return {
            "wallet_address": self.wallet_address,
            "market_id": self.market_id,
            "trade_id": self.trade_event.trade_id,
            "trade_size": str(self.trade_size_usdc),
            "trade_side": self.trade_event.side,
            "trade_price": str(self.trade_event.price),
            "rolling_24h_volume_usdc": str(self.rolling_24h_volume_usdc),
            "visible_book_depth_usdc": str(self.visible_book_depth_usdc),
            "volume_impact": self.volume_impact,
            "book_impact": self.book_impact,
            "confidence": self.confidence,
            "factors": self.factors,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass(frozen=True)
class SniperClusterSignal:
    """Signal emitted when a wallet is identified as part of a sniper cluster.

    Sniper clusters are groups of wallets that consistently enter markets
    within minutes of their creation, suggesting coordinated insider activity.

    Attributes:
        wallet_address: The wallet identified as a sniper.
        cluster_id: Unique identifier for this cluster.
        cluster_size: Number of wallets in the cluster.
        avg_entry_delta_seconds: Average time (seconds) from market creation to entry.
        markets_in_common: Number of markets where cluster members overlap.
        confidence: Confidence score (0.0 to 1.0) based on clustering strength.
        timestamp: When this signal was generated.
    """

    wallet_address: str
    cluster_id: str
    cluster_size: int
    avg_entry_delta_seconds: float
    markets_in_common: int
    confidence: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def is_high_confidence(self) -> bool:
        """Return True if confidence exceeds 0.7."""
        return self.confidence >= 0.7

    @property
    def is_very_high_confidence(self) -> bool:
        """Return True if confidence exceeds 0.85."""
        return self.confidence >= 0.85

    def to_dict(self) -> dict[str, object]:
        """Serialize to dictionary for Redis stream publishing."""
        return {
            "wallet_address": self.wallet_address,
            "cluster_id": self.cluster_id,
            "cluster_size": self.cluster_size,
            "avg_entry_delta_seconds": self.avg_entry_delta_seconds,
            "markets_in_common": self.markets_in_common,
            "confidence": self.confidence,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass(frozen=True)
class CoEntryCorrelationSignal:
    """Signal for repeated co-entry among first entrants.

    This is a deterministic, low-weight coordination signal that does not depend
    on clustering. It flags wallets that repeatedly appear among the first N
    entrants of the same markets alongside the same partner wallet.
    """

    wallet_address: str
    top_partner_wallet: str
    shared_markets: int
    markets_considered: int
    confidence: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def is_high_confidence(self) -> bool:
        return self.confidence >= 0.7

    @property
    def is_very_high_confidence(self) -> bool:
        return self.confidence >= 0.85

    def to_dict(self) -> dict[str, object]:
        return {
            "wallet_address": self.wallet_address,
            "top_partner_wallet": self.top_partner_wallet,
            "shared_markets": self.shared_markets,
            "markets_considered": self.markets_considered,
            "confidence": self.confidence,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass(frozen=True)
class FundingChainSignal:
    """Signal derived from a bounded on-chain funding trace."""

    wallet_address: str
    origin_address: str
    origin_type: str
    hop_count: int
    suspiciousness: float
    confidence: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def is_high_confidence(self) -> bool:
        return self.confidence >= 0.7

    @property
    def is_very_high_confidence(self) -> bool:
        return self.confidence >= 0.85

    def to_dict(self) -> dict[str, object]:
        return {
            "wallet_address": self.wallet_address,
            "origin_address": self.origin_address,
            "origin_type": self.origin_type,
            "hop_count": self.hop_count,
            "suspiciousness": self.suspiciousness,
            "confidence": self.confidence,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass(frozen=True)
class OrderToTradeRatioSignal:
    """Signal for unusually high order-to-trade ratio (wallet, market, window)."""

    wallet_address: str
    market_id: str
    window_minutes: int
    orders_placed: int
    trades_executed: int
    ratio: float
    ratio_percentile: float
    confidence: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, object]:
        return {
            "wallet_address": self.wallet_address,
            "market_id": self.market_id,
            "window_minutes": self.window_minutes,
            "orders_placed": self.orders_placed,
            "trades_executed": self.trades_executed,
            "ratio": self.ratio,
            "ratio_percentile": self.ratio_percentile,
            "confidence": self.confidence,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass(frozen=True)
class RapidCancelSignal:
    """Signal for unusually fast cancel behavior (wallet, market)."""

    wallet_address: str
    market_id: str
    order_hash: str
    cancel_latency_seconds: float
    latency_percentile: float
    fill_ratio: float
    confidence: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, object]:
        return {
            "wallet_address": self.wallet_address,
            "market_id": self.market_id,
            "order_hash": self.order_hash,
            "cancel_latency_seconds": self.cancel_latency_seconds,
            "latency_percentile": self.latency_percentile,
            "fill_ratio": self.fill_ratio,
            "confidence": self.confidence,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass(frozen=True)
class BookImpactWithoutFillSignal:
    """Signal for book-moving orders canceled quickly with no fill."""

    wallet_address: str
    market_id: str
    order_hash: str
    moved_best: bool
    impact_bps: float
    impact_percentile: float
    cancel_latency_seconds: float
    latency_percentile: float
    fill_ratio: float
    confidence: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, object]:
        return {
            "wallet_address": self.wallet_address,
            "market_id": self.market_id,
            "order_hash": self.order_hash,
            "moved_best": self.moved_best,
            "impact_bps": self.impact_bps,
            "impact_percentile": self.impact_percentile,
            "cancel_latency_seconds": self.cancel_latency_seconds,
            "latency_percentile": self.latency_percentile,
            "fill_ratio": self.fill_ratio,
            "confidence": self.confidence,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass(frozen=True)
class ModelScoreSignal:
    """Model probability signal (self-supervised)."""

    trade_id: str
    score: float
    confidence: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, object]:
        return {
            "trade_id": self.trade_id,
            "score": self.score,
            "confidence": self.confidence,
            "timestamp": self.timestamp.isoformat(),
        }

@dataclass(frozen=True)
class PreMoveSignal:
    """Signal for trades that precede outsized subsequent movement."""

    trade_event: TradeEvent
    horizons_seconds: tuple[int, ...]
    z_scores_by_horizon: dict[int, float]
    max_z_score: float
    confidence: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def wallet_address(self) -> str:
        return self.trade_event.wallet_address

    @property
    def market_id(self) -> str:
        return self.trade_event.market_id

    def to_dict(self) -> dict[str, object]:
        return {
            "wallet_address": self.wallet_address,
            "market_id": self.market_id,
            "trade_id": self.trade_event.trade_id,
            "horizons_seconds": list(self.horizons_seconds),
            "z_scores_by_horizon": self.z_scores_by_horizon,
            "max_z_score": self.max_z_score,
            "confidence": self.confidence,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass(frozen=True)
class TradeSizeOutlierSignal:
    """Signal for trade notional outliers vs rolling market baseline."""

    trade_event: TradeEvent
    baseline_quantile_usdc: Decimal
    multiple_of_quantile: float
    confidence: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def wallet_address(self) -> str:
        return self.trade_event.wallet_address

    @property
    def market_id(self) -> str:
        return self.trade_event.market_id

    def to_dict(self) -> dict[str, object]:
        return {
            "wallet_address": self.wallet_address,
            "market_id": self.market_id,
            "trade_id": self.trade_event.trade_id,
            "baseline_quantile_usdc": str(self.baseline_quantile_usdc),
            "multiple_of_quantile": self.multiple_of_quantile,
            "confidence": self.confidence,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass(frozen=True)
class DigitDistributionSignal:
    """Weak signal for digit/distribution irregularities (Benford-style)."""

    trade_event: TradeEvent
    l1_distance: float
    sample_size: int
    confidence: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def wallet_address(self) -> str:
        return self.trade_event.wallet_address

    @property
    def market_id(self) -> str:
        return self.trade_event.market_id

    def to_dict(self) -> dict[str, object]:
        return {
            "wallet_address": self.wallet_address,
            "market_id": self.market_id,
            "trade_id": self.trade_event.trade_id,
            "l1_distance": self.l1_distance,
            "sample_size": self.sample_size,
            "confidence": self.confidence,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass(frozen=True)
class TradeSlicingSignal:
    """Signal for iceberg-style slicing accumulation within a rolling window."""

    trade_event: TradeEvent
    window_minutes: int
    window_trade_count: int
    window_notional_usdc: Decimal
    window_avg_notional_usdc: Decimal
    market_median_trade_usdc: Decimal
    confidence: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def wallet_address(self) -> str:
        return self.trade_event.wallet_address

    @property
    def market_id(self) -> str:
        return self.trade_event.market_id

    def to_dict(self) -> dict[str, object]:
        return {
            "wallet_address": self.wallet_address,
            "market_id": self.market_id,
            "trade_id": self.trade_event.trade_id,
            "window_minutes": self.window_minutes,
            "window_trade_count": self.window_trade_count,
            "window_notional_usdc": str(self.window_notional_usdc),
            "window_avg_notional_usdc": str(self.window_avg_notional_usdc),
            "market_median_trade_usdc": str(self.market_median_trade_usdc),
            "confidence": self.confidence,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass(frozen=True)
class RiskAssessment:
    """Combined risk assessment aggregating all signal types.

    This represents the final scoring output that determines whether
    a trade should trigger an alert, combining signals from multiple
    detectors with configurable weights.

    Attributes:
        trade_event: The original trade event being assessed.
        wallet_address: The trader's wallet address.
        market_id: The market condition ID.
        fresh_wallet_signal: Signal from fresh wallet detector, if triggered.
        size_anomaly_signal: Signal from size anomaly detector, if triggered.
        signals_triggered: Count of how many signal types fired.
        weighted_score: Final weighted combination of all signals (0.0 to 1.0).
        should_alert: Whether this assessment meets alert threshold.
        assessment_id: Unique identifier for this assessment.
        timestamp: When this assessment was generated.
    """

    trade_event: TradeEvent
    wallet_address: str
    market_id: str

    # Individual signals (None if not triggered)
    fresh_wallet_signal: FreshWalletSignal | None
    size_anomaly_signal: SizeAnomalySignal | None
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

    # Combined scoring
    signals_triggered: int
    weighted_score: float
    should_alert: bool

    # Metadata
    assessment_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def is_high_risk(self) -> bool:
        """Return True if weighted score exceeds 0.7."""
        return self.weighted_score >= 0.7

    @property
    def is_very_high_risk(self) -> bool:
        """Return True if weighted score exceeds 0.85."""
        return self.weighted_score >= 0.85

    @property
    def trade_size_usdc(self) -> Decimal:
        """Return the trade size in USDC (notional value)."""
        return self.trade_event.notional_value

    def to_dict(self) -> dict[str, object]:
        """Serialize to dictionary for Redis stream publishing."""
        return {
            "assessment_id": self.assessment_id,
            "wallet_address": self.wallet_address,
            "market_id": self.market_id,
            "trade_id": self.trade_event.trade_id,
            "trade_size": str(self.trade_size_usdc),
            "trade_side": self.trade_event.side,
            "trade_price": str(self.trade_event.price),
            "signals_triggered": self.signals_triggered,
            "weighted_score": self.weighted_score,
            "should_alert": self.should_alert,
            "has_fresh_wallet_signal": self.fresh_wallet_signal is not None,
            "has_size_anomaly_signal": self.size_anomaly_signal is not None,
            "has_sniper_cluster_signal": self.sniper_cluster_signal is not None,
            "has_coentry_signal": self.coentry_signal is not None,
            "has_funding_signal": self.funding_signal is not None,
            "has_pre_move_signal": self.pre_move_signal is not None,
            "has_trade_size_outlier_signal": self.trade_size_outlier_signal is not None,
            "has_digit_distribution_signal": self.digit_distribution_signal is not None,
            "has_trade_slicing_signal": self.trade_slicing_signal is not None,
            "has_order_to_trade_ratio_signal": self.order_to_trade_ratio_signal is not None,
            "has_rapid_cancel_signal": self.rapid_cancel_signal is not None,
            "has_book_impact_without_fill_signal": self.book_impact_without_fill_signal is not None,
            "has_model_score_signal": self.model_score_signal is not None,
            "fresh_wallet_confidence": (
                self.fresh_wallet_signal.confidence if self.fresh_wallet_signal else None
            ),
            "size_anomaly_confidence": (
                self.size_anomaly_signal.confidence if self.size_anomaly_signal else None
            ),
            "sniper_cluster_confidence": (
                self.sniper_cluster_signal.confidence if self.sniper_cluster_signal else None
            ),
            "coentry_confidence": self.coentry_signal.confidence if self.coentry_signal else None,
            "funding_confidence": self.funding_signal.confidence if self.funding_signal else None,
            "pre_move_confidence": self.pre_move_signal.confidence if self.pre_move_signal else None,
            "trade_size_outlier_confidence": (
                self.trade_size_outlier_signal.confidence if self.trade_size_outlier_signal else None
            ),
            "digit_distribution_confidence": (
                self.digit_distribution_signal.confidence if self.digit_distribution_signal else None
            ),
            "trade_slicing_confidence": (
                self.trade_slicing_signal.confidence if self.trade_slicing_signal else None
            ),
            "order_to_trade_ratio_confidence": (
                self.order_to_trade_ratio_signal.confidence if self.order_to_trade_ratio_signal else None
            ),
            "rapid_cancel_confidence": (
                self.rapid_cancel_signal.confidence if self.rapid_cancel_signal else None
            ),
            "book_impact_without_fill_confidence": (
                self.book_impact_without_fill_signal.confidence
                if self.book_impact_without_fill_signal
                else None
            ),
            "model_score_confidence": (
                self.model_score_signal.confidence if self.model_score_signal else None
            ),
            "timestamp": self.timestamp.isoformat(),
        }
