"""Data models for the detector module."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.profiler.models import WalletProfile


@dataclass(frozen=True)
class FreshWalletSignal:
    """Signal emitted when a fresh wallet makes a suspicious trade.

    This signal combines trade event data with wallet profile analysis
    to produce a confidence score indicating the likelihood of suspicious
    activity.

    Attributes:
        trade_event: The original trade event that triggered this signal.
        wallet_profile: Analyzed profile of the trader's wallet.
        confidence: Overall confidence score (0.0 to 1.0).
        factors: Individual factor scores contributing to confidence.
        timestamp: When this signal was generated.
    """

    trade_event: TradeEvent
    wallet_profile: WalletProfile
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
            "wallet_nonce": self.wallet_profile.nonce,
            "wallet_age_hours": self.wallet_profile.age_hours,
            "wallet_is_fresh": self.wallet_profile.is_fresh,
            "confidence": self.confidence,
            "factors": self.factors,
            "timestamp": self.timestamp.isoformat(),
        }
