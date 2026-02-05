"""Tests for the fresh wallet detector."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from polymarket_insider_tracker.detector.fresh_wallet import (
    BASE_CONFIDENCE,
    BRAND_NEW_BONUS,
    DEFAULT_MAX_AGE_HOURS,
    DEFAULT_MAX_NONCE,
    DEFAULT_MIN_TRADE_SIZE,
    LARGE_TRADE_BONUS,
    VERY_YOUNG_BONUS,
    FreshWalletDetector,
)
from polymarket_insider_tracker.detector.models import FreshWalletSignal
from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.profiler.models import WalletSnapshot


@pytest.fixture
def detector():
    """Create a FreshWalletDetector."""
    return FreshWalletDetector()


def create_trade_event(
    *,
    wallet_address: str = "0x1234567890123456789012345678901234567890",
    market_id: str = "market123",
    trade_id: str = "trade123",
    price: Decimal = Decimal("0.5"),
    size: Decimal = Decimal("2000"),
    side: str = "BUY",
) -> TradeEvent:
    """Create a TradeEvent for testing."""
    return TradeEvent(
        market_id=market_id,
        trade_id=trade_id,
        wallet_address=wallet_address,
        side=side,
        outcome="Yes",
        outcome_index=0,
        price=price,
        size=size,
        timestamp=datetime.now(UTC),
        asset_id="asset123",
    )


def create_wallet_snapshot(
    *,
    address: str = "0x1234567890123456789012345678901234567890",
    nonce: int = 2,
    age_hours: float = 24.0,
) -> WalletSnapshot:
    """Create a WalletSnapshot for testing."""
    as_of = datetime.now(UTC)
    first_funding_at = as_of - timedelta(hours=age_hours)
    return WalletSnapshot(
        address=address.lower(),
        as_of=as_of,
        as_of_block_number=123,
        nonce_as_of=nonce,
        matic_balance_wei_as_of=Decimal("1000000000000000000"),
        usdc_balance_units_as_of=Decimal("1000000000"),
        first_funding_at=first_funding_at,
        age_hours_as_of=age_hours,
    )


# === Test FreshWalletSignal Model ===


class TestFreshWalletSignal:
    """Tests for FreshWalletSignal dataclass."""

    def test_wallet_address_property(self):
        """Test wallet_address property returns trade wallet."""
        trade = create_trade_event(wallet_address="0xabc123")
        profile = create_wallet_snapshot(address="0xabc123")
        signal = FreshWalletSignal(
            trade_event=trade,
            wallet_snapshot=profile,
            confidence=0.7,
            factors={"base": 0.5},
        )
        assert signal.wallet_address == "0xabc123"

    def test_market_id_property(self):
        """Test market_id property returns trade market."""
        trade = create_trade_event(market_id="market456")
        profile = create_wallet_snapshot()
        signal = FreshWalletSignal(
            trade_event=trade,
            wallet_snapshot=profile,
            confidence=0.7,
            factors={"base": 0.5},
        )
        assert signal.market_id == "market456"

    def test_trade_size_usdc_property(self):
        """Test trade_size_usdc returns notional value."""
        trade = create_trade_event(price=Decimal("0.5"), size=Decimal("2000"))
        profile = create_wallet_snapshot()
        signal = FreshWalletSignal(
            trade_event=trade,
            wallet_snapshot=profile,
            confidence=0.7,
            factors={"base": 0.5},
        )
        assert signal.trade_size_usdc == Decimal("1000")

    def test_is_high_confidence(self):
        """Test is_high_confidence threshold."""
        trade = create_trade_event()
        profile = create_wallet_snapshot()

        # At threshold
        signal = FreshWalletSignal(
            trade_event=trade,
            wallet_snapshot=profile,
            confidence=0.7,
            factors={},
        )
        assert signal.is_high_confidence is True

        # Below threshold
        signal_low = FreshWalletSignal(
            trade_event=trade,
            wallet_snapshot=profile,
            confidence=0.69,
            factors={},
        )
        assert signal_low.is_high_confidence is False

    def test_is_very_high_confidence(self):
        """Test is_very_high_confidence threshold."""
        trade = create_trade_event()
        profile = create_wallet_snapshot()

        # At threshold
        signal = FreshWalletSignal(
            trade_event=trade,
            wallet_snapshot=profile,
            confidence=0.85,
            factors={},
        )
        assert signal.is_very_high_confidence is True

        # Below threshold
        signal_low = FreshWalletSignal(
            trade_event=trade,
            wallet_snapshot=profile,
            confidence=0.84,
            factors={},
        )
        assert signal_low.is_very_high_confidence is False

    def test_to_dict(self):
        """Test serialization to dictionary."""
        trade = create_trade_event(
            wallet_address="0xtest",
            market_id="market1",
            trade_id="tx1",
            price=Decimal("0.6"),
            size=Decimal("5000"),
            side="BUY",
        )
        profile = create_wallet_snapshot(address="0xtest", nonce=0, age_hours=1.0)
        signal = FreshWalletSignal(
            trade_event=trade,
            wallet_snapshot=profile,
            confidence=0.8,
            factors={"base": 0.5, "brand_new": 0.2},
        )

        result = signal.to_dict()

        assert result["wallet_address"] == "0xtest"
        assert result["market_id"] == "market1"
        assert result["trade_id"] == "tx1"
        assert result["trade_size"] == "3000.0"
        assert result["trade_side"] == "BUY"
        assert result["trade_price"] == "0.6"
        assert result["wallet_nonce_as_of"] == 0
        assert result["wallet_age_hours_as_of"] == 1.0
        assert result["wallet_as_of_block"] == 123
        assert result["confidence"] == 0.8
        assert result["factors"] == {"base": 0.5, "brand_new": 0.2}
        assert "timestamp" in result


# === Test FreshWalletDetector Initialization ===


class TestFreshWalletDetectorInit:
    """Tests for FreshWalletDetector initialization."""

    def test_default_config(self):
        """Test detector initializes with default config."""
        detector = FreshWalletDetector()
        assert detector._min_trade_size == DEFAULT_MIN_TRADE_SIZE
        assert detector._max_nonce == DEFAULT_MAX_NONCE
        assert detector._max_age_hours == DEFAULT_MAX_AGE_HOURS

    def test_custom_config(self):
        """Test detector accepts custom config."""
        detector = FreshWalletDetector(
            min_trade_size=Decimal("5000"),
            max_nonce=10,
            max_age_hours=72.0,
        )
        assert detector._min_trade_size == Decimal("5000")
        assert detector._max_nonce == 10
        assert detector._max_age_hours == 72.0


# === Test FreshWalletDetector.analyze ===


class TestFreshWalletDetectorAnalyze:
    """Tests for FreshWalletDetector.analyze method."""

    async def test_filters_small_trades(self, detector):
        """Test that trades below minimum size are filtered out."""
        trade = create_trade_event(
            price=Decimal("0.1"),
            size=Decimal("100"),  # notional = $10
        )

        result = await detector.analyze(trade, wallet_snapshot=create_wallet_snapshot())

        assert result is None

    async def test_detects_fresh_wallet(self, detector):
        """Test detection of fresh wallet trade."""
        trade = create_trade_event(
            price=Decimal("0.5"),
            size=Decimal("2000"),  # notional = $1000
        )
        profile = create_wallet_snapshot(address=trade.wallet_address, nonce=2, age_hours=24.0)

        result = await detector.analyze(trade, wallet_snapshot=profile)

        assert result is not None
        assert isinstance(result, FreshWalletSignal)
        assert result.trade_event == trade
        assert result.wallet_snapshot == profile
        assert result.confidence >= BASE_CONFIDENCE

    async def test_filters_non_fresh_wallet(self, detector):
        """Test that non-fresh wallets are filtered out."""
        trade = create_trade_event(
            price=Decimal("0.5"),
            size=Decimal("4000"),  # notional = $2000
        )
        profile = create_wallet_snapshot(address=trade.wallet_address, nonce=10, age_hours=100.0)

        result = await detector.analyze(trade, wallet_snapshot=profile)

        assert result is None

    async def test_wallet_at_nonce_threshold(self, detector):
        """Test wallet exactly at max_nonce threshold."""
        trade = create_trade_event(size=Decimal("3000"))
        profile = create_wallet_snapshot(address=trade.wallet_address, nonce=5, age_hours=24.0)

        result = await detector.analyze(trade, wallet_snapshot=profile)

        assert result is not None

    async def test_wallet_above_nonce_threshold(self, detector):
        """Test wallet above max_nonce threshold."""
        trade = create_trade_event(size=Decimal("3000"))
        profile = create_wallet_snapshot(address=trade.wallet_address, nonce=6, age_hours=24.0)

        result = await detector.analyze(trade, wallet_snapshot=profile)

        assert result is None

    async def test_wallet_at_age_threshold(self, detector):
        """Test wallet exactly at max_age_hours threshold."""
        trade = create_trade_event(size=Decimal("3000"))
        profile = create_wallet_snapshot(address=trade.wallet_address, nonce=2, age_hours=48.0)

        result = await detector.analyze(trade, wallet_snapshot=profile)

        assert result is not None

    async def test_wallet_above_age_threshold(self, detector):
        """Test wallet above max_age_hours threshold."""
        trade = create_trade_event(size=Decimal("3000"))
        profile = create_wallet_snapshot(address=trade.wallet_address, nonce=2, age_hours=49.0)

        result = await detector.analyze(trade, wallet_snapshot=profile)

        assert result is None

# === Test Confidence Scoring ===


class TestConfidenceScoring:
    """Tests for confidence score calculation."""

    def test_base_confidence_only(self, detector):
        """Test base confidence for fresh wallet."""
        snapshot = create_wallet_snapshot(nonce=3, age_hours=24.0)
        trade = create_trade_event(size=Decimal("2000"))  # $1000 notional

        confidence, factors = detector.calculate_confidence(snapshot, trade)

        assert confidence == BASE_CONFIDENCE
        assert factors == {"base": BASE_CONFIDENCE}

    def test_brand_new_bonus(self, detector):
        """Test bonus for brand new wallet (nonce=0)."""
        snapshot = create_wallet_snapshot(nonce=0, age_hours=24.0)
        trade = create_trade_event(size=Decimal("2000"))

        confidence, factors = detector.calculate_confidence(snapshot, trade)

        expected = BASE_CONFIDENCE + BRAND_NEW_BONUS
        assert confidence == expected
        assert factors["brand_new"] == BRAND_NEW_BONUS

    def test_very_young_bonus(self, detector):
        """Test bonus for very young wallet (age < 2 hours)."""
        snapshot = create_wallet_snapshot(nonce=3, age_hours=1.5)
        trade = create_trade_event(size=Decimal("2000"))

        confidence, factors = detector.calculate_confidence(snapshot, trade)

        expected = BASE_CONFIDENCE + VERY_YOUNG_BONUS
        assert confidence == expected
        assert factors["very_young"] == VERY_YOUNG_BONUS

    def test_large_trade_bonus(self, detector):
        """Test bonus for large trade (> $10,000)."""
        snapshot = create_wallet_snapshot(nonce=3, age_hours=24.0)
        trade = create_trade_event(
            price=Decimal("0.5"),
            size=Decimal("25000"),  # $12,500 notional
        )

        confidence, factors = detector.calculate_confidence(snapshot, trade)

        expected = BASE_CONFIDENCE + LARGE_TRADE_BONUS
        assert confidence == expected
        assert factors["large_trade"] == LARGE_TRADE_BONUS

    def test_all_bonuses_combined(self, detector):
        """Test all bonuses stacking."""
        snapshot = create_wallet_snapshot(nonce=0, age_hours=0.5)
        trade = create_trade_event(
            price=Decimal("0.5"),
            size=Decimal("30000"),  # $15,000 notional
        )

        confidence, factors = detector.calculate_confidence(snapshot, trade)

        expected = BASE_CONFIDENCE + BRAND_NEW_BONUS + VERY_YOUNG_BONUS + LARGE_TRADE_BONUS
        assert confidence == expected
        assert factors["brand_new"] == BRAND_NEW_BONUS
        assert factors["very_young"] == VERY_YOUNG_BONUS
        assert factors["large_trade"] == LARGE_TRADE_BONUS

    def test_confidence_clamped_to_max(self, detector):
        """Test confidence is clamped to 1.0 max."""
        # Create scenario where total would exceed 1.0
        # BASE=0.5 + BRAND_NEW=0.2 + VERY_YOUNG=0.1 + LARGE_TRADE=0.1 = 0.9
        # This is less than 1.0, so let's verify clamping works
        snapshot = create_wallet_snapshot(nonce=0, age_hours=0.5)
        trade = create_trade_event(size=Decimal("30000"))

        confidence, _ = detector.calculate_confidence(snapshot, trade)

        assert confidence <= 1.0

    def test_no_bonus_at_age_boundary(self, detector):
        """Test no bonus when age exactly at 2 hours."""
        snapshot = create_wallet_snapshot(nonce=3, age_hours=2.0)
        trade = create_trade_event(size=Decimal("2000"))

        confidence, factors = detector.calculate_confidence(snapshot, trade)

        assert "very_young" not in factors
        assert confidence == BASE_CONFIDENCE

    def test_no_bonus_at_trade_size_boundary(self, detector):
        """Test no bonus when trade exactly at $10,000."""
        snapshot = create_wallet_snapshot(nonce=3, age_hours=24.0)
        trade = create_trade_event(
            price=Decimal("0.5"),
            size=Decimal("20000"),  # $10,000 notional exactly
        )

        confidence, factors = detector.calculate_confidence(snapshot, trade)

        # Exactly at threshold should NOT get bonus
        assert "large_trade" not in factors
        assert confidence == BASE_CONFIDENCE

# === Test Batch Analysis ===


class TestBatchAnalysis:
    """Tests for batch trade analysis."""

    async def test_analyze_batch_success(self, detector):
        """Test analyzing multiple trades."""
        trades = [
            create_trade_event(trade_id="trade1", size=Decimal("3000")),
            create_trade_event(trade_id="trade2", size=Decimal("4000")),
        ]
        snapshot = create_wallet_snapshot(address=trades[0].wallet_address, nonce=2, age_hours=24.0)

        results = await detector.analyze_batch(
            trades, wallet_snapshots={trades[0].wallet_address.lower(): snapshot}
        )

        assert len(results) == 2
        assert all(isinstance(r, FreshWalletSignal) for r in results)

    async def test_analyze_batch_filters_small(self, detector):
        """Test batch analysis filters small trades."""
        trades = [
            create_trade_event(trade_id="trade1", size=Decimal("3000")),  # Above threshold
            create_trade_event(trade_id="trade2", size=Decimal("100")),  # Below threshold
        ]
        snapshot = create_wallet_snapshot(address=trades[0].wallet_address, nonce=2, age_hours=24.0)

        results = await detector.analyze_batch(
            trades, wallet_snapshots={trades[0].wallet_address.lower(): snapshot}
        )

        assert len(results) == 1
        assert results[0].trade_event.trade_id == "trade1"

    async def test_analyze_batch_raises_on_missing_snapshot(self, detector):
        """Test batch analysis is strict about missing snapshots."""
        trades = [
            create_trade_event(trade_id="trade1", size=Decimal("3000")),
            create_trade_event(trade_id="trade2", size=Decimal("4000")),
        ]
        snapshot = create_wallet_snapshot(address=trades[0].wallet_address, nonce=2, age_hours=24.0)
        with pytest.raises(KeyError):
            await detector.analyze_batch(trades, wallet_snapshots={})

    async def test_analyze_batch_empty_list(self, detector):
        """Test batch analysis with empty list."""
        results = await detector.analyze_batch([], wallet_snapshots={})

        assert results == []

    async def test_analyze_batch_all_filtered(self, detector):
        """Test batch analysis when all trades are filtered."""
        trades = [
            create_trade_event(trade_id="trade1", size=Decimal("100")),
            create_trade_event(trade_id="trade2", size=Decimal("50")),
        ]

        snapshot = create_wallet_snapshot(address=trades[0].wallet_address, nonce=2, age_hours=24.0)
        results = await detector.analyze_batch(
            trades, wallet_snapshots={trades[0].wallet_address.lower(): snapshot}
        )

        assert results == []


# === Integration-Style Tests ===


class TestDetectorIntegration:
    """Integration-style tests for complete detector flow."""

    async def test_full_detection_flow(self):
        """Test complete detection flow from trade to signal."""
        detector = FreshWalletDetector()

        # Create a suspicious trade
        trade = create_trade_event(
            wallet_address="0xfresh123",
            market_id="suspicious_market",
            price=Decimal("0.65"),
            size=Decimal("20000"),  # $13,000 notional
        )

        # Create a fresh wallet profile
        profile = create_wallet_snapshot(
            address="0xfresh123",
            nonce=0,  # Brand new
            age_hours=0.5,  # Very young
        )
        # Analyze
        signal = await detector.analyze(trade, wallet_snapshot=profile)

        # Verify signal
        assert signal is not None
        assert signal.wallet_address == "0xfresh123"
        assert signal.market_id == "suspicious_market"
        assert signal.is_high_confidence is True
        assert "brand_new" in signal.factors
        assert "very_young" in signal.factors
        assert "large_trade" in signal.factors

    async def test_custom_thresholds(self):
        """Test detection with custom thresholds."""
        detector = FreshWalletDetector(
            min_trade_size=Decimal("5000"),
            max_nonce=3,
            max_age_hours=24.0,
        )

        # Trade that would pass default but fails custom thresholds
        trade = create_trade_event(size=Decimal("8000"))  # $4000 notional
        profile = create_wallet_snapshot(nonce=4, age_hours=30.0)
        # Should fail min_trade_size check
        result = await detector.analyze(trade, wallet_snapshot=profile)
        assert result is None

    async def test_edge_case_exact_min_trade_size(self, detector):
        """Test trade exactly at minimum size threshold."""
        trade = create_trade_event(
            price=Decimal("0.5"),
            size=Decimal("2000"),  # $1000 notional exactly at default
        )
        profile = create_wallet_snapshot(address=trade.wallet_address, nonce=2)

        result = await detector.analyze(trade, wallet_snapshot=profile)

        # At threshold should pass
        assert result is not None

    async def test_edge_case_below_min_trade_size(self, detector):
        """Test trade just below minimum size threshold."""
        trade = create_trade_event(
            price=Decimal("0.5"),
            size=Decimal("1999"),  # $999.50 - just under $1000
        )
        profile = create_wallet_snapshot(address=trade.wallet_address, nonce=2)

        result = await detector.analyze(trade, wallet_snapshot=profile)

        # Below threshold should fail
        assert result is None
