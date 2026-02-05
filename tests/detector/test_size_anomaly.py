"""Tests for position size anomaly detection (liquidity-aware)."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from polymarket_insider_tracker.detector.size_anomaly import LiquidityInputs, SizeAnomalyDetector
from polymarket_insider_tracker.ingestor.models import TradeEvent


def make_trade(*, notional: Decimal, trade_id: str = "tx_1") -> TradeEvent:
    # price * size = notional => size = notional / price
    price = Decimal("0.5")
    size = notional / price
    return TradeEvent(
        market_id="market_1",
        trade_id=trade_id,
        wallet_address="0xwallet",
        side="BUY",
        outcome="Yes",
        outcome_index=0,
        price=price,
        size=size,
        timestamp=datetime.now(UTC),
        asset_id="asset_1",
    )


class TestSizeAnomalyDetector:
    @pytest.mark.asyncio
    async def test_no_signal_when_below_thresholds(self) -> None:
        detector = SizeAnomalyDetector(volume_threshold=0.10, book_threshold=0.10)
        trade = make_trade(notional=Decimal("1000"))

        signal = await detector.analyze(
            trade,
            liquidity=LiquidityInputs(
                rolling_24h_volume_usdc=Decimal("100000"),  # 1%
                visible_book_depth_usdc=Decimal("50000"),  # 2%
            ),
        )
        assert signal is None

    @pytest.mark.asyncio
    async def test_signal_when_volume_impact_high(self) -> None:
        detector = SizeAnomalyDetector(volume_threshold=0.02, book_threshold=0.50)
        trade = make_trade(notional=Decimal("3000"))

        signal = await detector.analyze(
            trade,
            liquidity=LiquidityInputs(
                rolling_24h_volume_usdc=Decimal("100000"),  # 3%
                visible_book_depth_usdc=Decimal("1000000"),
            ),
        )
        assert signal is not None
        assert signal.volume_impact == pytest.approx(0.03)

    @pytest.mark.asyncio
    async def test_signal_when_book_impact_high(self) -> None:
        detector = SizeAnomalyDetector(volume_threshold=0.50, book_threshold=0.05)
        trade = make_trade(notional=Decimal("6000"))

        signal = await detector.analyze(
            trade,
            liquidity=LiquidityInputs(
                rolling_24h_volume_usdc=Decimal("1000000"),
                visible_book_depth_usdc=Decimal("100000"),  # 6%
            ),
        )
        assert signal is not None
        assert signal.book_impact == pytest.approx(0.06)

