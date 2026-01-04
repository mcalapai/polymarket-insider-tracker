"""Tests for ingestor data models."""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from polymarket_insider_tracker.ingestor.models import (
    Market,
    Orderbook,
    OrderbookLevel,
    Token,
)


class TestToken:
    """Tests for Token model."""

    def test_from_dict_with_price(self) -> None:
        """Test creating Token from dict with price."""
        data = {
            "token_id": "123abc",
            "outcome": "Yes",
            "price": "0.65",
        }
        token = Token.from_dict(data)

        assert token.token_id == "123abc"
        assert token.outcome == "Yes"
        assert token.price == Decimal("0.65")

    def test_from_dict_without_price(self) -> None:
        """Test creating Token from dict without price."""
        data = {
            "token_id": "456def",
            "outcome": "No",
        }
        token = Token.from_dict(data)

        assert token.token_id == "456def"
        assert token.outcome == "No"
        assert token.price is None

    def test_frozen(self) -> None:
        """Test that Token is immutable."""
        token = Token(token_id="123", outcome="Yes", price=Decimal("0.5"))
        with pytest.raises(AttributeError):
            token.token_id = "456"  # type: ignore[misc]


class TestMarket:
    """Tests for Market model."""

    def test_from_dict_full(self) -> None:
        """Test creating Market from complete dict."""
        data = {
            "condition_id": "0xabc123",
            "question": "Will it rain tomorrow?",
            "description": "Market for weather prediction",
            "tokens": [
                {"token_id": "t1", "outcome": "Yes", "price": "0.7"},
                {"token_id": "t2", "outcome": "No", "price": "0.3"},
            ],
            "end_date_iso": "2024-12-31T23:59:59Z",
            "active": True,
            "closed": False,
        }
        market = Market.from_dict(data)

        assert market.condition_id == "0xabc123"
        assert market.question == "Will it rain tomorrow?"
        assert market.description == "Market for weather prediction"
        assert len(market.tokens) == 2
        assert market.tokens[0].outcome == "Yes"
        assert market.tokens[1].outcome == "No"
        assert market.end_date == datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        assert market.active is True
        assert market.closed is False

    def test_from_dict_minimal(self) -> None:
        """Test creating Market from minimal dict."""
        data = {
            "condition_id": "0xdef456",
        }
        market = Market.from_dict(data)

        assert market.condition_id == "0xdef456"
        assert market.question == ""
        assert market.description == ""
        assert market.tokens == ()
        assert market.end_date is None
        assert market.active is True
        assert market.closed is False

    def test_from_dict_invalid_date(self) -> None:
        """Test that invalid date is handled gracefully."""
        data = {
            "condition_id": "0x123",
            "end_date_iso": "not-a-valid-date",
        }
        market = Market.from_dict(data)

        assert market.end_date is None

    def test_frozen(self) -> None:
        """Test that Market is immutable."""
        market = Market(
            condition_id="0x123",
            question="Test?",
            description="",
            tokens=(),
        )
        with pytest.raises(AttributeError):
            market.condition_id = "0x456"  # type: ignore[misc]


class TestOrderbookLevel:
    """Tests for OrderbookLevel model."""

    def test_from_dict(self) -> None:
        """Test creating OrderbookLevel from dict."""
        data = {"price": "0.55", "size": "100.5"}
        level = OrderbookLevel.from_dict(data)

        assert level.price == Decimal("0.55")
        assert level.size == Decimal("100.5")

    def test_frozen(self) -> None:
        """Test that OrderbookLevel is immutable."""
        level = OrderbookLevel(price=Decimal("0.5"), size=Decimal("10"))
        with pytest.raises(AttributeError):
            level.price = Decimal("0.6")  # type: ignore[misc]


class TestOrderbook:
    """Tests for Orderbook model."""

    def test_from_clob_orderbook(self) -> None:
        """Test creating Orderbook from py-clob-client response."""
        # Create mock bid/ask objects
        mock_bid = MagicMock()
        mock_bid.price = "0.50"
        mock_bid.size = "100"

        mock_ask = MagicMock()
        mock_ask.price = "0.52"
        mock_ask.size = "150"

        mock_orderbook = MagicMock()
        mock_orderbook.market = "0xmarket123"
        mock_orderbook.asset_id = "token123"
        mock_orderbook.tick_size = "0.01"
        mock_orderbook.bids = [mock_bid]
        mock_orderbook.asks = [mock_ask]

        orderbook = Orderbook.from_clob_orderbook(mock_orderbook)

        assert orderbook.market == "0xmarket123"
        assert orderbook.asset_id == "token123"
        assert orderbook.tick_size == Decimal("0.01")
        assert len(orderbook.bids) == 1
        assert len(orderbook.asks) == 1
        assert orderbook.bids[0].price == Decimal("0.50")
        assert orderbook.asks[0].price == Decimal("0.52")

    def test_from_clob_orderbook_empty(self) -> None:
        """Test creating Orderbook with empty bids/asks."""
        mock_orderbook = MagicMock()
        mock_orderbook.market = "0xmarket"
        mock_orderbook.asset_id = "token"
        mock_orderbook.tick_size = "0.01"
        mock_orderbook.bids = None
        mock_orderbook.asks = []

        orderbook = Orderbook.from_clob_orderbook(mock_orderbook)

        assert orderbook.bids == ()
        assert orderbook.asks == ()

    def test_best_bid(self) -> None:
        """Test best_bid property."""
        orderbook = Orderbook(
            market="0x",
            asset_id="t",
            bids=(
                OrderbookLevel(Decimal("0.50"), Decimal("100")),
                OrderbookLevel(Decimal("0.49"), Decimal("50")),
            ),
            asks=(),
            tick_size=Decimal("0.01"),
        )

        assert orderbook.best_bid == Decimal("0.50")

    def test_best_bid_empty(self) -> None:
        """Test best_bid with no bids."""
        orderbook = Orderbook(
            market="0x",
            asset_id="t",
            bids=(),
            asks=(),
            tick_size=Decimal("0.01"),
        )

        assert orderbook.best_bid is None

    def test_best_ask(self) -> None:
        """Test best_ask property."""
        orderbook = Orderbook(
            market="0x",
            asset_id="t",
            bids=(),
            asks=(
                OrderbookLevel(Decimal("0.52"), Decimal("100")),
                OrderbookLevel(Decimal("0.53"), Decimal("50")),
            ),
            tick_size=Decimal("0.01"),
        )

        assert orderbook.best_ask == Decimal("0.52")

    def test_spread(self) -> None:
        """Test spread calculation."""
        orderbook = Orderbook(
            market="0x",
            asset_id="t",
            bids=(OrderbookLevel(Decimal("0.50"), Decimal("100")),),
            asks=(OrderbookLevel(Decimal("0.52"), Decimal("100")),),
            tick_size=Decimal("0.01"),
        )

        assert orderbook.spread == Decimal("0.02")

    def test_spread_missing_data(self) -> None:
        """Test spread with missing bid or ask."""
        orderbook = Orderbook(
            market="0x",
            asset_id="t",
            bids=(OrderbookLevel(Decimal("0.50"), Decimal("100")),),
            asks=(),
            tick_size=Decimal("0.01"),
        )

        assert orderbook.spread is None

    def test_midpoint(self) -> None:
        """Test midpoint calculation."""
        orderbook = Orderbook(
            market="0x",
            asset_id="t",
            bids=(OrderbookLevel(Decimal("0.50"), Decimal("100")),),
            asks=(OrderbookLevel(Decimal("0.52"), Decimal("100")),),
            tick_size=Decimal("0.01"),
        )

        assert orderbook.midpoint == Decimal("0.51")

    def test_midpoint_missing_data(self) -> None:
        """Test midpoint with missing data."""
        orderbook = Orderbook(
            market="0x",
            asset_id="t",
            bids=(),
            asks=(OrderbookLevel(Decimal("0.52"), Decimal("100")),),
            tick_size=Decimal("0.01"),
        )

        assert orderbook.midpoint is None
