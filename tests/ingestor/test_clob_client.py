"""Tests for ClobClient wrapper."""

import time
from unittest.mock import MagicMock, patch

import pytest

from polymarket_insider_tracker.ingestor.clob_client import (
    ClobClient,
    ClobClientNotFoundError,
    RateLimiter,
    RetryError,
    with_retry,
)
from polymarket_insider_tracker.ingestor.models import Market, Orderbook
from py_clob_client.clob_types import ApiCreds


class TestRateLimiter:
    """Tests for RateLimiter."""

    def test_acquire_sync_no_wait_first_call(self) -> None:
        """First call should not wait."""
        limiter = RateLimiter(max_requests_per_second=10)
        start = time.monotonic()
        limiter.acquire_sync()
        elapsed = time.monotonic() - start

        # Should be nearly instant
        assert elapsed < 0.05

    def test_acquire_sync_enforces_rate(self) -> None:
        """Subsequent calls should be rate limited."""
        limiter = RateLimiter(max_requests_per_second=10)  # 100ms between calls

        # First call
        limiter.acquire_sync()

        # Second call should wait
        start = time.monotonic()
        limiter.acquire_sync()
        elapsed = time.monotonic() - start

        # Should wait at least 90ms (allowing some tolerance)
        assert elapsed >= 0.08


class TestWithRetry:
    """Tests for retry decorator."""

    def test_success_first_try(self) -> None:
        """Function succeeds on first try."""
        call_count = 0

        @with_retry(max_retries=3)
        def succeed() -> str:
            nonlocal call_count
            call_count += 1
            return "success"

        result = succeed()

        assert result == "success"
        assert call_count == 1

    def test_success_after_retries(self) -> None:
        """Function succeeds after some retries."""
        call_count = 0

        @with_retry(max_retries=3, base_delay=0.01)
        def succeed_eventually() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Not yet")
            return "success"

        result = succeed_eventually()

        assert result == "success"
        assert call_count == 3

    def test_exhausted_retries(self) -> None:
        """Raises RetryError after exhausting retries."""

        @with_retry(max_retries=2, base_delay=0.01)
        def always_fails() -> str:
            raise ValueError("Always fails")

        with pytest.raises(RetryError) as exc_info:
            always_fails()

        assert "3 attempts failed" in str(exc_info.value)
        assert isinstance(exc_info.value.last_exception, ValueError)

    def test_specific_exception_types(self) -> None:
        """Only retries on specified exception types."""
        call_count = 0

        @with_retry(max_retries=3, base_delay=0.01, retry_on=(ValueError,))
        def raise_type_error() -> str:
            nonlocal call_count
            call_count += 1
            raise TypeError("Not retried")

        with pytest.raises(TypeError):
            raise_type_error()

        # Should only be called once since TypeError is not in retry_on
        assert call_count == 1


class TestClobClient:
    """Tests for ClobClient wrapper."""

    @pytest.fixture
    def mock_base_client(self) -> MagicMock:
        """Create a mock base CLOB client."""
        with patch("polymarket_insider_tracker.ingestor.clob_client.BaseClobClient") as mock:
            yield mock.return_value

    def test_init_defaults(self, mock_base_client: MagicMock) -> None:  # noqa: ARG002
        """Test client initialization with defaults."""
        client = ClobClient()

        assert client._host == "https://clob.polymarket.com"
        assert client._max_retries == 3

    def test_level2_configured_flag(self, mock_base_client: MagicMock) -> None:  # noqa: ARG002
        """Level-2 endpoints require both private key and API creds."""
        client = ClobClient()
        assert client.is_level2_configured is False

        client2 = ClobClient(
            private_key="0x" + "1" * 64,
            api_creds=ApiCreds(api_key="k", api_secret="s", api_passphrase="p"),
        )
        assert client2.is_level2_configured is True

    def test_get_market_trades_prefers_level2(self, mock_base_client: MagicMock) -> None:
        """Market trade fetch should use L2 trades endpoint when configured."""
        mock_base_client.get_trades.return_value = [
            {
                "id": "trade_1",
                "market": "0xabc",
                "maker_address": "0xmaker",
                "asset_id": "asset_1",
                "side": "BUY",
                "price": "0.42",
                "size": "100",
                "timestamp": 1704067200,
            }
        ]

        client = ClobClient(
            private_key="0x" + "1" * 64,
            api_creds=ApiCreds(api_key="k", api_secret="s", api_passphrase="p"),
        )
        trades = client.get_market_trades("0xabc")

        assert len(trades) == 1
        assert trades[0].market_id == "0xabc"
        assert trades[0].trade_id == "trade_1"
        assert trades[0].wallet_address == "0xmaker"
        assert mock_base_client.get_trades.call_count == 1
        assert mock_base_client.get_market_trades_events.call_count == 0

    def test_get_market_trades_falls_back_to_events(self, mock_base_client: MagicMock) -> None:
        """If L2 returns no trades, fallback to public events endpoint."""
        mock_base_client.get_trades.return_value = []
        mock_base_client.get_market_trades_events.return_value = [
            {
                "transactionHash": "0xtradehash",
                "conditionId": "0xabc",
                "proxyWallet": "0xwallet",
                "asset": "asset_1",
                "side": "BUY",
                "price": "0.25",
                "size": "20",
                "timestamp": 1704067200,
            }
        ]

        client = ClobClient(
            private_key="0x" + "1" * 64,
            api_creds=ApiCreds(api_key="k", api_secret="s", api_passphrase="p"),
        )
        trades = client.get_market_trades("0xabc")

        assert len(trades) == 1
        assert trades[0].trade_id == "0xtradehash"
        assert mock_base_client.get_trades.call_count == 1
        assert mock_base_client.get_market_trades_events.call_count == 1

    def test_get_market_trades_raises_not_found_when_no_sources_have_data(
        self, mock_base_client: MagicMock
    ) -> None:
        """No data from either source should raise ClobClientNotFoundError."""
        mock_base_client.get_trades.return_value = []
        mock_base_client.get_market_trades_events.return_value = []

        client = ClobClient(
            private_key="0x" + "1" * 64,
            api_creds=ApiCreds(api_key="k", api_secret="s", api_passphrase="p"),
        )

        with pytest.raises(ClobClientNotFoundError, match="No trades available"):
            client.get_market_trades("0xabc")

    def test_health_check_success(self, mock_base_client: MagicMock) -> None:
        """Test health check returns True when API responds OK."""
        mock_base_client.get_ok.return_value = "OK"

        client = ClobClient()
        result = client.health_check()

        assert result is True
        mock_base_client.get_ok.assert_called_once()

    def test_health_check_failure(self, mock_base_client: MagicMock) -> None:
        """Test health check returns False on error."""
        mock_base_client.get_ok.side_effect = Exception("Connection failed")

        client = ClobClient()
        result = client.health_check()

        assert result is False

    def test_get_server_time(self, mock_base_client: MagicMock) -> None:
        """Test getting server time."""
        mock_base_client.get_server_time.return_value = 1704067200000

        client = ClobClient()
        result = client.get_server_time()

        assert result == 1704067200000

    def test_get_markets(self, mock_base_client: MagicMock) -> None:
        """Test fetching markets."""
        mock_base_client.get_simplified_markets.return_value = {
            "data": [
                {
                    "condition_id": "0x123",
                    "question": "Test market?",
                    "tokens": [],
                    "closed": False,
                },
            ],
            "next_cursor": "LTE=",
        }

        client = ClobClient()
        markets = client.get_markets()

        assert len(markets) == 1
        assert isinstance(markets[0], Market)
        assert markets[0].condition_id == "0x123"

    def test_get_markets_filters_closed(self, mock_base_client: MagicMock) -> None:
        """Test that closed markets are filtered when active_only=True."""
        mock_base_client.get_simplified_markets.return_value = {
            "data": [
                {"condition_id": "0x1", "closed": False},
                {"condition_id": "0x2", "closed": True},
            ],
            "next_cursor": "LTE=",
        }

        client = ClobClient()
        markets = client.get_markets(active_only=True)

        assert len(markets) == 1
        assert markets[0].condition_id == "0x1"

    def test_get_markets_filters_inactive(self, mock_base_client: MagicMock) -> None:
        """Test that inactive markets are filtered when active_only=True."""
        mock_base_client.get_simplified_markets.return_value = {
            "data": [
                {"condition_id": "0x1", "active": True, "closed": False},
                {"condition_id": "0x2", "active": False, "closed": False},
            ],
            "next_cursor": "LTE=",
        }

        client = ClobClient()
        markets = client.get_markets(active_only=True)

        assert len(markets) == 1
        assert markets[0].condition_id == "0x1"

    def test_get_markets_includes_closed(self, mock_base_client: MagicMock) -> None:
        """Test that closed markets are included when active_only=False."""
        mock_base_client.get_simplified_markets.return_value = {
            "data": [
                {"condition_id": "0x1", "closed": False},
                {"condition_id": "0x2", "closed": True},
            ],
            "next_cursor": "LTE=",
        }

        client = ClobClient()
        markets = client.get_markets(active_only=False)

        assert len(markets) == 2

    def test_get_markets_pagination(self, mock_base_client: MagicMock) -> None:
        """Test that pagination is handled correctly."""
        mock_base_client.get_simplified_markets.side_effect = [
            {
                "data": [{"condition_id": "0x1"}],
                "next_cursor": "cursor2",
            },
            {
                "data": [{"condition_id": "0x2"}],
                "next_cursor": "LTE=",
            },
        ]

        client = ClobClient()
        markets = client.get_markets()

        assert len(markets) == 2
        assert mock_base_client.get_simplified_markets.call_count == 2

    def test_get_market(self, mock_base_client: MagicMock) -> None:
        """Test fetching a single market."""
        mock_base_client.get_market.return_value = {
            "condition_id": "0xabc",
            "question": "Will it happen?",
            "tokens": [
                {"token_id": "t1", "outcome": "Yes"},
                {"token_id": "t2", "outcome": "No"},
            ],
        }

        client = ClobClient()
        market = client.get_market("0xabc")

        assert isinstance(market, Market)
        assert market.condition_id == "0xabc"
        assert len(market.tokens) == 2

    def test_get_market_not_found(self, mock_base_client: MagicMock) -> None:
        """Test error handling when market not found.

        When the underlying API call fails, the @with_retry decorator will
        retry the operation. After all retries are exhausted, it raises
        RetryError wrapping the original exception.
        """
        mock_base_client.get_market.side_effect = Exception("Not found")

        client = ClobClient()

        with pytest.raises(RetryError) as exc_info:
            client.get_market("0xnotfound")

        # The RetryError wraps the original exception
        assert "get_market" in str(exc_info.value)
        assert exc_info.value.last_exception is not None

    def test_get_orderbook(self, mock_base_client: MagicMock) -> None:
        """Test fetching an orderbook."""
        mock_bid = MagicMock()
        mock_bid.price = "0.50"
        mock_bid.size = "100"

        mock_ask = MagicMock()
        mock_ask.price = "0.52"
        mock_ask.size = "150"

        mock_orderbook = MagicMock()
        mock_orderbook.market = "0xmarket"
        mock_orderbook.asset_id = "token123"
        mock_orderbook.tick_size = "0.01"
        mock_orderbook.bids = [mock_bid]
        mock_orderbook.asks = [mock_ask]

        mock_base_client.get_order_book.return_value = mock_orderbook

        client = ClobClient()
        orderbook = client.get_orderbook("token123")

        assert isinstance(orderbook, Orderbook)
        assert orderbook.asset_id == "token123"
        assert len(orderbook.bids) == 1
        assert len(orderbook.asks) == 1

    def test_get_orderbooks(self, mock_base_client: MagicMock) -> None:
        """Test fetching multiple orderbooks."""
        mock_ob1 = MagicMock()
        mock_ob1.market = "m1"
        mock_ob1.asset_id = "t1"
        mock_ob1.tick_size = "0.01"
        mock_ob1.bids = []
        mock_ob1.asks = []

        mock_ob2 = MagicMock()
        mock_ob2.market = "m2"
        mock_ob2.asset_id = "t2"
        mock_ob2.tick_size = "0.01"
        mock_ob2.bids = []
        mock_ob2.asks = []

        mock_base_client.get_order_books.return_value = [mock_ob1, mock_ob2]

        client = ClobClient()
        orderbooks = client.get_orderbooks(["t1", "t2"])

        assert len(orderbooks) == 2
        assert all(isinstance(ob, Orderbook) for ob in orderbooks)

    def test_get_midpoint(self, mock_base_client: MagicMock) -> None:
        """Test fetching midpoint price."""
        mock_base_client.get_midpoint.return_value = {"mid": "0.55"}

        client = ClobClient()
        result = client.get_midpoint("token123")

        assert result == "0.55"

    def test_get_midpoint_error(self, mock_base_client: MagicMock) -> None:
        """Test midpoint returns None on error."""
        mock_base_client.get_midpoint.side_effect = Exception("API error")

        client = ClobClient()
        result = client.get_midpoint("token123")

        assert result is None

    def test_get_price_buy(self, mock_base_client: MagicMock) -> None:
        """Test fetching buy price."""
        mock_base_client.get_price.return_value = {"price": "0.53"}

        client = ClobClient()
        result = client.get_price("token123", side="BUY")

        assert result == "0.53"
        mock_base_client.get_price.assert_called_with("token123", side="BUY")

    def test_get_price_sell(self, mock_base_client: MagicMock) -> None:
        """Test fetching sell price."""
        mock_base_client.get_price.return_value = {"price": "0.51"}

        client = ClobClient()
        result = client.get_price("token123", side="SELL")

        assert result == "0.51"
        mock_base_client.get_price.assert_called_with("token123", side="SELL")
