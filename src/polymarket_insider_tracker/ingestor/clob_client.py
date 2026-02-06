"""Wrapper around py-clob-client with rate limiting and retry logic."""

import asyncio
import logging
import time
from collections.abc import Iterator
from collections.abc import Callable
from functools import wraps
from typing import Any, ParamSpec, TypeVar
from urllib.parse import urlencode

from py_clob_client.client import ClobClient as BaseClobClient
from py_clob_client.clob_types import ApiCreds, BookParams, TradeParams
from py_clob_client.exceptions import PolyApiException
from py_clob_client.http_helpers.helpers import get as http_get

from polymarket_insider_tracker.ingestor.models import Market, Orderbook, TradeEvent

logger = logging.getLogger(__name__)

P = ParamSpec("P")
T = TypeVar("T")

# Constants
DEFAULT_HOST = "https://clob.polymarket.com"
DEFAULT_DATA_API_HOST = "https://data-api.polymarket.com"
MAX_REQUESTS_PER_SECOND = 10
MIN_REQUEST_INTERVAL = 1.0 / MAX_REQUESTS_PER_SECOND  # 0.1 seconds

DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BASE_DELAY = 1.0
RETRY_STATUS_CODES = (429, 500, 502, 503, 504)


class RateLimiter:
    """Token bucket rate limiter for API requests."""

    def __init__(self, max_requests_per_second: float = MAX_REQUESTS_PER_SECOND) -> None:
        """Initialize the rate limiter.

        Args:
            max_requests_per_second: Maximum requests allowed per second.
        """
        self._min_interval = 1.0 / max_requests_per_second
        self._last_request_time: float = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until a request slot is available."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            if elapsed < self._min_interval:
                wait_time = self._min_interval - elapsed
                await asyncio.sleep(wait_time)
            self._last_request_time = time.monotonic()

    def acquire_sync(self) -> None:
        """Synchronous version of acquire for sync operations."""
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            wait_time = self._min_interval - elapsed
            time.sleep(wait_time)
        self._last_request_time = time.monotonic()


class RetryError(Exception):
    """Raised when all retry attempts are exhausted."""

    def __init__(self, message: str, last_exception: Exception | None = None) -> None:
        super().__init__(message)
        self.last_exception = last_exception


def with_retry(
    max_retries: int = DEFAULT_MAX_RETRIES,
    base_delay: float = DEFAULT_RETRY_BASE_DELAY,
    retry_on: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator for adding retry logic with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts.
        base_delay: Base delay in seconds (doubles with each retry).
        retry_on: Tuple of exception types to retry on.

    Returns:
        Decorated function with retry logic.
    """

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            last_exception: Exception | None = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retry_on as e:
                    last_exception = e
                    if attempt == max_retries:
                        break

                    delay = base_delay * (2**attempt)
                    logger.warning(
                        "Attempt %d/%d failed: %s. Retrying in %.1f seconds...",
                        attempt + 1,
                        max_retries + 1,
                        str(e),
                        delay,
                    )
                    time.sleep(delay)

            raise RetryError(
                f"All {max_retries + 1} attempts failed for {func.__name__}",
                last_exception=last_exception,
            )

        return wrapper

    return decorator


class ClobClientError(Exception):
    """Base exception for ClobClient errors."""


class ClobClientNotFoundError(ClobClientError):
    """Raised when a requested resource does not exist (e.g., 404)."""


class ClobClientTransientError(ClobClientError):
    """Raised for retryable/transient errors (e.g., 429/5xx, network issues)."""


class ClobClient:
    """Wrapper around py-clob-client with rate limiting and retry logic.

    This client provides a clean interface for querying Polymarket CLOB data
    with built-in rate limiting (10 requests/second) and automatic retry
    with exponential backoff on transient errors.

    Example:
        >>> client = ClobClient()  # Uses POLYMARKET_API_KEY env var
        >>> markets = client.get_markets()
        >>> orderbook = client.get_orderbook("token_id_here")
    """

    def __init__(
        self,
        *,
        host: str = DEFAULT_HOST,
        data_api_host: str = DEFAULT_DATA_API_HOST,
        chain_id: int = 137,
        private_key: str | None = None,
        api_creds: ApiCreds | None = None,
        signature_type: int | None = None,
        funder: str | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        requests_per_second: float = MAX_REQUESTS_PER_SECOND,
    ) -> None:
        """Initialize the CLOB client.

        Args:
            host: CLOB API endpoint URL.
            data_api_host: Polymarket Data API endpoint URL.
            chain_id: Chain ID for signing (Polygon=137).
            private_key: Private key used for L2 authentication.
            api_creds: CLOB API credentials (key/secret/passphrase) for L2 endpoints.
            signature_type: Optional signature type override.
            funder: Optional funder override.
            max_retries: Maximum retry attempts for failed requests.
            requests_per_second: Rate limit for API requests.
        """
        self._host = host
        self._data_api_host = data_api_host.rstrip("/")
        self._chain_id = chain_id
        self._private_key = private_key
        self._api_creds = api_creds
        self._signature_type = signature_type
        self._funder = funder
        self._max_retries = max_retries
        self._rate_limiter = RateLimiter(requests_per_second)

        # Initialize the underlying client (can be Level 0/1/2).
        self._client = BaseClobClient(
            host,
            chain_id=chain_id,
            key=private_key,
            creds=api_creds,
            signature_type=signature_type,
            funder=funder,
        )

        logger.info(
            "Initialized ClobClient with host=%s data_api_host=%s rate_limit=%.1f req/s",
            host,
            self._data_api_host,
            requests_per_second,
        )

    @property
    def is_level2_configured(self) -> bool:
        return self._private_key is not None and self._api_creds is not None

    def _require_level2(self) -> None:
        if not self.is_level2_configured:
            raise ClobClientError("Level-2 auth is required for this endpoint")

    def _with_rate_limit(self, func: Callable[P, T]) -> Callable[P, T]:
        """Wrap a function with rate limiting."""

        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            self._rate_limiter.acquire_sync()
            return func(*args, **kwargs)

        return wrapper

    @with_retry()
    def _get_simplified_markets_page(self, cursor: str | None = None) -> dict[str, Any]:
        """Fetch a single simplified-markets page (with rate limiting + retries)."""
        self._rate_limiter.acquire_sync()
        if cursor:
            resp = self._client.get_simplified_markets(cursor)
        else:
            resp = self._client.get_simplified_markets()
        if not isinstance(resp, dict):
            raise ClobClientError("Unexpected simplified markets response shape")
        return resp

    @with_retry()
    def get_markets(self, active_only: bool = True) -> list[Market]:
        """Fetch all markets from the CLOB.

        Args:
            active_only: If True, only return active (non-closed) markets.

        Returns:
            List of Market objects.
        """
        markets = list(self.iter_markets(active_only=active_only))
        logger.debug("Fetched %d markets", len(markets))
        return markets

    def iter_markets(self, *, active_only: bool = True) -> Iterator[Market]:
        """Iterate markets from the CLOB (streaming).

        This is the production-friendly variant used by scan/backtest indexing.
        It yields markets incrementally so callers can persist and embed in
        bounded chunks without holding the full market universe in memory.
        """
        cursor: str | None = None
        while True:
            response = self._get_simplified_markets_page(cursor)

            data = response.get("data", [])
            for market_data in data:
                market = Market.from_dict(market_data)
                if active_only and (market.closed or not market.active):
                    continue
                yield market

            next_cursor = response.get("next_cursor")
            if not next_cursor or next_cursor == "LTE=":
                return
            cursor = next_cursor

    @with_retry()
    def get_market(self, condition_id: str) -> Market:
        """Fetch a specific market by its condition ID.

        Args:
            condition_id: The market's condition ID.

        Returns:
            Market object.

        Raises:
            ClobClientError: If the market is not found.
        """
        self._rate_limiter.acquire_sync()

        try:
            response = self._client.get_market(condition_id)
            return Market.from_dict(response)
        except Exception as e:
            raise ClobClientError(f"Failed to fetch market {condition_id}: {e}") from e

    @with_retry()
    def get_orderbook(self, token_id: str) -> Orderbook:
        """Fetch the orderbook for a specific token.

        Args:
            token_id: The token ID to fetch the orderbook for.

        Returns:
            Orderbook object with bids, asks, and spread information.
        """
        self._rate_limiter.acquire_sync()

        try:
            orderbook = self._client.get_order_book(token_id)
            return Orderbook.from_clob_orderbook(orderbook)
        except Exception as e:
            raise ClobClientError(f"Failed to fetch orderbook for {token_id}: {e}") from e

    @with_retry()
    def get_order(self, order_id: str) -> dict[str, Any]:
        """Fetch an order by its hash (requires L2 auth)."""
        self._require_level2()
        self._rate_limiter.acquire_sync()
        try:
            return self._client.get_order(order_id)
        except Exception as e:
            raise ClobClientError(f"Failed to fetch order {order_id}: {e}") from e

    @with_retry()
    def get_trades(self, params: TradeParams | None = None) -> list[dict[str, Any]]:
        """Fetch trades via the Level-2 trades endpoint (requires L2 auth)."""
        self._require_level2()
        self._rate_limiter.acquire_sync()
        try:
            return self._client.get_trades(params=params)
        except Exception as e:
            raise ClobClientError(f"Failed to fetch trades: {e}") from e

    def _normalize_trade_events(self, *, resp: Any, source: str) -> list[TradeEvent]:
        items: Any = resp
        if isinstance(resp, dict) and isinstance(resp.get("data"), list):
            items = resp["data"]
        if not isinstance(items, list):
            raise ClobClientError(f"Unexpected {source} trade response shape")

        out: list[TradeEvent] = []
        skipped = 0
        for raw in items:
            if not isinstance(raw, dict):
                skipped += 1
                continue
            trade = TradeEvent.from_websocket_message(raw)
            if not (trade.trade_id and trade.market_id and trade.wallet_address):
                skipped += 1
                continue
            out.append(trade)
        if skipped:
            logger.debug("Skipped %d malformed trades from %s", skipped, source)
        if items and not out:
            raise ClobClientError(f"{source} returned trades but none had required identifiers")
        return out

    @with_retry(retry_on=(ClobClientTransientError,))
    def _get_data_api_trades_page(
        self,
        *,
        condition_id: str,
        limit: int,
        next_cursor: str | None,
    ) -> dict[str, Any] | list[Any]:
        """Fetch one page of market trades from the Polymarket Data API."""
        self._rate_limiter.acquire_sync()
        params: dict[str, str] = {
            "market": condition_id,
            "limit": str(limit),
        }
        if next_cursor:
            params["next_cursor"] = next_cursor
        url = f"{self._data_api_host}/trades?{urlencode(params)}"
        try:
            resp = http_get(url)
        except PolyApiException as e:
            status = getattr(e, "status_code", None)
            if status == 404:
                raise ClobClientNotFoundError(
                    f"Data API trades endpoint returned 404 for {condition_id}"
                ) from e
            if status in RETRY_STATUS_CODES or status is None:
                raise ClobClientTransientError(
                    f"Failed to fetch data-api trades for {condition_id}: {e}"
                ) from e
            raise ClobClientError(f"Failed to fetch data-api trades for {condition_id}: {e}") from e
        except Exception as e:
            raise ClobClientTransientError(
                f"Failed to fetch data-api trades for {condition_id}: {e}"
            ) from e
        if not isinstance(resp, (dict, list)):
            raise ClobClientError("Unexpected data-api trades response shape")
        return resp

    def _extract_page(
        self,
        *,
        resp: dict[str, Any] | list[Any],
        source: str,
    ) -> tuple[list[Any], str | None]:
        """Extract page items and next_cursor from a paginated response."""
        if isinstance(resp, list):
            return resp, None
        items_any = resp.get("data")
        if not isinstance(items_any, list):
            if isinstance(resp.get("trades"), list):
                items_any = resp["trades"]
            elif isinstance(resp.get("results"), list):
                items_any = resp["results"]
            else:
                raise ClobClientError(f"Unexpected {source} page shape")
        next_cursor = resp.get("next_cursor") or resp.get("nextCursor") or resp.get("cursor")
        if next_cursor is not None:
            next_cursor = str(next_cursor)
        return items_any, next_cursor

    def _get_market_trades_data_api(
        self,
        *,
        condition_id: str,
        max_trades: int,
    ) -> list[TradeEvent]:
        """Fetch market-level historical trades from Data API with bounded pagination."""
        if max_trades < 1:
            raise ValueError("max_trades must be >= 1")
        next_cursor: str | None = None
        out: list[TradeEvent] = []
        page_size = min(max_trades, 500)

        while len(out) < max_trades:
            resp = self._get_data_api_trades_page(
                condition_id=condition_id,
                limit=page_size,
                next_cursor=next_cursor,
            )
            page_items, page_cursor = self._extract_page(resp=resp, source="data-api/trades")
            page_trades = self._normalize_trade_events(resp=page_items, source="data-api/trades")
            if page_trades:
                out.extend(page_trades)
            if not page_items:
                break
            if not page_cursor or page_cursor == "LTE=" or page_cursor == next_cursor:
                break
            next_cursor = page_cursor

        if not out:
            raise ClobClientNotFoundError(f"No trades available from Data API for {condition_id}")
        return out[:max_trades]

    def _get_market_trades_events(self, condition_id: str) -> list[TradeEvent]:
        """Fetch historical trades from public market events endpoint."""
        self._rate_limiter.acquire_sync()
        try:
            resp = self._client.get_market_trades_events(condition_id)
        except PolyApiException as e:
            status = getattr(e, "status_code", None)
            if status == 404:
                raise ClobClientNotFoundError(
                    f"Market trades endpoint returned 404 for {condition_id}"
                ) from e
            if status in RETRY_STATUS_CODES:
                raise ClobClientTransientError(
                    f"Failed to fetch market trades for {condition_id}: {e}"
                ) from e
            raise ClobClientError(f"Failed to fetch market trades for {condition_id}: {e}") from e
        except Exception as e:
            raise ClobClientTransientError(f"Failed to fetch market trades for {condition_id}: {e}") from e
        return self._normalize_trade_events(resp=resp, source="live-activity/events")

    @with_retry(retry_on=(ClobClientTransientError,))
    def get_market_trades(self, condition_id: str, *, max_trades: int = 50_000) -> list[TradeEvent]:
        """Fetch historical market trades via deterministic source selection.

        Strategy:
        1) Use Data API `/trades?market=...` for market-level historical trades.
        2) Fall back to public `/live-activity/events/{condition_id}` when needed.
        """
        attempted: list[str] = []
        terminal_error: ClobClientError | None = None

        try:
            data_api_trades = self._get_market_trades_data_api(
                condition_id=condition_id,
                max_trades=max_trades,
            )
            if data_api_trades:
                return data_api_trades
            attempted.append("data-api/trades:empty")
        except ClobClientNotFoundError:
            attempted.append("data-api/trades:404_or_empty")
        except ClobClientError as e:
            attempted.append("data-api/trades:error")
            terminal_error = e

        try:
            events_trades = self._get_market_trades_events(condition_id)
            if events_trades:
                return events_trades
            attempted.append("live-activity/events:empty")
        except ClobClientNotFoundError:
            attempted.append("live-activity/events:404")
        except ClobClientError as e:
            attempted.append("live-activity/events:error")
            if terminal_error is None:
                terminal_error = e

        if terminal_error is not None:
            raise terminal_error
        details = ", ".join(attempted) if attempted else "no sources attempted"
        raise ClobClientNotFoundError(
            f"No trades available for {condition_id} (attempts: {details})"
        )

    @with_retry()
    def get_orderbooks(self, token_ids: list[str]) -> list[Orderbook]:
        """Fetch orderbooks for multiple tokens in a single request.

        Args:
            token_ids: List of token IDs to fetch orderbooks for.

        Returns:
            List of Orderbook objects.
        """
        self._rate_limiter.acquire_sync()

        params = [BookParams(token_id=tid) for tid in token_ids]

        try:
            orderbooks = self._client.get_order_books(params)
            return [Orderbook.from_clob_orderbook(ob) for ob in orderbooks]
        except Exception as e:
            raise ClobClientError(f"Failed to fetch orderbooks: {e}") from e

    @with_retry()
    def get_midpoint(self, token_id: str) -> str | None:
        """Fetch the midpoint price for a token.

        Args:
            token_id: The token ID.

        Returns:
            Midpoint price as a string, or None if unavailable.
        """
        self._rate_limiter.acquire_sync()

        try:
            response = self._client.get_midpoint(token_id)
            mid = response.get("mid")
            return str(mid) if mid is not None else None
        except Exception as e:
            logger.warning("Failed to get midpoint for %s: %s", token_id, e)
            return None

    @with_retry()
    def get_price(self, token_id: str, side: str = "BUY") -> str | None:
        """Fetch the best price for a token on a given side.

        Args:
            token_id: The token ID.
            side: Either "BUY" or "SELL".

        Returns:
            Best price as a string, or None if unavailable.
        """
        self._rate_limiter.acquire_sync()

        try:
            response = self._client.get_price(token_id, side=side)
            price = response.get("price")
            return str(price) if price is not None else None
        except Exception as e:
            logger.warning("Failed to get %s price for %s: %s", side, token_id, e)
            return None

    def health_check(self) -> bool:
        """Check if the CLOB API is reachable.

        Returns:
            True if the API responds with "OK", False otherwise.
        """
        try:
            self._rate_limiter.acquire_sync()
            result = self._client.get_ok()
            return str(result) == "OK"
        except Exception as e:
            logger.error("Health check failed: %s", e)
            return False

    def get_server_time(self) -> int | None:
        """Get the server timestamp.

        Returns:
            Server timestamp in milliseconds, or None on error.
        """
        try:
            self._rate_limiter.acquire_sync()
            result = self._client.get_server_time()
            return int(result) if result is not None else None
        except Exception as e:
            logger.error("Failed to get server time: %s", e)
            return None
