"""Data models for the ingestor module."""

import contextlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal


@dataclass(frozen=True)
class Token:
    """Represents a token in a Polymarket market."""

    token_id: str
    outcome: str
    price: Decimal | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Token":
        """Create a Token from a dictionary."""
        price = data.get("price")
        return cls(
            token_id=str(data["token_id"]),
            outcome=str(data["outcome"]),
            price=Decimal(str(price)) if price is not None else None,
        )


@dataclass(frozen=True)
class Market:
    """Represents a Polymarket prediction market."""

    condition_id: str
    question: str
    description: str
    tokens: tuple[Token, ...]
    end_date: datetime | None = None
    active: bool = True
    closed: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Market":
        """Create a Market from a dictionary response."""
        tokens_data = data.get("tokens", [])
        tokens = tuple(Token.from_dict(t) for t in tokens_data)

        end_date = None
        end_date_iso = data.get("end_date_iso")
        if end_date_iso:
            with contextlib.suppress(ValueError, AttributeError):
                end_date = datetime.fromisoformat(end_date_iso.replace("Z", "+00:00"))

        return cls(
            condition_id=str(data["condition_id"]),
            question=str(data.get("question", "")),
            description=str(data.get("description", "")),
            tokens=tokens,
            end_date=end_date,
            active=bool(data.get("active", True)),
            closed=bool(data.get("closed", False)),
        )


@dataclass(frozen=True)
class OrderbookLevel:
    """Represents a single price level in an orderbook."""

    price: Decimal
    size: Decimal

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "OrderbookLevel":
        """Create an OrderbookLevel from a dictionary."""
        return cls(
            price=Decimal(str(data["price"])),
            size=Decimal(str(data["size"])),
        )


@dataclass(frozen=True)
class Orderbook:
    """Represents an orderbook for a Polymarket token."""

    market: str
    asset_id: str
    bids: tuple[OrderbookLevel, ...]
    asks: tuple[OrderbookLevel, ...]
    tick_size: Decimal
    timestamp: datetime = field(default_factory=datetime.utcnow)

    @classmethod
    def from_clob_orderbook(cls, orderbook: Any) -> "Orderbook":
        """Create an Orderbook from a py-clob-client orderbook object."""
        bids = tuple(
            OrderbookLevel(
                price=Decimal(str(bid.price)),
                size=Decimal(str(bid.size)),
            )
            for bid in (orderbook.bids or [])
        )
        asks = tuple(
            OrderbookLevel(
                price=Decimal(str(ask.price)),
                size=Decimal(str(ask.size)),
            )
            for ask in (orderbook.asks or [])
        )

        return cls(
            market=str(orderbook.market),
            asset_id=str(orderbook.asset_id),
            bids=bids,
            asks=asks,
            tick_size=Decimal(str(orderbook.tick_size)),
        )

    @property
    def best_bid(self) -> Decimal | None:
        """Return the best bid price, or None if no bids."""
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        """Return the best ask price, or None if no asks."""
        return self.asks[0].price if self.asks else None

    @property
    def spread(self) -> Decimal | None:
        """Return the bid-ask spread, or None if missing data."""
        if self.best_bid is not None and self.best_ask is not None:
            return self.best_ask - self.best_bid
        return None

    @property
    def midpoint(self) -> Decimal | None:
        """Return the midpoint price, or None if missing data."""
        if self.best_bid is not None and self.best_ask is not None:
            return (self.best_bid + self.best_ask) / 2
        return None


@dataclass(frozen=True)
class TradeEvent:
    """Represents a trade event from the Polymarket WebSocket feed.

    This captures all the information about a single trade execution,
    including the market, wallet, trade details, and metadata.
    """

    # Core trade identifiers
    market_id: str  # conditionId - the market/CTF condition ID
    trade_id: str  # transactionHash - unique trade identifier
    wallet_address: str  # proxyWallet - trader's wallet address

    # Trade details
    side: Literal["BUY", "SELL"]
    outcome: str  # Human-readable outcome (e.g., "Yes", "No")
    outcome_index: int  # Index of the outcome (0 or 1)
    price: Decimal
    size: Decimal  # Number of shares traded
    timestamp: datetime

    # Asset information
    asset_id: str  # ERC1155 token ID

    # Market metadata
    market_slug: str = ""
    event_slug: str = ""
    event_title: str = ""

    # Trader metadata (optional - may not be available for all trades)
    trader_name: str = ""
    trader_pseudonym: str = ""

    @classmethod
    def from_websocket_message(cls, data: dict[str, Any]) -> "TradeEvent":
        """Create a TradeEvent from a WebSocket activity/trade message.

        Args:
            data: The payload from a WebSocket trade message.

        Returns:
            TradeEvent instance.
        """
        # Parse timestamp from either RTDS/CLOB event payloads or L2 trade-history payloads.
        raw_timestamp = data.get("timestamp", data.get("time", 0))
        timestamp = datetime.now(UTC)
        if isinstance(raw_timestamp, (int, float)):
            ts_f = float(raw_timestamp)
            if ts_f > 1e12:
                ts_f /= 1000.0
            timestamp = datetime.fromtimestamp(ts_f, tz=UTC)
        elif isinstance(raw_timestamp, str):
            with contextlib.suppress(ValueError):
                parsed_ts = datetime.fromisoformat(raw_timestamp.replace("Z", "+00:00"))
                if parsed_ts.tzinfo is None:
                    parsed_ts = parsed_ts.replace(tzinfo=UTC)
                timestamp = parsed_ts.astimezone(UTC)

        # Parse side - normalize to uppercase
        side_raw = str(data.get("side", "BUY")).upper()
        side: Literal["BUY", "SELL"] = "BUY" if side_raw == "BUY" else "SELL"

        market_id = str(
            data.get("conditionId")
            or data.get("condition_id")
            or data.get("market")
            or data.get("market_id")
            or ""
        )
        trade_id = str(
            data.get("transactionHash")
            or data.get("transaction_hash")
            or data.get("id")
            or ""
        )
        wallet_address = str(
            data.get("proxyWallet")
            or data.get("proxy_wallet")
            or data.get("maker_address")
            or data.get("makerAddress")
            or data.get("taker_address")
            or data.get("takerAddress")
            or ""
        )
        asset_id = str(data.get("asset") or data.get("asset_id") or data.get("assetId") or "")
        outcome_index_raw = data.get("outcomeIndex", data.get("outcome_index", 0))

        return cls(
            market_id=market_id,
            trade_id=trade_id,
            wallet_address=wallet_address,
            side=side,
            outcome=str(data.get("outcome", "")),
            outcome_index=int(outcome_index_raw),
            price=Decimal(str(data.get("price", data.get("avg_price", 0)))),
            size=Decimal(str(data.get("size", data.get("amount", 0)))),
            timestamp=timestamp,
            asset_id=asset_id,
            market_slug=str(data.get("slug", "")),
            event_slug=str(data.get("eventSlug", "")),
            event_title=str(data.get("title", "")),
            trader_name=str(data.get("name", "")),
            trader_pseudonym=str(data.get("pseudonym", "")),
        )

    @property
    def is_buy(self) -> bool:
        """Return True if this is a buy trade."""
        return self.side == "BUY"

    @property
    def is_sell(self) -> bool:
        """Return True if this is a sell trade."""
        return self.side == "SELL"


@dataclass(frozen=True)
class ClobPriceChange:
    """A single order-driven book delta from the CLOB market channel."""

    order_hash: str
    price: Decimal
    size: Decimal
    side: Literal["BUY", "SELL"]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ClobPriceChange":
        side_raw = str(data.get("side", "BUY")).upper()
        side: Literal["BUY", "SELL"] = "BUY" if side_raw == "BUY" else "SELL"
        return cls(
            order_hash=str(data.get("hash", "")),
            price=Decimal(str(data.get("price", "0"))),
            size=Decimal(str(data.get("size", "0"))),
            side=side,
        )


@dataclass(frozen=True)
class ClobPriceChangeEvent:
    """A price_change event from the CLOB market channel."""

    asset_id: str
    market: str
    timestamp: datetime
    price_changes: tuple[ClobPriceChange, ...]
    best_bid: Decimal | None
    best_ask: Decimal | None

    @classmethod
    def from_websocket_message(cls, data: dict[str, Any]) -> "ClobPriceChangeEvent":
        raw_ts = data.get("timestamp")
        if isinstance(raw_ts, (int, float)):
            timestamp = datetime.fromtimestamp(float(raw_ts) / 1000.0, tz=UTC)
        else:
            timestamp = datetime.now(UTC)

        changes = tuple(
            ClobPriceChange.from_dict(x) for x in (data.get("price_changes") or []) if isinstance(x, dict)
        )
        best_bid = data.get("best_bid")
        best_ask = data.get("best_ask")
        return cls(
            asset_id=str(data.get("asset_id", "")),
            market=str(data.get("market", "")),
            timestamp=timestamp,
            price_changes=changes,
            best_bid=Decimal(str(best_bid)) if best_bid is not None else None,
            best_ask=Decimal(str(best_ask)) if best_ask is not None else None,
        )


@dataclass(frozen=True)
class ClobBookEvent:
    """A book snapshot event from the CLOB market channel."""

    asset_id: str
    market: str
    timestamp: datetime
    bids: tuple[OrderbookLevel, ...]
    asks: tuple[OrderbookLevel, ...]

    @classmethod
    def from_websocket_message(cls, data: dict[str, Any]) -> "ClobBookEvent":
        raw_ts = data.get("timestamp")
        if isinstance(raw_ts, (int, float)):
            timestamp = datetime.fromtimestamp(float(raw_ts) / 1000.0, tz=UTC)
        else:
            timestamp = datetime.now(UTC)
        bids = tuple(
            OrderbookLevel.from_dict(x) for x in (data.get("bids") or []) if isinstance(x, dict)
        )
        asks = tuple(
            OrderbookLevel.from_dict(x) for x in (data.get("asks") or []) if isinstance(x, dict)
        )
        return cls(
            asset_id=str(data.get("asset_id", "")),
            market=str(data.get("market", "")),
            timestamp=timestamp,
            bids=bids,
            asks=asks,
        )

    @property
    def notional_value(self) -> Decimal:
        """Return the notional value of the trade (price * size)."""
        return self.price * self.size


@dataclass(frozen=True)
class MarketMetadata:
    """Extended market metadata with derived fields and caching support.

    This combines core Market data with cached liquidity fields and is designed
    for efficient caching in Redis.
    """

    # Core market data
    condition_id: str
    question: str
    description: str
    tokens: tuple[Token, ...]
    end_date: datetime | None = None
    active: bool = True
    closed: bool = False

    # Liquidity metrics (optional; updated by liquidity service)
    rolling_24h_volume_usdc: Decimal | None = None
    visible_book_depth_usdc: Decimal | None = None
    mid_price: Decimal | None = None
    last_liquidity_update_at: datetime | None = None

    # Cache metadata
    last_updated: datetime = field(default_factory=lambda: datetime.now(UTC))

    @classmethod
    def from_market(cls, market: Market) -> "MarketMetadata":
        """Create MarketMetadata from a Market object.

        Args:
            market: The source Market object.

        Returns:
            MarketMetadata with derived fields populated.
        """
        return cls(
            condition_id=market.condition_id,
            question=market.question,
            description=market.description,
            tokens=market.tokens,
            end_date=market.end_date,
            active=market.active,
            closed=market.closed,
            last_updated=datetime.now(UTC),
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dictionary for Redis storage.

        Returns:
            Dictionary representation suitable for JSON serialization.
        """
        return {
            "condition_id": self.condition_id,
            "question": self.question,
            "description": self.description,
            "tokens": [
                {
                    "token_id": t.token_id,
                    "outcome": t.outcome,
                    "price": str(t.price) if t.price is not None else None,
                }
                for t in self.tokens
            ],
            "end_date": self.end_date.isoformat() if self.end_date else None,
            "active": self.active,
            "closed": self.closed,
            "rolling_24h_volume_usdc": str(self.rolling_24h_volume_usdc)
            if self.rolling_24h_volume_usdc is not None
            else None,
            "visible_book_depth_usdc": str(self.visible_book_depth_usdc)
            if self.visible_book_depth_usdc is not None
            else None,
            "mid_price": str(self.mid_price) if self.mid_price is not None else None,
            "last_liquidity_update_at": self.last_liquidity_update_at.isoformat()
            if self.last_liquidity_update_at is not None
            else None,
            "last_updated": self.last_updated.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MarketMetadata":
        """Deserialize from a dictionary (from Redis storage).

        Args:
            data: Dictionary from Redis.

        Returns:
            MarketMetadata instance.
        """
        tokens_data = data.get("tokens", [])
        tokens = tuple(Token.from_dict(t) for t in tokens_data)

        end_date = None
        end_date_str = data.get("end_date")
        if end_date_str:
            with contextlib.suppress(ValueError, AttributeError):
                end_date = datetime.fromisoformat(end_date_str)

        last_updated_str = data.get("last_updated")
        if last_updated_str:
            try:
                last_updated = datetime.fromisoformat(last_updated_str)
            except (ValueError, AttributeError):
                last_updated = datetime.now(UTC)
        else:
            last_updated = datetime.now(UTC)

        return cls(
            condition_id=str(data["condition_id"]),
            question=str(data.get("question", "")),
            description=str(data.get("description", "")),
            tokens=tokens,
            end_date=end_date,
            active=bool(data.get("active", True)),
            closed=bool(data.get("closed", False)),
            rolling_24h_volume_usdc=Decimal(str(v))
            if (v := data.get("rolling_24h_volume_usdc")) is not None
            else None,
            visible_book_depth_usdc=Decimal(str(v))
            if (v := data.get("visible_book_depth_usdc")) is not None
            else None,
            mid_price=Decimal(str(v)) if (v := data.get("mid_price")) is not None else None,
            last_liquidity_update_at=datetime.fromisoformat(v)
            if (v := data.get("last_liquidity_update_at")) is not None
            else None,
            last_updated=last_updated,
        )
