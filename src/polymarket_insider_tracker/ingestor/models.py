"""Data models for the ingestor module."""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any


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
            try:
                end_date = datetime.fromisoformat(end_date_iso.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                pass

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
