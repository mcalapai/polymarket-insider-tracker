"""Repository pattern implementations for data access.

This module provides clean data access abstractions for wallet profiles,
funding transfers, and wallet relationships.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from polymarket_insider_tracker.storage.models import (
    BacktestRunModel,
    CancelModel,
    ERC20TransferModel,
    MarketEntryModel,
    MarketDailyBaselineModel,
    MarketModel,
    MarketPriceBarModel,
    MarketStateModel,
    LiquiditySnapshotModel,
    ModelArtifactModel,
    OrderEventModel,
    OrderModel,
    SniperClusterMemberModel,
    SniperClusterModel,
    TradeModel,
    TradeFeatureModel,
    TradeLabelModel,
    TradeSignalModel,
    TradeProcessingErrorModel,
    WalletSnapshotModel,
    WalletRelationshipModel,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


@dataclass
class WalletSnapshotDTO:
    """Data transfer object for wallet snapshots."""

    address: str
    as_of_block_number: int
    as_of: datetime
    nonce_as_of: int
    first_funding_at: datetime
    age_hours_as_of: Decimal
    matic_balance_wei_as_of: Decimal
    usdc_balance_units_as_of: Decimal
    computed_at: datetime
    created_at: datetime | None = None

    @classmethod
    def from_model(cls, model: WalletSnapshotModel) -> WalletSnapshotDTO:
        return cls(
            address=model.address,
            as_of_block_number=model.as_of_block_number,
            as_of=model.as_of,
            nonce_as_of=model.nonce_as_of,
            first_funding_at=model.first_funding_at,
            age_hours_as_of=model.age_hours_as_of,
            matic_balance_wei_as_of=model.matic_balance_wei_as_of,
            usdc_balance_units_as_of=model.usdc_balance_units_as_of,
            computed_at=model.computed_at,
            created_at=model.created_at,
        )


@dataclass
class ERC20TransferDTO:
    """Data transfer object for indexed ERC20 Transfer events."""

    token_address: str
    from_address: str
    to_address: str
    amount_units: Decimal
    tx_hash: str
    log_index: int
    block_number: int
    timestamp: datetime
    created_at: datetime | None = None

    @classmethod
    def from_model(cls, model: ERC20TransferModel) -> ERC20TransferDTO:
        return cls(
            token_address=model.token_address,
            from_address=model.from_address,
            to_address=model.to_address,
            amount_units=model.amount_units,
            tx_hash=model.tx_hash,
            log_index=model.log_index,
            block_number=model.block_number,
            timestamp=model.timestamp,
            created_at=model.created_at,
        )


@dataclass
class WalletRelationshipDTO:
    """Data transfer object for wallet relationships."""

    wallet_a: str
    wallet_b: str
    relationship_type: str
    confidence: Decimal
    created_at: datetime | None = None

    @classmethod
    def from_model(cls, model: WalletRelationshipModel) -> WalletRelationshipDTO:
        """Create DTO from SQLAlchemy model."""
        return cls(
            wallet_a=model.wallet_a,
            wallet_b=model.wallet_b,
            relationship_type=model.relationship_type,
            confidence=model.confidence,
            created_at=model.created_at,
        )


@dataclass
class TradeDTO:
    """Data transfer object for trades."""

    trade_id: str
    market_id: str
    asset_id: str
    wallet_address: str
    side: str
    outcome: str
    outcome_index: int
    price: Decimal
    size: Decimal
    notional_usdc: Decimal
    ts: datetime
    created_at: datetime | None = None

    @classmethod
    def from_model(cls, model: TradeModel) -> TradeDTO:
        return cls(
            trade_id=model.trade_id,
            market_id=model.market_id,
            asset_id=model.asset_id,
            wallet_address=model.wallet_address,
            side=model.side,
            outcome=model.outcome,
            outcome_index=model.outcome_index,
            price=model.price,
            size=model.size,
            notional_usdc=model.notional_usdc,
            ts=model.ts,
            created_at=model.created_at,
        )


class TradeRepository:
    """Repository for persisted trades."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_trade_id(self, trade_id: str) -> TradeDTO | None:
        result = await self.session.execute(select(TradeModel).where(TradeModel.trade_id == trade_id))
        model = result.scalar_one_or_none()
        return TradeDTO.from_model(model) if model else None

    async def insert(self, dto: TradeDTO) -> TradeDTO:
        model = TradeModel(
            trade_id=dto.trade_id,
            market_id=dto.market_id,
            asset_id=dto.asset_id,
            wallet_address=dto.wallet_address.lower(),
            side=dto.side,
            outcome=dto.outcome,
            outcome_index=dto.outcome_index,
            price=dto.price,
            size=dto.size,
            notional_usdc=dto.notional_usdc,
            ts=dto.ts,
        )
        self.session.add(model)
        await self.session.flush()
        return dto

    async def upsert(self, dto: TradeDTO) -> TradeDTO:
        """Upsert trade by trade_id (idempotent ingestion)."""
        values = {
            "trade_id": dto.trade_id,
            "market_id": dto.market_id,
            "asset_id": dto.asset_id,
            "wallet_address": dto.wallet_address.lower(),
            "side": dto.side,
            "outcome": dto.outcome,
            "outcome_index": dto.outcome_index,
            "price": dto.price,
            "size": dto.size,
            "notional_usdc": dto.notional_usdc,
            "ts": dto.ts,
        }
        now = datetime.now(UTC)
        try:
            stmt = pg_insert(TradeModel).values(**values, created_at=now)
            stmt = stmt.on_conflict_do_update(
                index_elements=["trade_id"],
                set_={
                    "market_id": stmt.excluded.market_id,
                    "asset_id": stmt.excluded.asset_id,
                    "wallet_address": stmt.excluded.wallet_address,
                    "side": stmt.excluded.side,
                    "outcome": stmt.excluded.outcome,
                    "outcome_index": stmt.excluded.outcome_index,
                    "price": stmt.excluded.price,
                    "size": stmt.excluded.size,
                    "notional_usdc": stmt.excluded.notional_usdc,
                    "ts": stmt.excluded.ts,
                },
            )
            await self.session.execute(stmt)
        except Exception:
            sqlite_stmt = sqlite_insert(TradeModel).values(**values, created_at=now)
            sqlite_stmt = sqlite_stmt.on_conflict_do_update(
                index_elements=["trade_id"],
                set_={
                    "market_id": sqlite_stmt.excluded.market_id,
                    "asset_id": sqlite_stmt.excluded.asset_id,
                    "wallet_address": sqlite_stmt.excluded.wallet_address,
                    "side": sqlite_stmt.excluded.side,
                    "outcome": sqlite_stmt.excluded.outcome,
                    "outcome_index": sqlite_stmt.excluded.outcome_index,
                    "price": sqlite_stmt.excluded.price,
                    "size": sqlite_stmt.excluded.size,
                    "notional_usdc": sqlite_stmt.excluded.notional_usdc,
                    "ts": sqlite_stmt.excluded.ts,
                },
            )
            await self.session.execute(sqlite_stmt)
        await self.session.flush()
        return dto


@dataclass
class OrderDTO:
    order_hash: str
    market_id: str
    asset_id: str
    wallet_address: str
    side: str
    price: Decimal
    size: Decimal
    size_matched: Decimal
    status: str
    created_ts: datetime
    indexed_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_model(cls, model: OrderModel) -> OrderDTO:
        return cls(
            order_hash=model.order_hash,
            market_id=model.market_id,
            asset_id=model.asset_id,
            wallet_address=model.wallet_address,
            side=model.side,
            price=model.price,
            size=model.size,
            size_matched=model.size_matched,
            status=model.status,
            created_ts=model.created_ts,
            indexed_at=model.indexed_at,
            updated_at=model.updated_at,
        )


class OrderRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert(self, dto: OrderDTO) -> None:
        now = datetime.now(UTC)
        values = {
            "order_hash": dto.order_hash,
            "market_id": dto.market_id,
            "asset_id": dto.asset_id,
            "wallet_address": dto.wallet_address.lower(),
            "side": dto.side,
            "price": dto.price,
            "size": dto.size,
            "size_matched": dto.size_matched,
            "status": dto.status,
            "created_ts": dto.created_ts,
        }
        try:
            stmt = pg_insert(OrderModel).values(**values, indexed_at=now, updated_at=now)
            stmt = stmt.on_conflict_do_update(
                index_elements=["order_hash"],
                set_={
                    "market_id": stmt.excluded.market_id,
                    "asset_id": stmt.excluded.asset_id,
                    "wallet_address": stmt.excluded.wallet_address,
                    "side": stmt.excluded.side,
                    "price": stmt.excluded.price,
                    "size": stmt.excluded.size,
                    "size_matched": stmt.excluded.size_matched,
                    "status": stmt.excluded.status,
                    "created_ts": stmt.excluded.created_ts,
                    "updated_at": now,
                },
            )
            await self.session.execute(stmt)
        except Exception:
            sqlite_stmt = sqlite_insert(OrderModel).values(**values, indexed_at=now, updated_at=now)
            sqlite_stmt = sqlite_stmt.on_conflict_do_update(
                index_elements=["order_hash"],
                set_={
                    "market_id": sqlite_stmt.excluded.market_id,
                    "asset_id": sqlite_stmt.excluded.asset_id,
                    "wallet_address": sqlite_stmt.excluded.wallet_address,
                    "side": sqlite_stmt.excluded.side,
                    "price": sqlite_stmt.excluded.price,
                    "size": sqlite_stmt.excluded.size,
                    "size_matched": sqlite_stmt.excluded.size_matched,
                    "status": sqlite_stmt.excluded.status,
                    "created_ts": sqlite_stmt.excluded.created_ts,
                    "updated_at": now,
                },
            )
            await self.session.execute(sqlite_stmt)


@dataclass
class OrderEventDTO:
    order_hash: str
    market_id: str
    asset_id: str
    wallet_address: str
    event_type: str
    ts: datetime
    best_bid: Decimal | None
    best_ask: Decimal | None
    created_at: datetime | None = None

    @classmethod
    def from_model(cls, model: OrderEventModel) -> OrderEventDTO:
        return cls(
            order_hash=model.order_hash,
            market_id=model.market_id,
            asset_id=model.asset_id,
            wallet_address=model.wallet_address,
            event_type=model.event_type,
            ts=model.ts,
            best_bid=model.best_bid,
            best_ask=model.best_ask,
            created_at=model.created_at,
        )


class OrderEventRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def insert(self, dto: OrderEventDTO) -> None:
        model = OrderEventModel(
            order_hash=dto.order_hash,
            market_id=dto.market_id,
            asset_id=dto.asset_id,
            wallet_address=dto.wallet_address.lower(),
            event_type=dto.event_type,
            ts=dto.ts,
            best_bid=dto.best_bid,
            best_ask=dto.best_ask,
        )
        self.session.add(model)
        await self.session.flush()


@dataclass
class CancelDTO:
    order_hash: str
    market_id: str
    asset_id: str
    wallet_address: str
    created_ts: datetime
    canceled_at: datetime
    cancel_latency_seconds: Decimal
    size: Decimal
    size_matched: Decimal
    moved_best: bool
    impact_bps: Decimal | None
    created_at: datetime | None = None

    @classmethod
    def from_model(cls, model: CancelModel) -> CancelDTO:
        return cls(
            order_hash=model.order_hash,
            market_id=model.market_id,
            asset_id=model.asset_id,
            wallet_address=model.wallet_address,
            created_ts=model.created_ts,
            canceled_at=model.canceled_at,
            cancel_latency_seconds=model.cancel_latency_seconds,
            size=model.size,
            size_matched=model.size_matched,
            moved_best=model.moved_best,
            impact_bps=model.impact_bps,
            created_at=model.created_at,
        )


class CancelRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert(self, dto: CancelDTO) -> None:
        now = datetime.now(UTC)
        values = {
            "order_hash": dto.order_hash,
            "market_id": dto.market_id,
            "asset_id": dto.asset_id,
            "wallet_address": dto.wallet_address.lower(),
            "created_ts": dto.created_ts,
            "canceled_at": dto.canceled_at,
            "cancel_latency_seconds": dto.cancel_latency_seconds,
            "size": dto.size,
            "size_matched": dto.size_matched,
            "moved_best": dto.moved_best,
            "impact_bps": dto.impact_bps,
        }
        try:
            stmt = pg_insert(CancelModel).values(**values, created_at=now)
            stmt = stmt.on_conflict_do_update(
                index_elements=["order_hash"],
                set_={
                    "market_id": stmt.excluded.market_id,
                    "asset_id": stmt.excluded.asset_id,
                    "wallet_address": stmt.excluded.wallet_address,
                    "created_ts": stmt.excluded.created_ts,
                    "canceled_at": stmt.excluded.canceled_at,
                    "cancel_latency_seconds": stmt.excluded.cancel_latency_seconds,
                    "size": stmt.excluded.size,
                    "size_matched": stmt.excluded.size_matched,
                    "moved_best": stmt.excluded.moved_best,
                    "impact_bps": stmt.excluded.impact_bps,
                },
            )
            await self.session.execute(stmt)
        except Exception:
            sqlite_stmt = sqlite_insert(CancelModel).values(**values, created_at=now)
            sqlite_stmt = sqlite_stmt.on_conflict_do_update(
                index_elements=["order_hash"],
                set_={
                    "market_id": sqlite_stmt.excluded.market_id,
                    "asset_id": sqlite_stmt.excluded.asset_id,
                    "wallet_address": sqlite_stmt.excluded.wallet_address,
                    "created_ts": sqlite_stmt.excluded.created_ts,
                    "canceled_at": sqlite_stmt.excluded.canceled_at,
                    "cancel_latency_seconds": sqlite_stmt.excluded.cancel_latency_seconds,
                    "size": sqlite_stmt.excluded.size,
                    "size_matched": sqlite_stmt.excluded.size_matched,
                    "moved_best": sqlite_stmt.excluded.moved_best,
                    "impact_bps": sqlite_stmt.excluded.impact_bps,
                },
            )
            await self.session.execute(sqlite_stmt)


@dataclass
class LiquiditySnapshotDTO:
    condition_id: str
    asset_id: str
    computed_at: datetime
    rolling_24h_volume_usdc: Decimal | None
    visible_book_depth_usdc: Decimal
    mid_price: Decimal
    created_at: datetime | None = None

    @classmethod
    def from_model(cls, model: LiquiditySnapshotModel) -> LiquiditySnapshotDTO:
        return cls(
            condition_id=model.condition_id,
            asset_id=model.asset_id,
            computed_at=model.computed_at,
            rolling_24h_volume_usdc=model.rolling_24h_volume_usdc,
            visible_book_depth_usdc=model.visible_book_depth_usdc,
            mid_price=model.mid_price,
            created_at=model.created_at,
        )


class LiquiditySnapshotRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert(self, dto: LiquiditySnapshotDTO) -> None:
        now = datetime.now(UTC)
        values = {
            "condition_id": dto.condition_id,
            "asset_id": dto.asset_id,
            "computed_at": dto.computed_at,
            "rolling_24h_volume_usdc": dto.rolling_24h_volume_usdc,
            "visible_book_depth_usdc": dto.visible_book_depth_usdc,
            "mid_price": dto.mid_price,
        }
        try:
            stmt = pg_insert(LiquiditySnapshotModel).values(**values, created_at=now)
            stmt = stmt.on_conflict_do_update(
                index_elements=["condition_id", "asset_id", "computed_at"],
                set_={
                    "rolling_24h_volume_usdc": stmt.excluded.rolling_24h_volume_usdc,
                    "visible_book_depth_usdc": stmt.excluded.visible_book_depth_usdc,
                    "mid_price": stmt.excluded.mid_price,
                },
            )
            await self.session.execute(stmt)
        except Exception:
            sqlite_stmt = sqlite_insert(LiquiditySnapshotModel).values(**values, created_at=now)
            sqlite_stmt = sqlite_stmt.on_conflict_do_update(
                index_elements=["condition_id", "asset_id", "computed_at"],
                set_={
                    "rolling_24h_volume_usdc": sqlite_stmt.excluded.rolling_24h_volume_usdc,
                    "visible_book_depth_usdc": sqlite_stmt.excluded.visible_book_depth_usdc,
                    "mid_price": sqlite_stmt.excluded.mid_price,
                },
            )
            await self.session.execute(sqlite_stmt)
        await self.session.flush()

    async def get_latest_before(
        self,
        *,
        condition_id: str,
        asset_id: str,
        as_of: datetime,
    ) -> LiquiditySnapshotDTO | None:
        if as_of.tzinfo is None:
            raise ValueError("as_of must be timezone-aware")
        result = await self.session.execute(
            select(LiquiditySnapshotModel)
            .where(
                (LiquiditySnapshotModel.condition_id == condition_id)
                & (LiquiditySnapshotModel.asset_id == asset_id)
                & (LiquiditySnapshotModel.computed_at <= as_of)
            )
            .order_by(LiquiditySnapshotModel.computed_at.desc())
            .limit(1)
        )
        model = result.scalar_one_or_none()
        return LiquiditySnapshotDTO.from_model(model) if model else None

@dataclass
class MarketPriceBarDTO:
    market_id: str
    bucket_start: datetime
    first_trade_ts: datetime
    last_trade_ts: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_model(cls, model: MarketPriceBarModel) -> MarketPriceBarDTO:
        return cls(
            market_id=model.market_id,
            bucket_start=model.bucket_start,
            first_trade_ts=model.first_trade_ts,
            last_trade_ts=model.last_trade_ts,
            open_price=model.open_price,
            high_price=model.high_price,
            low_price=model.low_price,
            close_price=model.close_price,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )


class MarketPriceBarRepository:
    """Repository for derived market price bars."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert_trade_price(self, *, market_id: str, ts: datetime, price: Decimal) -> MarketPriceBarDTO:
        if ts.tzinfo is None:
            raise ValueError("ts must be timezone-aware")
        bucket = ts.replace(second=0, microsecond=0)
        result = await self.session.execute(
            select(MarketPriceBarModel).where(
                (MarketPriceBarModel.market_id == market_id) & (MarketPriceBarModel.bucket_start == bucket)
            )
        )
        model = result.scalar_one_or_none()
        now = datetime.now(UTC)
        if model is None:
            model = MarketPriceBarModel(
                market_id=market_id,
                bucket_start=bucket,
                first_trade_ts=ts,
                last_trade_ts=ts,
                open_price=price,
                high_price=price,
                low_price=price,
                close_price=price,
                created_at=now,
                updated_at=now,
            )
            self.session.add(model)
            await self.session.flush()
            return MarketPriceBarDTO.from_model(model)

        if ts < model.first_trade_ts:
            model.first_trade_ts = ts
            model.open_price = price
        if ts > model.last_trade_ts:
            model.last_trade_ts = ts
            model.close_price = price

        if price > model.high_price:
            model.high_price = price
        if price < model.low_price:
            model.low_price = price

        model.updated_at = now
        await self.session.flush()
        return MarketPriceBarDTO.from_model(model)

    async def get_close_at_or_after(
        self,
        *,
        market_id: str,
        ts: datetime,
    ) -> tuple[datetime, Decimal] | None:
        if ts.tzinfo is None:
            raise ValueError("ts must be timezone-aware")
        bucket = ts.replace(second=0, microsecond=0)
        result = await self.session.execute(
            select(MarketPriceBarModel)
            .where((MarketPriceBarModel.market_id == market_id) & (MarketPriceBarModel.bucket_start >= bucket))
            .order_by(MarketPriceBarModel.bucket_start.asc())
            .limit(1)
        )
        model = result.scalar_one_or_none()
        if model is None:
            return None
        return (model.last_trade_ts, model.close_price)

    async def list_close_series(
        self,
        *,
        market_id: str,
        start: datetime,
        end: datetime,
    ) -> list[tuple[datetime, Decimal]]:
        if start.tzinfo is None or end.tzinfo is None:
            raise ValueError("start/end must be timezone-aware")
        start_bucket = start.replace(second=0, microsecond=0)
        end_bucket = end.replace(second=0, microsecond=0)
        result = await self.session.execute(
            select(MarketPriceBarModel.bucket_start, MarketPriceBarModel.close_price)
            .where(
                (MarketPriceBarModel.market_id == market_id)
                & (MarketPriceBarModel.bucket_start >= start_bucket)
                & (MarketPriceBarModel.bucket_start <= end_bucket)
            )
            .order_by(MarketPriceBarModel.bucket_start.asc())
        )
        return [(row[0], row[1]) for row in result.all()]


@dataclass
class MarketStateDTO:
    market_id: str
    first_trade_at: datetime
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_model(cls, model: MarketStateModel) -> MarketStateDTO:
        return cls(
            market_id=model.market_id,
            first_trade_at=model.first_trade_at,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )


class MarketStateRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, market_id: str) -> MarketStateDTO | None:
        result = await self.session.execute(
            select(MarketStateModel).where(MarketStateModel.market_id == market_id)
        )
        model = result.scalar_one_or_none()
        return MarketStateDTO.from_model(model) if model else None

    async def ensure_first_trade_at(self, market_id: str, *, ts: datetime) -> MarketStateDTO:
        existing = await self.get(market_id)
        now = datetime.now(UTC)
        if existing is None:
            model = MarketStateModel(market_id=market_id, first_trade_at=ts, created_at=now, updated_at=now)
            self.session.add(model)
            await self.session.flush()
            return MarketStateDTO.from_model(model)

        if ts < existing.first_trade_at:
            await self.session.execute(
                update(MarketStateModel)
                .where(MarketStateModel.market_id == market_id)
                .values(first_trade_at=ts, updated_at=now)
            )
            await self.session.flush()
            return MarketStateDTO(market_id=market_id, first_trade_at=ts, created_at=existing.created_at, updated_at=now)

        return existing


@dataclass
class MarketDTO:
    condition_id: str
    question: str
    description: str
    tokens_json: str
    active: bool
    closed: bool
    end_date: datetime | None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_model(cls, model: MarketModel) -> MarketDTO:
        return cls(
            condition_id=model.condition_id,
            question=model.question,
            description=model.description,
            tokens_json=model.tokens_json,
            active=model.active,
            closed=model.closed,
            end_date=model.end_date,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )


class MarketRepository:
    """Repository for semantic market retrieval (markets + embeddings)."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert(
        self,
        *,
        dto: MarketDTO,
        embedding: list[float],
        now: datetime | None = None,
    ) -> None:
        now = now or datetime.now(UTC)
        values = {
            "condition_id": dto.condition_id,
            "question": dto.question,
            "description": dto.description,
            "tokens_json": dto.tokens_json,
            "active": dto.active,
            "closed": dto.closed,
            "end_date": dto.end_date,
            "embedding": embedding,
        }
        try:
            stmt = pg_insert(MarketModel).values(**values, created_at=now, updated_at=now)
            stmt = stmt.on_conflict_do_update(
                index_elements=["condition_id"],
                set_={
                    "question": stmt.excluded.question,
                    "description": stmt.excluded.description,
                    "tokens_json": stmt.excluded.tokens_json,
                    "active": stmt.excluded.active,
                    "closed": stmt.excluded.closed,
                    "end_date": stmt.excluded.end_date,
                    "embedding": stmt.excluded.embedding,
                    "updated_at": now,
                },
            )
            await self.session.execute(stmt)
        except Exception:
            sqlite_stmt = sqlite_insert(MarketModel).values(**values, created_at=now, updated_at=now)
            sqlite_stmt = sqlite_stmt.on_conflict_do_update(
                index_elements=["condition_id"],
                set_={
                    "question": sqlite_stmt.excluded.question,
                    "description": sqlite_stmt.excluded.description,
                    "tokens_json": sqlite_stmt.excluded.tokens_json,
                    "active": sqlite_stmt.excluded.active,
                    "closed": sqlite_stmt.excluded.closed,
                    "end_date": sqlite_stmt.excluded.end_date,
                    "embedding": sqlite_stmt.excluded.embedding,
                    "updated_at": now,
                },
            )
            await self.session.execute(sqlite_stmt)
        await self.session.flush()

    async def search_by_embedding(
        self,
        *,
        query_embedding: list[float],
        top_k: int,
        active_only: bool = False,
    ) -> list[MarketDTO]:
        bind = self.session.get_bind()
        if bind.dialect.name != "postgresql":
            raise RuntimeError("Market vector search requires PostgreSQL + pgvector")
        # We pass the query vector as a pgvector literal string and cast in SQL.
        query_vec = "[" + ",".join(str(float(x)) for x in query_embedding) + "]"
        distance = sa.text("embedding <=> (:q)::vector")
        stmt = select(MarketModel).order_by(distance).params(q=query_vec).limit(top_k)
        if active_only:
            stmt = stmt.where((MarketModel.closed.is_(False)) & (MarketModel.active.is_(True)))
        result = await self.session.execute(stmt)
        return [MarketDTO.from_model(m) for m in result.scalars().all()]


@dataclass
class BacktestRunDTO:
    run_id: str
    command: str
    query: str
    started_at: datetime
    finished_at: datetime
    lookback_days: int
    top_k_markets: int
    markets_considered: int
    trades_considered: int
    flagged_trades: int
    hit_rate: Decimal | None
    output_path: str
    params_json: str
    created_at: datetime | None = None

    @classmethod
    def from_model(cls, model: BacktestRunModel) -> BacktestRunDTO:
        return cls(
            run_id=model.run_id,
            command=model.command,
            query=model.query,
            started_at=model.started_at,
            finished_at=model.finished_at,
            lookback_days=model.lookback_days,
            top_k_markets=model.top_k_markets,
            markets_considered=model.markets_considered,
            trades_considered=model.trades_considered,
            flagged_trades=model.flagged_trades,
            hit_rate=model.hit_rate,
            output_path=model.output_path,
            params_json=model.params_json,
            created_at=model.created_at,
        )


class BacktestRunRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def insert(self, dto: BacktestRunDTO) -> None:
        model = BacktestRunModel(
            run_id=dto.run_id,
            command=dto.command,
            query=dto.query,
            started_at=dto.started_at,
            finished_at=dto.finished_at,
            lookback_days=dto.lookback_days,
            top_k_markets=dto.top_k_markets,
            markets_considered=dto.markets_considered,
            trades_considered=dto.trades_considered,
            flagged_trades=dto.flagged_trades,
            hit_rate=dto.hit_rate,
            output_path=dto.output_path,
            params_json=dto.params_json,
        )
        self.session.add(model)
        await self.session.flush()

@dataclass
class MarketEntryDTO:
    market_id: str
    wallet_address: str
    ts: datetime
    notional_usdc: Decimal
    entry_rank: int
    entry_delta_seconds: Decimal
    created_at: datetime | None = None

    @classmethod
    def from_model(cls, model: MarketEntryModel) -> MarketEntryDTO:
        return cls(
            market_id=model.market_id,
            wallet_address=model.wallet_address,
            ts=model.ts,
            notional_usdc=model.notional_usdc,
            entry_rank=model.entry_rank,
            entry_delta_seconds=model.entry_delta_seconds,
            created_at=model.created_at,
        )


class MarketEntryRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_market_and_wallet(self, market_id: str, wallet_address: str) -> MarketEntryDTO | None:
        result = await self.session.execute(
            select(MarketEntryModel).where(
                (MarketEntryModel.market_id == market_id)
                & (MarketEntryModel.wallet_address == wallet_address.lower())
            )
        )
        model = result.scalar_one_or_none()
        return MarketEntryDTO.from_model(model) if model else None

    async def insert_first_entry(
        self,
        *,
        market_id: str,
        wallet_address: str,
        ts: datetime,
        notional_usdc: Decimal,
        first_trade_at: datetime,
    ) -> MarketEntryDTO | None:
        existing = await self.get_by_market_and_wallet(market_id, wallet_address)
        if existing is not None:
            return None

        # Rank is defined relative to first observed entries in this market.
        count_result = await self.session.execute(
            select(sa.func.count()).select_from(MarketEntryModel).where(MarketEntryModel.market_id == market_id)
        )
        rank = int(count_result.scalar_one() or 0) + 1

        delta_seconds = Decimal(str((ts - first_trade_at).total_seconds()))
        model = MarketEntryModel(
            market_id=market_id,
            wallet_address=wallet_address.lower(),
            ts=ts,
            notional_usdc=notional_usdc,
            entry_rank=rank,
            entry_delta_seconds=delta_seconds,
        )
        self.session.add(model)
        await self.session.flush()
        return MarketEntryDTO.from_model(model)

    async def list_since(self, *, since: datetime) -> list[MarketEntryDTO]:
        result = await self.session.execute(
            select(MarketEntryModel).where(MarketEntryModel.ts >= since).order_by(MarketEntryModel.ts.asc())
        )
        return [MarketEntryDTO.from_model(m) for m in result.scalars().all()]


@dataclass
class SniperClusterDTO:
    cluster_id: str
    cluster_size: int
    avg_entry_delta_seconds: Decimal
    markets_in_common: int
    confidence: Decimal
    window_start: datetime
    window_end: datetime
    computed_at: datetime
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_model(cls, model: SniperClusterModel) -> SniperClusterDTO:
        return cls(
            cluster_id=model.cluster_id,
            cluster_size=model.cluster_size,
            avg_entry_delta_seconds=model.avg_entry_delta_seconds,
            markets_in_common=model.markets_in_common,
            confidence=model.confidence,
            window_start=model.window_start,
            window_end=model.window_end,
            computed_at=model.computed_at,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )


class SniperClusterRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert_cluster(
        self,
        cluster: SniperClusterDTO,
        *,
        member_wallets: set[str],
    ) -> None:
        now = datetime.now(UTC)
        values = {
            "cluster_id": cluster.cluster_id,
            "cluster_size": cluster.cluster_size,
            "avg_entry_delta_seconds": cluster.avg_entry_delta_seconds,
            "markets_in_common": cluster.markets_in_common,
            "confidence": cluster.confidence,
            "window_start": cluster.window_start,
            "window_end": cluster.window_end,
            "computed_at": cluster.computed_at,
        }

        try:
            stmt = pg_insert(SniperClusterModel).values(**values, created_at=now, updated_at=now)
            stmt = stmt.on_conflict_do_update(
                index_elements=["cluster_id"],
                set_={
                    "cluster_size": stmt.excluded.cluster_size,
                    "avg_entry_delta_seconds": stmt.excluded.avg_entry_delta_seconds,
                    "markets_in_common": stmt.excluded.markets_in_common,
                    "confidence": stmt.excluded.confidence,
                    "window_start": stmt.excluded.window_start,
                    "window_end": stmt.excluded.window_end,
                    "computed_at": stmt.excluded.computed_at,
                    "updated_at": now,
                },
            )
            await self.session.execute(stmt)
        except Exception:
            sqlite_stmt = sqlite_insert(SniperClusterModel).values(**values, created_at=now, updated_at=now)
            sqlite_stmt = sqlite_stmt.on_conflict_do_update(
                index_elements=["cluster_id"],
                set_={
                    "cluster_size": sqlite_stmt.excluded.cluster_size,
                    "avg_entry_delta_seconds": sqlite_stmt.excluded.avg_entry_delta_seconds,
                    "markets_in_common": sqlite_stmt.excluded.markets_in_common,
                    "confidence": sqlite_stmt.excluded.confidence,
                    "window_start": sqlite_stmt.excluded.window_start,
                    "window_end": sqlite_stmt.excluded.window_end,
                    "computed_at": sqlite_stmt.excluded.computed_at,
                    "updated_at": now,
                },
            )
            await self.session.execute(sqlite_stmt)

        if member_wallets:
            normalized = {w.lower() for w in member_wallets}
            await self.session.execute(
                delete(SniperClusterMemberModel).where(
                    (SniperClusterMemberModel.cluster_id == cluster.cluster_id)
                    & (~SniperClusterMemberModel.wallet_address.in_(sorted(normalized)))
                )
            )

            # Insert missing members
            rows = [
                {"cluster_id": cluster.cluster_id, "wallet_address": wallet.lower(), "created_at": now}
                for wallet in sorted(normalized)
            ]
            try:
                stmt = pg_insert(SniperClusterMemberModel).values(rows)
                stmt = stmt.on_conflict_do_nothing(index_elements=["cluster_id", "wallet_address"])
                await self.session.execute(stmt)
            except Exception:
                stmt = sqlite_insert(SniperClusterMemberModel).values(rows)
                stmt = stmt.on_conflict_do_nothing(index_elements=["cluster_id", "wallet_address"])
                await self.session.execute(stmt)

        await self.session.flush()

    async def get_latest_cluster_for_wallet(self, wallet_address: str) -> SniperClusterDTO | None:
        wallet = wallet_address.lower()
        result = await self.session.execute(
            select(SniperClusterModel)
            .join(
                SniperClusterMemberModel,
                SniperClusterMemberModel.cluster_id == SniperClusterModel.cluster_id,
            )
            .where(SniperClusterMemberModel.wallet_address == wallet)
            .order_by(SniperClusterModel.computed_at.desc())
            .limit(1)
        )
        model = result.scalar_one_or_none()
        return SniperClusterDTO.from_model(model) if model else None

    async def prune_old_clusters(self, *, keep_since: datetime) -> int:
        # Delete old clusters and cascade members (manual for portability).
        old_ids_result = await self.session.execute(
            select(SniperClusterModel.cluster_id).where(SniperClusterModel.computed_at < keep_since)
        )
        old_ids = [row[0] for row in old_ids_result.all()]
        if not old_ids:
            return 0
        await self.session.execute(
            delete(SniperClusterMemberModel).where(SniperClusterMemberModel.cluster_id.in_(old_ids))
        )
        await self.session.execute(delete(SniperClusterModel).where(SniperClusterModel.cluster_id.in_(old_ids)))
        await self.session.flush()
        return len(old_ids)


@dataclass
class TradeSignalDTO:
    trade_id: str
    signal_type: str
    confidence: Decimal
    payload_json: str
    computed_at: datetime
    created_at: datetime | None = None

    @classmethod
    def from_model(cls, model: TradeSignalModel) -> TradeSignalDTO:
        return cls(
            trade_id=model.trade_id,
            signal_type=model.signal_type,
            confidence=model.confidence,
            payload_json=model.payload_json,
            computed_at=model.computed_at,
            created_at=model.created_at,
        )


class TradeSignalRepository:
    """Repository for persisted detector signals."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert(self, dto: TradeSignalDTO) -> TradeSignalDTO:
        now = datetime.now(UTC)
        values = {
            "trade_id": dto.trade_id,
            "signal_type": dto.signal_type,
            "confidence": dto.confidence,
            "payload_json": dto.payload_json,
            "computed_at": dto.computed_at,
        }
        index_cols = ["trade_id", "signal_type"]
        try:
            stmt = pg_insert(TradeSignalModel).values(**values, created_at=now)
            stmt = stmt.on_conflict_do_update(
                index_elements=index_cols,
                set_={
                    "confidence": stmt.excluded.confidence,
                    "payload_json": stmt.excluded.payload_json,
                    "computed_at": stmt.excluded.computed_at,
                },
            )
            await self.session.execute(stmt)
        except Exception:
            stmt = sqlite_insert(TradeSignalModel).values(**values, created_at=now)
            stmt = stmt.on_conflict_do_update(
                index_elements=index_cols,
                set_={
                    "confidence": stmt.excluded.confidence,
                    "payload_json": stmt.excluded.payload_json,
                    "computed_at": stmt.excluded.computed_at,
                },
            )
            await self.session.execute(stmt)

        await self.session.flush()
        return dto


@dataclass
class TradeFeatureDTO:
    trade_id: str
    computed_at: datetime
    trade_notional_usdc: Decimal
    features_json: str
    wallet_nonce_as_of: int | None = None
    wallet_age_hours_as_of: Decimal | None = None
    wallet_usdc_balance_units_as_of: Decimal | None = None
    wallet_matic_balance_wei_as_of: Decimal | None = None
    volume_impact: Decimal | None = None
    book_impact: Decimal | None = None
    fresh_wallet_confidence: Decimal | None = None
    size_anomaly_confidence: Decimal | None = None
    sniper_cluster_confidence: Decimal | None = None
    coentry_confidence: Decimal | None = None
    funding_confidence: Decimal | None = None
    trade_size_outlier_confidence: Decimal | None = None
    digit_distribution_confidence: Decimal | None = None
    trade_slicing_confidence: Decimal | None = None
    order_to_trade_ratio_confidence: Decimal | None = None
    rapid_cancel_confidence: Decimal | None = None
    book_impact_without_fill_confidence: Decimal | None = None
    model_score: Decimal | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_model(cls, model: TradeFeatureModel) -> TradeFeatureDTO:
        return cls(
            trade_id=model.trade_id,
            computed_at=model.computed_at,
            trade_notional_usdc=model.trade_notional_usdc,
            features_json=model.features_json,
            wallet_nonce_as_of=model.wallet_nonce_as_of,
            wallet_age_hours_as_of=model.wallet_age_hours_as_of,
            wallet_usdc_balance_units_as_of=model.wallet_usdc_balance_units_as_of,
            wallet_matic_balance_wei_as_of=model.wallet_matic_balance_wei_as_of,
            volume_impact=model.volume_impact,
            book_impact=model.book_impact,
            fresh_wallet_confidence=model.fresh_wallet_confidence,
            size_anomaly_confidence=model.size_anomaly_confidence,
            sniper_cluster_confidence=model.sniper_cluster_confidence,
            coentry_confidence=model.coentry_confidence,
            funding_confidence=model.funding_confidence,
            trade_size_outlier_confidence=model.trade_size_outlier_confidence,
            digit_distribution_confidence=model.digit_distribution_confidence,
            trade_slicing_confidence=model.trade_slicing_confidence,
            order_to_trade_ratio_confidence=model.order_to_trade_ratio_confidence,
            rapid_cancel_confidence=model.rapid_cancel_confidence,
            book_impact_without_fill_confidence=model.book_impact_without_fill_confidence,
            model_score=model.model_score,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )


class TradeFeatureRepository:
    """Repository for per-trade feature rows."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert(self, dto: TradeFeatureDTO) -> None:
        now = datetime.now(UTC)
        values = {
            "trade_id": dto.trade_id,
            "computed_at": dto.computed_at,
            "wallet_nonce_as_of": dto.wallet_nonce_as_of,
            "wallet_age_hours_as_of": dto.wallet_age_hours_as_of,
            "wallet_usdc_balance_units_as_of": dto.wallet_usdc_balance_units_as_of,
            "wallet_matic_balance_wei_as_of": dto.wallet_matic_balance_wei_as_of,
            "trade_notional_usdc": dto.trade_notional_usdc,
            "volume_impact": dto.volume_impact,
            "book_impact": dto.book_impact,
            "fresh_wallet_confidence": dto.fresh_wallet_confidence,
            "size_anomaly_confidence": dto.size_anomaly_confidence,
            "sniper_cluster_confidence": dto.sniper_cluster_confidence,
            "coentry_confidence": dto.coentry_confidence,
            "funding_confidence": dto.funding_confidence,
            "trade_size_outlier_confidence": dto.trade_size_outlier_confidence,
            "digit_distribution_confidence": dto.digit_distribution_confidence,
            "trade_slicing_confidence": dto.trade_slicing_confidence,
            "order_to_trade_ratio_confidence": dto.order_to_trade_ratio_confidence,
            "rapid_cancel_confidence": dto.rapid_cancel_confidence,
            "book_impact_without_fill_confidence": dto.book_impact_without_fill_confidence,
            "model_score": dto.model_score,
            "features_json": dto.features_json,
        }
        try:
            stmt = pg_insert(TradeFeatureModel).values(**values, created_at=now, updated_at=now)
            stmt = stmt.on_conflict_do_update(
                index_elements=["trade_id"],
                set_={**{k: getattr(stmt.excluded, k) for k in values.keys() if k != "trade_id"}, "updated_at": now},
            )
            await self.session.execute(stmt)
        except Exception:
            sqlite_stmt = sqlite_insert(TradeFeatureModel).values(**values, created_at=now, updated_at=now)
            sqlite_stmt = sqlite_stmt.on_conflict_do_update(
                index_elements=["trade_id"],
                set_={
                    **{k: getattr(sqlite_stmt.excluded, k) for k in values.keys() if k != "trade_id"},
                    "updated_at": now,
                },
            )
            await self.session.execute(sqlite_stmt)
        await self.session.flush()


@dataclass
class TradeLabelDTO:
    trade_id: str
    label_type: str
    horizon_seconds: int
    value: Decimal
    label: bool
    params_json: str
    computed_at: datetime
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_model(cls, model: TradeLabelModel) -> TradeLabelDTO:
        return cls(
            trade_id=model.trade_id,
            label_type=model.label_type,
            horizon_seconds=model.horizon_seconds,
            value=model.value,
            label=model.label,
            params_json=model.params_json,
            computed_at=model.computed_at,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )


class TradeLabelRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert(self, dto: TradeLabelDTO) -> None:
        now = datetime.now(UTC)
        values = {
            "trade_id": dto.trade_id,
            "label_type": dto.label_type,
            "horizon_seconds": dto.horizon_seconds,
            "value": dto.value,
            "label": dto.label,
            "params_json": dto.params_json,
            "computed_at": dto.computed_at,
        }
        index_cols = ["trade_id", "label_type", "horizon_seconds"]
        try:
            stmt = pg_insert(TradeLabelModel).values(**values, created_at=now, updated_at=now)
            stmt = stmt.on_conflict_do_update(
                index_elements=index_cols,
                set_={
                    "value": stmt.excluded.value,
                    "label": stmt.excluded.label,
                    "params_json": stmt.excluded.params_json,
                    "computed_at": stmt.excluded.computed_at,
                    "updated_at": now,
                },
            )
            await self.session.execute(stmt)
        except Exception:
            sqlite_stmt = sqlite_insert(TradeLabelModel).values(**values, created_at=now, updated_at=now)
            sqlite_stmt = sqlite_stmt.on_conflict_do_update(
                index_elements=index_cols,
                set_={
                    "value": sqlite_stmt.excluded.value,
                    "label": sqlite_stmt.excluded.label,
                    "params_json": sqlite_stmt.excluded.params_json,
                    "computed_at": sqlite_stmt.excluded.computed_at,
                    "updated_at": now,
                },
            )
            await self.session.execute(sqlite_stmt)
        await self.session.flush()


@dataclass
class ModelArtifactDTO:
    model_id: str
    algorithm: str
    label_type: str
    horizon_seconds: int
    z_threshold: Decimal
    artifact_path: str
    metrics_json: str
    schema_json: str
    blessed: bool
    trained_at: datetime
    created_at: datetime | None = None

    @classmethod
    def from_model(cls, model: ModelArtifactModel) -> ModelArtifactDTO:
        return cls(
            model_id=model.model_id,
            algorithm=model.algorithm,
            label_type=model.label_type,
            horizon_seconds=model.horizon_seconds,
            z_threshold=model.z_threshold,
            artifact_path=model.artifact_path,
            metrics_json=model.metrics_json,
            schema_json=model.schema_json,
            blessed=model.blessed,
            trained_at=model.trained_at,
            created_at=model.created_at,
        )


class ModelArtifactRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def insert(self, dto: ModelArtifactDTO) -> None:
        model = ModelArtifactModel(
            model_id=dto.model_id,
            algorithm=dto.algorithm,
            label_type=dto.label_type,
            horizon_seconds=dto.horizon_seconds,
            z_threshold=dto.z_threshold,
            artifact_path=dto.artifact_path,
            metrics_json=dto.metrics_json,
            schema_json=dto.schema_json,
            blessed=dto.blessed,
            trained_at=dto.trained_at,
        )
        self.session.add(model)
        await self.session.flush()

    async def get_latest_blessed(self) -> ModelArtifactDTO | None:
        result = await self.session.execute(
            select(ModelArtifactModel)
            .where(ModelArtifactModel.blessed.is_(True))
            .order_by(ModelArtifactModel.trained_at.desc())
            .limit(1)
        )
        model = result.scalar_one_or_none()
        return ModelArtifactDTO.from_model(model) if model else None

    async def unbless_all(self) -> None:
        await self.session.execute(update(ModelArtifactModel).values(blessed=False))
        await self.session.flush()


@dataclass
class TradeProcessingErrorDTO:
    trade_id: str
    stage: str
    error_type: str
    message: str
    created_at: datetime | None = None

    @classmethod
    def from_model(cls, model: TradeProcessingErrorModel) -> TradeProcessingErrorDTO:
        return cls(
            trade_id=model.trade_id,
            stage=model.stage,
            error_type=model.error_type,
            message=model.message,
            created_at=model.created_at,
        )


class TradeProcessingErrorRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def insert_many(self, errors: list[TradeProcessingErrorDTO]) -> None:
        if not errors:
            return
        rows = [
            {
                "trade_id": e.trade_id,
                "stage": e.stage,
                "error_type": e.error_type,
                "message": e.message,
                "created_at": e.created_at or datetime.now(UTC),
            }
            for e in errors
        ]
        await self.session.execute(sa.insert(TradeProcessingErrorModel), rows)
        await self.session.flush()

@dataclass
class MarketDailyBaselineDTO:
    market_id: str
    day: date
    baseline_type: str
    payload_json: str
    computed_at: datetime
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_model(cls, model: MarketDailyBaselineModel) -> MarketDailyBaselineDTO:
        return cls(
            market_id=model.market_id,
            day=model.day,
            baseline_type=model.baseline_type,
            payload_json=model.payload_json,
            computed_at=model.computed_at,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )


class MarketDailyBaselineRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert(self, dto: MarketDailyBaselineDTO) -> MarketDailyBaselineDTO:
        now = datetime.now(UTC)
        values = {
            "market_id": dto.market_id,
            "day": dto.day,
            "baseline_type": dto.baseline_type,
            "payload_json": dto.payload_json,
            "computed_at": dto.computed_at,
        }
        index_cols = ["market_id", "day", "baseline_type"]
        try:
            stmt = pg_insert(MarketDailyBaselineModel).values(**values, created_at=now, updated_at=now)
            stmt = stmt.on_conflict_do_update(
                index_elements=index_cols,
                set_={
                    "payload_json": stmt.excluded.payload_json,
                    "computed_at": stmt.excluded.computed_at,
                    "updated_at": now,
                },
            )
            await self.session.execute(stmt)
        except Exception:
            stmt = sqlite_insert(MarketDailyBaselineModel).values(**values, created_at=now, updated_at=now)
            stmt = stmt.on_conflict_do_update(
                index_elements=index_cols,
                set_={
                    "payload_json": stmt.excluded.payload_json,
                    "computed_at": stmt.excluded.computed_at,
                    "updated_at": now,
                },
            )
            await self.session.execute(stmt)

        await self.session.flush()
        return dto


class WalletSnapshotRepository:
    """Repository for wallet snapshot data access."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize repository with database session.

        Args:
            session: SQLAlchemy async session.
        """
        self.session = session

    async def get_by_address_and_block(
        self, address: str, *, as_of_block_number: int
    ) -> WalletSnapshotDTO | None:
        result = await self.session.execute(
            select(WalletSnapshotModel).where(
                (WalletSnapshotModel.address == address.lower())
                & (WalletSnapshotModel.as_of_block_number == as_of_block_number)
            )
        )
        model = result.scalar_one_or_none()
        return WalletSnapshotDTO.from_model(model) if model else None

    async def get_latest_by_address(self, address: str) -> WalletSnapshotDTO | None:
        result = await self.session.execute(
            select(WalletSnapshotModel)
            .where(WalletSnapshotModel.address == address.lower())
            .order_by(WalletSnapshotModel.as_of.desc())
            .limit(1)
        )
        model = result.scalar_one_or_none()
        return WalletSnapshotDTO.from_model(model) if model else None

    async def get_latest_many(self, addresses: list[str]) -> list[WalletSnapshotDTO]:
        normalized = [addr.lower() for addr in addresses]
        result = await self.session.execute(
            select(WalletSnapshotModel)
            .where(WalletSnapshotModel.address.in_(normalized))
            .order_by(WalletSnapshotModel.as_of.desc())
        )
        models = result.scalars().all()
        seen: set[str] = set()
        latest: list[WalletSnapshotDTO] = []
        for model in models:
            if model.address in seen:
                continue
            seen.add(model.address)
            latest.append(WalletSnapshotDTO.from_model(model))
        return latest

    async def upsert(self, dto: WalletSnapshotDTO) -> WalletSnapshotDTO:
        now = datetime.now(UTC)
        values = {
            "address": dto.address.lower(),
            "as_of_block_number": dto.as_of_block_number,
            "as_of": dto.as_of,
            "nonce_as_of": dto.nonce_as_of,
            "first_funding_at": dto.first_funding_at,
            "age_hours_as_of": dto.age_hours_as_of,
            "matic_balance_wei_as_of": dto.matic_balance_wei_as_of,
            "usdc_balance_units_as_of": dto.usdc_balance_units_as_of,
            "computed_at": dto.computed_at,
        }

        try:
            stmt = pg_insert(WalletSnapshotModel).values(**values, created_at=now)
            stmt = stmt.on_conflict_do_update(
                index_elements=["address", "as_of_block_number"],
                set_={
                    "as_of": stmt.excluded.as_of,
                    "nonce_as_of": stmt.excluded.nonce_as_of,
                    "first_funding_at": stmt.excluded.first_funding_at,
                    "age_hours_as_of": stmt.excluded.age_hours_as_of,
                    "matic_balance_wei_as_of": stmt.excluded.matic_balance_wei_as_of,
                    "usdc_balance_units_as_of": stmt.excluded.usdc_balance_units_as_of,
                    "computed_at": stmt.excluded.computed_at,
                },
            )
            await self.session.execute(stmt)
        except Exception:
            sqlite_stmt = sqlite_insert(WalletSnapshotModel).values(**values, created_at=now)
            sqlite_stmt = sqlite_stmt.on_conflict_do_update(
                index_elements=["address", "as_of_block_number"],
                set_={
                    "as_of": sqlite_stmt.excluded.as_of,
                    "nonce_as_of": sqlite_stmt.excluded.nonce_as_of,
                    "first_funding_at": sqlite_stmt.excluded.first_funding_at,
                    "age_hours_as_of": sqlite_stmt.excluded.age_hours_as_of,
                    "matic_balance_wei_as_of": sqlite_stmt.excluded.matic_balance_wei_as_of,
                    "usdc_balance_units_as_of": sqlite_stmt.excluded.usdc_balance_units_as_of,
                    "computed_at": sqlite_stmt.excluded.computed_at,
                },
            )
            await self.session.execute(sqlite_stmt)

        await self.session.flush()
        return dto


class ERC20TransferRepository:
    """Repository for indexed ERC20 transfers."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize repository with database session.

        Args:
            session: SQLAlchemy async session.
        """
        self.session = session

    async def get_transfers_to(self, address: str, limit: int = 100) -> list[ERC20TransferDTO]:
        """Get transfers to a wallet address.

        Args:
            address: Destination wallet address.
            limit: Maximum number of results.

        Returns:
            List of ERC20TransferDTOs ordered by timestamp.
        """
        result = await self.session.execute(
            select(ERC20TransferModel)
            .where(ERC20TransferModel.to_address == address.lower())
            .order_by(ERC20TransferModel.timestamp.asc())
            .limit(limit)
        )
        return [ERC20TransferDTO.from_model(m) for m in result.scalars().all()]

    async def get_transfers_from(self, address: str, limit: int = 100) -> list[ERC20TransferDTO]:
        """Get transfers from a wallet address.

        Args:
            address: Source wallet address.
            limit: Maximum number of results.

        Returns:
            List of ERC20TransferDTOs ordered by timestamp.
        """
        result = await self.session.execute(
            select(ERC20TransferModel)
            .where(ERC20TransferModel.from_address == address.lower())
            .order_by(ERC20TransferModel.timestamp.asc())
            .limit(limit)
        )
        return [ERC20TransferDTO.from_model(m) for m in result.scalars().all()]

    async def list_transfers_from_in_window(
        self,
        address: str,
        *,
        token_addresses: Sequence[str] | None = None,
        start: datetime,
        end: datetime,
        limit: int = 50_000,
    ) -> list[ERC20TransferDTO]:
        stmt = select(ERC20TransferModel).where(ERC20TransferModel.from_address == address.lower())
        if token_addresses is not None:
            normalized = [t.lower() for t in token_addresses]
            stmt = stmt.where(ERC20TransferModel.token_address.in_(normalized))
        stmt = stmt.where((ERC20TransferModel.timestamp >= start) & (ERC20TransferModel.timestamp <= end))
        stmt = stmt.order_by(ERC20TransferModel.timestamp.asc(), ERC20TransferModel.log_index.asc()).limit(limit)
        result = await self.session.execute(stmt)
        return [ERC20TransferDTO.from_model(m) for m in result.scalars().all()]

    async def get_first_transfer_to(
        self,
        address: str,
        *,
        token_addresses: Sequence[str] | None = None,
        since: datetime | None = None,
    ) -> ERC20TransferDTO | None:
        """Get the first transfer to a wallet (optionally filtered by token address list).

        Args:
            address: Wallet address.
            token_addresses: Optional token address allowlist.
            since: Optional lower-bound timestamp.

        Returns:
            First ERC20TransferDTO if found, None otherwise.
        """
        stmt = select(ERC20TransferModel).where(ERC20TransferModel.to_address == address.lower())
        if token_addresses is not None:
            normalized = [t.lower() for t in token_addresses]
            stmt = stmt.where(ERC20TransferModel.token_address.in_(normalized))
        if since is not None:
            stmt = stmt.where(ERC20TransferModel.timestamp >= since)
        stmt = stmt.order_by(ERC20TransferModel.timestamp.asc(), ERC20TransferModel.log_index.asc()).limit(1)
        result = await self.session.execute(stmt)
        model = result.scalar_one_or_none()
        return ERC20TransferDTO.from_model(model) if model else None

    async def get_by_tx_hash(self, tx_hash: str) -> ERC20TransferDTO | None:
        """Get transfer by transaction hash (first matching event).

        Args:
            tx_hash: Transaction hash.

        Returns:
            ERC20TransferDTO if found, None otherwise.
        """
        result = await self.session.execute(
            select(ERC20TransferModel).where(ERC20TransferModel.tx_hash == tx_hash.lower())
        )
        model = result.scalar_one_or_none()
        return ERC20TransferDTO.from_model(model) if model else None

    async def insert(self, dto: ERC20TransferDTO) -> ERC20TransferDTO:
        """Insert a new transfer.

        Args:
            dto: Transfer data.

        Returns:
            Inserted ERC20TransferDTO.

        Raises:
            IntegrityError if tx_hash already exists.
        """
        model = ERC20TransferModel(
            token_address=dto.token_address.lower(),
            from_address=dto.from_address.lower(),
            to_address=dto.to_address.lower(),
            amount_units=dto.amount_units,
            tx_hash=dto.tx_hash.lower(),
            log_index=dto.log_index,
            block_number=dto.block_number,
            timestamp=dto.timestamp,
        )
        self.session.add(model)
        await self.session.flush()
        return dto

    async def insert_many(self, dtos: list[ERC20TransferDTO]) -> int:
        """Insert multiple transfers (idempotent).

        Returns the number of attempted inserts (not the number of newly created
        rows), for portability across dialects.
        """
        if not dtos:
            return 0

        now = datetime.now(UTC)
        rows = [
            {
                "token_address": dto.token_address.lower(),
                "from_address": dto.from_address.lower(),
                "to_address": dto.to_address.lower(),
                "amount_units": dto.amount_units,
                "tx_hash": dto.tx_hash.lower(),
                "log_index": dto.log_index,
                "block_number": dto.block_number,
                "timestamp": dto.timestamp,
                "created_at": now,
            }
            for dto in dtos
        ]

        index_cols = ["token_address", "to_address", "block_number", "tx_hash", "log_index"]
        try:
            stmt = pg_insert(ERC20TransferModel).values(rows)
            stmt = stmt.on_conflict_do_nothing(index_elements=index_cols)
            await self.session.execute(stmt)
        except Exception:
            stmt = sqlite_insert(ERC20TransferModel).values(rows)
            stmt = stmt.on_conflict_do_nothing(index_elements=index_cols)
            await self.session.execute(stmt)

        await self.session.flush()
        return len(dtos)


class RelationshipRepository:
    """Repository for wallet relationship data access.

    Provides CRUD operations for wallet relationships with async support.
    """

    def __init__(self, session: AsyncSession) -> None:
        """Initialize repository with database session.

        Args:
            session: SQLAlchemy async session.
        """
        self.session = session

    async def get_relationships(
        self, wallet: str, relationship_type: str | None = None
    ) -> list[WalletRelationshipDTO]:
        """Get relationships for a wallet.

        Args:
            wallet: Wallet address.
            relationship_type: Optional filter by type.

        Returns:
            List of WalletRelationshipDTOs.
        """
        stmt = select(WalletRelationshipModel).where(
            (WalletRelationshipModel.wallet_a == wallet.lower())
            | (WalletRelationshipModel.wallet_b == wallet.lower())
        )
        if relationship_type:
            stmt = stmt.where(WalletRelationshipModel.relationship_type == relationship_type)

        result = await self.session.execute(stmt)
        return [WalletRelationshipDTO.from_model(m) for m in result.scalars().all()]

    async def get_related_wallets(
        self, wallet: str, relationship_type: str | None = None
    ) -> list[str]:
        """Get addresses of related wallets.

        Args:
            wallet: Wallet address.
            relationship_type: Optional filter by type.

        Returns:
            List of related wallet addresses.
        """
        relationships = await self.get_relationships(wallet, relationship_type)
        related = set()
        normalized = wallet.lower()
        for rel in relationships:
            if rel.wallet_a == normalized:
                related.add(rel.wallet_b)
            else:
                related.add(rel.wallet_a)
        return list(related)

    async def upsert(self, dto: WalletRelationshipDTO) -> WalletRelationshipDTO:
        """Insert or update wallet relationship.

        Args:
            dto: Wallet relationship data.

        Returns:
            Updated WalletRelationshipDTO.
        """
        now = datetime.now(UTC)
        values = {
            "wallet_a": dto.wallet_a.lower(),
            "wallet_b": dto.wallet_b.lower(),
            "relationship_type": dto.relationship_type,
            "confidence": dto.confidence,
            "created_at": now,
        }

        # Try PostgreSQL upsert first, fall back to SQLite for testing
        try:
            stmt = pg_insert(WalletRelationshipModel).values(**values)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_wallet_relationship",
                set_={"confidence": stmt.excluded.confidence},
            )
            await self.session.execute(stmt)
        except Exception:
            # Fall back to SQLite upsert for testing
            sqlite_stmt = sqlite_insert(WalletRelationshipModel).values(**values)
            sqlite_stmt = sqlite_stmt.on_conflict_do_update(
                index_elements=["wallet_a", "wallet_b", "relationship_type"],
                set_={"confidence": sqlite_stmt.excluded.confidence},
            )
            await self.session.execute(sqlite_stmt)

        await self.session.flush()
        return dto

    async def delete(self, wallet_a: str, wallet_b: str, relationship_type: str) -> bool:
        """Delete a specific relationship.

        Args:
            wallet_a: First wallet address.
            wallet_b: Second wallet address.
            relationship_type: Type of relationship.

        Returns:
            True if deleted, False if not found.
        """
        result = await self.session.execute(
            delete(WalletRelationshipModel).where(
                WalletRelationshipModel.wallet_a == wallet_a.lower(),
                WalletRelationshipModel.wallet_b == wallet_b.lower(),
                WalletRelationshipModel.relationship_type == relationship_type,
            )
        )
        # SQLAlchemy Result does have rowcount but typing doesn't reflect it
        return (result.rowcount or 0) > 0  # type: ignore[attr-defined]
