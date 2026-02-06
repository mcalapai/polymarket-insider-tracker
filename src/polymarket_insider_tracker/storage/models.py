"""SQLAlchemy models for persistent storage.

This module defines the database schema for storing wallet profiles,
funding transfers, and wallet relationships.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from polymarket_insider_tracker.storage.vector import Vector

if TYPE_CHECKING:
    pass


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models."""

    pass


class WalletSnapshotModel(Base):
    """SQLAlchemy model for time-aware wallet snapshots."""

    __tablename__ = "wallet_snapshots"

    address: Mapped[str] = mapped_column(String(42), primary_key=True, nullable=False)
    as_of_block_number: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)

    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    nonce_as_of: Mapped[int] = mapped_column(Integer, nullable=False)
    first_funding_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    age_hours_as_of: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)

    matic_balance_wei_as_of: Mapped[Decimal] = mapped_column(Numeric(30, 0), nullable=False)
    usdc_balance_units_as_of: Mapped[Decimal] = mapped_column(Numeric(30, 0), nullable=False)

    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        Index("idx_wallet_snapshots_address", "address"),
        Index("idx_wallet_snapshots_as_of", "as_of"),
    )


class ERC20TransferModel(Base):
    """Indexed ERC20 transfer events (on-demand, bounded)."""

    __tablename__ = "erc20_transfers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    token_address: Mapped[str] = mapped_column(String(42), nullable=False)
    from_address: Mapped[str] = mapped_column(String(42), nullable=False)
    to_address: Mapped[str] = mapped_column(String(42), nullable=False)

    # Raw token units as emitted by the Transfer event (uint256).
    amount_units: Mapped[Decimal] = mapped_column(Numeric(40, 0), nullable=False)

    tx_hash: Mapped[str] = mapped_column(String(66), nullable=False)
    log_index: Mapped[int] = mapped_column(Integer, nullable=False)
    block_number: Mapped[int] = mapped_column(Integer, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        UniqueConstraint(
            "token_address",
            "to_address",
            "block_number",
            "tx_hash",
            "log_index",
            name="uq_erc20_transfers_event",
        ),
        Index("idx_erc20_transfers_to_ts", "to_address", "timestamp"),
        Index("idx_erc20_transfers_from_ts", "from_address", "timestamp"),
        Index("idx_erc20_transfers_token_to", "token_address", "to_address"),
    )


class WalletRelationshipModel(Base):
    """SQLAlchemy model for wallet relationships.

    Stores graph edges between wallets representing funding relationships
    or entity linkages.
    """

    __tablename__ = "wallet_relationships"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    wallet_a: Mapped[str] = mapped_column(String(42), nullable=False)
    wallet_b: Mapped[str] = mapped_column(String(42), nullable=False)
    relationship_type: Mapped[str] = mapped_column(String(20), nullable=False)
    confidence: Mapped[Decimal] = mapped_column(Numeric(3, 2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        UniqueConstraint(
            "wallet_a", "wallet_b", "relationship_type", name="uq_wallet_relationship"
        ),
        Index("idx_wallet_relationships_a", "wallet_a"),
        Index("idx_wallet_relationships_b", "wallet_b"),
    )


class TradeModel(Base):
    """Executed trade events (durable truth)."""

    __tablename__ = "trades"

    trade_id: Mapped[str] = mapped_column(String(80), primary_key=True)  # tx hash or unique id
    market_id: Mapped[str] = mapped_column(String(80), nullable=False)
    asset_id: Mapped[str] = mapped_column(String(80), nullable=False)
    wallet_address: Mapped[str] = mapped_column(String(42), nullable=False)

    side: Mapped[str] = mapped_column(String(4), nullable=False)  # BUY/SELL
    outcome: Mapped[str] = mapped_column(String(64), nullable=False)
    outcome_index: Mapped[int] = mapped_column(Integer, nullable=False)

    price: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    size: Mapped[Decimal] = mapped_column(Numeric(30, 10), nullable=False)
    notional_usdc: Mapped[Decimal] = mapped_column(Numeric(30, 10), nullable=False)

    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        Index("idx_trades_market_ts", "market_id", "ts"),
        Index("idx_trades_wallet_ts", "wallet_address", "ts"),
    )


class LiquiditySnapshotModel(Base):
    """Persisted liquidity snapshots for market tokens (asset_id)."""

    __tablename__ = "liquidity_snapshots"

    condition_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    asset_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)

    rolling_24h_volume_usdc: Mapped[Decimal | None] = mapped_column(Numeric(30, 10), nullable=True)
    visible_book_depth_usdc: Mapped[Decimal] = mapped_column(Numeric(30, 10), nullable=False)
    mid_price: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        Index("idx_liquidity_snapshots_condition_ts", "condition_id", "computed_at"),
        Index("idx_liquidity_snapshots_asset_ts", "asset_id", "computed_at"),
    )


class LiquidityCoverageModel(Base):
    """Coverage metrics for historical liquidity availability windows."""

    __tablename__ = "liquidity_coverage"

    condition_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    asset_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)

    cadence_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    expected_snapshots: Mapped[int] = mapped_column(Integer, nullable=False)
    observed_snapshots: Mapped[int] = mapped_column(Integer, nullable=False)
    coverage_ratio: Mapped[Decimal] = mapped_column(Numeric(10, 8), nullable=False)
    availability_status: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # measured|unavailable
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )

    __table_args__ = (
        Index(
            "idx_liquidity_coverage_condition_asset_window_end",
            "condition_id",
            "asset_id",
            "window_end",
        ),
        Index("idx_liquidity_coverage_window_end", "window_end"),
    )


class OrderModel(Base):
    """Order details indexed by order hash (Level-2)."""

    __tablename__ = "orders"

    order_hash: Mapped[str] = mapped_column(String(80), primary_key=True)
    market_id: Mapped[str] = mapped_column(String(80), nullable=False)
    asset_id: Mapped[str] = mapped_column(String(80), nullable=False)
    wallet_address: Mapped[str] = mapped_column(String(42), nullable=False)

    side: Mapped[str] = mapped_column(String(4), nullable=False)  # BUY/SELL
    price: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    size: Mapped[Decimal] = mapped_column(Numeric(30, 10), nullable=False)
    size_matched: Mapped[Decimal] = mapped_column(Numeric(30, 10), nullable=False)

    status: Mapped[str] = mapped_column(String(24), nullable=False)
    created_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    indexed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )

    __table_args__ = (
        Index("idx_orders_wallet_created", "wallet_address", "created_ts"),
        Index("idx_orders_market_created", "market_id", "created_ts"),
        Index("idx_orders_asset_created", "asset_id", "created_ts"),
    )


class OrderEventModel(Base):
    """Raw order lifecycle events derived from the market channel."""

    __tablename__ = "order_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_hash: Mapped[str] = mapped_column(String(80), nullable=False)
    market_id: Mapped[str] = mapped_column(String(80), nullable=False)
    asset_id: Mapped[str] = mapped_column(String(80), nullable=False)
    wallet_address: Mapped[str] = mapped_column(String(42), nullable=False)

    event_type: Mapped[str] = mapped_column(String(24), nullable=False)  # PLACED/CANCELED/UPDATED
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    best_bid: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    best_ask: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        Index("idx_order_events_wallet_ts", "wallet_address", "ts"),
        Index("idx_order_events_market_ts", "market_id", "ts"),
        Index("idx_order_events_order_ts", "order_hash", "ts"),
    )


class CancelModel(Base):
    """Derived cancel events for rapid-cancel and spoofing analysis."""

    __tablename__ = "cancels"

    order_hash: Mapped[str] = mapped_column(String(80), primary_key=True)
    market_id: Mapped[str] = mapped_column(String(80), nullable=False)
    asset_id: Mapped[str] = mapped_column(String(80), nullable=False)
    wallet_address: Mapped[str] = mapped_column(String(42), nullable=False)

    created_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    canceled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    cancel_latency_seconds: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)

    size: Mapped[Decimal] = mapped_column(Numeric(30, 10), nullable=False)
    size_matched: Mapped[Decimal] = mapped_column(Numeric(30, 10), nullable=False)

    moved_best: Mapped[bool] = mapped_column(Boolean, nullable=False)
    impact_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        Index("idx_cancels_wallet_ts", "wallet_address", "canceled_at"),
        Index("idx_cancels_market_ts", "market_id", "canceled_at"),
    )


class BacktestRunModel(Base):
    """Metadata + summary for historical scan/backtest runs."""

    __tablename__ = "backtest_runs"

    run_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    command: Mapped[str] = mapped_column(String(20), nullable=False)  # scan/train-model
    query: Mapped[str] = mapped_column(Text, nullable=False)

    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    lookback_days: Mapped[int] = mapped_column(Integer, nullable=False)
    top_k_markets: Mapped[int] = mapped_column(Integer, nullable=False)
    markets_considered: Mapped[int] = mapped_column(Integer, nullable=False)

    trades_considered: Mapped[int] = mapped_column(Integer, nullable=False)
    flagged_trades: Mapped[int] = mapped_column(Integer, nullable=False)
    hit_rate: Mapped[Decimal | None] = mapped_column(Numeric(6, 5), nullable=True)

    output_path: Mapped[str] = mapped_column(Text, nullable=False)
    params_json: Mapped[str] = mapped_column(Text, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    __table_args__ = (Index("idx_backtest_runs_started_at", "started_at"),)


class TradeFeatureModel(Base):
    """Feature row per trade for model training/serving (auditable)."""

    __tablename__ = "trade_features"

    trade_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    wallet_nonce_as_of: Mapped[int | None] = mapped_column(Integer, nullable=True)
    wallet_age_hours_as_of: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    wallet_usdc_balance_units_as_of: Mapped[Decimal | None] = mapped_column(Numeric(40, 0), nullable=True)
    wallet_matic_balance_wei_as_of: Mapped[Decimal | None] = mapped_column(Numeric(40, 0), nullable=True)

    trade_notional_usdc: Mapped[Decimal] = mapped_column(Numeric(30, 10), nullable=False)

    volume_impact: Mapped[Decimal | None] = mapped_column(Numeric(18, 8), nullable=True)
    book_impact: Mapped[Decimal | None] = mapped_column(Numeric(18, 8), nullable=True)

    fresh_wallet_confidence: Mapped[Decimal | None] = mapped_column(Numeric(6, 5), nullable=True)
    size_anomaly_confidence: Mapped[Decimal | None] = mapped_column(Numeric(6, 5), nullable=True)
    sniper_cluster_confidence: Mapped[Decimal | None] = mapped_column(Numeric(6, 5), nullable=True)
    coentry_confidence: Mapped[Decimal | None] = mapped_column(Numeric(6, 5), nullable=True)
    funding_confidence: Mapped[Decimal | None] = mapped_column(Numeric(6, 5), nullable=True)

    trade_size_outlier_confidence: Mapped[Decimal | None] = mapped_column(Numeric(6, 5), nullable=True)
    digit_distribution_confidence: Mapped[Decimal | None] = mapped_column(Numeric(6, 5), nullable=True)
    trade_slicing_confidence: Mapped[Decimal | None] = mapped_column(Numeric(6, 5), nullable=True)

    order_to_trade_ratio_confidence: Mapped[Decimal | None] = mapped_column(Numeric(6, 5), nullable=True)
    rapid_cancel_confidence: Mapped[Decimal | None] = mapped_column(Numeric(6, 5), nullable=True)
    book_impact_without_fill_confidence: Mapped[Decimal | None] = mapped_column(Numeric(6, 5), nullable=True)

    model_score: Mapped[Decimal | None] = mapped_column(Numeric(10, 8), nullable=True)

    features_json: Mapped[str] = mapped_column(Text, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )

    __table_args__ = (
        Index("idx_trade_features_computed_at", "computed_at"),
    )


class TradeLabelModel(Base):
    """Self-supervised labels derived from future market movement."""

    __tablename__ = "trade_labels"

    trade_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    label_type: Mapped[str] = mapped_column(String(40), primary_key=True)
    horizon_seconds: Mapped[int] = mapped_column(Integer, primary_key=True)

    value: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    label: Mapped[bool] = mapped_column(Boolean, nullable=False)
    params_json: Mapped[str] = mapped_column(Text, nullable=False)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )

    __table_args__ = (Index("idx_trade_labels_type_horizon", "label_type", "horizon_seconds"),)


class ModelArtifactModel(Base):
    """Versioned model artifacts + metadata."""

    __tablename__ = "ml_models"

    model_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    algorithm: Mapped[str] = mapped_column(String(40), nullable=False)
    label_type: Mapped[str] = mapped_column(String(40), nullable=False)
    horizon_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    z_threshold: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)

    artifact_path: Mapped[str] = mapped_column(Text, nullable=False)
    metrics_json: Mapped[str] = mapped_column(Text, nullable=False)
    schema_json: Mapped[str] = mapped_column(Text, nullable=False)

    blessed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    trained_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    __table_args__ = (Index("idx_ml_models_trained_at", "trained_at"),)


class TradeProcessingErrorModel(Base):
    """Per-trade processing errors (strict, non-silent failures)."""

    __tablename__ = "trade_processing_errors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trade_id: Mapped[str] = mapped_column(String(80), nullable=False)
    stage: Mapped[str] = mapped_column(String(40), nullable=False)
    error_type: Mapped[str] = mapped_column(String(80), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    __table_args__ = (Index("idx_trade_processing_errors_trade", "trade_id"),)


class MarketPriceBarModel(Base):
    """Per-market minute bars derived from trades (price series)."""

    __tablename__ = "market_price_bars"

    market_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    bucket_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)

    first_trade_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_trade_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    open_price: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    high_price: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    low_price: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    close_price: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )

    __table_args__ = (
        Index("idx_market_price_bars_market_bucket", "market_id", "bucket_start"),
    )


class MarketStateModel(Base):
    """Per-market derived state anchors (first observed trade, etc.)."""

    __tablename__ = "market_state"

    market_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    first_trade_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )


class MarketModel(Base):
    """Persisted market metadata + embedding for semantic search."""

    __tablename__ = "markets"

    condition_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    question: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    tokens_json: Mapped[str] = mapped_column(Text, nullable=False)

    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    closed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    end_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    embedding: Mapped[object] = mapped_column(Vector(), nullable=False)

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        Index("idx_markets_active_closed", "active", "closed"),
    )


class MarketEntryModel(Base):
    """First observed entry of a wallet into a market (based on trades)."""

    __tablename__ = "market_entries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market_id: Mapped[str] = mapped_column(String(80), nullable=False)
    wallet_address: Mapped[str] = mapped_column(String(42), nullable=False)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    notional_usdc: Mapped[Decimal] = mapped_column(Numeric(30, 10), nullable=False)
    entry_rank: Mapped[int] = mapped_column(Integer, nullable=False)
    entry_delta_seconds: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        UniqueConstraint("market_id", "wallet_address", name="uq_market_entry_wallet"),
        Index("idx_market_entries_market_ts", "market_id", "ts"),
        Index("idx_market_entries_wallet_ts", "wallet_address", "ts"),
    )


class SniperClusterModel(Base):
    """DBSCAN-derived wallet coordination clusters for sniper detection."""

    __tablename__ = "sniper_clusters"

    cluster_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    cluster_size: Mapped[int] = mapped_column(Integer, nullable=False)
    avg_entry_delta_seconds: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    markets_in_common: Mapped[int] = mapped_column(Integer, nullable=False)
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False)

    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )

    __table_args__ = (
        Index("idx_sniper_clusters_computed_at", "computed_at"),
        Index("idx_sniper_clusters_window_end", "window_end"),
    )


class SniperClusterMemberModel(Base):
    """Cluster membership mapping (wallets ↔ clusters)."""

    __tablename__ = "sniper_cluster_members"

    cluster_id: Mapped[str] = mapped_column(String(80), primary_key=True, nullable=False)
    wallet_address: Mapped[str] = mapped_column(String(42), primary_key=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        Index("idx_sniper_cluster_members_wallet", "wallet_address"),
        Index("idx_sniper_cluster_members_cluster", "cluster_id"),
    )


class TradeSignalModel(Base):
    """Persisted detector signal payloads (audit + backtests)."""

    __tablename__ = "trade_signals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trade_id: Mapped[str] = mapped_column(String(80), nullable=False)
    signal_type: Mapped[str] = mapped_column(String(40), nullable=False)
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        UniqueConstraint("trade_id", "signal_type", name="uq_trade_signals_trade_type"),
        Index("idx_trade_signals_trade", "trade_id"),
        Index("idx_trade_signals_type", "signal_type"),
        Index("idx_trade_signals_computed_at", "computed_at"),
    )


class MarketDailyBaselineModel(Base):
    """Daily persisted baseline aggregates (auditability)."""

    __tablename__ = "market_daily_baselines"

    market_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    day: Mapped[date] = mapped_column(Date, primary_key=True)
    baseline_type: Mapped[str] = mapped_column(String(40), primary_key=True)

    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )

    __table_args__ = (
        Index("idx_market_daily_baselines_market_day", "market_id", "day"),
        Index("idx_market_daily_baselines_type", "baseline_type"),
    )
