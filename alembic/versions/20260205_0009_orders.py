"""Add orders, order_events, and cancels tables.

Revision ID: 010_orders
Revises: 009_market_daily_baselines
Create Date: 2026-02-05 00:09:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "010_orders"
down_revision: Union[str, None] = "009_market_daily_baselines"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "orders",
        sa.Column("order_hash", sa.String(80), nullable=False),
        sa.Column("market_id", sa.String(80), nullable=False),
        sa.Column("asset_id", sa.String(80), nullable=False),
        sa.Column("wallet_address", sa.String(42), nullable=False),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("price", sa.Numeric(20, 10), nullable=False),
        sa.Column("size", sa.Numeric(30, 10), nullable=False),
        sa.Column("size_matched", sa.Numeric(30, 10), nullable=False),
        sa.Column("status", sa.String(24), nullable=False),
        sa.Column("created_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("indexed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("order_hash"),
    )
    op.create_index("idx_orders_wallet_created", "orders", ["wallet_address", "created_ts"])
    op.create_index("idx_orders_market_created", "orders", ["market_id", "created_ts"])
    op.create_index("idx_orders_asset_created", "orders", ["asset_id", "created_ts"])

    op.create_table(
        "order_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("order_hash", sa.String(80), nullable=False),
        sa.Column("market_id", sa.String(80), nullable=False),
        sa.Column("asset_id", sa.String(80), nullable=False),
        sa.Column("wallet_address", sa.String(42), nullable=False),
        sa.Column("event_type", sa.String(24), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("best_bid", sa.Numeric(20, 10), nullable=True),
        sa.Column("best_ask", sa.Numeric(20, 10), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_order_events_wallet_ts", "order_events", ["wallet_address", "ts"])
    op.create_index("idx_order_events_market_ts", "order_events", ["market_id", "ts"])
    op.create_index("idx_order_events_order_ts", "order_events", ["order_hash", "ts"])

    op.create_table(
        "cancels",
        sa.Column("order_hash", sa.String(80), nullable=False),
        sa.Column("market_id", sa.String(80), nullable=False),
        sa.Column("asset_id", sa.String(80), nullable=False),
        sa.Column("wallet_address", sa.String(42), nullable=False),
        sa.Column("created_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("canceled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("cancel_latency_seconds", sa.Numeric(18, 6), nullable=False),
        sa.Column("size", sa.Numeric(30, 10), nullable=False),
        sa.Column("size_matched", sa.Numeric(30, 10), nullable=False),
        sa.Column("moved_best", sa.Boolean(), nullable=False),
        sa.Column("impact_bps", sa.Numeric(18, 6), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("order_hash"),
    )
    op.create_index("idx_cancels_wallet_ts", "cancels", ["wallet_address", "canceled_at"])
    op.create_index("idx_cancels_market_ts", "cancels", ["market_id", "canceled_at"])


def downgrade() -> None:
    op.drop_index("idx_cancels_market_ts", table_name="cancels")
    op.drop_index("idx_cancels_wallet_ts", table_name="cancels")
    op.drop_table("cancels")

    op.drop_index("idx_order_events_order_ts", table_name="order_events")
    op.drop_index("idx_order_events_market_ts", table_name="order_events")
    op.drop_index("idx_order_events_wallet_ts", table_name="order_events")
    op.drop_table("order_events")

    op.drop_index("idx_orders_asset_created", table_name="orders")
    op.drop_index("idx_orders_market_created", table_name="orders")
    op.drop_index("idx_orders_wallet_created", table_name="orders")
    op.drop_table("orders")

