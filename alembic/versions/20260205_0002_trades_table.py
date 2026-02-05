"""Add trades table for durable execution history.

Revision ID: 003_trades_table
Revises: 002_wallet_snapshots
Create Date: 2026-02-05 00:02:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "003_trades_table"
down_revision: Union[str, None] = "002_wallet_snapshots"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "trades",
        sa.Column("trade_id", sa.String(80), nullable=False),
        sa.Column("market_id", sa.String(80), nullable=False),
        sa.Column("asset_id", sa.String(80), nullable=False),
        sa.Column("wallet_address", sa.String(42), nullable=False),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("outcome", sa.String(64), nullable=False),
        sa.Column("outcome_index", sa.Integer(), nullable=False),
        sa.Column("price", sa.Numeric(20, 10), nullable=False),
        sa.Column("size", sa.Numeric(30, 10), nullable=False),
        sa.Column("notional_usdc", sa.Numeric(30, 10), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("trade_id"),
    )
    op.create_index("idx_trades_market_ts", "trades", ["market_id", "ts"])
    op.create_index("idx_trades_wallet_ts", "trades", ["wallet_address", "ts"])


def downgrade() -> None:
    op.drop_index("idx_trades_wallet_ts", table_name="trades")
    op.drop_index("idx_trades_market_ts", table_name="trades")
    op.drop_table("trades")

