"""Add market_state and market_entries for sniper detection anchors.

Revision ID: 004_market_entries
Revises: 003_trades_table
Create Date: 2026-02-05 00:03:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "004_market_entries"
down_revision: Union[str, None] = "003_trades_table"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "market_state",
        sa.Column("market_id", sa.String(80), nullable=False),
        sa.Column("first_trade_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("market_id"),
    )

    op.create_table(
        "market_entries",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("market_id", sa.String(80), nullable=False),
        sa.Column("wallet_address", sa.String(42), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("notional_usdc", sa.Numeric(30, 10), nullable=False),
        sa.Column("entry_rank", sa.Integer(), nullable=False),
        sa.Column("entry_delta_seconds", sa.Numeric(18, 6), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("market_id", "wallet_address", name="uq_market_entry_wallet"),
    )
    op.create_index("idx_market_entries_market_ts", "market_entries", ["market_id", "ts"])
    op.create_index("idx_market_entries_wallet_ts", "market_entries", ["wallet_address", "ts"])


def downgrade() -> None:
    op.drop_index("idx_market_entries_wallet_ts", table_name="market_entries")
    op.drop_index("idx_market_entries_market_ts", table_name="market_entries")
    op.drop_table("market_entries")
    op.drop_table("market_state")

