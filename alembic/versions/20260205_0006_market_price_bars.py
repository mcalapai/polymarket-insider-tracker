"""Add per-market minute price bars (derived from trades).

Revision ID: 007_market_price_bars
Revises: 006_erc20_transfers
Create Date: 2026-02-05 00:06:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "007_market_price_bars"
down_revision: Union[str, None] = "006_erc20_transfers"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "market_price_bars",
        sa.Column("market_id", sa.String(80), nullable=False),
        sa.Column("bucket_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("first_trade_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_trade_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open_price", sa.Numeric(20, 10), nullable=False),
        sa.Column("high_price", sa.Numeric(20, 10), nullable=False),
        sa.Column("low_price", sa.Numeric(20, 10), nullable=False),
        sa.Column("close_price", sa.Numeric(20, 10), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("market_id", "bucket_start"),
    )
    op.create_index(
        "idx_market_price_bars_market_bucket",
        "market_price_bars",
        ["market_id", "bucket_start"],
    )


def downgrade() -> None:
    op.drop_index("idx_market_price_bars_market_bucket", table_name="market_price_bars")
    op.drop_table("market_price_bars")

