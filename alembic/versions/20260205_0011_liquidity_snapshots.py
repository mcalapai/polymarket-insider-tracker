"""Add liquidity_snapshots table for durable liquidity history.

Revision ID: 012_liquidity_snapshots
Revises: 011_markets_embeddings
Create Date: 2026-02-05 00:11:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "012_liquidity_snapshots"
down_revision: Union[str, None] = "011_markets_embeddings"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "liquidity_snapshots",
        sa.Column("condition_id", sa.String(80), nullable=False),
        sa.Column("asset_id", sa.String(80), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("rolling_24h_volume_usdc", sa.Numeric(30, 10), nullable=True),
        sa.Column("visible_book_depth_usdc", sa.Numeric(30, 10), nullable=False),
        sa.Column("mid_price", sa.Numeric(20, 10), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("condition_id", "asset_id", "computed_at"),
    )
    op.create_index(
        "idx_liquidity_snapshots_condition_ts",
        "liquidity_snapshots",
        ["condition_id", "computed_at"],
    )
    op.create_index(
        "idx_liquidity_snapshots_asset_ts",
        "liquidity_snapshots",
        ["asset_id", "computed_at"],
    )


def downgrade() -> None:
    op.drop_index("idx_liquidity_snapshots_asset_ts", table_name="liquidity_snapshots")
    op.drop_index("idx_liquidity_snapshots_condition_ts", table_name="liquidity_snapshots")
    op.drop_table("liquidity_snapshots")

