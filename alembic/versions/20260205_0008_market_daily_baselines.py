"""Add daily persisted baseline aggregates.

Revision ID: 009_market_daily_baselines
Revises: 008_trade_signals
Create Date: 2026-02-05 00:08:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "009_market_daily_baselines"
down_revision: Union[str, None] = "008_trade_signals"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "market_daily_baselines",
        sa.Column("market_id", sa.String(80), nullable=False),
        sa.Column("day", sa.Date(), nullable=False),
        sa.Column("baseline_type", sa.String(40), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("market_id", "day", "baseline_type"),
    )
    op.create_index(
        "idx_market_daily_baselines_market_day",
        "market_daily_baselines",
        ["market_id", "day"],
    )
    op.create_index(
        "idx_market_daily_baselines_type",
        "market_daily_baselines",
        ["baseline_type"],
    )


def downgrade() -> None:
    op.drop_index("idx_market_daily_baselines_type", table_name="market_daily_baselines")
    op.drop_index("idx_market_daily_baselines_market_day", table_name="market_daily_baselines")
    op.drop_table("market_daily_baselines")

