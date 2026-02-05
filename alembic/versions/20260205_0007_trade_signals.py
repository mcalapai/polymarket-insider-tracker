"""Add persisted trade signal payloads.

Revision ID: 008_trade_signals
Revises: 007_market_price_bars
Create Date: 2026-02-05 00:07:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "008_trade_signals"
down_revision: Union[str, None] = "007_market_price_bars"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "trade_signals",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("trade_id", sa.String(80), nullable=False),
        sa.Column("signal_type", sa.String(40), nullable=False),
        sa.Column("confidence", sa.Numeric(5, 4), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("trade_id", "signal_type", name="uq_trade_signals_trade_type"),
    )
    op.create_index("idx_trade_signals_trade", "trade_signals", ["trade_id"])
    op.create_index("idx_trade_signals_type", "trade_signals", ["signal_type"])
    op.create_index("idx_trade_signals_computed_at", "trade_signals", ["computed_at"])


def downgrade() -> None:
    op.drop_index("idx_trade_signals_computed_at", table_name="trade_signals")
    op.drop_index("idx_trade_signals_type", table_name="trade_signals")
    op.drop_index("idx_trade_signals_trade", table_name="trade_signals")
    op.drop_table("trade_signals")

