"""Add backtest_runs table.

Revision ID: 013_backtest_runs
Revises: 012_liquidity_snapshots
Create Date: 2026-02-05 00:12:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "013_backtest_runs"
down_revision: Union[str, None] = "012_liquidity_snapshots"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "backtest_runs",
        sa.Column("run_id", sa.String(36), nullable=False),
        sa.Column("command", sa.String(20), nullable=False),
        sa.Column("query", sa.Text(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lookback_days", sa.Integer(), nullable=False),
        sa.Column("top_k_markets", sa.Integer(), nullable=False),
        sa.Column("markets_considered", sa.Integer(), nullable=False),
        sa.Column("trades_considered", sa.Integer(), nullable=False),
        sa.Column("flagged_trades", sa.Integer(), nullable=False),
        sa.Column("hit_rate", sa.Numeric(6, 5), nullable=True),
        sa.Column("output_path", sa.Text(), nullable=False),
        sa.Column("params_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("run_id"),
    )
    op.create_index("idx_backtest_runs_started_at", "backtest_runs", ["started_at"])


def downgrade() -> None:
    op.drop_index("idx_backtest_runs_started_at", table_name="backtest_runs")
    op.drop_table("backtest_runs")

