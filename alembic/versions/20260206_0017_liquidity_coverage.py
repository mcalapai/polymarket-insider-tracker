"""Add liquidity_coverage table for historical quality gates.

Revision ID: 018_liquidity_coverage
Revises: 017_trade_processing_errors
Create Date: 2026-02-06 00:17:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "018_liquidity_coverage"
down_revision: Union[str, None] = "017_trade_processing_errors"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "liquidity_coverage",
        sa.Column("condition_id", sa.String(80), nullable=False),
        sa.Column("asset_id", sa.String(80), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("cadence_seconds", sa.Integer(), nullable=False),
        sa.Column("expected_snapshots", sa.Integer(), nullable=False),
        sa.Column("observed_snapshots", sa.Integer(), nullable=False),
        sa.Column("coverage_ratio", sa.Numeric(10, 8), nullable=False),
        sa.Column("availability_status", sa.String(20), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("condition_id", "asset_id", "window_start", "window_end"),
    )
    op.create_index(
        "idx_liquidity_coverage_condition_asset_window_end",
        "liquidity_coverage",
        ["condition_id", "asset_id", "window_end"],
    )
    op.create_index(
        "idx_liquidity_coverage_window_end",
        "liquidity_coverage",
        ["window_end"],
    )


def downgrade() -> None:
    op.drop_index("idx_liquidity_coverage_window_end", table_name="liquidity_coverage")
    op.drop_index(
        "idx_liquidity_coverage_condition_asset_window_end",
        table_name="liquidity_coverage",
    )
    op.drop_table("liquidity_coverage")
