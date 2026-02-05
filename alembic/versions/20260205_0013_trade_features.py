"""Add trade_features table.

Revision ID: 014_trade_features
Revises: 013_backtest_runs
Create Date: 2026-02-05 00:13:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "014_trade_features"
down_revision: Union[str, None] = "013_backtest_runs"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "trade_features",
        sa.Column("trade_id", sa.String(80), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("wallet_nonce_as_of", sa.Integer(), nullable=True),
        sa.Column("wallet_age_hours_as_of", sa.Numeric(18, 6), nullable=True),
        sa.Column("wallet_usdc_balance_units_as_of", sa.Numeric(40, 0), nullable=True),
        sa.Column("wallet_matic_balance_wei_as_of", sa.Numeric(40, 0), nullable=True),
        sa.Column("trade_notional_usdc", sa.Numeric(30, 10), nullable=False),
        sa.Column("volume_impact", sa.Numeric(18, 8), nullable=True),
        sa.Column("book_impact", sa.Numeric(18, 8), nullable=True),
        sa.Column("fresh_wallet_confidence", sa.Numeric(6, 5), nullable=True),
        sa.Column("size_anomaly_confidence", sa.Numeric(6, 5), nullable=True),
        sa.Column("sniper_cluster_confidence", sa.Numeric(6, 5), nullable=True),
        sa.Column("coentry_confidence", sa.Numeric(6, 5), nullable=True),
        sa.Column("funding_confidence", sa.Numeric(6, 5), nullable=True),
        sa.Column("trade_size_outlier_confidence", sa.Numeric(6, 5), nullable=True),
        sa.Column("digit_distribution_confidence", sa.Numeric(6, 5), nullable=True),
        sa.Column("trade_slicing_confidence", sa.Numeric(6, 5), nullable=True),
        sa.Column("order_to_trade_ratio_confidence", sa.Numeric(6, 5), nullable=True),
        sa.Column("rapid_cancel_confidence", sa.Numeric(6, 5), nullable=True),
        sa.Column("book_impact_without_fill_confidence", sa.Numeric(6, 5), nullable=True),
        sa.Column("model_score", sa.Numeric(10, 8), nullable=True),
        sa.Column("features_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("trade_id"),
    )
    op.create_index("idx_trade_features_computed_at", "trade_features", ["computed_at"])


def downgrade() -> None:
    op.drop_index("idx_trade_features_computed_at", table_name="trade_features")
    op.drop_table("trade_features")

