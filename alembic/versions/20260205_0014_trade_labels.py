"""Add trade_labels table.

Revision ID: 015_trade_labels
Revises: 014_trade_features
Create Date: 2026-02-05 00:14:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "015_trade_labels"
down_revision: Union[str, None] = "014_trade_features"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "trade_labels",
        sa.Column("trade_id", sa.String(80), nullable=False),
        sa.Column("label_type", sa.String(40), nullable=False),
        sa.Column("horizon_seconds", sa.Integer(), nullable=False),
        sa.Column("value", sa.Numeric(18, 8), nullable=False),
        sa.Column("label", sa.Boolean(), nullable=False),
        sa.Column("params_json", sa.Text(), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("trade_id", "label_type", "horizon_seconds"),
    )
    op.create_index("idx_trade_labels_type_horizon", "trade_labels", ["label_type", "horizon_seconds"])


def downgrade() -> None:
    op.drop_index("idx_trade_labels_type_horizon", table_name="trade_labels")
    op.drop_table("trade_labels")

