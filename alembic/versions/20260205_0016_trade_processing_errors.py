"""Add trade_processing_errors table.

Revision ID: 017_trade_processing_errors
Revises: 016_ml_models
Create Date: 2026-02-05 00:16:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "017_trade_processing_errors"
down_revision: Union[str, None] = "016_ml_models"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "trade_processing_errors",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("trade_id", sa.String(80), nullable=False),
        sa.Column("stage", sa.String(40), nullable=False),
        sa.Column("error_type", sa.String(80), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_trade_processing_errors_trade", "trade_processing_errors", ["trade_id"])


def downgrade() -> None:
    op.drop_index("idx_trade_processing_errors_trade", table_name="trade_processing_errors")
    op.drop_table("trade_processing_errors")

