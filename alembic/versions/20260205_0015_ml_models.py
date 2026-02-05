"""Add ml_models table.

Revision ID: 016_ml_models
Revises: 015_trade_labels
Create Date: 2026-02-05 00:15:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "016_ml_models"
down_revision: Union[str, None] = "015_trade_labels"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "ml_models",
        sa.Column("model_id", sa.String(36), nullable=False),
        sa.Column("algorithm", sa.String(40), nullable=False),
        sa.Column("label_type", sa.String(40), nullable=False),
        sa.Column("horizon_seconds", sa.Integer(), nullable=False),
        sa.Column("z_threshold", sa.Numeric(18, 8), nullable=False),
        sa.Column("artifact_path", sa.Text(), nullable=False),
        sa.Column("metrics_json", sa.Text(), nullable=False),
        sa.Column("schema_json", sa.Text(), nullable=False),
        sa.Column("blessed", sa.Boolean(), nullable=False),
        sa.Column("trained_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("model_id"),
    )
    op.create_index("idx_ml_models_trained_at", "ml_models", ["trained_at"])


def downgrade() -> None:
    op.drop_index("idx_ml_models_trained_at", table_name="ml_models")
    op.drop_table("ml_models")

