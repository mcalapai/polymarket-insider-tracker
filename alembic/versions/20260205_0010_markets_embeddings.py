"""Add markets table for semantic retrieval (pgvector).

Revision ID: 011_markets_embeddings
Revises: 010_orders
Create Date: 2026-02-05 00:10:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

from polymarket_insider_tracker.storage.vector import Vector

revision: str = "011_markets_embeddings"
down_revision: Union[str, None] = "010_orders"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.create_table(
        "markets",
        sa.Column("condition_id", sa.String(80), nullable=False),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("tokens_json", sa.Text(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("closed", sa.Boolean(), nullable=False),
        sa.Column("end_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("embedding", Vector(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("condition_id"),
    )
    op.create_index("idx_markets_active_closed", "markets", ["active", "closed"])


def downgrade() -> None:
    op.drop_index("idx_markets_active_closed", table_name="markets")
    op.drop_table("markets")
    # Do not drop the extension, as other tables may depend on it.
