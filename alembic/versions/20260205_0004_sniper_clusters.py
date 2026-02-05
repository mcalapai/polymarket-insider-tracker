"""Add persisted sniper clusters and membership mapping.

Revision ID: 005_sniper_clusters
Revises: 004_market_entries
Create Date: 2026-02-05 00:04:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "005_sniper_clusters"
down_revision: Union[str, None] = "004_market_entries"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "sniper_clusters",
        sa.Column("cluster_id", sa.String(80), nullable=False),
        sa.Column("cluster_size", sa.Integer(), nullable=False),
        sa.Column("avg_entry_delta_seconds", sa.Numeric(18, 6), nullable=False),
        sa.Column("markets_in_common", sa.Integer(), nullable=False),
        sa.Column("confidence", sa.Numeric(5, 4), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("cluster_id"),
    )
    op.create_index("idx_sniper_clusters_computed_at", "sniper_clusters", ["computed_at"])
    op.create_index("idx_sniper_clusters_window_end", "sniper_clusters", ["window_end"])

    op.create_table(
        "sniper_cluster_members",
        sa.Column("cluster_id", sa.String(80), nullable=False),
        sa.Column("wallet_address", sa.String(42), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("cluster_id", "wallet_address"),
    )
    op.create_index(
        "idx_sniper_cluster_members_wallet",
        "sniper_cluster_members",
        ["wallet_address"],
    )
    op.create_index(
        "idx_sniper_cluster_members_cluster",
        "sniper_cluster_members",
        ["cluster_id"],
    )


def downgrade() -> None:
    op.drop_index("idx_sniper_cluster_members_cluster", table_name="sniper_cluster_members")
    op.drop_index("idx_sniper_cluster_members_wallet", table_name="sniper_cluster_members")
    op.drop_table("sniper_cluster_members")

    op.drop_index("idx_sniper_clusters_window_end", table_name="sniper_clusters")
    op.drop_index("idx_sniper_clusters_computed_at", table_name="sniper_clusters")
    op.drop_table("sniper_clusters")

