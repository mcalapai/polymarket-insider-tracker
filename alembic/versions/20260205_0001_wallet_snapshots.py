"""Replace wallet_profiles with time-aware wallet_snapshots.

Revision ID: 002_wallet_snapshots
Revises: 001_initial
Create Date: 2026-02-05 00:01:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "002_wallet_snapshots"
down_revision: Union[str, None] = "001_initial"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_index("idx_wallet_profiles_address", table_name="wallet_profiles")
    op.drop_table("wallet_profiles")

    op.create_table(
        "wallet_snapshots",
        sa.Column("address", sa.String(42), nullable=False),
        sa.Column("as_of_block_number", sa.Integer(), nullable=False),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("nonce_as_of", sa.Integer(), nullable=False),
        sa.Column("first_funding_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("age_hours_as_of", sa.Numeric(18, 6), nullable=False),
        sa.Column("matic_balance_wei_as_of", sa.Numeric(30, 0), nullable=False),
        sa.Column("usdc_balance_units_as_of", sa.Numeric(30, 0), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("address", "as_of_block_number"),
    )
    op.create_index("idx_wallet_snapshots_address", "wallet_snapshots", ["address"])
    op.create_index("idx_wallet_snapshots_as_of", "wallet_snapshots", ["as_of"])


def downgrade() -> None:
    op.drop_index("idx_wallet_snapshots_as_of", table_name="wallet_snapshots")
    op.drop_index("idx_wallet_snapshots_address", table_name="wallet_snapshots")
    op.drop_table("wallet_snapshots")

    op.create_table(
        "wallet_profiles",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("address", sa.String(42), nullable=False),
        sa.Column("nonce", sa.Integer(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_fresh", sa.Boolean(), nullable=False),
        sa.Column("matic_balance", sa.Numeric(30, 0), nullable=True),
        sa.Column("usdc_balance", sa.Numeric(20, 6), nullable=True),
        sa.Column("analyzed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("address"),
    )
    op.create_index("idx_wallet_profiles_address", "wallet_profiles", ["address"])

