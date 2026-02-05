"""Add indexed ERC20 transfer table for bounded funding tracing.

Revision ID: 006_erc20_transfers
Revises: 005_sniper_clusters
Create Date: 2026-02-05 00:05:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "006_erc20_transfers"
down_revision: Union[str, None] = "005_sniper_clusters"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "erc20_transfers",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("token_address", sa.String(42), nullable=False),
        sa.Column("from_address", sa.String(42), nullable=False),
        sa.Column("to_address", sa.String(42), nullable=False),
        sa.Column("amount_units", sa.Numeric(40, 0), nullable=False),
        sa.Column("tx_hash", sa.String(66), nullable=False),
        sa.Column("log_index", sa.Integer(), nullable=False),
        sa.Column("block_number", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "token_address",
            "to_address",
            "block_number",
            "tx_hash",
            "log_index",
            name="uq_erc20_transfers_event",
        ),
    )
    op.create_index("idx_erc20_transfers_to_ts", "erc20_transfers", ["to_address", "timestamp"])
    op.create_index("idx_erc20_transfers_from_ts", "erc20_transfers", ["from_address", "timestamp"])
    op.create_index("idx_erc20_transfers_token_to", "erc20_transfers", ["token_address", "to_address"])


def downgrade() -> None:
    op.drop_index("idx_erc20_transfers_token_to", table_name="erc20_transfers")
    op.drop_index("idx_erc20_transfers_from_ts", table_name="erc20_transfers")
    op.drop_index("idx_erc20_transfers_to_ts", table_name="erc20_transfers")
    op.drop_table("erc20_transfers")

