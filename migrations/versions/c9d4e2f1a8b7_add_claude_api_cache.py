"""Add claude_api_cache table

Revision ID: c9d4e2f1a8b7
Revises: b8c3d2e1f0a9
Create Date: 2026-05-08 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = 'c9d4e2f1a8b7'
down_revision: Union[str, Sequence[str], None] = 'b8c3d2e1f0a9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if 'claude_api_cache' in inspector.get_table_names():
        return

    op.create_table(
        'claude_api_cache',
        sa.Column('request_hash', sa.String(length=64), nullable=False),
        sa.Column('model', sa.String(length=128), nullable=False),
        sa.Column('response', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('last_hit_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('hit_count', sa.Integer(), nullable=False, server_default='0'),
        sa.PrimaryKeyConstraint('request_hash'),
    )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if 'claude_api_cache' not in inspector.get_table_names():
        return

    op.drop_table('claude_api_cache')
