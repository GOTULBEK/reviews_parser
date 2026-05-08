"""Add recommendations column to task_topics_cache

Revision ID: d1e3a4f5b6c8
Revises: c9d4e2f1a8b7
Create Date: 2026-05-08 12:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = 'd1e3a4f5b6c8'
down_revision: Union[str, Sequence[str], None] = 'c9d4e2f1a8b7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {c['name'] for c in inspector.get_columns('task_topics_cache')}
    if 'recommendations' not in columns:
        op.add_column(
            'task_topics_cache',
            sa.Column('recommendations', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {c['name'] for c in inspector.get_columns('task_topics_cache')}
    if 'recommendations' in columns:
        op.drop_column('task_topics_cache', 'recommendations')
