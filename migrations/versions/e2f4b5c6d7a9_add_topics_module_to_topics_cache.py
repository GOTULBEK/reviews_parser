"""Add topics_module column to task_topics_cache

Revision ID: e2f4b5c6d7a9
Revises: d1e3a4f5b6c8
Create Date: 2026-05-08 13:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = 'e2f4b5c6d7a9'
down_revision: Union[str, Sequence[str], None] = 'd1e3a4f5b6c8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {c['name'] for c in inspector.get_columns('task_topics_cache')}
    if 'topics_module' not in columns:
        op.add_column(
            'task_topics_cache',
            sa.Column('topics_module', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {c['name'] for c in inspector.get_columns('task_topics_cache')}
    if 'topics_module' in columns:
        op.drop_column('task_topics_cache', 'topics_module')
