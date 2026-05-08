"""Add reply_templates column to task_topics_cache

Revision ID: f3a5c6d7e8b1
Revises: e2f4b5c6d7a9
Create Date: 2026-05-08 13:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = 'f3a5c6d7e8b1'
down_revision: Union[str, Sequence[str], None] = 'e2f4b5c6d7a9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {c['name'] for c in inspector.get_columns('task_topics_cache')}
    if 'reply_templates' not in columns:
        op.add_column(
            'task_topics_cache',
            sa.Column('reply_templates', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {c['name'] for c in inspector.get_columns('task_topics_cache')}
    if 'reply_templates' in columns:
        op.drop_column('task_topics_cache', 'reply_templates')
