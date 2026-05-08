"""Dedupe task_topics_cache and enforce null-safe uniqueness on (task_id, days)

Revision ID: a4b3c2d1e0f9
Revises: f3a5c6d7e8b1
Create Date: 2026-05-08 14:30:00.000000

Postgres treats NULL as distinct in plain unique constraints, so multiple rows
with (task_id, NULL) could be inserted whenever a request hit `/overview` (or
any cache-writing endpoint) without a `days` filter. This migration:

1. Deletes duplicate rows, keeping the row with the highest id per logical group
   (treating NULL days as a single value via COALESCE).
2. Drops the legacy unique constraint that did not handle NULLs.
3. Adds a unique functional index using COALESCE so NULL counts as one bucket.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'a4b3c2d1e0f9'
down_revision: Union[str, Sequence[str], None] = 'f3a5c6d7e8b1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        DELETE FROM task_topics_cache
        WHERE id NOT IN (
            SELECT MAX(id)
            FROM task_topics_cache
            GROUP BY task_id, COALESCE(days, -1)
        )
        """
    )

    bind = op.get_bind()
    inspector = sa.inspect(bind)
    constraints = {c['name'] for c in inspector.get_unique_constraints('task_topics_cache')}
    if 'uq_task_days_topics' in constraints:
        op.drop_constraint('uq_task_days_topics', 'task_topics_cache', type_='unique')

    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_task_days_topics_null_safe
        ON task_topics_cache (task_id, COALESCE(days, -1))
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_task_days_topics_null_safe")
    op.create_unique_constraint(
        'uq_task_days_topics',
        'task_topics_cache',
        ['task_id', 'days'],
    )
