"""Add source/city to task_topics_cache key

Revision ID: c1a2b3d4e5f6
Revises: d047b4b780d5, b7c8d9e0f1a2
Create Date: 2026-06-01 16:30:00.000000

AI-аналитика (problems/actions/recommendations/topics_module/reply_templates,
top_problems/top_praise) кэшировалась по (task_id, days), без учёта source и
city. Поэтому фильтрация дашборда по источнику/городу не отражалась в выдаче
AI-эндпоинтов: первый посчитанный город/источник обслуживал все остальные.

Эта миграция:
1. Добавляет колонки source, city (nullable — старые строки остаются валидными).
2. Заменяет уникальность на null-safe функциональный индекс по
   (task_id, COALESCE(days,-1), COALESCE(source,'2gis'), COALESCE(city,'all')).
   COALESCE сохраняет обратную совместимость: старые NULL-строки матчатся на
   дефолтный запрос (source=2gis, city=all).

Также сливает две висящие головы (d047b4b780d5, b7c8d9e0f1a2).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c1a2b3d4e5f6"
down_revision: Union[str, Sequence[str], None] = ("d047b4b780d5", "b7c8d9e0f1a2")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    columns = {c["name"] for c in inspector.get_columns("task_topics_cache")}
    if "source" not in columns:
        op.add_column("task_topics_cache", sa.Column("source", sa.String(length=64), nullable=True))
    if "city" not in columns:
        op.add_column("task_topics_cache", sa.Column("city", sa.String(length=64), nullable=True))

    # Схлопываем возможные дубли по новой логической группе, оставляя max(id).
    op.execute(
        """
        DELETE FROM task_topics_cache
        WHERE id NOT IN (
            SELECT MAX(id)
            FROM task_topics_cache
            GROUP BY task_id,
                     COALESCE(days, -1),
                     COALESCE(source, '2gis'),
                     COALESCE(city, 'all')
        )
        """
    )

    # Убираем прежние варианты уникальности (plain constraint и/или null-safe индекс).
    constraints = {c["name"] for c in inspector.get_unique_constraints("task_topics_cache")}
    if "uq_task_days_source_city_topics" in constraints:
        op.drop_constraint("uq_task_days_source_city_topics", "task_topics_cache", type_="unique")
    if "uq_task_days_topics" in constraints:
        op.drop_constraint("uq_task_days_topics", "task_topics_cache", type_="unique")
    op.execute("DROP INDEX IF EXISTS uq_task_days_topics_null_safe")
    op.execute("DROP INDEX IF EXISTS uq_task_days_topics")

    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_topics_cache_key_null_safe
        ON task_topics_cache (
            task_id,
            COALESCE(days, -1),
            COALESCE(source, '2gis'),
            COALESCE(city, 'all')
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_topics_cache_key_null_safe")
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_task_days_topics_null_safe
        ON task_topics_cache (task_id, COALESCE(days, -1))
        """
    )
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {c["name"] for c in inspector.get_columns("task_topics_cache")}
    if "city" in columns:
        op.drop_column("task_topics_cache", "city")
    if "source" in columns:
        op.drop_column("task_topics_cache", "source")
