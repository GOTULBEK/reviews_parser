"""Add phones to branches and collect kinds to search_tasks

Revision ID: a7d2c4e9b1f3
Revises: 7d3e9f1a2b4c
Create Date: 2026-10-23 16:30:00.000000

Сбор телефонов филиалов наравне с отзывами:
1. branches.phones (jsonb, NULL — не собирались) и branches.phones_scraped_at.
2. search_tasks.collect — что собирает задача ("reviews"/"phones"); старые задачи
   собирали только отзывы, поэтому DEFAULT '{reviews}'.
3. search_tasks.total_phones_collected — счётчик прогресса, как total_reviews_collected.

Идемпотентна: те же колонки досоздаёт на старте app.db.schema.ensure_added_columns,
поэтому на базе, где сервис уже стартовал с новым кодом, миграция ничего не меняет.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "a7d2c4e9b1f3"
down_revision: Union[str, Sequence[str], None] = "7d3e9f1a2b4c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _columns(table: str) -> set[str]:
    return {c["name"] for c in sa.inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    task_cols = _columns("search_tasks")
    if "collect" not in task_cols:
        op.add_column(
            "search_tasks",
            sa.Column(
                "collect",
                postgresql.ARRAY(sa.String(length=16)),
                nullable=False,
                server_default=sa.text("'{reviews}'"),
            ),
        )
    if "total_phones_collected" not in task_cols:
        op.add_column(
            "search_tasks",
            sa.Column("total_phones_collected", sa.Integer(), nullable=False, server_default=sa.text("0")),
        )

    branch_cols = _columns("branches")
    if "phones" not in branch_cols:
        op.add_column("branches", sa.Column("phones", postgresql.JSONB(), nullable=True))
    if "phones_scraped_at" not in branch_cols:
        op.add_column("branches", sa.Column("phones_scraped_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    branch_cols = _columns("branches")
    if "phones_scraped_at" in branch_cols:
        op.drop_column("branches", "phones_scraped_at")
    if "phones" in branch_cols:
        op.drop_column("branches", "phones")

    task_cols = _columns("search_tasks")
    if "total_phones_collected" in task_cols:
        op.drop_column("search_tasks", "total_phones_collected")
    if "collect" in task_cols:
        op.drop_column("search_tasks", "collect")
