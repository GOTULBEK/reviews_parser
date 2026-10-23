"""Колонки, которые create_all не добавит в уже существующие таблицы.

На старте `Base.metadata.create_all` создаёт только недостающие ТАБЛИЦЫ, а новые
колонки в существующих — нет, и alembic-цепочка прогоняется не на каждой базе (см.
scripts/apply_city_cache_migration.py). Без этого шага код с новой колонкой в модели
падал бы на любом SELECT по таблице, пока кто-нибудь не применит миграцию руками.

Только добавление колонок (nullable или с дефолтом), идемпотентно. ALTER — лишь для
отсутствующих колонок: иначе каждый старт брал бы ACCESS EXCLUSIVE на горячие таблицы.
Зеркалит миграции 7d3e9f1a2b4c (gis_org_id) и a7d2c4e9b1f3 (телефоны, collect).
"""
import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

logger = logging.getLogger(__name__)

# (таблица, колонка, определение) — имена фиксированы здесь, не из ввода.
ADDED_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("branches", "gis_org_id", "bigint"),
    ("search_tasks", "collect", "varchar(16)[] NOT NULL DEFAULT '{reviews}'"),
    ("search_tasks", "total_phones_collected", "integer NOT NULL DEFAULT 0"),
    ("branches", "phones", "jsonb"),
    ("branches", "phones_scraped_at", "timestamp with time zone"),
)


async def ensure_added_columns(conn: AsyncConnection) -> list[str]:
    """Добавляет недостающие колонки из ADDED_COLUMNS. Возвращает добавленные."""
    tables = sorted({table for table, _, _ in ADDED_COLUMNS})
    rows = await conn.execute(
        text(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = ANY(:tables)"
        ),
        {"tables": tables},
    )
    existing = {(table, column) for table, column in rows.all()}

    added: list[str] = []
    for table, column, definition in ADDED_COLUMNS:
        if (table, column) in existing:
            continue
        if not added:
            # Не висеть на старте за чужой долгой транзакцией: лучше упасть громко.
            await conn.execute(text("SET LOCAL lock_timeout = '10s'"))
        await conn.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {definition}"))
        added.append(f"{table}.{column}")
    if added:
        logger.warning("Schema: added columns %s", ", ".join(added))
    return added
