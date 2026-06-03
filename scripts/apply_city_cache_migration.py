"""Idempotently apply the source/city cache-key change to task_topics_cache.

Use this in environments where the alembic chain can't run cleanly (e.g. a DB
bootstrapped via Base.metadata.create_all, where `alembic upgrade head` would
trip over already-existing tables like `users`).

Mirrors migration c1a2b3d4e5f6 exactly, but applies ONLY the additive change:
  - adds nullable columns task_topics_cache.source, task_topics_cache.city
  - dedupes by the new logical key
  - swaps the uniqueness to a null-safe composite index

Safe to run multiple times. Run with:  python -m scripts.apply_city_cache_migration
"""
import asyncio

from sqlalchemy import text

from app.db.database import engine


async def main() -> None:
    async with engine.begin() as c:
        cols = [
            r[0]
            for r in (
                await c.execute(
                    text(
                        "select column_name from information_schema.columns "
                        "where table_name='task_topics_cache'"
                    )
                )
            ).all()
        ]
        if "source" not in cols:
            await c.execute(text("ALTER TABLE task_topics_cache ADD COLUMN source varchar(64)"))
            print("added column: source")
        if "city" not in cols:
            await c.execute(text("ALTER TABLE task_topics_cache ADD COLUMN city varchar(64)"))
            print("added column: city")

        await c.execute(
            text(
                """
                DELETE FROM task_topics_cache WHERE id NOT IN (
                    SELECT MAX(id) FROM task_topics_cache
                    GROUP BY task_id, COALESCE(days, -1),
                             COALESCE(source, '2gis'), COALESCE(city, 'all'))
                """
            )
        )
        await c.execute(text("ALTER TABLE task_topics_cache DROP CONSTRAINT IF EXISTS uq_task_days_topics"))
        await c.execute(text("DROP INDEX IF EXISTS uq_task_days_topics_null_safe"))
        await c.execute(text("DROP INDEX IF EXISTS uq_task_days_topics"))
        await c.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_topics_cache_key_null_safe
                ON task_topics_cache (
                    task_id, COALESCE(days, -1),
                    COALESCE(source, '2gis'), COALESCE(city, 'all'))
                """
            )
        )
    print("task_topics_cache: source/city cache key applied.")


if __name__ == "__main__":
    asyncio.run(main())
