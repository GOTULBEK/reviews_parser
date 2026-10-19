"""Idempotently add branches.gis_org_id — the 2GIS organization of a branch.

Mirrors migration 7d3e9f1a2b4c exactly, for environments where the alembic chain can't run cleanly (a DB bootstrapped
via Base.metadata.create_all — see scripts/apply_city_cache_migration.py). The column is nullable and additive, so it
is safe to add while the previous version of the service is still running.

Safe to run multiple times. Run with:  python -m scripts.apply_branch_org_id_column
"""
import asyncio

from sqlalchemy import text

from app.db.database import engine


async def main() -> None:
    async with engine.begin() as c:
        await c.execute(text("ALTER TABLE branches ADD COLUMN IF NOT EXISTS gis_org_id BIGINT"))
        n = (await c.execute(text("SELECT count(*) FROM branches WHERE gis_org_id IS NULL"))).scalar_one()
    print(f"branches.gis_org_id is in place; branches without it: {n}")
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
