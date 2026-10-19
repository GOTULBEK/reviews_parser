"""Fill branches.gis_org_id for 2GIS branches scraped before the column existed.

The organization id is what the owner's link needs: replies in the 2GIS business account live at
/orgs/<organization>/reviews/<branch>. New scrapes store it themselves (scraper.scrape_branch); this fills the rest
from each branch's public firm page — one page per branch, with the same pause between requests as a scrape.

Safe to run multiple times: only branches without an organization are fetched.
Run with:  python -m scripts.backfill_branch_org_ids [--limit N]
"""
import argparse
import asyncio
import random

import httpx
from sqlalchemy import select, update

from app.core.config import settings
from app.db.database import AsyncSessionLocal, engine
from app.models.core import Branch
from app.services import scraper


async def main(limit: int | None) -> None:
    async with AsyncSessionLocal() as session:
        stmt = (
            select(Branch.id, Branch.gis_branch_id, Branch.url)
            .where(Branch.gis_org_id.is_(None), Branch.source == "2gis")
            .order_by(Branch.scraped_at.desc().nulls_last())
        )
        if limit:
            stmt = stmt.limit(limit)
        rows = (await session.execute(stmt)).all()

    print(f"branches to fill: {len(rows)}")
    filled = missed = 0
    timeout = httpx.Timeout(settings.request_timeout_seconds)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        for i, (branch_id, gis_branch_id, url) in enumerate(rows):
            org_id = await scraper.fetch_org_id(client, gis_branch_id, url)
            if org_id is None:
                missed += 1
            else:
                async with AsyncSessionLocal() as session:
                    await session.execute(
                        update(Branch).where(Branch.id == branch_id, Branch.gis_org_id.is_(None)).values(gis_org_id=org_id)
                    )
                    await session.commit()
                filled += 1
            if i < len(rows) - 1:
                await asyncio.sleep(random.uniform(settings.rate_limit_sleep_min, settings.rate_limit_sleep_max))
    print(f"filled: {filled}, organization not found: {missed}")
    await engine.dispose()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="fill at most N branches (newest scraped first)")
    asyncio.run(main(ap.parse_args().limit))
