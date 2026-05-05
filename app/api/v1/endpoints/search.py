import asyncio
import logging
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
import httpx

from app.db.database import get_session
from app.models.tasks import SearchTask
from app.schemas.search import PreviewRequest, PreviewResponse, BranchPreviewItem, ScrapeRequest
from app.schemas.tasks import SearchTaskResponse
from app.services import scraper, zapis_scraper
from app.schemas.common import SourceType
from app.workers.tasks import run_scrape_task
from app.core.config import settings

router = APIRouter()

@router.post(
    "/preview",
    response_model=PreviewResponse,
    summary="Найти кандидатов по тексту запроса (без сбора отзывов)",
)
async def search_preview(payload: PreviewRequest):
    timeout = httpx.Timeout(settings.request_timeout_seconds, connect=10)
    limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)

    async with httpx.AsyncClient(timeout=timeout, limits=limits, follow_redirects=True) as client:
        async def _run_search(src: str):
            try:
                if src == "2gis" and payload.source in ("2gis", "all"):
                    return await scraper.search_branches(client, payload.query, payload.city, payload.max_results)
                if src == "zapis" and payload.source in ("zapis", "all"):
                    return await zapis_scraper.search_branches(client, payload.query, payload.city, payload.max_results)
                return []
            except Exception as e:
                raise HTTPException(status_code=503, detail=f"Scraper error ({src}): {str(e)}")

        found_twogis, found_zapis = await asyncio.gather(
            _run_search("2gis"),
            _run_search("zapis")
        )
        for f in found_twogis:
            f["source"] = SourceType.twogis
        for f in found_zapis:
            f["source"] = SourceType.zapis
            
        found = found_twogis + found_zapis
        if not found:
            return PreviewResponse(query=payload.query, city=payload.city, count=0, branches=[])

        sem = asyncio.Semaphore(settings.max_concurrent_branches)

        async def enrich(entry: dict) -> BranchPreviewItem:
            async with sem:
                source = entry["source"]
                try:
                    if source == SourceType.twogis:
                        data = await scraper.scrape_branch_preview(client, entry["gis_branch_id"], entry["firm_url"])
                    else:
                        data = await zapis_scraper.scrape_branch_preview(client, entry["gis_branch_id"], entry["firm_url"])
                except Exception as e:
                    logging.exception("Preview enrich failed for %s (%s): %s", entry["gis_branch_id"], source, e)
                    return BranchPreviewItem(
                        gis_branch_id=entry["gis_branch_id"],
                        source=source,
                        firm_url=entry["firm_url"],
                        name=None,
                        address=None,
                    )
                return BranchPreviewItem(source=source, **data)

        items = await asyncio.gather(*(enrich(e) for e in found))

    return PreviewResponse(
        query=payload.query, city=payload.city, count=len(items), branches=list(items)
    )


@router.post(
    "/scrape",
    response_model=SearchTaskResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Запустить сбор отзывов для выбранных филиалов",
)
async def start_scrape(
    payload: ScrapeRequest,
    bg: BackgroundTasks,
    session: AsyncSession = Depends(get_session),
):
    # Determine the first source just as primary or store empty if none
    first_source = payload.branches[0].source if payload.branches else SourceType.twogis
    task = SearchTask(query=payload.query, city=payload.city, source=first_source)
    session.add(task)
    await session.commit()
    await session.refresh(task)

    branches = []
    for b in payload.branches:
        if b.source == SourceType.twogis:
            firm_url = f"{scraper.SITE_BASE}/{payload.city}/firm/{b.gis_branch_id}"
        else:
            firm_url = f"{zapis_scraper.SITE_BASE}/rest/clients-app/v1/firms/{b.gis_branch_id}"
        branches.append({
            "gis_branch_id": int(b.gis_branch_id),
            "firm_url": firm_url,
            "source": b.source
        })

    bg.add_task(run_scrape_task, task.id, branches)

    return SearchTaskResponse(
        task_id=task.id,
        status=task.status.value,
        query=task.query,
        city=task.city,
    )
