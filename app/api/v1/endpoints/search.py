import asyncio
import logging
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
import httpx

from app.db.database import get_session
from app.models.tasks import SearchTask
from app.schemas.search import PreviewRequest, PreviewResponse, BranchPreviewItem, ScrapeRequest
from app.schemas.tasks import SearchTaskResponse
from app.services.scraper import SITE_BASE, scrape_branch_preview, search_branches
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
        found = await search_branches(client, payload.query, payload.city, payload.max_results)
        if not found:
            return PreviewResponse(query=payload.query, city=payload.city, count=0, branches=[])

        sem = asyncio.Semaphore(settings.max_concurrent_branches)

        async def enrich(entry: dict) -> BranchPreviewItem:
            async with sem:
                try:
                    data = await scrape_branch_preview(client, entry["gis_branch_id"], entry["firm_url"])
                except Exception as e:
                    logging.exception("Preview enrich failed for %s: %s", entry["gis_branch_id"], e)
                    return BranchPreviewItem(
                        gis_branch_id=entry["gis_branch_id"],
                        firm_url=entry["firm_url"],
                        name=None,
                        address=None,
                    )
                return BranchPreviewItem(**data)

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
    task = SearchTask(query=payload.query, city=payload.city)
    session.add(task)
    await session.commit()
    await session.refresh(task)

    branches = [
        {
            "gis_branch_id": int(fid),
            "firm_url": f"{SITE_BASE}/{payload.city}/firm/{fid}",
        }
        for fid in payload.gis_branch_ids
    ]

    bg.add_task(run_scrape_task, task.id, branches)

    return SearchTaskResponse(
        task_id=task.id,
        status=task.status.value,
        query=task.query,
        city=task.city,
    )
