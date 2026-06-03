import asyncio
import logging
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
import httpx

from app.db.database import get_session
from app.models.tasks import SearchTask
from app.schemas.search import PreviewRequest, PreviewResponse, BranchPreviewItem, ScrapeRequest
from app.schemas.tasks import SearchTaskResponse
from app.services import scraper, zapis_scraper, cities
from app.schemas.common import SourceType
from app.workers.tasks import run_scrape_task
from app.core.config import settings

router = APIRouter()


def _interleave(groups: list[list[dict]], limit: int) -> list[dict]:
    """Round-robin по городам, чтобы при city='all' выдача не была забита одним городом.

    Берём по одному элементу из каждого города по кругу, пока не наберём limit.
    limit <= 0 → без лимита: возвращаем все элементы из всех городов.
    """
    if limit <= 0:
        limit = sum(len(g) for g in groups)
    out: list[dict] = []
    idx = 0
    while len(out) < limit:
        took = False
        for g in groups:
            if idx < len(g):
                out.append(g[idx])
                took = True
                if len(out) >= limit:
                    break
        if not took:
            break
        idx += 1
    return out

@router.post(
    "/preview",
    response_model=PreviewResponse,
    summary="Найти кандидатов по тексту запроса (без сбора отзывов)",
)
async def search_preview(payload: PreviewRequest):
    timeout = httpx.Timeout(settings.request_timeout_seconds, connect=10)
    limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)

    all_cities = payload.city == cities.ALL_CITIES
    target_cities = await cities.list_city_slugs() if all_cities else [payload.city]

    async with httpx.AsyncClient(timeout=timeout, limits=limits, follow_redirects=True) as client:
        async def _search_twogis():
            if payload.source not in ("2gis", "all"):
                return []
            try:
                if not all_cities:
                    return await scraper.search_branches(
                        client, payload.query, payload.city, payload.max_results
                    )
                # city="all": ищем в каждом городе, но с ограничением параллелизма,
                # иначе 2ГИС троттлит весь залп. Затем round-robin до max_results.
                #
                # Итоговая выдача всё равно обрезается до max_results, поэтому из
                # каждого города тянем лишь столько, сколько нужно для заполнения
                # (+небольшой запас на города без результатов) — это резко
                # сокращает пагинацию и нагрузку на 2ГИС.
                # max_results=0 → без лимита: тянем всё из каждого города.
                if payload.max_results == 0:
                    per_city_limit = 0
                else:
                    n = max(1, len(target_cities))
                    per_city_limit = min(
                        payload.max_results,
                        max(3, -(-payload.max_results // n) + 1),  # ceil(max/n)+1, не меньше 3
                    )
                city_sem = asyncio.Semaphore(settings.max_concurrent_cities)

                async def _one_city(c: str):
                    async with city_sem:
                        return await scraper.search_branches(
                            client, payload.query, c, per_city_limit
                        )

                per_city = await asyncio.gather(*(_one_city(c) for c in target_cities))
                return _interleave(per_city, payload.max_results)
            except Exception as e:
                raise HTTPException(status_code=503, detail=f"Scraper error (2gis): {str(e)}")

        async def _search_zapis():
            # Zapis игнорирует город (city_id зашит), поэтому при city="all" запускаем один раз.
            if payload.source not in ("zapis", "all"):
                return []
            try:
                return await zapis_scraper.search_branches(
                    client, payload.query, payload.city, payload.max_results
                )
            except Exception as e:
                raise HTTPException(status_code=503, detail=f"Scraper error (zapis): {str(e)}")

        found_twogis, found_zapis = await asyncio.gather(
            _search_twogis(),
            _search_zapis()
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
                        city=entry.get("city"),
                        name=None,
                        address=None,
                    )
                return BranchPreviewItem(source=source, city=entry.get("city"), **data)

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

    all_cities = payload.city == cities.ALL_CITIES

    branches = []
    for b in payload.branches:
        # Город филиала: из preview (b.city) или город задачи. Для city='all' без
        # явного города оставляем None — настоящий город определится при скрапе.
        branch_city = b.city or (None if all_cities else payload.city)
        if b.source == SourceType.twogis:
            # firm-id глобален, 2ГИС редиректит с любого города на правильный,
            # поэтому для URL подойдёт любой валидный slug (fallback — дефолтный).
            url_city = branch_city or settings.default_city_slug
            firm_url = f"{scraper.SITE_BASE}/{url_city}/firm/{b.gis_branch_id}"
        else:
            firm_url = f"{zapis_scraper.SITE_BASE}/rest/clients-app/v1/firms/{b.gis_branch_id}"
        branches.append({
            "gis_branch_id": int(b.gis_branch_id),
            "firm_url": firm_url,
            "city": branch_city,
            "source": b.source
        })

    bg.add_task(run_scrape_task, task.id, branches)

    return SearchTaskResponse(
        task_id=task.id,
        status=task.status.value,
        query=task.query,
        city=task.city,
    )
