from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Literal
from uuid import UUID

import httpx
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from .config import settings
from .database import Base, engine, get_session
from .models import Branch, Review, SearchTask, SearchTaskBranch, TaskStatus
from .schemas import (
    ActionsResponse,
    BranchesListResponse,
    BranchListItem,
    BranchPreviewItem,
    BranchRatingSummary,
    BranchResponse,
    BranchWithReviewsResponse,
    KPIs,
    OverviewResponse,
    PreviewRequest,
    PreviewResponse,
    ProblemsResponse,
    ReviewListItem,
    ReviewResponse,
    ReviewsListResponse,
    ScrapeRequest,
    SearchTaskResponse,
    SentimentBreakdown,
    TaskResultResponse,
    TaskStatusResponse,
)
from .scraper import SITE_BASE, scrape_branch_preview, search_branches
from .tasks import run_scrape_task

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# Константы для sentiment — синхронизированы со схемами
_SENT_POS_MIN = 4
_SENT_NEG_MAX = 2
_ANALYTICS_STUB_NOTE = (
    "NLP analytics layer is not implemented yet. Empty arrays are intentional "
    "and stable — safe to integrate UI against this contract."
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()


app = FastAPI(
    title="2GIS Reviews Scraper",
    version="2.1.0",
    description=(
        "Workflow:\n"
        "1. POST /search/preview — найти кандидатов\n"
        "2. POST /search/scrape — запустить сбор по выбранным\n"
        "3. GET /tasks/{id} — опрос статуса\n"
        "4. Dashboard endpoints: /overview /branches /reviews /problems /actions\n"
    ),
    lifespan=lifespan,
)

cors_origins = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sentiment_from_rating(rating: int | None) -> Literal["pos", "neg", "neu", "unknown"]:
    if rating is None:
        return "unknown"
    if rating >= _SENT_POS_MIN:
        return "pos"
    if rating <= _SENT_NEG_MAX:
        return "neg"
    return "neu"


async def _require_task(task_id: UUID, session: AsyncSession) -> SearchTask:
    """404 если нет, 409 если еще не завершилась (running/pending)."""
    task = await session.get(SearchTask, task_id)
    if not task:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Task not found")
    if task.status not in (TaskStatus.completed, TaskStatus.failed):
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"Task is still {task.status.value}",
        )
    return task


def _pct(numerator: int, denominator: int) -> float:
    """Безопасное деление на 100% с округлением до 1 знака."""
    if denominator <= 0:
        return 0.0
    return round(numerator * 100.0 / denominator, 1)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health", tags=["meta"])
async def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Step 1 — Preview
# ---------------------------------------------------------------------------

@app.post(
    "/search/preview",
    response_model=PreviewResponse,
    tags=["search"],
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


# ---------------------------------------------------------------------------
# Step 2 — Scrape
# ---------------------------------------------------------------------------

@app.post(
    "/search/scrape",
    response_model=SearchTaskResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["search"],
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


# ---------------------------------------------------------------------------
# Task lifecycle
# ---------------------------------------------------------------------------

@app.get("/tasks/{task_id}", response_model=TaskStatusResponse, tags=["tasks"])
async def get_task_status(task_id: UUID, session: AsyncSession = Depends(get_session)):
    task = await session.get(SearchTask, task_id)
    if not task:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Task not found")

    return TaskStatusResponse(
        task_id=task.id,
        status=task.status.value,
        query=task.query,
        city=task.city,
        total_branches_found=task.total_branches_found,
        branches_completed=task.branches_completed,
        total_reviews_collected=task.total_reviews_collected,
        error_message=task.error_message,
        created_at=task.created_at,
        started_at=task.started_at,
        completed_at=task.completed_at,
    )


# ---------------------------------------------------------------------------
# Dashboard — /overview
# ---------------------------------------------------------------------------

@app.get(
    "/tasks/{task_id}/overview",
    response_model=OverviewResponse,
    tags=["dashboard"],
    summary="KPI + sentiment + топ-филиалы",
)
async def get_overview(
    task_id: UUID,
    top_branches_limit: int = Query(10, ge=1, le=50),
    session: AsyncSession = Depends(get_session),
):
    task = await _require_task(task_id, session)

    # Филиалы задачи + краткие метрики. Одной выборкой.
    branches_stmt = (
        select(Branch)
        .join(SearchTaskBranch, SearchTaskBranch.branch_id == Branch.id)
        .where(SearchTaskBranch.task_id == task_id)
    )
    branches = (await session.execute(branches_stmt)).scalars().all()
    branches_total = len(branches)

    # Aggregate по review-таблице одной SQL — эффективнее чем по-python.
    # count(*) filter (where ...) — стандарт Postgres, SQLAlchemy поддерживает.
    agg_stmt = (
        select(
            func.count().label("total"),
            func.count().filter(Review.rating >= _SENT_POS_MIN).label("pos"),
            func.count().filter(Review.rating == 3).label("neu"),
            func.count().filter(Review.rating <= _SENT_NEG_MAX).label("neg"),
            func.count().filter(Review.official_answer_text.isnot(None)).label("replied"),
            func.avg(Review.rating).filter(Review.rating.isnot(None)).label("avg_rating"),
        )
        .select_from(Review)
        .join(SearchTaskBranch, SearchTaskBranch.branch_id == Review.branch_id)
        .where(SearchTaskBranch.task_id == task_id)
    )
    row = (await session.execute(agg_stmt)).one()

    reviews_total = row.total or 0
    pos_count = row.pos or 0
    neu_count = row.neu or 0
    neg_count = row.neg or 0
    replied = row.replied or 0
    avg_rating = float(row.avg_rating) if row.avg_rating is not None else None

    kpis = KPIs(
        avg_rating=round(avg_rating, 2) if avg_rating is not None else None,
        branches_total=branches_total,
        reviews_total=reviews_total,
        negative_pct=_pct(neg_count, reviews_total),
        replies_pct=_pct(replied, reviews_total),
    )

    sentiment = SentimentBreakdown(
        positive_pct=_pct(pos_count, reviews_total),
        negative_pct=_pct(neg_count, reviews_total),
        neutral_pct=_pct(neu_count, reviews_total),
    )

    # Топ филиалов по рейтингу (ties — по числу отзывов)
    sorted_branches = sorted(
        branches,
        key=lambda b: (b.rating if b.rating is not None else -1.0, b.total_reviews or 0),
        reverse=True,
    )[:top_branches_limit]

    branch_ratings = [
        BranchRatingSummary(
            branch_id=b.id,
            name=b.name,
            rating=b.rating,
            total_reviews=b.total_reviews or 0,
        )
        for b in sorted_branches
    ]

    return OverviewResponse(
        task_id=task.id,
        status=task.status.value,
        query=task.query,
        city=task.city,
        kpis=kpis,
        sentiment=sentiment,
        branch_ratings=branch_ratings,
        top_problems=[],
        top_praise=[],
        analytics_note=_ANALYTICS_STUB_NOTE,
    )


# ---------------------------------------------------------------------------
# Dashboard — /branches
# ---------------------------------------------------------------------------

_BRANCH_SORT_MAP = {
    "rating_desc": (Branch.rating.desc().nulls_last(), Branch.total_reviews.desc().nulls_last()),
    "reviews_desc": (Branch.total_reviews.desc().nulls_last(), Branch.rating.desc().nulls_last()),
    "name_asc": (Branch.name.asc().nulls_last(),),
}


@app.get(
    "/tasks/{task_id}/branches",
    response_model=BranchesListResponse,
    tags=["dashboard"],
    summary="Список филиалов задачи (с пагинацией и сортировкой)",
)
async def get_task_branches(
    task_id: UUID,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    sort: Literal["rating_desc", "reviews_desc", "name_asc"] = Query("rating_desc"),
    session: AsyncSession = Depends(get_session),
):
    task = await _require_task(task_id, session)

    # Подзапрос: кол-во отзывов с ответами на каждый филиал
    replies_sub = (
        select(
            Review.branch_id.label("bid"),
            func.count().label("reviews_n"),
            func.count().filter(Review.official_answer_text.isnot(None)).label("replied_n"),
        )
        .group_by(Review.branch_id)
        .subquery()
    )

    order_cols = _BRANCH_SORT_MAP[sort]

    stmt = (
        select(Branch, replies_sub.c.reviews_n, replies_sub.c.replied_n)
        .join(SearchTaskBranch, SearchTaskBranch.branch_id == Branch.id)
        .outerjoin(replies_sub, replies_sub.c.bid == Branch.id)
        .where(SearchTaskBranch.task_id == task_id)
        .order_by(*order_cols)
        .limit(limit)
        .offset(offset)
    )

    # Count total (без лимита)
    count_stmt = (
        select(func.count())
        .select_from(Branch)
        .join(SearchTaskBranch, SearchTaskBranch.branch_id == Branch.id)
        .where(SearchTaskBranch.task_id == task_id)
    )
    total = (await session.execute(count_stmt)).scalar_one()

    rows = (await session.execute(stmt)).all()

    items: list[BranchListItem] = []
    for b, reviews_n, replied_n in rows:
        reviews_n = reviews_n or 0
        replied_n = replied_n or 0
        items.append(BranchListItem(
            id=b.id,
            gis_branch_id=b.gis_branch_id,  # int → str via BranchIdStr
            name=b.name,
            address=b.address,
            rating=b.rating,
            total_reviews=b.total_reviews,
            url=b.url,
            rating_distribution=b.rating_distribution,
            replies_pct=_pct(replied_n, reviews_n) if reviews_n else None,
            top_tags=[],
        ))

    return BranchesListResponse(
        task_id=task.id,
        status=task.status.value,
        count=total,
        branches=items,
    )


# ---------------------------------------------------------------------------
# Dashboard — /reviews
# ---------------------------------------------------------------------------

@app.get(
    "/tasks/{task_id}/reviews",
    response_model=ReviewsListResponse,
    tags=["dashboard"],
    summary="Отзывы задачи с фильтрами и пагинацией",
)
async def get_task_reviews(
    task_id: UUID,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    sentiment: Literal["all", "pos", "neg", "neu"] = Query("all"),
    branch_id: UUID | None = Query(None),
    min_rating: int | None = Query(None, ge=1, le=5),
    max_rating: int | None = Query(None, ge=1, le=5),
    session: AsyncSession = Depends(get_session),
):
    task = await _require_task(task_id, session)

    # Базовый WHERE: отзывы из филиалов этой задачи
    where_clauses = [SearchTaskBranch.task_id == task_id]

    if branch_id is not None:
        where_clauses.append(Review.branch_id == branch_id)

    if min_rating is not None:
        where_clauses.append(Review.rating >= min_rating)
    if max_rating is not None:
        where_clauses.append(Review.rating <= max_rating)

    # sentiment → rating range (определение совпадает с _sentiment_from_rating)
    if sentiment == "pos":
        where_clauses.append(Review.rating >= _SENT_POS_MIN)
    elif sentiment == "neg":
        where_clauses.append(Review.rating <= _SENT_NEG_MAX)
    elif sentiment == "neu":
        where_clauses.append(Review.rating == 3)
    # sentiment == "all" — без доп. условия

    base_from = (
        Review.__table__
        .join(Branch.__table__, Review.branch_id == Branch.id)
        .join(SearchTaskBranch.__table__, SearchTaskBranch.branch_id == Branch.id)
    )

    # Count total с теми же фильтрами
    count_stmt = select(func.count()).select_from(base_from).where(and_(*where_clauses))
    total = (await session.execute(count_stmt)).scalar_one()

    # Данные (select Review + Branch.name)
    data_stmt = (
        select(Review, Branch.name.label("branch_name"))
        .select_from(base_from)
        .where(and_(*where_clauses))
        .order_by(Review.date_created.desc().nulls_last())
        .limit(limit)
        .offset(offset)
    )

    rows = (await session.execute(data_stmt)).all()

    items = [
        ReviewListItem(
            id=r.id,
            gis_review_id=r.gis_review_id,
            branch_id=r.branch_id,
            branch_name=branch_name,
            rating=r.rating,
            text=r.text,
            official_answer_text=r.official_answer_text,
            official_answer_date=r.official_answer_date,
            date_created=r.date_created,
            review_url=r.review_url,
            sentiment=_sentiment_from_rating(r.rating),
        )
        for r, branch_name in rows
    ]

    return ReviewsListResponse(
        task_id=task.id,
        status=task.status.value,
        count=total,
        limit=limit,
        offset=offset,
        reviews=items,
    )


# ---------------------------------------------------------------------------
# Dashboard — /problems  (stub)
# ---------------------------------------------------------------------------

@app.get(
    "/tasks/{task_id}/problems",
    response_model=ProblemsResponse,
    tags=["dashboard"],
    summary="Агрегированные проблемы (NLP не реализован — возвращает пустой массив)",
)
async def get_task_problems(task_id: UUID, session: AsyncSession = Depends(get_session)):
    task = await _require_task(task_id, session)
    return ProblemsResponse(
        task_id=task.id,
        status=task.status.value,
        items=[],
        analytics_note=_ANALYTICS_STUB_NOTE,
    )


# ---------------------------------------------------------------------------
# Dashboard — /actions  (stub)
# ---------------------------------------------------------------------------

@app.get(
    "/tasks/{task_id}/actions",
    response_model=ActionsResponse,
    tags=["dashboard"],
    summary="План действий (NLP не реализован — возвращает пустые массивы)",
)
async def get_task_actions(task_id: UUID, session: AsyncSession = Depends(get_session)):
    task = await _require_task(task_id, session)
    return ActionsResponse(
        task_id=task.id,
        status=task.status.value,
        priorities=[],
        insights=[],
        analytics_note=_ANALYTICS_STUB_NOTE,
    )


# ---------------------------------------------------------------------------
# Legacy /results — kept for backward compat (heavy, frontend migrating off)
# ---------------------------------------------------------------------------

@app.get(
    "/tasks/{task_id}/results",
    response_model=TaskResultResponse,
    tags=["tasks"],
    deprecated=True,
    summary="[DEPRECATED] Полный JSON, мигрируй на /overview /branches /reviews",
)
async def get_task_results(
    task_id: UUID,
    include_reviews: bool = Query(True),
    session: AsyncSession = Depends(get_session),
):
    task = await _require_task(task_id, session)

    stmt = (
        select(Branch)
        .join(SearchTaskBranch, SearchTaskBranch.branch_id == Branch.id)
        .where(SearchTaskBranch.task_id == task_id)
    )
    if include_reviews:
        stmt = stmt.options(selectinload(Branch.reviews))

    branches = (await session.execute(stmt)).scalars().unique().all()

    out: list[BranchWithReviewsResponse] = []
    for b in branches:
        payload = BranchWithReviewsResponse.model_validate(b)
        if include_reviews:
            payload.reviews = [ReviewResponse.model_validate(r) for r in b.reviews]
        out.append(payload)

    return TaskResultResponse(
        task_id=task.id,
        status=task.status.value,
        query=task.query,
        city=task.city,
        branches=out,
        total_reviews=task.total_reviews_collected,
    )


# ---------------------------------------------------------------------------
# Single-resource retrieval
# ---------------------------------------------------------------------------

@app.get("/reviews/{review_uuid}", response_model=ReviewResponse, tags=["reviews"])
async def get_review(review_uuid: UUID, session: AsyncSession = Depends(get_session)):
    review = await session.get(Review, review_uuid)
    if not review:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Review not found")
    return ReviewResponse.model_validate(review)


@app.get("/branches/{branch_uuid}", response_model=BranchResponse, tags=["branches"])
async def get_branch(branch_uuid: UUID, session: AsyncSession = Depends(get_session)):
    branch = await session.get(Branch, branch_uuid)
    if not branch:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Branch not found")
    return BranchResponse.model_validate(branch)


@app.get(
    "/branches/{branch_uuid}/reviews",
    response_model=list[ReviewResponse],
    tags=["branches"],
)
async def list_branch_reviews(
    branch_uuid: UUID,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    session: AsyncSession = Depends(get_session),
):
    stmt = (
        select(Review)
        .where(Review.branch_id == branch_uuid)
        .order_by(Review.date_created.desc().nulls_last())
        .limit(limit)
        .offset(offset)
    )
    result = await session.execute(stmt)
    reviews = result.scalars().all()
    return [ReviewResponse.model_validate(r) for r in reviews]