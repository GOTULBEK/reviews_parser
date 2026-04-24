from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from time import monotonic
from typing import Literal
from uuid import UUID

import httpx
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import DateTime, and_, bindparam, case, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from . import claude_service
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
    BranchKPIs,
    KPIs,
    OverviewBranchItem,
    OverviewBranchAnalytics,
    OverviewResponse,
    PreviewRequest,
    PreviewResponse,
    ProblemsResponse,
    RatingBucket,
    RatingDistribution,
    ReviewDynamics,
    ReviewDynamicsPoint,
    ReviewListItem,
    ReviewResponse,
    ReviewsListResponse,
    ScrapeRequest,
    SearchTaskResponse,
    SentimentBreakdown,
    TaskResultResponse,
    TaskStatusResponse,
    TopMention,
)
from .scraper import SITE_BASE, scrape_branch_preview, search_branches
from .tasks import run_scrape_task
from .topics import ReviewDoc, extract_topics, extract_topics_bert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# Константы для sentiment — синхронизированы со схемами
_SENT_POS_MIN = 4
_SENT_NEG_MAX = 2
_OVERVIEW_TZ = "Asia/Almaty"

# /overview is relatively heavy (multiple SQL + NLP/AI). Cache identical requests briefly.
_OVERVIEW_CACHE_TTL_S = 15.0
_overview_cache: dict[tuple[str, str, int, int | None], tuple[float, OverviewResponse]] = {}
_overview_inflight: dict[tuple[str, str, int, int | None], asyncio.Task[OverviewResponse]] = {}
_overview_lock = asyncio.Lock()

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

    reviews_total_stmt = (
        select(func.coalesce(func.sum(Branch.total_reviews), 0))
        .select_from(Branch)
        .join(SearchTaskBranch, SearchTaskBranch.branch_id == Branch.id)
        .where(SearchTaskBranch.task_id == task_id)
    )
    reviews_total = (await session.execute(reviews_total_stmt)).scalar_one()

    return TaskStatusResponse(
        task_id=task.id,
        status=task.status.value,
        query=task.query,
        city=task.city,
        total_branches_found=task.total_branches_found,
        branches_completed=task.branches_completed,
        total_reviews_collected=task.total_reviews_collected,
        reviews_total=int(reviews_total or 0),
        reviews_parsed=task.total_reviews_collected,
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
    days: int | None = Query(
        None,
        ge=1,
        alias="params",
        description="If set, all analytics are computed using reviews from the last N days (by date_created).",
    ),
    session: AsyncSession = Depends(get_session),
):
    cache_key = (str(task_id), "overview", int(top_branches_limit), int(days) if days is not None else None)
    now = monotonic()

    async with _overview_lock:
        cached = _overview_cache.get(cache_key)
        if cached is not None:
            exp, payload = cached
            if exp > now:
                return payload
            _overview_cache.pop(cache_key, None)

        inflight = _overview_inflight.get(cache_key)
        if inflight is not None:
            return await inflight

        async def _compute() -> OverviewResponse:
            task = await _require_task(task_id, session)
            since: datetime | None = None
            if days is not None:
                since = datetime.now(timezone.utc) - timedelta(days=int(days))

            # Филиалы задачи + краткие метрики. Одной выборкой.
            branches_stmt = (
                select(Branch)
                .join(SearchTaskBranch, SearchTaskBranch.branch_id == Branch.id)
                .where(SearchTaskBranch.task_id == task_id)
            )
            branches = (await session.execute(branches_stmt)).scalars().all()
            branches_total = len(branches)

            # Aggregate по review-таблице одной SQL — эффективнее чем по-python.
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
            if since is not None:
                agg_stmt = agg_stmt.where(Review.date_created.isnot(None)).where(Review.date_created >= since)
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

            rating_stmt = (
                select(Review.rating, func.count().label("cnt"))
                .select_from(Review)
                .join(SearchTaskBranch, SearchTaskBranch.branch_id == Review.branch_id)
                .where(SearchTaskBranch.task_id == task_id)
                .where(Review.rating.in_([1, 2, 3, 4, 5]))
                .group_by(Review.rating)
            )
            if since is not None:
                rating_stmt = rating_stmt.where(Review.date_created.isnot(None)).where(Review.date_created >= since)
            rating_rows = (await session.execute(rating_stmt)).all()
            counts: dict[int, int] = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
            for r in rating_rows:
                if r.rating in counts:
                    counts[int(r.rating)] = int(r.cnt or 0)

            total_rated = sum(counts.values())

            def _pct_int(n: int, d: int) -> int:
                if d <= 0:
                    return 0
                return int(round(n * 100.0 / d))

            stars_payload: dict[str, RatingBucket] = {
                str(star): RatingBucket(count=counts[star], pct=_pct_int(counts[star], total_rated))
                for star in (1, 2, 3, 4, 5)
            }
            one_two_count = counts[1] + counts[2]
            rating_distribution = RatingDistribution(
                total_rated=total_rated,
                stars=stars_payload,
                one_two=RatingBucket(count=one_two_count, pct=_pct_int(one_two_count, total_rated)),
            )

            sorted_branches: list[Branch] = []
            if since is None:
                sorted_branches = sorted(
                    branches,
                    key=lambda b: (b.rating if b.rating is not None else -1.0, b.total_reviews or 0),
                    reverse=True,
                )[:top_branches_limit]

            overview_branches = [
                OverviewBranchItem(
                    branch_id=b.id,
                    gis_branch_id=str(b.gis_branch_id),
                    name=(b.name or "").strip() or f"Филиал {b.gis_branch_id}",
                    city=task.city,
                    address=(b.address or "").strip() or "Адрес не найден",
                    district=None,
                    lat=None,
                    lng=None,
                    url=b.url if getattr(b, "url", None) else None,
                )
                for b in branches
            ]

            branch_ratings: list[BranchRatingSummary] = []
            if since is None:
                branch_ratings = [
                    BranchRatingSummary(
                        branch_id=b.id,
                        name=b.name,
                        rating=b.rating,
                        total_reviews=b.total_reviews or 0,
                    )
                    for b in sorted_branches
                ]

            reviews_dicts = await _fetch_reviews_as_dicts(task_id, session, since=since)
            top_problems, top_praise = await claude_service.generate_top_mentions(reviews_dicts)

            if not top_problems and not top_praise:
                topic_docs = [ReviewDoc(id=str(i), text=r["text"], rating=r["rating"]) for i, r in enumerate(reviews_dicts)]
                loop = asyncio.get_running_loop()
                try:
                    raw_problems, raw_praise = await loop.run_in_executor(None, _run_topic_extraction_bert, topic_docs)
                except Exception:
                    logging.exception("BERT topic extraction failed, falling back to TF-IDF topics")
                    raw_problems, raw_praise = await loop.run_in_executor(None, _run_topic_extraction, topic_docs)
                top_problems = [TopMention(label=t.label, mentions=t.mentions, examples=t.examples) for t in raw_problems]
                top_praise = [TopMention(label=t.label, mentions=t.mentions, examples=t.examples) for t in raw_praise]

            if days is not None and int(days) <= 31:
                # Daily buckets (zero-filled), keyed by local date in _OVERVIEW_TZ.
                review_dynamics_result = await session.execute(
                    text(
                        """
WITH days AS (
  SELECT generate_series(
    (timezone(:tz, now())::date - (:range_days - 1)),
    timezone(:tz, now())::date,
    interval '1 day'
  )::date AS day
),
scoped_reviews AS (
  SELECT
    (timezone(:tz, r.date_created))::date AS day,
    r.rating
  FROM reviews r
  JOIN search_task_branches stb ON stb.branch_id = r.branch_id
  WHERE stb.task_id = :task_id
    AND r.date_created IS NOT NULL
    AND (timezone(:tz, r.date_created))::date BETWEEN
      (timezone(:tz, now())::date - (:range_days - 1)) AND timezone(:tz, now())::date
),
agg AS (
  SELECT
    r.day,
    count(*) AS all,
    count(*) FILTER (WHERE r.rating IS NOT NULL AND r.rating >= 4) AS pos,
    count(*) FILTER (WHERE r.rating IS NOT NULL AND r.rating <= 2) AS neg,
    count(*) FILTER (WHERE r.rating IS NOT NULL AND r.rating = 3) AS neu
  FROM scoped_reviews r
  GROUP BY 1
)
SELECT
  to_char(d.day, 'YYYY-MM-DD') AS date,
  coalesce(a.all, 0) AS all,
  coalesce(a.pos, 0) AS pos,
  coalesce(a.neg, 0) AS neg,
  coalesce(a.neu, 0) AS neu
FROM days d
LEFT JOIN agg a USING (day)
ORDER BY d.day ASC
"""
                    ),
                    {"task_id": task_id, "tz": _OVERVIEW_TZ, "range_days": int(days)},
                )

                review_dynamics_points = [
                    ReviewDynamicsPoint(
                        date=str(r.date),
                        all=int(r.all or 0),
                        pos=int(r.pos or 0),
                        neg=int(r.neg or 0),
                        neu=int(r.neu or 0),
                    )
                    for r in review_dynamics_result.mappings().all()
                ]
                review_dynamics = ReviewDynamics(
                    range="last_days",
                    range_days=int(days),
                    timezone=_OVERVIEW_TZ,
                    granularity="day",
                    points=review_dynamics_points,
                )
            else:
                # Monthly buckets (12 months, zero-filled). If params is provided, still filtered by :since window.
                review_dynamics_result = await session.execute(
                    text(
                        """
WITH months AS (
  SELECT generate_series(
    date_trunc('month', timezone(:tz, now())) - interval '11 months',
    date_trunc('month', timezone(:tz, now())),
    interval '1 month'
  ) AS month_start
),
scoped_reviews AS (
  SELECT r.date_created, r.rating
  FROM reviews r
  JOIN search_task_branches stb ON stb.branch_id = r.branch_id
  WHERE stb.task_id = :task_id
    AND r.date_created IS NOT NULL
    AND (:since IS NULL OR r.date_created >= :since)
),
agg AS (
  SELECT
    date_trunc('month', timezone(:tz, r.date_created)) AS month_start,
    count(*) AS all,
    count(*) FILTER (WHERE r.rating IS NOT NULL AND r.rating >= 4) AS pos,
    count(*) FILTER (WHERE r.rating IS NOT NULL AND r.rating <= 2) AS neg,
    count(*) FILTER (WHERE r.rating IS NOT NULL AND r.rating = 3) AS neu
  FROM scoped_reviews r
  GROUP BY 1
)
SELECT
  to_char(m.month_start, 'YYYY-MM') AS month,
  coalesce(a.all, 0) AS all,
  coalesce(a.pos, 0) AS pos,
  coalesce(a.neg, 0) AS neg,
  coalesce(a.neu, 0) AS neu
FROM months m
LEFT JOIN agg a USING (month_start)
ORDER BY m.month_start ASC
"""
                    ).bindparams(bindparam("since", type_=DateTime(timezone=True))),
                    {"task_id": task_id, "tz": _OVERVIEW_TZ, "since": since},
                )

                review_dynamics_points = [
                    ReviewDynamicsPoint(
                        month=str(r.month),
                        all=int(r.all or 0),
                        pos=int(r.pos or 0),
                        neg=int(r.neg or 0),
                        neu=int(r.neu or 0),
                    )
                    for r in review_dynamics_result.mappings().all()
                ]
                review_dynamics = ReviewDynamics(
                    range="last_12_months",
                    range_days=int(days) if days is not None else None,
                    timezone=_OVERVIEW_TZ,
                    granularity="month",
                    points=review_dynamics_points,
                )

            by_branch: dict[str, OverviewBranchAnalytics] = {}
            if branches:
                per_branch_kpis_result = await session.execute(
                    text(
                        """
SELECT
  r.branch_id AS branch_id,
  count(*) AS reviews_total,
  count(*) FILTER (WHERE r.official_answer_text IS NOT NULL) AS replied,
  avg(r.rating) FILTER (WHERE r.rating IS NOT NULL) AS avg_rating,
  count(*) FILTER (WHERE r.rating IN (1,2,3,4,5)) AS rated_total,
  count(*) FILTER (WHERE r.rating IS NOT NULL AND r.rating <= 2) AS neg
FROM reviews r
JOIN search_task_branches stb ON stb.branch_id = r.branch_id
WHERE stb.task_id = :task_id
  AND (:since IS NULL OR (r.date_created IS NOT NULL AND r.date_created >= :since))
GROUP BY r.branch_id
"""
                    ).bindparams(bindparam("since", type_=DateTime(timezone=True))),
                    {"task_id": task_id, "since": since},
                )
                per_branch_kpis = {str(r["branch_id"]): r for r in per_branch_kpis_result.mappings().all()}

                if since is not None:
                    scored: list[tuple[Branch, float, int]] = []
                    for b in branches:
                        k = per_branch_kpis.get(str(b.id))
                        avg_rating_b = float(k["avg_rating"]) if (k and k["avg_rating"] is not None) else -1.0
                        reviews_total_b = int((k["reviews_total"] if k else 0) or 0)
                        scored.append((b, avg_rating_b, reviews_total_b))
                    scored.sort(key=lambda x: (x[1], x[2]), reverse=True)
                    branch_ratings = [
                        BranchRatingSummary(
                            branch_id=b.id,
                            name=b.name,
                            rating=(round(avg, 2) if avg >= 0 else None),
                            total_reviews=reviews_total,
                        )
                        for (b, avg, reviews_total) in scored[:top_branches_limit]
                    ]

                per_branch_rating_result = await session.execute(
                    text(
                        """
SELECT r.branch_id AS branch_id, r.rating AS rating, count(*) AS cnt
FROM reviews r
JOIN search_task_branches stb ON stb.branch_id = r.branch_id
WHERE stb.task_id = :task_id
  AND r.rating IN (1,2,3,4,5)
  AND (:since IS NULL OR (r.date_created IS NOT NULL AND r.date_created >= :since))
GROUP BY r.branch_id, r.rating
"""
                    ).bindparams(bindparam("since", type_=DateTime(timezone=True))),
                    {"task_id": task_id, "since": since},
                )
                per_branch_star_counts: dict[str, dict[int, int]] = {}
                for rr in per_branch_rating_result.mappings().all():
                    bid = str(rr["branch_id"])
                    star = int(rr["rating"])
                    per_branch_star_counts.setdefault(bid, {1: 0, 2: 0, 3: 0, 4: 0, 5: 0})
                    per_branch_star_counts[bid][star] = int(rr["cnt"] or 0)

                if days is not None and int(days) <= 31:
                    per_branch_dyn_result = await session.execute(
                        text(
                            """
WITH task_branches AS (
  SELECT stb.branch_id
  FROM search_task_branches stb
  WHERE stb.task_id = :task_id
),
days AS (
  SELECT generate_series(
    (timezone(:tz, now())::date - (:range_days - 1)),
    timezone(:tz, now())::date,
    interval '1 day'
  )::date AS day
),
grid AS (
  SELECT tb.branch_id, d.day
  FROM task_branches tb
  CROSS JOIN days d
),
agg AS (
  SELECT
    r.branch_id,
    (timezone(:tz, r.date_created))::date AS day,
    count(*) AS all,
    count(*) FILTER (WHERE r.rating IS NOT NULL AND r.rating >= 4) AS pos,
    count(*) FILTER (WHERE r.rating IS NOT NULL AND r.rating <= 2) AS neg,
    count(*) FILTER (WHERE r.rating IS NOT NULL AND r.rating = 3) AS neu
  FROM reviews r
  JOIN search_task_branches stb ON stb.branch_id = r.branch_id
  WHERE stb.task_id = :task_id
    AND r.date_created IS NOT NULL
    AND (timezone(:tz, r.date_created))::date BETWEEN
      (timezone(:tz, now())::date - (:range_days - 1)) AND timezone(:tz, now())::date
  GROUP BY r.branch_id, day
)
SELECT
  g.branch_id AS branch_id,
  to_char(g.day, 'YYYY-MM-DD') AS date,
  coalesce(a.all, 0) AS all,
  coalesce(a.pos, 0) AS pos,
  coalesce(a.neg, 0) AS neg,
  coalesce(a.neu, 0) AS neu
FROM grid g
LEFT JOIN agg a
  ON a.branch_id = g.branch_id AND a.day = g.day
ORDER BY g.branch_id, g.day ASC
"""
                        ),
                        {"task_id": task_id, "tz": _OVERVIEW_TZ, "range_days": int(days)},
                    )
                else:
                    per_branch_dyn_result = await session.execute(
                        text(
                            """
WITH task_branches AS (
  SELECT stb.branch_id
  FROM search_task_branches stb
  WHERE stb.task_id = :task_id
),
months AS (
  SELECT generate_series(
    date_trunc('month', timezone(:tz, now())) - interval '11 months',
    date_trunc('month', timezone(:tz, now())),
    interval '1 month'
  ) AS month_start
),
grid AS (
  SELECT tb.branch_id, m.month_start
  FROM task_branches tb
  CROSS JOIN months m
),
agg AS (
  SELECT
    r.branch_id,
    date_trunc('month', timezone(:tz, r.date_created)) AS month_start,
    count(*) AS all,
    count(*) FILTER (WHERE r.rating IS NOT NULL AND r.rating >= 4) AS pos,
    count(*) FILTER (WHERE r.rating IS NOT NULL AND r.rating <= 2) AS neg,
    count(*) FILTER (WHERE r.rating IS NOT NULL AND r.rating = 3) AS neu
  FROM reviews r
  JOIN search_task_branches stb ON stb.branch_id = r.branch_id
  WHERE stb.task_id = :task_id
    AND r.date_created IS NOT NULL
    AND (:since IS NULL OR r.date_created >= :since)
  GROUP BY r.branch_id, month_start
)
SELECT
  g.branch_id AS branch_id,
  to_char(g.month_start, 'YYYY-MM') AS month,
  coalesce(a.all, 0) AS all,
  coalesce(a.pos, 0) AS pos,
  coalesce(a.neg, 0) AS neg,
  coalesce(a.neu, 0) AS neu
FROM grid g
LEFT JOIN agg a
  ON a.branch_id = g.branch_id AND a.month_start = g.month_start
ORDER BY g.branch_id, g.month_start ASC
"""
                        ).bindparams(bindparam("since", type_=DateTime(timezone=True))),
                        {"task_id": task_id, "tz": _OVERVIEW_TZ, "since": since},
                    )

                per_branch_points: dict[str, list[ReviewDynamicsPoint]] = {}
                for rr in per_branch_dyn_result.mappings().all():
                    bid = str(rr["branch_id"])
                    per_branch_points.setdefault(bid, [])
                    if days is not None and int(days) <= 31:
                        per_branch_points[bid].append(
                            ReviewDynamicsPoint(
                                date=str(rr["date"]),
                                all=int(rr["all"] or 0),
                                pos=int(rr["pos"] or 0),
                                neg=int(rr["neg"] or 0),
                                neu=int(rr["neu"] or 0),
                            )
                        )
                    else:
                        per_branch_points[bid].append(
                            ReviewDynamicsPoint(
                                month=str(rr["month"]),
                                all=int(rr["all"] or 0),
                                pos=int(rr["pos"] or 0),
                                neg=int(rr["neg"] or 0),
                                neu=int(rr["neu"] or 0),
                            )
                        )

                for b in branches:
                    bid = str(b.id)
                    k = per_branch_kpis.get(bid)
                    reviews_total_b = int((k["reviews_total"] if k else 0) or 0)
                    replied_b = int((k["replied"] if k else 0) or 0)
                    rated_total_b = int((k["rated_total"] if k else 0) or 0)
                    neg_b = int((k["neg"] if k else 0) or 0)
                    avg_rating_b = float(k["avg_rating"]) if (k and k["avg_rating"] is not None) else None

                    branch_kpis = BranchKPIs(
                        avg_rating=round(avg_rating_b, 2) if avg_rating_b is not None else None,
                        reviews_total=reviews_total_b,
                        negative_pct=_pct_int(neg_b, rated_total_b),
                        replies_pct=_pct_int(replied_b, reviews_total_b),
                    )

                    star_counts = per_branch_star_counts.get(bid, {1: 0, 2: 0, 3: 0, 4: 0, 5: 0})
                    total_rated_b = sum(star_counts.values())
                    stars_payload_b: dict[str, RatingBucket] = {
                        str(star): RatingBucket(count=star_counts[star], pct=_pct_int(star_counts[star], total_rated_b))
                        for star in (1, 2, 3, 4, 5)
                    }
                    one_two_b = star_counts[1] + star_counts[2]
                    branch_rating_distribution = RatingDistribution(
                        total_rated=total_rated_b,
                        stars=stars_payload_b,
                        one_two=RatingBucket(count=one_two_b, pct=_pct_int(one_two_b, total_rated_b)),
                    )

                    branch_review_dynamics = ReviewDynamics(
                        range=("last_days" if (days is not None and int(days) <= 31) else "last_12_months"),
                        range_days=int(days) if days is not None else None,
                        timezone=_OVERVIEW_TZ,
                        granularity=("day" if (days is not None and int(days) <= 31) else "month"),
                        points=per_branch_points.get(bid, []),
                    )

                    by_branch[bid] = OverviewBranchAnalytics(
                        kpis=branch_kpis,
                        rating_distribution=branch_rating_distribution,
                        review_dynamics=branch_review_dynamics,
                        top_praise=[],
                        top_problems=[],
                    )

            return OverviewResponse(
                task_id=task.id,
                status=task.status.value,
                query=task.query,
                city=task.city,
                kpis=kpis,
                sentiment=sentiment,
                rating_distribution=rating_distribution,
                branches=overview_branches,
                branch_ratings=branch_ratings,
                top_problems=top_problems,
                top_praise=top_praise,
                analytics_note=None,
                review_dynamics=review_dynamics,
                by_branch=by_branch,
            )

        t = asyncio.create_task(_compute())
        _overview_inflight[cache_key] = t

    try:
        payload = await t
    finally:
        async with _overview_lock:
            _overview_inflight.pop(cache_key, None)

    async with _overview_lock:
        _overview_cache[cache_key] = (monotonic() + _OVERVIEW_CACHE_TTL_S, payload)
    return payload


def _run_topic_extraction(docs):
    """Вызывается из threadpool. Отдельная функция чтобы мокать в тестах."""
    return extract_topics(docs, top_n=8, min_mentions=3)


def _run_topic_extraction_bert(docs):
    """BERT embeddings fallback (threadpool)."""
    return extract_topics_bert(docs, top_n=8, min_mentions=3)


async def _fetch_reviews_as_dicts(
    task_id: UUID, session: AsyncSession, since: datetime | None = None
) -> list[dict]:
    """Returns all reviews for the task as plain dicts with 'rating' and 'text' keys."""
    stmt = (
        select(Review.rating, Review.text)
        .join(SearchTaskBranch, SearchTaskBranch.branch_id == Review.branch_id)
        .where(SearchTaskBranch.task_id == task_id)
        .where(Review.text.isnot(None))
        .where(Review.text != "")
    )
    if since is not None:
        stmt = stmt.where(Review.date_created.isnot(None)).where(Review.date_created >= since)
    rows = (await session.execute(stmt)).all()
    return [{"rating": r.rating, "text": r.text} for r in rows]

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
    days: int | None = Query(
        None,
        ge=1,
        alias="params",
        description="If set, rating/total_reviews/replies_pct/rating_distribution are computed using reviews from the last N days (by date_created).",
    ),
    session: AsyncSession = Depends(get_session),
):
    task = await _require_task(task_id, session)
    since: datetime | None = None
    if days is not None:
        since = datetime.now(timezone.utc) - timedelta(days=int(days))

    # Подзапрос: агрегаты по отзывам на каждый филиал (для replies_pct и сортировки по окну)
    replies_sub_q = (
        select(
            Review.branch_id.label("bid"),
            func.count().label("reviews_n"),
            func.count().filter(Review.official_answer_text.isnot(None)).label("replied_n"),
            func.avg(Review.rating).filter(Review.rating.isnot(None)).label("avg_rating"),
        )
    )
    if since is not None:
        replies_sub_q = replies_sub_q.where(Review.date_created.isnot(None)).where(Review.date_created >= since)
    replies_sub = replies_sub_q.group_by(Review.branch_id).subquery()

    if since is None:
        order_cols = _BRANCH_SORT_MAP[sort]
    else:
        if sort == "rating_desc":
            order_cols = (replies_sub.c.avg_rating.desc().nulls_last(), replies_sub.c.reviews_n.desc().nulls_last())
        elif sort == "reviews_desc":
            order_cols = (replies_sub.c.reviews_n.desc().nulls_last(), replies_sub.c.avg_rating.desc().nulls_last())
        else:
            order_cols = (Branch.name.asc().nulls_last(),)

    stmt = (
        select(Branch, replies_sub.c.reviews_n, replies_sub.c.replied_n, replies_sub.c.avg_rating)
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

    rating_dist_by_branch: dict[str, dict] = {}
    if since is not None and rows:
        page_branch_ids = [b.id for (b, _, _, _) in rows]
        stars_stmt = (
            select(Review.branch_id, Review.rating, func.count().label("cnt"))
            .where(Review.branch_id.in_(page_branch_ids))
            .where(Review.rating.in_([1, 2, 3, 4, 5]))
            .where(Review.date_created.isnot(None))
            .where(Review.date_created >= since)
            .group_by(Review.branch_id, Review.rating)
        )
        stars_rows = (await session.execute(stars_stmt)).all()
        tmp: dict[str, dict[int, int]] = {}
        for bid, rating, cnt in stars_rows:
            k = str(bid)
            tmp.setdefault(k, {1: 0, 2: 0, 3: 0, 4: 0, 5: 0})
            if rating is not None and int(rating) in tmp[k]:
                tmp[k][int(rating)] = int(cnt or 0)

        def _pct_int(n: int, d: int) -> int:
            if d <= 0:
                return 0
            return int(round(n * 100.0 / d))

        for k, counts in tmp.items():
            total_rated = sum(counts.values())
            stars_payload = {
                str(star): {"count": counts[star], "pct": _pct_int(counts[star], total_rated)}
                for star in (1, 2, 3, 4, 5)
            }
            one_two = counts[1] + counts[2]
            rating_dist_by_branch[k] = RatingDistribution(
                total_rated=total_rated,
                stars={s: RatingBucket(**v) for s, v in stars_payload.items()},
                one_two=RatingBucket(count=one_two, pct=_pct_int(one_two, total_rated)),
            ).model_dump()

    items: list[BranchListItem] = []
    for b, reviews_n, replied_n, avg_rating in rows:
        reviews_n = reviews_n or 0
        replied_n = replied_n or 0
        items.append(BranchListItem(
            id=b.id,
            gis_branch_id=b.gis_branch_id,  # int → str via BranchIdStr
            name=b.name,
            address=b.address,
            rating=(float(avg_rating) if avg_rating is not None else None) if since is not None else b.rating,
            total_reviews=(int(reviews_n) if since is not None else b.total_reviews),
            url=b.url,
            rating_distribution=rating_dist_by_branch.get(str(b.id)) if since is not None else b.rating_distribution,
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
    days: int | None = Query(
        None,
        ge=1,
        alias="params",
        description="If set, returns only reviews from the last N days (by date_created).",
    ),
    session: AsyncSession = Depends(get_session),
):
    task = await _require_task(task_id, session)
    since: datetime | None = None
    if days is not None:
        since = datetime.now(timezone.utc) - timedelta(days=int(days))

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

    if since is not None:
        where_clauses.append(Review.date_created.isnot(None))
        where_clauses.append(Review.date_created >= since)

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
# Dashboard — /problems
# ---------------------------------------------------------------------------

@app.get(
    "/tasks/{task_id}/problems",
    response_model=ProblemsResponse,
    tags=["dashboard"],
    summary="Агрегированные проблемы из отзывов (Claude AI)",
)
async def get_task_problems(
    task_id: UUID,
    days: int | None = Query(
        None,
        ge=1,
        alias="params",
        description="If set, problems are generated using reviews from the last N days (by date_created).",
    ),
    session: AsyncSession = Depends(get_session),
):
    task = await _require_task(task_id, session)
    since: datetime | None = None
    if days is not None:
        since = datetime.now(timezone.utc) - timedelta(days=int(days))
    reviews = await _fetch_reviews_as_dicts(task_id, session, since=since)
    problems = await claude_service.generate_problems(reviews)
    note = (
        "Generated by Claude AI based on negative/neutral reviews."
        if problems
        else "Not enough data or AI unavailable."
    )
    return ProblemsResponse(
        task_id=task.id,
        status=task.status.value,
        items=problems,
        analytics_note=note,
    )


# ---------------------------------------------------------------------------
# Dashboard — /actions
# ---------------------------------------------------------------------------

@app.get(
    "/tasks/{task_id}/actions",
    response_model=ActionsResponse,
    tags=["dashboard"],
    summary="План действий и инсайты (Claude AI)",
)
async def get_task_actions(
    task_id: UUID,
    days: int | None = Query(
        None,
        ge=1,
        alias="params",
        description="If set, actions are generated using reviews from the last N days (by date_created).",
    ),
    session: AsyncSession = Depends(get_session),
):
    task = await _require_task(task_id, session)
    since: datetime | None = None
    if days is not None:
        since = datetime.now(timezone.utc) - timedelta(days=int(days))
    reviews = await _fetch_reviews_as_dicts(task_id, session, since=since)
    priorities, insights = await claude_service.generate_actions(reviews)
    return ActionsResponse(
        task_id=task.id,
        status=task.status.value,
        priorities=priorities,
        insights=insights,
        analytics_note=(
            "Priorities and insights synthesized by Claude AI." if priorities else None
        ),
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
        # IMPORTANT: Branch.reviews is an async lazy relationship.
        # If we call BranchWithReviewsResponse.model_validate(b) while reviews are not
        # eagerly loaded, Pydantic will try to access `b.reviews` and crash with
        # MissingGreenlet. Only validate the "with reviews" model when reviews are loaded.
        if include_reviews:
            payload = BranchWithReviewsResponse.model_validate(b)
            payload.reviews = [ReviewResponse.model_validate(r) for r in b.reviews]
        else:
            base = BranchResponse.model_validate(b)
            payload = BranchWithReviewsResponse(**base.model_dump(), reviews=[])
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