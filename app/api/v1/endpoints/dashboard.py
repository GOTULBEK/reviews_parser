from datetime import datetime, timedelta, timezone
from time import monotonic
from typing import Literal
from uuid import UUID
import asyncio
import logging

from fastapi import APIRouter, Depends, HTTPException, Query, status
import sqlalchemy as sa
from sqlalchemy import DateTime, and_, bindparam, case, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.database import get_session
from app.models.tasks import SearchTask, TaskStatus, SearchTaskBranch, TaskTopicsCache
from app.models.core import Branch, Review
from app.schemas.common import SourceType
from app.schemas.dashboard import *
from app.services import claude as claude_service
from app.services.topics import ReviewDoc

from app.api.v1.endpoints.tasks import _require_task
from app.schemas.dashboard import TopicTimeSeries, TopicTimeSeriesPoint

router = APIRouter()

_SENT_POS_MIN = 4
_SENT_NEG_MAX = 2
_OVERVIEW_TZ = "Asia/Almaty"

def _pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator * 100.0 / denominator, 1)

def _sentiment_from_rating(rating: int | None) -> Literal["pos", "neg", "neu", "unknown"]:
    if rating is None:
        return "unknown"
    if rating >= _SENT_POS_MIN:
        return "pos"
    if rating <= _SENT_NEG_MAX:
        return "neg"
    return "neu"

_OVERVIEW_CACHE_TTL_S = 15.0
_overview_cache = {}
_overview_inflight = {}
_overview_lock = asyncio.Lock()

from app.services.topics import extract_topics, extract_topics_embeddings

_TOPIC_STOP = {"и", "в", "на", "с", "за", "по", "к", "о", "из", "от", "для", "при", "что"}


def _topic_keywords(label: str) -> list[str]:
    words = [w.strip(".,!?-") for w in label.lower().split()]
    kws = [w for w in words if len(w) > 3 and w not in _TOPIC_STOP]
    return kws if kws else ([words[0]] if words else [label.lower()])


async def _compute_topic_timeseries(
    task_id: UUID,
    topic_bars: list,
    session: AsyncSession,
    source: str,
) -> list[dict]:
    """Single DB fetch → keyword matching per topic → monthly pos/neg counts."""
    rows = (
        await session.execute(
            text("""
                SELECT
                    to_char(date_trunc('month', timezone(:tz, r.date_created)), 'YYYY-MM') AS month,
                    lower(r.text) AS text,
                    r.rating
                FROM reviews r
                JOIN search_task_branches stb ON stb.branch_id = r.branch_id
                JOIN branches b ON b.id = r.branch_id
                WHERE stb.task_id = :task_id
                  AND r.date_created IS NOT NULL
                  AND r.text IS NOT NULL
                  AND r.text != ''
                  AND (:source IS NULL OR b.source = :source)
                  AND r.date_created >= (now() - interval '12 months')
            """).bindparams(
                bindparam("tz", type_=sa.String()),
                bindparam("source", type_=sa.String()),
            ),
            {
                "task_id": task_id,
                "tz": _OVERVIEW_TZ,
                "source": source if source and source != "all" else None,
            },
        )
    ).all()

    result = []
    for bar in topic_bars:
        kws = _topic_keywords(bar.label if hasattr(bar, "label") else bar["label"])
        monthly: dict[str, dict[str, int]] = {}
        for row in rows:
            if not any(kw in (row.text or "") for kw in kws):
                continue
            m = row.month
            if m not in monthly:
                monthly[m] = {"positive": 0, "negative": 0}
            if row.rating is not None:
                if row.rating >= 4:
                    monthly[m]["positive"] += 1
                elif row.rating <= 2:
                    monthly[m]["negative"] += 1
        result.append(
            {
                "label": bar.label if hasattr(bar, "label") else bar["label"],
                "monthly": [
                    {"month": m, "positive": v["positive"], "negative": v["negative"]}
                    for m, v in sorted(monthly.items())
                ],
            }
        )
    return result


def _run_topic_extraction(docs):
    return extract_topics(docs, top_n=8, min_mentions=3)

def _run_topic_extraction_embeddings(docs):
    return extract_topics_embeddings(docs, top_n=8, min_mentions=3)

async def _fetch_reviews_as_dicts(
    task_id: UUID, session: AsyncSession, since: datetime | None = None, source: Literal["2gis", "zapis", "all"] = "2gis"
) -> list[dict]:
    stmt = (
        select(Review.rating, Review.text)
        .join(SearchTaskBranch, SearchTaskBranch.branch_id == Review.branch_id)
        .join(Branch, Branch.id == Review.branch_id)
        .where(SearchTaskBranch.task_id == task_id)
        .where(Review.text.isnot(None))
        .where(Review.text != "")
    )
    if since is not None:
        stmt = stmt.where(Review.date_created.isnot(None)).where(Review.date_created >= since)
    if source is not None and source != "all":
        stmt = stmt.where(Branch.source == source)
    rows = (await session.execute(stmt)).all()
    return [{"rating": r.rating, "text": r.text} for r in rows]


async def _load_task_top_mentions(
    task_id: UUID, session: AsyncSession
) -> tuple[list[dict], list[dict]]:
    """Look up cached top_problems/top_praise for the task across all (task_id, days)
    rows. Returns the first populated pair found. Never calls Claude — the caller
    decides whether to generate when missing."""
    stmt = (
        select(TaskTopicsCache.top_problems, TaskTopicsCache.top_praise, TaskTopicsCache.days)
        .where(TaskTopicsCache.task_id == task_id)
        .where(
            or_(
                TaskTopicsCache.top_problems.isnot(None),
                TaskTopicsCache.top_praise.isnot(None),
            )
        )
        .order_by(TaskTopicsCache.days.is_(None).desc(), TaskTopicsCache.days.asc())
    )
    rows = (await session.execute(stmt)).all()
    top_problems: list[dict] = []
    top_praise: list[dict] = []
    for r in rows:
        if not top_problems and r.top_problems:
            top_problems = list(r.top_problems)
        if not top_praise and r.top_praise:
            top_praise = list(r.top_praise)
        if top_problems and top_praise:
            break
    return top_problems, top_praise


async def _fetch_reviews_with_dates(
    task_id: UUID, session: AsyncSession, since: datetime | None = None, source: Literal["2gis", "zapis", "all"] = "2gis"
) -> list[dict]:
    stmt = (
        select(Review.rating, Review.text, Review.date_created)
        .join(SearchTaskBranch, SearchTaskBranch.branch_id == Review.branch_id)
        .join(Branch, Branch.id == Review.branch_id)
        .where(SearchTaskBranch.task_id == task_id)
        .where(Review.text.isnot(None))
        .where(Review.text != "")
    )
    if since is not None:
        stmt = stmt.where(Review.date_created.isnot(None)).where(Review.date_created >= since)
    if source is not None and source != "all":
        stmt = stmt.where(Branch.source == source)
    stmt = stmt.order_by(Review.date_created.desc().nulls_last())
    rows = (await session.execute(stmt)).all()
    return [
        {"rating": r.rating, "text": r.text, "date_created": r.date_created}
        for r in rows
    ]

# Dashboard — /overview
# ---------------------------------------------------------------------------

@router.get(
    "/{task_id}/overview",
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
    source: Literal["2gis", "zapis", "all"] = Query("2gis", description="Filter by data source"),
    session: AsyncSession = Depends(get_session),
):
    cache_key = (str(task_id), "overview", int(top_branches_limit), int(days) if days is not None else None, source if source and source != "all" else None)
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
            if source is not None and source != "all":
                branches_stmt = branches_stmt.where(Branch.source == source)
                
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
                .join(Branch, Branch.id == Review.branch_id)
                .where(SearchTaskBranch.task_id == task_id)
            )
            if since is not None:
                agg_stmt = agg_stmt.where(Review.date_created.isnot(None)).where(Review.date_created >= since)
            if source is not None and source != "all":
                agg_stmt = agg_stmt.where(Branch.source == source)
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
                .join(Branch, Branch.id == Review.branch_id)
                .where(SearchTaskBranch.task_id == task_id)
                .where(Review.rating.in_([1, 2, 3, 4, 5]))
                .group_by(Review.rating)
            )
            if since is not None:
                rating_stmt = rating_stmt.where(Review.date_created.isnot(None)).where(Review.date_created >= since)
            if source is not None and source != "all":
                rating_stmt = rating_stmt.where(Branch.source == source)
                
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
                    source=b.source,
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

            from app.models.tasks import TaskTopicsCache
            cache_stmt = (
                select(TaskTopicsCache)
                .where(
                    TaskTopicsCache.task_id == task_id,
                    TaskTopicsCache.days == (int(days) if days is not None else None),
                )
                .order_by(TaskTopicsCache.id.desc())
                .limit(1)
            )
            topics_cache_row = (await session.execute(cache_stmt)).scalars().first()

            top_problems: list[TopMention] = []
            top_praise: list[TopMention] = []

            if topics_cache_row is not None and (topics_cache_row.top_problems or topics_cache_row.top_praise):
                top_problems = [TopMention(**t) for t in (topics_cache_row.top_problems or []) if isinstance(t, dict)]
                top_praise = [TopMention(**t) for t in (topics_cache_row.top_praise or []) if isinstance(t, dict)]
            elif topics_cache_row is not None and topics_cache_row.topics_module:
                # /topics was already called — derive top-5 from the shared cache, no extra Claude call.
                _module = topics_cache_row.topics_module
                top_problems = [
                    TopMention(label=t["label"], mentions=t.get("mentions", 0), examples=[])
                    for t in (_module.get("top_negative") or [])[:5]
                    if isinstance(t, dict)
                ]
                top_praise = [
                    TopMention(label=t["label"], mentions=t.get("mentions", 0), examples=[])
                    for t in (_module.get("top_positive") or [])[:5]
                    if isinstance(t, dict)
                ]
            else:
                # Reuse top_mentions cached on any (task_id, *) row before paying for Claude again.
                shared_problems, shared_praise = await _load_task_top_mentions(task_id, session)
                if shared_problems or shared_praise:
                    top_problems = [TopMention(**t) for t in shared_problems if isinstance(t, dict)]
                    top_praise = [TopMention(**t) for t in shared_praise if isinstance(t, dict)]

                if not top_problems and not top_praise:
                    reviews_dicts = await _fetch_reviews_as_dicts(task_id, session, since=since, source=source)
                    top_problems, top_praise = await claude_service.generate_top_mentions(reviews_dicts)

                    if not top_problems and not top_praise:
                        topic_docs = [ReviewDoc(id=str(i), text=r["text"], rating=r["rating"]) for i, r in enumerate(reviews_dicts)]
                        loop = asyncio.get_running_loop()
                        try:
                            raw_problems, raw_praise = await loop.run_in_executor(None, _run_topic_extraction_embeddings, topic_docs)
                        except Exception:
                            logging.exception("Embeddings topic extraction failed, falling back to TF-IDF topics")
                            raw_problems, raw_praise = await loop.run_in_executor(None, _run_topic_extraction, topic_docs)
                        top_problems = [TopMention(label=t.label, mentions=t.mentions, examples=t.examples) for t in raw_problems]
                        top_praise = [TopMention(label=t.label, mentions=t.mentions, examples=t.examples) for t in raw_praise]

                if topics_cache_row is None:
                    topics_cache_row = TaskTopicsCache(
                        task_id=task_id,
                        days=int(days) if days is not None else None,
                        top_problems=[t.model_dump() for t in top_problems] or None,
                        top_praise=[t.model_dump() for t in top_praise] or None,
                    )
                    session.add(topics_cache_row)
                else:
                    if not topics_cache_row.top_problems and top_problems:
                        topics_cache_row.top_problems = [t.model_dump() for t in top_problems]
                    if not topics_cache_row.top_praise and top_praise:
                        topics_cache_row.top_praise = [t.model_dump() for t in top_praise]
                try:
                    await session.commit()
                except Exception:
                    await session.rollback()
                    logging.exception("Failed to save topics cache")

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
                    {"task_id": task_id, "tz": _OVERVIEW_TZ, "range_days": int(days), "source": source if source and source != "all" else None},
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
                    {"task_id": task_id, "tz": _OVERVIEW_TZ, "since": since, "source": source if source and source != "all" else None},
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
                        {"task_id": task_id, "tz": _OVERVIEW_TZ, "range_days": int(days), "source": source if source and source != "all" else None},
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
                        {"task_id": task_id, "tz": _OVERVIEW_TZ, "since": since, "source": source if source and source != "all" else None},
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


def _run_topic_extraction_embeddings(docs):
    """Sentence-embeddings fallback (threadpool)."""
    return extract_topics_embeddings(docs, top_n=8, min_mentions=3)


# removed duplicate _fetch_reviews_as_dicts

# ---------------------------------------------------------------------------
# Dashboard — /branches
# ---------------------------------------------------------------------------

_BRANCH_SORT_MAP = {
    "rating_desc": (Branch.rating.desc().nulls_last(), Branch.total_reviews.desc().nulls_last()),
    "reviews_desc": (Branch.total_reviews.desc().nulls_last(), Branch.rating.desc().nulls_last()),
    "name_asc": (Branch.name.asc().nulls_last(),),
}


@router.get(
    "/{task_id}/branches",
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
    source: Literal["2gis", "zapis", "all"] = Query("2gis", description="Filter by data source"),
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

    if source is not None and source != "all":
        stmt = stmt.where(Branch.source == source)
        count_stmt = count_stmt.where(Branch.source == source)

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
            source=b.source,
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

@router.get(
    "/{task_id}/reviews",
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
    source: Literal["2gis", "zapis", "all"] = Query("2gis", description="Filter by data source"),
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
        
    if source is not None and source != "all":
        where_clauses.append(Branch.source == source)

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

@router.get(
    "/{task_id}/problems",
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
    source: Literal["2gis", "zapis", "all"] = Query("2gis", description="Filter by data source"),
    session: AsyncSession = Depends(get_session),
):
    task = await _require_task(task_id, session)
    since: datetime | None = None
    if days is not None:
        since = datetime.now(timezone.utc) - timedelta(days=int(days))
        
    cache_stmt = (
        select(TaskTopicsCache)
        .where(
            TaskTopicsCache.task_id == task_id,
            TaskTopicsCache.days == (int(days) if days is not None else None),
        )
        .order_by(TaskTopicsCache.id.desc())
        .limit(1)
    )
    topics_cache_row = (await session.execute(cache_stmt)).scalars().first()

    if topics_cache_row is not None and topics_cache_row.problems is not None:
        problems = [ProblemItem(**p) for p in topics_cache_row.problems]
    else:
        reviews = await _fetch_reviews_as_dicts(task_id, session, since=since, source=source)
        problems = await claude_service.generate_problems(reviews)
        
        if topics_cache_row is None:
            new_cache = TaskTopicsCache(
                task_id=task_id,
                days=int(days) if days is not None else None,
                problems=[p.model_dump() for p in problems]
            )
            session.add(new_cache)
        else:
            topics_cache_row.problems = [p.model_dump() for p in problems]
            
        try:
            await session.commit()
        except Exception:
            await session.rollback()
            logging.exception("Failed to save problems cache")

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

@router.get(
    "/{task_id}/actions",
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
    source: Literal["2gis", "zapis", "all"] = Query("2gis", description="Filter by data source"),
    session: AsyncSession = Depends(get_session),
):
    task = await _require_task(task_id, session)
    since: datetime | None = None
    if days is not None:
        since = datetime.now(timezone.utc) - timedelta(days=int(days))
        
    cache_stmt = (
        select(TaskTopicsCache)
        .where(
            TaskTopicsCache.task_id == task_id,
            TaskTopicsCache.days == (int(days) if days is not None else None),
        )
        .order_by(TaskTopicsCache.id.desc())
        .limit(1)
    )
    topics_cache_row = (await session.execute(cache_stmt)).scalars().first()

    if topics_cache_row is not None and topics_cache_row.priorities is not None and topics_cache_row.insights is not None:
        priorities = [PriorityItem(**p) for p in topics_cache_row.priorities]
        insights = [InsightItem(**i) for i in topics_cache_row.insights]
    else:
        reviews = await _fetch_reviews_as_dicts(task_id, session, since=since, source=source)
        priorities, insights = await claude_service.generate_actions(reviews)
        
        if topics_cache_row is None:
            new_cache = TaskTopicsCache(
                task_id=task_id,
                days=int(days) if days is not None else None,
                priorities=[p.model_dump() for p in priorities],
                insights=[i.model_dump() for i in insights]
            )
            session.add(new_cache)
        else:
            topics_cache_row.priorities = [p.model_dump() for p in priorities]
            topics_cache_row.insights = [i.model_dump() for i in insights]
            
        try:
            await session.commit()
        except Exception:
            await session.rollback()
            logging.exception("Failed to save actions cache")

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
# Dashboard — /recommendations
# ---------------------------------------------------------------------------

@router.get(
    "/{task_id}/recommendations",
    response_model=RecommendationsResponse,
    tags=["dashboard"],
    summary="Системные рекомендации (Claude AI)",
)
async def get_task_recommendations(
    task_id: UUID,
    days: int | None = Query(
        None,
        ge=1,
        alias="params",
        description="If set, recommendations are generated from reviews of the last N days.",
    ),
    source: Literal["2gis", "zapis", "all"] = Query("2gis", description="Filter by data source"),
    session: AsyncSession = Depends(get_session),
):
    task = await _require_task(task_id, session)
    since: datetime | None = None
    if days is not None:
        since = datetime.now(timezone.utc) - timedelta(days=int(days))

    cache_stmt = (
        select(TaskTopicsCache)
        .where(
            TaskTopicsCache.task_id == task_id,
            TaskTopicsCache.days == (int(days) if days is not None else None),
        )
        .order_by(TaskTopicsCache.id.desc())
        .limit(1)
    )
    cache_row = (await session.execute(cache_stmt)).scalars().first()

    if cache_row is not None and cache_row.recommendations is not None:
        items = [RecommendationItem(**r) for r in cache_row.recommendations]
        return RecommendationsResponse(
            task_id=task.id,
            status=task.status.value,
            items=items,
            analytics_note=("Synthesized by Claude AI." if items else None),
        )

    # Aggregate KPIs (negative_pct / replies_pct / avg_rating / reviews_total) for the same window/source.
    agg_stmt = (
        select(
            func.count().label("total"),
            func.count().filter(Review.rating <= _SENT_NEG_MAX).label("neg"),
            func.count().filter(Review.official_answer_text.isnot(None)).label("replied"),
            func.avg(Review.rating).filter(Review.rating.isnot(None)).label("avg_rating"),
        )
        .select_from(Review)
        .join(SearchTaskBranch, SearchTaskBranch.branch_id == Review.branch_id)
        .join(Branch, Branch.id == Review.branch_id)
        .where(SearchTaskBranch.task_id == task_id)
    )
    if since is not None:
        agg_stmt = agg_stmt.where(Review.date_created.isnot(None)).where(Review.date_created >= since)
    if source is not None and source != "all":
        agg_stmt = agg_stmt.where(Branch.source == source)
    agg_row = (await session.execute(agg_stmt)).one()

    reviews_total = int(agg_row.total or 0)
    kpis = {
        "avg_rating": round(float(agg_row.avg_rating), 2) if agg_row.avg_rating is not None else None,
        "negative_pct": _pct(int(agg_row.neg or 0), reviews_total),
        "replies_pct": _pct(int(agg_row.replied or 0), reviews_total),
        "reviews_total": reviews_total,
    }

    top_problems_raw: list[dict] = []
    top_praise_raw: list[dict] = []
    if cache_row is not None:
        top_problems_raw = list(cache_row.top_problems or [])
        top_praise_raw = list(cache_row.top_praise or [])

    if not top_problems_raw and not top_praise_raw:
        # Try shared top_mentions cache first.
        top_problems_raw, top_praise_raw = await _load_task_top_mentions(task_id, session)

    if not top_problems_raw and not top_praise_raw:
        # Try deriving from topics_module cache (populated by /topics endpoint).
        any_cache = (
            await session.execute(
                select(TaskTopicsCache)
                .where(TaskTopicsCache.task_id == task_id)
                .where(TaskTopicsCache.topics_module.isnot(None))
                .order_by(TaskTopicsCache.id.desc())
                .limit(1)
            )
        ).scalars().first()
        if any_cache and any_cache.topics_module:
            _mod = any_cache.topics_module
            top_problems_raw = [
                {"label": t["label"], "mentions": t.get("mentions", 0), "examples": []}
                for t in (_mod.get("top_negative") or [])[:6]
                if isinstance(t, dict)
            ]
            top_praise_raw = [
                {"label": t["label"], "mentions": t.get("mentions", 0), "examples": []}
                for t in (_mod.get("top_positive") or [])[:6]
                if isinstance(t, dict)
            ]

    if not top_problems_raw and not top_praise_raw:
        # Last resort: generate top mentions fresh from reviews.
        reviews_dicts = await _fetch_reviews_as_dicts(task_id, session, since=since, source=source)
        _problems_list, _praise_list = await claude_service.generate_top_mentions(reviews_dicts)
        top_problems_raw = [t.model_dump() for t in _problems_list]
        top_praise_raw = [t.model_dump() for t in _praise_list]

    items = await claude_service.generate_recommendations(top_problems_raw, top_praise_raw, kpis)

    payload = [i.model_dump() for i in items]
    if cache_row is None:
        new_cache = TaskTopicsCache(
            task_id=task_id,
            days=int(days) if days is not None else None,
            top_problems=top_problems_raw or None,
            top_praise=top_praise_raw or None,
            recommendations=payload,
        )
        session.add(new_cache)
    else:
        cache_row.recommendations = payload
        if not cache_row.top_problems and top_problems_raw:
            cache_row.top_problems = top_problems_raw
        if not cache_row.top_praise and top_praise_raw:
            cache_row.top_praise = top_praise_raw

    try:
        await session.commit()
    except Exception:
        await session.rollback()
        logging.exception("Failed to save recommendations cache")

    return RecommendationsResponse(
        task_id=task.id,
        status=task.status.value,
        items=items,
        analytics_note=("Synthesized by Claude AI." if items else None),
    )


# ---------------------------------------------------------------------------
# Dashboard — /topics (Pro module)
# ---------------------------------------------------------------------------

@router.get(
    "/{task_id}/topics",
    response_model=TopicsModuleResponse,
    tags=["dashboard"],
    summary="Темы отзывов: кластеризация, частые формулировки, тренды",
)
async def get_task_topics_module(
    task_id: UUID,
    days: int | None = Query(
        None,
        ge=1,
        alias="params",
        description="If set, topics are derived from reviews of the last N days.",
    ),
    source: Literal["2gis", "zapis", "all"] = Query("2gis", description="Filter by data source"),
    session: AsyncSession = Depends(get_session),
):
    task = await _require_task(task_id, session)
    since: datetime | None = None
    if days is not None:
        since = datetime.now(timezone.utc) - timedelta(days=int(days))

    cache_stmt = (
        select(TaskTopicsCache)
        .where(
            TaskTopicsCache.task_id == task_id,
            TaskTopicsCache.days == (int(days) if days is not None else None),
        )
        .order_by(TaskTopicsCache.id.desc())
        .limit(1)
    )
    cache_row = (await session.execute(cache_stmt)).scalars().first()

    cached: dict | None = None
    if cache_row is not None and cache_row.topics_module is not None:
        cached = cache_row.topics_module

    # reviews_total for the same window/source (deterministic, cheap)
    count_stmt = (
        select(func.count())
        .select_from(Review)
        .join(SearchTaskBranch, SearchTaskBranch.branch_id == Review.branch_id)
        .join(Branch, Branch.id == Review.branch_id)
        .where(SearchTaskBranch.task_id == task_id)
        .where(Review.text.isnot(None))
        .where(Review.text != "")
    )
    if since is not None:
        count_stmt = count_stmt.where(Review.date_created.isnot(None)).where(Review.date_created >= since)
    if source is not None and source != "all":
        count_stmt = count_stmt.where(Branch.source == source)
    reviews_total = int((await session.execute(count_stmt)).scalar_one() or 0)

    # Monthly average rating (always computed, never via Claude). Last 12 months in TZ.
    monthly_stmt = await session.execute(
        text(
            """
WITH months AS (
  SELECT generate_series(
    date_trunc('month', timezone(:tz, now())) - interval '11 months',
    date_trunc('month', timezone(:tz, now())),
    interval '1 month'
  ) AS month_start
),
agg AS (
  SELECT
    date_trunc('month', timezone(:tz, r.date_created)) AS month_start,
    avg(r.rating) FILTER (WHERE r.rating IS NOT NULL) AS avg_rating
  FROM reviews r
  JOIN search_task_branches stb ON stb.branch_id = r.branch_id
  JOIN branches b ON b.id = r.branch_id
  WHERE stb.task_id = :task_id
    AND r.date_created IS NOT NULL
    AND (:source IS NULL OR b.source = :source)
  GROUP BY 1
)
SELECT
  to_char(m.month_start, 'YYYY-MM') AS month,
  a.avg_rating
FROM months m
LEFT JOIN agg a USING (month_start)
ORDER BY m.month_start ASC
"""
        ).bindparams(
            bindparam("tz", type_=sa.String()),
            bindparam("source", type_=sa.String()),
        ),
        {"task_id": task_id, "tz": _OVERVIEW_TZ, "source": source if source and source != "all" else None},
    )
    monthly_avg_rating = [
        MonthlyAvgRatingPoint(
            month=str(r.month),
            avg_rating=round(float(r.avg_rating), 2) if r.avg_rating is not None else None,
        )
        for r in monthly_stmt.all()
    ]

    if cached is not None:
        topic_bars = [TopicBarItem(**t) for t in (cached.get("topic_bars") or [])]
        top_positive = [TopicListItem(**t) for t in (cached.get("top_positive") or [])]
        top_negative = [TopicListItem(**t) for t in (cached.get("top_negative") or [])]
        frequent_phrases = list(cached.get("frequent_phrases") or [])
        fgn = TopicTrend(**cached["fastest_growing_negative"]) if cached.get("fastest_growing_negative") else None
        sp = TopicTrend(**cached["strongest_positive"]) if cached.get("strongest_positive") else None
        # Timeseries may be missing from older cache rows — compute and persist it on-demand.
        _ts_raw = cached.get("topic_timeseries")
        if _ts_raw is None and topic_bars:
            _ts_raw = await _compute_topic_timeseries(task_id, topic_bars, session, source)
            try:
                cache_row.topics_module = {**cached, "topic_timeseries": _ts_raw}
                await session.commit()
            except Exception:
                await session.rollback()
        topic_timeseries = [TopicTimeSeries(**t) for t in (_ts_raw or [])]
    else:
        reviews = await _fetch_reviews_with_dates(task_id, session, since=since, source=source)
        result = await claude_service.generate_topics_module(reviews)
        if result is None:
            topic_bars, top_positive, top_negative = [], [], []
            frequent_phrases = []
            fgn = None
            sp = None
            topic_timeseries = []
        else:
            topic_bars = [TopicBarItem(**t) for t in result["topic_bars"]]
            top_positive = [TopicListItem(**t) for t in result["top_positive"]]
            top_negative = [TopicListItem(**t) for t in result["top_negative"]]
            frequent_phrases = list(result["frequent_phrases"])
            fgn = TopicTrend(**result["fastest_growing_negative"]) if result.get("fastest_growing_negative") else None
            sp = TopicTrend(**result["strongest_positive"]) if result.get("strongest_positive") else None

            _ts_raw = await _compute_topic_timeseries(task_id, topic_bars, session, source)
            topic_timeseries = [TopicTimeSeries(**t) for t in _ts_raw]

            payload = {
                "topic_bars": [t.model_dump() for t in topic_bars],
                "top_positive": [t.model_dump() for t in top_positive],
                "top_negative": [t.model_dump() for t in top_negative],
                "frequent_phrases": frequent_phrases,
                "fastest_growing_negative": fgn.model_dump() if fgn else None,
                "strongest_positive": sp.model_dump() if sp else None,
                "topic_timeseries": _ts_raw,
            }

            if cache_row is None:
                cache_row = TaskTopicsCache(
                    task_id=task_id,
                    days=int(days) if days is not None else None,
                    topics_module=payload,
                )
                session.add(cache_row)
            else:
                cache_row.topics_module = payload

            try:
                await session.commit()
            except Exception:
                await session.rollback()
                logging.exception("Failed to save topics_module cache")

    topics_count = len(topic_bars)

    return TopicsModuleResponse(
        task_id=task.id,
        status=task.status.value,
        period_days=int(days) if days is not None else None,
        reviews_total=reviews_total,
        topics_count=topics_count,
        topic_bars=topic_bars,
        top_positive=top_positive,
        top_negative=top_negative,
        frequent_phrases=frequent_phrases,
        fastest_growing_negative=fgn,
        strongest_positive=sp,
        monthly_avg_rating=monthly_avg_rating,
        topic_timeseries=topic_timeseries,
        analytics_note=("Topics synthesized by Claude AI." if topic_bars else "Not enough data or AI unavailable."),
    )


# ---------------------------------------------------------------------------
# Dashboard — /replies (Pro module)
# ---------------------------------------------------------------------------

def _reply_priority(rating: int | None, age_hours: float | None, sla_hours: int) -> str:
    if rating is not None and rating <= _SENT_NEG_MAX:
        if age_hours is not None and age_hours >= sla_hours:
            return "urgent"
        return "high"
    if rating == 3:
        return "medium"
    return "low"


@router.get(
    "/{task_id}/replies",
    response_model=RepliesModuleResponse,
    tags=["dashboard"],
    summary="Работа с ответами: KPI, очередь и шаблоны (Claude AI)",
)
async def get_task_replies_module(
    task_id: UUID,
    days: int | None = Query(
        None,
        ge=1,
        alias="params",
        description="If set, KPIs and queue are computed on reviews of the last N days.",
    ),
    sla_hours: int = Query(24, ge=1, le=24 * 30, description="SLA window in hours."),
    queue_limit: int = Query(20, ge=1, le=100),
    sort: Literal[
        "negative_first", "newest_first", "oldest_first", "urgent_first"
    ] = Query("negative_first", description="Queue ordering."),
    source: Literal["2gis", "zapis", "all"] = Query("2gis", description="Filter by data source"),
    session: AsyncSession = Depends(get_session),
):
    task = await _require_task(task_id, session)
    since: datetime | None = None
    if days is not None:
        since = datetime.now(timezone.utc) - timedelta(days=int(days))

    base_filters = [SearchTaskBranch.task_id == task_id]
    if since is not None:
        base_filters.append(Review.date_created.isnot(None))
        base_filters.append(Review.date_created >= since)
    if source is not None and source != "all":
        base_filters.append(Branch.source == source)

    base_join = (
        select(Review)
        .join(SearchTaskBranch, SearchTaskBranch.branch_id == Review.branch_id)
        .join(Branch, Branch.id == Review.branch_id)
    )

    now = datetime.now(timezone.utc)

    # Single aggregate query for KPIs.
    response_seconds = func.extract(
        "epoch", Review.official_answer_date - Review.date_created
    )
    overdue_clause = and_(
        Review.official_answer_text.is_(None),
        Review.date_created.isnot(None),
        Review.date_created < (now - timedelta(hours=sla_hours)),
    )
    agg_stmt = (
        select(
            func.count().label("total"),
            func.count().filter(Review.official_answer_text.isnot(None)).label("answered"),
            func.count().filter(Review.rating <= _SENT_NEG_MAX).label("neg_total"),
            func.count()
            .filter(
                and_(
                    Review.rating <= _SENT_NEG_MAX,
                    Review.official_answer_text.isnot(None),
                )
            )
            .label("neg_answered"),
            func.avg(response_seconds)
            .filter(
                and_(
                    Review.official_answer_text.isnot(None),
                    Review.official_answer_date.isnot(None),
                    Review.date_created.isnot(None),
                )
            )
            .label("avg_resp_seconds"),
            func.count().filter(Review.official_answer_text.is_(None)).label("unanswered"),
            func.count().filter(overdue_clause).label("overdue"),
        )
        .select_from(Review)
        .join(SearchTaskBranch, SearchTaskBranch.branch_id == Review.branch_id)
        .join(Branch, Branch.id == Review.branch_id)
        .where(and_(*base_filters))
    )
    agg = (await session.execute(agg_stmt)).one()

    total = int(agg.total or 0)
    answered = int(agg.answered or 0)
    unanswered = int(agg.unanswered or 0)
    overdue = int(agg.overdue or 0)
    neg_total = int(agg.neg_total or 0)
    neg_answered = int(agg.neg_answered or 0)
    avg_resp_hours = (
        round(float(agg.avg_resp_seconds) / 3600.0, 1)
        if agg.avg_resp_seconds is not None
        else None
    )

    def _pct_int(n: int, d: int) -> int:
        if d <= 0:
            return 0
        return int(round(n * 100.0 / d))

    kpis = RepliesKpis(
        answered_count=answered,
        answered_pct=_pct_int(answered, total),
        avg_response_hours=avg_resp_hours,
        negatives_replied_pct=_pct_int(neg_answered, neg_total),
        overdue_sla_count=overdue,
    )

    # Urgent count (negatives older than SLA, unanswered).
    urgent_stmt = (
        select(func.count())
        .select_from(Review)
        .join(SearchTaskBranch, SearchTaskBranch.branch_id == Review.branch_id)
        .join(Branch, Branch.id == Review.branch_id)
        .where(and_(*base_filters))
        .where(Review.official_answer_text.is_(None))
        .where(Review.rating <= _SENT_NEG_MAX)
        .where(Review.date_created.isnot(None))
        .where(Review.date_created < (now - timedelta(hours=sla_hours)))
    )
    urgent_count = int((await session.execute(urgent_stmt)).scalar_one() or 0)

    # Queue ordering depends on the `sort` param.
    rating_priority = case(
        (Review.rating <= _SENT_NEG_MAX, 0),
        (Review.rating == 3, 1),
        else_=2,
    )
    overdue_priority = case(
        (
            and_(
                Review.date_created.isnot(None),
                Review.date_created < (now - timedelta(hours=sla_hours)),
            ),
            0,
        ),
        else_=1,
    )

    if sort == "newest_first":
        order_cols = (Review.date_created.desc().nulls_last(),)
    elif sort == "oldest_first":
        order_cols = (Review.date_created.asc().nulls_last(),)
    elif sort == "urgent_first":
        order_cols = (
            overdue_priority.asc(),
            rating_priority.asc(),
            Review.date_created.desc().nulls_last(),
        )
    else:  # negative_first (default)
        order_cols = (rating_priority.asc(), Review.date_created.desc().nulls_last())

    queue_stmt = (
        select(Review, Branch.name.label("branch_name"))
        .join(SearchTaskBranch, SearchTaskBranch.branch_id == Review.branch_id)
        .join(Branch, Branch.id == Review.branch_id)
        .where(and_(*base_filters))
        .where(Review.official_answer_text.is_(None))
        .where(Review.text.isnot(None))
        .where(Review.text != "")
        .order_by(*order_cols)
        .limit(queue_limit)
    )
    queue_rows = (await session.execute(queue_stmt)).all()

    queue: list[ReplyQueueItem] = []
    for r, branch_name in queue_rows:
        age_hours: float | None = None
        if r.date_created is not None:
            delta = now - r.date_created
            age_hours = round(delta.total_seconds() / 3600.0, 1)
        overdue_sla = (
            r.date_created is not None
            and r.date_created < (now - timedelta(hours=sla_hours))
        )
        priority = _reply_priority(r.rating, age_hours, sla_hours)
        queue.append(
            ReplyQueueItem(
                id=r.id,
                branch_id=r.branch_id,
                branch_name=branch_name,
                user_name=r.user_name,
                rating=r.rating,
                text=r.text,
                date_created=r.date_created,
                review_url=r.review_url,
                sentiment=_sentiment_from_rating(r.rating),
                priority=priority,
                overdue_sla=overdue_sla,
                age_hours=age_hours,
            )
        )

    # Templates — Claude-generated and cached per (task_id, days).
    cache_stmt = (
        select(TaskTopicsCache)
        .where(
            TaskTopicsCache.task_id == task_id,
            TaskTopicsCache.days == (int(days) if days is not None else None),
        )
        .order_by(TaskTopicsCache.id.desc())
        .limit(1)
    )
    cache_row = (await session.execute(cache_stmt)).scalars().first()

    templates: list[ReplyTemplate] = []
    if cache_row is not None and cache_row.reply_templates:
        templates = [ReplyTemplate(**t) for t in cache_row.reply_templates if isinstance(t, dict)]
    else:
        top_problems_raw: list[dict] = list(cache_row.top_problems or []) if cache_row else []
        top_praise_raw: list[dict] = list(cache_row.top_praise or []) if cache_row else []
        if not top_problems_raw and not top_praise_raw:
            top_problems_raw, top_praise_raw = await _load_task_top_mentions(task_id, session)

        templates = await claude_service.generate_reply_templates(top_problems_raw, top_praise_raw)
        payload = [t.model_dump() for t in templates]

        if cache_row is None:
            cache_row = TaskTopicsCache(
                task_id=task_id,
                days=int(days) if days is not None else None,
                top_problems=top_problems_raw or None,
                top_praise=top_praise_raw or None,
                reply_templates=payload,
            )
            session.add(cache_row)
        else:
            cache_row.reply_templates = payload
            if not cache_row.top_problems and top_problems_raw:
                cache_row.top_problems = top_problems_raw
            if not cache_row.top_praise and top_praise_raw:
                cache_row.top_praise = top_praise_raw

        try:
            await session.commit()
        except Exception:
            await session.rollback()
            logging.exception("Failed to save reply_templates cache")

    return RepliesModuleResponse(
        task_id=task.id,
        status=task.status.value,
        sla_hours=sla_hours,
        unanswered_count=unanswered,
        urgent_count=urgent_count,
        kpis=kpis,
        queue=queue,
        templates=templates,
        analytics_note=("Templates synthesized by Claude AI." if templates else None),
    )


# ---------------------------------------------------------------------------
# Dashboard — /compare
# ---------------------------------------------------------------------------

from app.models.core import Company

@router.get(
    "/{task_id}/compare",
    response_model=CompareResponse,
    tags=["dashboard"],
    summary="Сравнение клубов (Target vs Competitors)",
)
async def get_compare(
    task_id: UUID,
    grouped: bool = Query(False, description="Если True, филиалы одной компании объединяются"),
    session: AsyncSession = Depends(get_session),
):
    task = await _require_task(task_id, session)

    target_branches = (
        await session.execute(
            select(Branch).join(SearchTaskBranch).where(SearchTaskBranch.task_id == task_id)
        )
    ).scalars().all()

    if not target_branches:
        raise HTTPException(status_code=404, detail="No branches found in task")

    target_company_ids = {b.company_id for b in target_branches}
    target_branch_ids = {b.id for b in target_branches}

    # Build the full set of categories we want to match against competitors.
    # Priority: multi-category array, then single legacy category, then task query.
    target_categories: list[str] = []
    for b in target_branches:
        if b.categories:
            target_categories.extend(b.categories)
        elif b.category:
            target_categories.append(b.category)
    target_categories = list(dict.fromkeys(target_categories))  # dedupe, preserve order

    target_city = next((b.city for b in target_branches if b.city), None)

    if grouped:
        stmt = (
            select(
                Company.id.label("company_id"),
                Company.name.label("company_name"),
                func.count(Review.id).label("reviews_n"),
                func.count(Review.id).filter(Review.rating.in_([1, 2, 3, 4, 5])).label("rated_n"),
                func.count(Review.id).filter(Review.rating <= 2).label("neg_n"),
                func.count(Review.id).filter(Review.official_answer_text.isnot(None)).label("replied_n"),
                func.avg(Review.rating).filter(Review.rating.isnot(None)).label("avg_rating"),
            )
            .select_from(Company)
            .join(Branch, Branch.company_id == Company.id)
            .outerjoin(Review, Review.branch_id == Branch.id)
        )
        if target_categories:
            stmt = stmt.where(
                or_(
                    Branch.categories.overlap(target_categories),
                    Branch.category.in_(target_categories),
                    Branch.id.in_(target_branch_ids)
                )
            )
        if target_city:
            stmt = stmt.where(Branch.city == target_city)
        stmt = stmt.group_by(Company.id, Company.name)
    else:
        stmt = (
            select(
                Branch,
                Company.name.label("company_name"),
                func.count(Review.id).label("reviews_n"),
                func.count(Review.id).filter(Review.rating.in_([1, 2, 3, 4, 5])).label("rated_n"),
                func.count(Review.id).filter(Review.rating <= 2).label("neg_n"),
                func.count(Review.id).filter(Review.official_answer_text.isnot(None)).label("replied_n"),
                func.avg(Review.rating).filter(Review.rating.isnot(None)).label("avg_rating")
            )
            .select_from(Branch)
            .join(Company, Company.id == Branch.company_id)
            .outerjoin(Review, Review.branch_id == Branch.id)
        )
        if target_categories:
            stmt = stmt.where(
                or_(
                    Branch.categories.overlap(target_categories),
                    Branch.category.in_(target_categories),
                    Branch.id.in_(target_branch_ids)
                )
            )
        if target_city:
            stmt = stmt.where(Branch.city == target_city)
        stmt = stmt.group_by(Branch.id, Company.name)

    rows = (await session.execute(stmt)).all()

    cats_by_company: dict = {}
    if grouped:
        cat_stmt = (
            select(Branch.company_id, Branch.categories)
            .where(Branch.categories.isnot(None))
        )
        if target_categories:
            cat_stmt = cat_stmt.where(
                or_(
                    Branch.categories.overlap(target_categories),
                    Branch.category.in_(target_categories),
                    Branch.id.in_(target_branch_ids),
                )
            )
        if target_city:
            cat_stmt = cat_stmt.where(Branch.city == target_city)
        for cid, cats in (await session.execute(cat_stmt)).all():
            if not cats:
                continue
            cats_by_company.setdefault(cid, set()).update(cats)

    competitors_data = []
    total_rating = 0.0
    total_rated_companies = 0
    total_neg_pct = 0.0
    total_companies = 0

    for row in rows:
        reviews_n = row.reviews_n or 0
        rated_n = row.rated_n or 0
        neg_n = row.neg_n or 0
        replied_n = row.replied_n or 0
        avg_rating = float(row.avg_rating) if row.avg_rating is not None else None
        
        neg_pct = (neg_n / rated_n * 100) if rated_n > 0 else 0.0
        replies_pct = (replied_n / reviews_n * 100) if reviews_n > 0 else 0.0
        
        if avg_rating is not None:
            total_rating += avg_rating
            total_rated_companies += 1
        total_neg_pct += neg_pct
        total_companies += 1
        
        if grouped:
            comp_id = row.company_id
            is_target = comp_id in target_company_ids
            name = row.company_name
            address = None
            branch_id = None
            categories = sorted(cats_by_company.get(comp_id, set()))
        else:
            b = row.Branch
            branch_id = b.id
            is_target = branch_id in target_branch_ids
            name = b.name or row.company_name
            address = b.address
            categories = b.categories or []
            
        competitors_data.append({
            "branch_id": branch_id,
            "is_target": is_target,
            "name": name,
            "address": address,
            "rating": round(avg_rating, 2) if avg_rating else None,
            "reviews_total": reviews_n,
            "negative_pct": round(neg_pct, 1),
            "replies_pct": round(replies_pct, 1),
            "dynamics": 0.0, # Placeholder dynamics
            "categories": categories
        })

    # Sort by rating (desc) and reviews_total (desc)
    competitors_data.sort(key=lambda x: (x["rating"] or -1.0, x["reviews_total"]), reverse=True)

    # Assign rank
    for i, comp in enumerate(competitors_data):
        comp["rank"] = i + 1

    # Calculate KPIs
    market_avg_rating = total_rating / total_rated_companies if total_rated_companies > 0 else 0.0
    market_avg_neg_pct = total_neg_pct / total_companies if total_companies > 0 else 0.0

    target_comps = [c for c in competitors_data if c["is_target"]]
    if not target_comps:
        raise HTTPException(status_code=404, detail="Target not found in calculated competitors")

    best_target = target_comps[0] # Highest ranked target
    best_rating_diff = None
    if best_target["rating"] is not None and market_avg_rating > 0:
        best_rating_diff = round(best_target["rating"] - market_avg_rating, 1)

    replies_sorted = sorted(competitors_data, key=lambda x: x["replies_pct"], reverse=True)
    replies_rank = next((i + 1 for i, c in enumerate(replies_sorted) if c["is_target"]), 0)

    kpis = CompareKPIs(
        rank_in_district=best_target["rank"],
        total_competitors=len(competitors_data),
        best_rating=best_target["rating"],
        best_rating_diff_from_avg=best_rating_diff,
        negative_pct=round(best_target["negative_pct"], 1),
        negative_pct_avg=round(market_avg_neg_pct, 1),
        replies_pct=round(best_target["replies_pct"], 1),
        replies_rank=replies_rank
    )

    # Strengths
    strengths = []
    if best_target["rating"] is not None and best_target["rank"] <= 3:
        strengths.append(CompareStrengthItem(
            label="Рейтинг",
            value=f"{best_target['rating']} / 5.0",
            subtext="Лучший показатель среди конкурентов." if best_target["rank"] == 1 else "Один из лучших рейтингов в районе.",
            meter_pct=92 if best_target["rank"] == 1 else 78
        ))

    # Check TaskTopicsCache for top_praise
    cache_stmt = select(TaskTopicsCache).where(TaskTopicsCache.task_id == task_id)
    cache = (await session.execute(cache_stmt)).scalars().first()
    if cache and cache.top_praise:
        for praise in cache.top_praise[:2]:
            mentions = praise.get("mentions", 0)
            strengths.append(CompareStrengthItem(
                label=praise.get("label", "Похвала").capitalize(),
                value=f"{mentions} упоминаний",
                subtext="Одна из ключевых причин высокой оценки.",
                meter_pct=min(100, 60 + mentions * 2)
            ))

    response = CompareResponse(
        task_id=task_id,
        status=task.status.value,
        kpis=kpis,
        competitors=[CompareCompetitorItem(**c) for c in competitors_data],
        strengths=strengths
    )
    return response
