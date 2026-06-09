from datetime import datetime, timedelta, timezone
from time import monotonic
from typing import Literal
from uuid import UUID
import asyncio
import logging

from fastapi import APIRouter, Depends, HTTPException, Query, status
import sqlalchemy as sa
from sqlalchemy import DateTime, and_, bindparam, case, func, or_, select, text
from sqlalchemy.exc import IntegrityError
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
from app.schemas.cities import TaskCityItem, TaskCityListResponse
from app.services import cities as cities_service

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

def _apply_branch_filters(stmt, source: str | None, city: str | None):
    """Сужает выборку по источнику и городу филиала. 'all'/None — без фильтра.

    Требует, чтобы в запросе уже был join на Branch.
    """
    if source and source != "all":
        stmt = stmt.where(Branch.source == source)
    if city and city != "all":
        stmt = stmt.where(Branch.city == city)
    return stmt


def _topics_cache_lookup(task_id: UUID, days: int | None, city: str | None):
    """Select строки AI-кэша по ключу (task_id, days, city), null-safe.

    Источник (source) НЕ входит в ключ: AI-аналитика считается по всем источникам
    (source-agnostic), поэтому одна строка обслуживает любой source. Это экономит
    Claude-вызовы (нет фрагментации кэша по 2gis/zapis/all). Старые строки с NULL
    city матчатся на city=all через COALESCE — обратная совместимость.
    """
    return (
        select(TaskTopicsCache)
        .where(
            TaskTopicsCache.task_id == task_id,
            func.coalesce(TaskTopicsCache.days, -1) == (int(days) if days is not None else -1),
            func.coalesce(TaskTopicsCache.city, "all") == (city or "all"),
        )
        .order_by(TaskTopicsCache.id.desc())
        .limit(1)
    )


# Per-(task,days,city) lock: дедуплицирует параллельные первые загрузки дашборда,
# чтобы AI считался ОДИН раз, а не по razу на каждый одновременный эндпоинт.
_analysis_locks: dict[tuple, asyncio.Lock] = {}
_analysis_locks_guard = asyncio.Lock()


async def _get_analysis_lock(key: tuple) -> asyncio.Lock:
    async with _analysis_locks_guard:
        lk = _analysis_locks.get(key)
        if lk is None:
            lk = asyncio.Lock()
            _analysis_locks[key] = lk
        return lk


async def _write_topics_cache(
    session: AsyncSession, task_id: UUID, days: int | None, city: str | None, fields: dict
) -> "TaskTopicsCache | None":
    """Идемпотентно пишет поля в строку AI-кэша (task,days,city).

    Устойчиво к гонке: при конфликте уникального индекса (другой одновременный
    запрос уже вставил строку) откатываемся и обновляем существующую. Так дашборд,
    который параллельно дёргает /overview /recommendations /topics и т.д., не падает
    с UniqueViolation. Обновляются ТОЛЬКО переданные поля (merge, не затирает чужие).
    """
    row = (await session.execute(_topics_cache_lookup(task_id, days, city))).scalars().first()
    if row is None:
        row = TaskTopicsCache(
            task_id=task_id, days=int(days) if days is not None else None, city=city, **fields
        )
        session.add(row)
        try:
            await session.commit()
            return row
        except IntegrityError:
            await session.rollback()
            row = (await session.execute(_topics_cache_lookup(task_id, days, city))).scalars().first()
    if row is not None:
        for k, v in fields.items():
            setattr(row, k, v)
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
    return row


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


def _topic_stems(label: str) -> list[str]:
    """Стеммы ключевых слов темы для подстрочного матчинга по тексту отзыва.

    Берём 6-символьный префикс (или слово целиком, если короче) — этого хватает,
    чтобы поймать падежи/формы: «обслуживание/обслуживания/обслужили» → «обслуж»,
    «доступность/доступно/недоступ» → «доступ». Достаточно совпадения ЛЮБОГО стемма.
    """
    return [w if len(w) <= 6 else w[:6] for w in _topic_keywords(label)]


async def _compute_topic_quant(
    task_id: UUID,
    labels: list[str],
    session: AsyncSession,
    source: str,
    city: str | None,
    year: int | None,
) -> tuple[list[dict], list[dict]]:
    """ЕДИНЫЙ источник правды для topic_bars и topic_timeseries.

    Один проход по отзывам выбранного года → для каждой темы помесячная разбивка
    pos/neg, а bar = сумма этих же помесячных значений. Поэтому
    `Σ monthly[].positive == bar.positive` и `… negative` ВСЕГДА (by construction).
    Ось — фиксированные 12 месяцев года (`YYYY-01..YYYY-12`), пустые = 0/0 — как
    в monthly_avg_rating. Матчинг отзыва к теме — по стеммам слов лейбла (детерм.).

    Возвращает (bars, timeseries). Если нет года/лейблов — ([], []).
    """
    if year is None or not labels:
        return [], []

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
                  AND (:city IS NULL OR b.city = :city)
                  AND EXTRACT(YEAR FROM date_trunc('month', timezone(:tz, r.date_created))) = :year
            """).bindparams(
                bindparam("tz", type_=sa.String()),
                bindparam("source", type_=sa.String()),
                bindparam("city", type_=sa.String()),
                bindparam("year", type_=sa.Integer()),
            ),
            {
                "task_id": task_id,
                "tz": _OVERVIEW_TZ,
                "source": source if source and source != "all" else None,
                "city": city if city and city != "all" else None,
                "year": year,
            },
        )
    ).all()

    months = [f"{year}-{mm:02d}" for mm in range(1, 13)]
    # Предкомпилируем стеммы и (текст, месяц, оценка) один раз.
    stems_by_label = {label: _topic_stems(label) for label in labels}
    docs = [(r.text or "", r.month, r.rating) for r in rows]

    bars: list[dict] = []
    series: list[dict] = []
    for label in labels:
        stems = stems_by_label[label]
        bucket = {m: {"positive": 0, "negative": 0} for m in months}
        pos_total = neg_total = 0
        for text_l, month, rating in docs:
            if rating is None or month not in bucket:
                continue
            if not any(s in text_l for s in stems):
                continue
            if rating >= 4:
                bucket[month]["positive"] += 1
                pos_total += 1
            elif rating <= 2:
                bucket[month]["negative"] += 1
                neg_total += 1
        bars.append({"label": label, "positive": pos_total, "negative": neg_total})
        series.append({
            "label": label,
            "monthly": [
                {"month": m, "positive": bucket[m]["positive"], "negative": bucket[m]["negative"]}
                for m in months
            ],
        })
    return bars, series


def _run_topic_extraction(docs):
    return extract_topics(docs, top_n=8, min_mentions=3)

def _run_topic_extraction_embeddings(docs):
    return extract_topics_embeddings(docs, top_n=8, min_mentions=3)

async def _fetch_reviews_as_dicts(
    task_id: UUID, session: AsyncSession, since: datetime | None = None,
    source: Literal["2gis", "zapis", "all"] = "2gis", city: str | None = None,
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
    stmt = _apply_branch_filters(stmt, source, city)
    rows = (await session.execute(stmt)).all()
    return [{"rating": r.rating, "text": r.text} for r in rows]


async def _load_task_top_mentions(
    task_id: UUID, session: AsyncSession, city: str | None = None,
) -> tuple[list[dict], list[dict]]:
    """Look up cached top_problems/top_praise for the task across all (task_id, days)
    rows for the same city (source-agnostic). Returns the first populated pair found.
    Never calls Claude — the caller decides whether to generate when missing."""
    stmt = (
        select(TaskTopicsCache.top_problems, TaskTopicsCache.top_praise, TaskTopicsCache.days)
        .where(TaskTopicsCache.task_id == task_id)
        .where(func.coalesce(TaskTopicsCache.city, "all") == (city or "all"))
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
    task_id: UUID, session: AsyncSession, since: datetime | None = None,
    source: Literal["2gis", "zapis", "all"] = "2gis", city: str | None = None,
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
    stmt = _apply_branch_filters(stmt, source, city)
    stmt = stmt.order_by(Review.date_created.desc().nulls_last())
    rows = (await session.execute(stmt)).all()
    return [
        {"rating": r.rating, "text": r.text, "date_created": r.date_created}
        for r in rows
    ]


async def _ensure_full_analysis(
    task_id: UUID, days: int | None, city: str | None, session: AsyncSession, want: str
) -> "TaskTopicsCache | None":
    """Гарантирует, что AI-аналитика для (task, days, city) посчитана, и возвращает
    строку кэша.

    ОДИН вызов Claude (generate_full_analysis) заполняет сразу ВСЕ поля
    (top_problems/top_praise/problems/priorities/insights/topics_module), поэтому
    остальные AI-эндпоинты для той же комбинации читают готовое из кэша без новых
    обращений к Claude — это и есть консолидация 4 вызовов в 1.

    `want` — поле, нужное вызывающему. Если оно уже в кэше, Claude не вызывается.
    AI считается по всем источникам (source-agnostic).
    """
    row = (await session.execute(_topics_cache_lookup(task_id, days, city))).scalars().first()
    if row is not None and getattr(row, want) is not None:
        return row

    # Сериализуем вычисление по (task,days,city): один Claude-вызов на комбинацию,
    # даже если дашборд параллельно дёрнул несколько AI-эндпоинтов сразу.
    lock = await _get_analysis_lock((str(task_id), days, city or "all"))
    async with lock:
        # Двойная проверка: пока ждали лок, другой запрос мог всё посчитать.
        row = (await session.execute(_topics_cache_lookup(task_id, days, city))).scalars().first()
        if row is not None and getattr(row, want) is not None:
            return row

        since: datetime | None = None
        if days is not None:
            since = datetime.now(timezone.utc) - timedelta(days=int(days))
        reviews = await _fetch_reviews_with_dates(task_id, session, since=since, source="all", city=city)
        full = await claude_service.generate_full_analysis(reviews)
        if not full:
            return row

        # Пустые списки сохраняем как есть (маркер «посчитано, пусто»), иначе
        # эндпоинт будет дёргать Claude снова и снова.
        return await _write_topics_cache(session, task_id, days, city, {
            "top_problems": full["top_problems"],
            "top_praise": full["top_praise"],
            "problems": full["problems"],
            "priorities": full["priorities"],
            "insights": full["insights"],
            "topics_module": full["topics_module"],
        })


# Dashboard — /cities
# ---------------------------------------------------------------------------

@router.get(
    "/{task_id}/cities",
    response_model=TaskCityListResponse,
    tags=["dashboard"],
    summary="Города, представленные в отчёте (для фильтра city)",
)
async def get_task_cities(
    task_id: UUID,
    source: Literal["2gis", "zapis", "all"] = Query("all", description="Filter by data source"),
    session: AsyncSession = Depends(get_session),
):
    """Distinct branch cities for this task, each with its branch count.

    Drives the city picker for the dashboard's `city` filter. Slugs match
    `Branch.city` (e.g. 'almaty') — pass them back as `?city=<slug>`.
    """
    await _require_task(task_id, session)

    stmt = (
        select(Branch.city, func.count(func.distinct(Branch.id)).label("branch_count"))
        .join(SearchTaskBranch, SearchTaskBranch.branch_id == Branch.id)
        .where(SearchTaskBranch.task_id == task_id)
        .where(Branch.city.isnot(None))
        .where(Branch.city != "")
        .group_by(Branch.city)
    )
    if source and source != "all":
        stmt = stmt.where(Branch.source == source)
    stmt = stmt.order_by(func.count(func.distinct(Branch.id)).desc())

    rows = (await session.execute(stmt)).all()

    # slug -> display name from the KZ catalog (best-effort; unknown slugs keep name=None)
    try:
        name_by_slug = {c["slug"]: c["name"] for c in await cities_service.get_cities()}
    except Exception:
        logging.exception("Failed to load city catalog for name mapping")
        name_by_slug = {}

    cities = [
        TaskCityItem(
            slug=city,
            name=name_by_slug.get(city),
            branch_count=int(branch_count or 0),
        )
        for city, branch_count in rows
    ]
    return TaskCityListResponse(count=len(cities), cities=cities)


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
    city: str = Query("all", description="Filter by branch city slug (e.g. almaty, astana); 'all' = no filter"),
    session: AsyncSession = Depends(get_session),
):
    cache_key = (str(task_id), "overview", int(top_branches_limit), int(days) if days is not None else None, source if source and source != "all" else None, city if city and city != "all" else None)
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
            branches_stmt = _apply_branch_filters(branches_stmt, source, city)
                
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
            agg_stmt = _apply_branch_filters(agg_stmt, source, city)
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
            rating_stmt = _apply_branch_filters(rating_stmt, source, city)
                
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
                    city=(b.city or task.city),
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

            topics_cache_row = (await session.execute(_topics_cache_lookup(task_id, days, city))).scalars().first()

            top_problems: list[TopMention] = []
            top_praise: list[TopMention] = []

            if topics_cache_row is not None and (topics_cache_row.top_problems or topics_cache_row.top_praise):
                top_problems = [TopMention(**t) for t in (topics_cache_row.top_problems or []) if isinstance(t, dict)]
                top_praise = [TopMention(**t) for t in (topics_cache_row.top_praise or []) if isinstance(t, dict)]
            elif topics_cache_row is not None and topics_cache_row.topics_module:
                # /topics already computed — derive top-5 from the shared cache, no Claude call.
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
                # Cold path: one consolidated Claude call fills top_problems/top_praise
                # (and the rest), so /problems /actions /topics reuse it for free.
                row = await _ensure_full_analysis(task_id, days, city, session, "top_problems")
                if row is not None:
                    top_problems = [TopMention(**t) for t in (row.top_problems or []) if isinstance(t, dict)]
                    top_praise = [TopMention(**t) for t in (row.top_praise or []) if isinstance(t, dict)]

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
  JOIN branches b ON b.id = r.branch_id
  WHERE stb.task_id = :task_id
    AND r.date_created IS NOT NULL
    AND (:source IS NULL OR b.source = :source)
    AND (:city IS NULL OR b.city = :city)
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
                    ).bindparams(
                        bindparam("source", type_=sa.String()),
                        bindparam("city", type_=sa.String()),
                    ),
                    {
                        "task_id": task_id, "tz": _OVERVIEW_TZ, "range_days": int(days),
                        "source": source if source and source != "all" else None,
                        "city": city if city and city != "all" else None,
                    },
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
  JOIN branches b ON b.id = r.branch_id
  WHERE stb.task_id = :task_id
    AND r.date_created IS NOT NULL
    AND (:source IS NULL OR b.source = :source)
    AND (:city IS NULL OR b.city = :city)
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
                    ).bindparams(
                        bindparam("since", type_=DateTime(timezone=True)),
                        bindparam("source", type_=sa.String()),
                        bindparam("city", type_=sa.String()),
                    ),
                    {
                        "task_id": task_id, "tz": _OVERVIEW_TZ, "since": since,
                        "source": source if source and source != "all" else None,
                        "city": city if city and city != "all" else None,
                    },
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
    city: str = Query("all", description="Filter by branch city slug (e.g. almaty, astana); 'all' = no filter"),
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

    stmt = _apply_branch_filters(stmt, source, city)
    count_stmt = _apply_branch_filters(count_stmt, source, city)

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
    city: str = Query("all", description="Filter by branch city slug (e.g. almaty, astana); 'all' = no filter"),
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
    if city and city != "all":
        where_clauses.append(Branch.city == city)

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
    city: str = Query("all", description="Filter by branch city slug (e.g. almaty, astana); 'all' = no filter"),
    session: AsyncSession = Depends(get_session),
):
    task = await _require_task(task_id, session)

    # Один консолидированный Claude-вызов заполняет все AI-поля; здесь берём problems.
    row = await _ensure_full_analysis(task_id, days, city, session, "problems")
    problems = [ProblemItem(**p) for p in (row.problems or [])] if row else []

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
    city: str = Query("all", description="Filter by branch city slug (e.g. almaty, astana); 'all' = no filter"),
    session: AsyncSession = Depends(get_session),
):
    task = await _require_task(task_id, session)

    # Один консолидированный Claude-вызов; здесь берём priorities + insights.
    row = await _ensure_full_analysis(task_id, days, city, session, "priorities")
    priorities = [PriorityItem(**p) for p in (row.priorities or [])] if row else []
    insights = [InsightItem(**i) for i in (row.insights or [])] if row else []

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
    city: str = Query("all", description="Filter by branch city slug (e.g. almaty, astana); 'all' = no filter"),
    session: AsyncSession = Depends(get_session),
):
    task = await _require_task(task_id, session)
    since: datetime | None = None
    if days is not None:
        since = datetime.now(timezone.utc) - timedelta(days=int(days))

    cache_stmt = _topics_cache_lookup(task_id, days, city)
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
    agg_stmt = _apply_branch_filters(agg_stmt, source, city)
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
        top_problems_raw, top_praise_raw = await _load_task_top_mentions(task_id, session, city)

    if not top_problems_raw and not top_praise_raw:
        # Try deriving from topics_module cache (populated by /topics endpoint).
        any_cache = (
            await session.execute(
                select(TaskTopicsCache)
                .where(TaskTopicsCache.task_id == task_id)
                .where(func.coalesce(TaskTopicsCache.city, "all") == (city or "all"))
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
        # Last resort: consolidated analysis fills top_mentions (shared with all AI endpoints).
        full_row = await _ensure_full_analysis(task_id, days, city, session, "top_problems")
        if full_row is not None:
            top_problems_raw = list(full_row.top_problems or [])
            top_praise_raw = list(full_row.top_praise or [])

    items = await claude_service.generate_recommendations(top_problems_raw, top_praise_raw, kpis)

    payload = [i.model_dump() for i in items]
    # Конфликт-безопасная запись: только своё поле, не затирая чужие (top_problems
    # и пр. владеет _ensure_full_analysis).
    await _write_topics_cache(session, task_id, days, city, {"recommendations": payload})

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
    city: str = Query("all", description="Filter by branch city slug (e.g. almaty, astana); 'all' = no filter"),
    year: int | None = Query(
        None,
        ge=2000,
        le=2100,
        description="Год для графика monthly_avg_rating (Янв..Дек). По умолчанию — последний год с отзывами.",
    ),
    session: AsyncSession = Depends(get_session),
):
    task = await _require_task(task_id, session)
    since: datetime | None = None
    if days is not None:
        since = datetime.now(timezone.utc) - timedelta(days=int(days))

    # Один консолидированный Claude-вызов заполняет topics_module (если ещё не посчитан).
    cache_row = await _ensure_full_analysis(task_id, days, city, session, "topics_module")
    cached: dict | None = (
        cache_row.topics_module if (cache_row is not None and cache_row.topics_module is not None) else None
    )

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
    count_stmt = _apply_branch_filters(count_stmt, source, city)
    reviews_total = int((await session.execute(count_stmt)).scalar_one() or 0)

    # Monthly average rating (always computed, never via Claude). Год = 12 месяцев Янв..Дек.
    #
    # РАНЬШЕ: жёстко «последние 12 месяцев». Для филиалов с историей с 2018 это
    # показывало ~15% отзывов и не билось с reviews_total. ТЕПЕРЬ: выбор года.
    # Считаем годы с датированными отзывами (для фильтра), берём выбранный (или
    # последний по умолчанию) и строим Янв..Дек этого года с avg_rating И числом
    # отзывов помесячно — так график сходится сам с собой (sum(reviews)=за год).
    src_param = source if source and source != "all" else None
    city_param = city if city and city != "all" else None
    common_params = {"task_id": task_id, "tz": _OVERVIEW_TZ, "source": src_param, "city": city_param}
    _year_binds = [
        bindparam("tz", type_=sa.String()),
        bindparam("source", type_=sa.String()),
        bindparam("city", type_=sa.String()),
    ]

    years_rows = (await session.execute(
        text(
            """
SELECT DISTINCT EXTRACT(YEAR FROM date_trunc('month', timezone(:tz, r.date_created)))::int AS yr
FROM reviews r
JOIN search_task_branches stb ON stb.branch_id = r.branch_id
JOIN branches b ON b.id = r.branch_id
WHERE stb.task_id = :task_id
  AND r.date_created IS NOT NULL
  AND (:source IS NULL OR b.source = :source)
  AND (:city IS NULL OR b.city = :city)
ORDER BY yr
"""
        ).bindparams(*_year_binds),
        common_params,
    )).all()
    available_years = [int(r.yr) for r in years_rows]

    # Выбранный год: запрошенный (если есть данные) → иначе последний год с отзывами.
    if year is not None and year in available_years:
        selected_year = year
    elif available_years:
        selected_year = max(available_years)
    else:
        selected_year = None

    monthly_avg_rating: list[MonthlyAvgRatingPoint] = []
    monthly_reviews_total = 0
    if selected_year is not None:
        monthly_stmt = await session.execute(
            text(
                """
WITH months AS (
  SELECT generate_series(
    make_date(:year, 1, 1)::timestamp,
    make_date(:year, 12, 1)::timestamp,
    interval '1 month'
  ) AS month_start
),
agg AS (
  SELECT
    date_trunc('month', timezone(:tz, r.date_created)) AS month_start,
    avg(r.rating) FILTER (WHERE r.rating IS NOT NULL) AS avg_rating,
    count(*) AS n
  FROM reviews r
  JOIN search_task_branches stb ON stb.branch_id = r.branch_id
  JOIN branches b ON b.id = r.branch_id
  WHERE stb.task_id = :task_id
    AND r.date_created IS NOT NULL
    AND (:source IS NULL OR b.source = :source)
    AND (:city IS NULL OR b.city = :city)
  GROUP BY 1
)
SELECT
  to_char(m.month_start, 'YYYY-MM') AS month,
  a.avg_rating,
  COALESCE(a.n, 0) AS n
FROM months m
LEFT JOIN agg a USING (month_start)
ORDER BY m.month_start ASC
"""
            ).bindparams(*_year_binds, bindparam("year", type_=sa.Integer())),
            {**common_params, "year": selected_year},
        )
        for r in monthly_stmt.all():
            n = int(r.n or 0)
            monthly_reviews_total += n
            monthly_avg_rating.append(MonthlyAvgRatingPoint(
                month=str(r.month),
                avg_rating=round(float(r.avg_rating), 2) if r.avg_rating is not None else None,
                reviews=n,
            ))

    if cached is not None:
        # Лейблы тем (семантическая кластеризация) — от Claude. КОЛИЧЕСТВА (bars и
        # timeseries) считаем ДЕТЕРМИНИРОВАННО из одних и тех же отзывов выбранного
        # года, поэтому бар = сумма помесячного ряда (см. _compute_topic_quant) и
        # «цифры бьются». Раньше бар брался из оценки Claude (весь корпус), а ряд —
        # из отдельного keyword-матчинга за 12 мес → расхождение в 10–30 раз.
        labels = [str(t.get("label")) for t in (cached.get("topic_bars") or []) if t.get("label")]
        bars_raw, ts_raw = await _compute_topic_quant(task_id, labels, session, source, city, selected_year)
        topic_bars = [TopicBarItem(**b) for b in bars_raw]
        topic_timeseries = [TopicTimeSeries(**t) for t in ts_raw]
        # top_positive / top_negative — те же темы, ранжированные по pos/neg за год
        # (а не отдельный список Claude), чтобы и они сходились с барами.
        top_positive = [
            TopicListItem(label=b["label"], sentiment="pos", mentions=b["positive"])
            for b in sorted(bars_raw, key=lambda x: x["positive"], reverse=True)
            if b["positive"] > 0
        ][:6]
        top_negative = [
            TopicListItem(label=b["label"], sentiment="neg", mentions=b["negative"])
            for b in sorted(bars_raw, key=lambda x: x["negative"], reverse=True)
            if b["negative"] > 0
        ][:6]
        # Качественные поля остаются от Claude.
        frequent_phrases = list(cached.get("frequent_phrases") or [])
        fgn = TopicTrend(**cached["fastest_growing_negative"]) if cached.get("fastest_growing_negative") else None
        sp = TopicTrend(**cached["strongest_positive"]) if cached.get("strongest_positive") else None
    else:
        # _ensure_full_analysis не смог посчитать (нет ключа/отзывов/сбой) — пусто.
        topic_bars, top_positive, top_negative = [], [], []
        frequent_phrases = []
        fgn = None
        sp = None
        topic_timeseries = []

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
        selected_year=selected_year,
        available_years=available_years,
        monthly_reviews_total=monthly_reviews_total,
        topic_timeseries=topic_timeseries,
        analytics_note=(
            f"Темы — Claude AI; счётчики bars/ряда — по датам отзывов за {selected_year}."
            if topic_bars else "Not enough data or AI unavailable."
        ),
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
    city: str = Query("all", description="Filter by branch city slug (e.g. almaty, astana); 'all' = no filter"),
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
    if city and city != "all":
        base_filters.append(Branch.city == city)

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

    # AI-suggested replies for the top of the queue (first 3 items only, one Claude call).
    top_items = queue[:3]
    if top_items:
        suggestions = await claude_service.generate_review_replies(
            [
                {
                    "id": str(item.id),
                    "text": item.text,
                    "rating": item.rating,
                    "branch_name": item.branch_name,
                    "user_name": item.user_name,
                }
                for item in top_items
            ]
        )
        for item in top_items:
            item.suggested_reply = suggestions.get(str(item.id))

    # Templates — Claude-generated and cached per (task_id, days).
    cache_stmt = _topics_cache_lookup(task_id, days, city)
    cache_row = (await session.execute(cache_stmt)).scalars().first()

    templates: list[ReplyTemplate] = []
    if cache_row is not None and cache_row.reply_templates:
        templates = [ReplyTemplate(**t) for t in cache_row.reply_templates if isinstance(t, dict)]
    else:
        top_problems_raw: list[dict] = list(cache_row.top_problems or []) if cache_row else []
        top_praise_raw: list[dict] = list(cache_row.top_praise or []) if cache_row else []
        if not top_problems_raw and not top_praise_raw:
            top_problems_raw, top_praise_raw = await _load_task_top_mentions(task_id, session, city)

        templates = await claude_service.generate_reply_templates(top_problems_raw, top_praise_raw)
        payload = [t.model_dump() for t in templates]
        # Конфликт-безопасная запись только своего поля.
        await _write_topics_cache(session, task_id, days, city, {"reply_templates": payload})

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
    summary="Сравнение выбранных филиалов задачи",
)
async def get_compare(
    task_id: UUID,
    grouped: bool = Query(False, description="Если True, филиалы одной компании объединяются"),
    city: str = Query("all", description="Filter by branch city slug (e.g. almaty); 'all' = все города задачи"),
    session: AsyncSession = Depends(get_session),
):
    # Сравнение охватывает РОВНО филиалы задачи (без рыночных конкурентов).
    task = await _require_task(task_id, session)

    target_stmt = select(Branch).join(SearchTaskBranch).where(SearchTaskBranch.task_id == task_id)
    if city and city != "all":
        target_stmt = target_stmt.where(Branch.city == city)
    target_branches = (await session.execute(target_stmt)).scalars().all()

    if not target_branches:
        detail = f"No branches found in task for city '{city}'" if city and city != "all" else "No branches found in task"
        raise HTTPException(status_code=404, detail=detail)

    target_company_ids = {b.company_id for b in target_branches}
    target_branch_ids = {b.id for b in target_branches}

    # Сравниваемое множество — ровно выбранные филиалы (target_branch_ids уже сужен
    # по городу, если city задан), поэтому фильтр по городу отдельно не нужен.
    branch_scope = Branch.id.in_(target_branch_ids)

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
        stmt = stmt.where(branch_scope)
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
        stmt = stmt.where(branch_scope)
        stmt = stmt.group_by(Branch.id, Company.name)

    rows = (await session.execute(stmt)).all()

    cats_by_company: dict = {}
    if grouped:
        cat_stmt = (
            select(Branch.company_id, Branch.categories)
            .where(Branch.categories.isnot(None))
            .where(branch_scope)
        )
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

    # Check TaskTopicsCache for top_praise (scoped to the same city)
    cache_stmt = (
        select(TaskTopicsCache)
        .where(TaskTopicsCache.task_id == task_id)
        .where(func.coalesce(TaskTopicsCache.city, "all") == (city or "all"))
    )
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
