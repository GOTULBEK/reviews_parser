from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from uuid import UUID

import httpx
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from .config import settings
from .dataset import append_place_row, append_review_row, build_place_row, build_review_row
from .database import AsyncSessionLocal
from .models import Branch, Company, Review, SearchTask, SearchTaskBranch, TaskStatus
from .scraper import scrape_branch

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Upsert helpers
# ---------------------------------------------------------------------------

async def _upsert_company(session: AsyncSession, name: str) -> Company:
    stmt = (
        pg_insert(Company)
        .values(name=name)
        .on_conflict_do_nothing(index_elements=["name"])
    )
    await session.execute(stmt)
    # Неважно, вставили мы или уже было — читаем актуальный id
    result = await session.execute(select(Company).where(Company.name == name))
    return result.scalar_one()


async def _get_unknown_company(session: AsyncSession) -> Company:
    """
    Техническая заглушка для создания "пустых" Branch до того,
    как узнаем org_name из 2ГИС. Потом company_id будет обновлён при upsert.
    """
    return await _upsert_company(session, "__unknown__")


async def _seed_task_branches(
    session: AsyncSession,
    task_id: UUID,
    branches: list[dict],
    unknown_company_id: UUID,
) -> None:
    """
    Привязывает ВСЕ филиалы задачи заранее, чтобы агрегации (например reviews_total)
    считались по полному списку, даже пока скрап ещё идёт.
    """
    if not branches:
        return

    for b in branches:
        stmt = (
            pg_insert(Branch)
            .values(
                gis_branch_id=int(b["gis_branch_id"]),
                company_id=unknown_company_id,
                url=b["firm_url"],
            )
            .on_conflict_do_update(
                index_elements=["gis_branch_id"],
                set_={"url": b["firm_url"]},
            )
        )
        await session.execute(stmt)

    gis_ids = [int(b["gis_branch_id"]) for b in branches]
    result = await session.execute(select(Branch).where(Branch.gis_branch_id.in_(gis_ids)))
    existing = result.scalars().all()

    for br in existing:
        await session.execute(
            pg_insert(SearchTaskBranch)
            .values(task_id=task_id, branch_id=br.id)
            .on_conflict_do_nothing()
        )


async def _upsert_branch(session: AsyncSession, data: dict, company_id: UUID) -> Branch:
    now = datetime.now(tz=timezone.utc)
    values = {
        "gis_branch_id": data["gis_branch_id"],
        "company_id": company_id,
        "name": data.get("company_name"),
        "address": data.get("address"),
        "rating": data.get("rating"),
        "total_reviews": data.get("total_reviews"),
        "url": data["url"],
        "rating_distribution": data.get("rating_distribution"),
        "scraped_at": now,
    }
    stmt = (
        pg_insert(Branch)
        .values(**values)
        .on_conflict_do_update(
            index_elements=["gis_branch_id"],
            set_={
                "name": values["name"],
                "address": values["address"],
                "rating": values["rating"],
                "total_reviews": values["total_reviews"],
                "url": values["url"],
                "rating_distribution": values["rating_distribution"],
                "scraped_at": values["scraped_at"],
            },
        )
    )
    await session.execute(stmt)
    result = await session.execute(
        select(Branch).where(Branch.gis_branch_id == data["gis_branch_id"])
    )
    return result.scalar_one()


async def _upsert_reviews(session: AsyncSession, reviews: list[dict], branch_id: UUID) -> int:
    """Upsert по gis_review_id. Обновляет текст/рейтинг/даты/ответ на случай редактирования."""
    if not reviews:
        return 0

    for r in reviews:
        values = {
            "gis_review_id": r["gis_review_id"],
            "branch_id": branch_id,
            "user_name": r.get("user_name"),
            "rating": r.get("rating"),
            "text": r.get("text"),
            "official_answer_text": r.get("official_answer_text"),
            "official_answer_date": r.get("official_answer_date"),
            "hiding_reason": r.get("hiding_reason"),
            "is_rated": r.get("is_rated", True),
            "date_created": r.get("date_created"),
            "date_edited": r.get("date_edited"),
            "review_url": r["review_url"],
            "raw": r.get("raw"),
        }
        stmt = (
            pg_insert(Review)
            .values(**values)
            .on_conflict_do_update(
                index_elements=["gis_review_id"],
                set_={
                    "text": values["text"],
                    "rating": values["rating"],
                    "user_name": values["user_name"],
                    "official_answer_text": values["official_answer_text"],
                    "official_answer_date": values["official_answer_date"],
                    "hiding_reason": values["hiding_reason"],
                    "is_rated": values["is_rated"],
                    "date_edited": values["date_edited"],
                    "raw": values["raw"],
                },
            )
        )
        await session.execute(stmt)
    return len(reviews)


# ---------------------------------------------------------------------------
# Per-branch persistence
# ---------------------------------------------------------------------------

async def _persist_branch_result(task_id: UUID, data: dict) -> int:
    """Открывает отдельную сессию на один филиал. Возвращает кол-во upsert'нутых отзывов."""
    company_name = data.get("company_name") or f"Неизвестная компания (branch_id={data['gis_branch_id']})"

    async with AsyncSessionLocal() as session:
        task = await session.get(SearchTask, task_id)
        task_city = task.city if task is not None else ""

        company = await _upsert_company(session, company_name)
        await session.flush()

        branch = await _upsert_branch(session, data, company.id)
        await session.flush()

        reviews_count = await _upsert_reviews(session, data.get("reviews", []), branch.id)

        # Dataset logging (CSV) — best-effort, should never fail persistence.
        try:
            await append_place_row(
                build_place_row(task_id=str(task_id), city=task_city, branch_data=data)
            )
            for r in data.get("reviews", []) or []:
                await append_review_row(
                    build_review_row(
                        task_id=str(task_id),
                        place_id=int(data["gis_branch_id"]),
                        review=r,
                    )
                )
        except Exception:
            logger.exception("Dataset CSV write failed (task=%s branch=%s)", task_id, data.get("gis_branch_id"))

        # Связь задача↔филиал
        await session.execute(
            pg_insert(SearchTaskBranch)
            .values(task_id=task_id, branch_id=branch.id)
            .on_conflict_do_nothing()
        )

        # Инкрементальный апдейт прогресса задачи
        task = await session.get(SearchTask, task_id)
        if task is not None:
            task.branches_completed += 1
            task.total_reviews_collected += reviews_count

        await session.commit()

    return reviews_count


# ---------------------------------------------------------------------------
# Top-level task runner
# ---------------------------------------------------------------------------

async def run_scrape_task(task_id: UUID, branches: list[dict]) -> None:
    """
    Оркестратор для предварительно выбранных филиалов.

    `branches` — список {"gis_branch_id": int, "firm_url": str}, подготовленный
    на /search/scrape эндпоинте. Шаг поиска по тексту выполнен заранее (в превью),
    здесь только параллельный скрап + персист.
    """
    # Pending → running
    async with AsyncSessionLocal() as session:
        task = await session.get(SearchTask, task_id)
        if task is None:
            logger.error("Task %s disappeared", task_id)
            return
        task.status = TaskStatus.running
        task.started_at = datetime.now(tz=timezone.utc)
        task.total_branches_found = len(branches)

        unknown_company = await _get_unknown_company(session)
        await session.flush()
        await _seed_task_branches(session, task_id, branches, unknown_company.id)

        await session.commit()

    timeout = httpx.Timeout(settings.request_timeout_seconds, connect=10)
    limits = httpx.Limits(max_connections=30, max_keepalive_connections=10)

    try:
        async with httpx.AsyncClient(timeout=timeout, limits=limits, follow_redirects=True) as client:
            sem = asyncio.Semaphore(settings.max_concurrent_branches)

            async def process_one(entry: dict) -> None:
                async with sem:
                    try:
                        data = await scrape_branch(client, entry["gis_branch_id"], entry["firm_url"])
                    except Exception as e:
                        logger.exception("Scrape failed for branch %s: %s", entry["gis_branch_id"], e)
                        return
                    try:
                        await _persist_branch_result(task_id, data)
                    except Exception as e:
                        logger.exception("Persist failed for branch %s: %s", entry["gis_branch_id"], e)

            await asyncio.gather(*(process_one(e) for e in branches))

            async with AsyncSessionLocal() as session:
                t = await session.get(SearchTask, task_id)
                t.status = TaskStatus.completed
                t.completed_at = datetime.now(tz=timezone.utc)
                await session.commit()

            logger.info(
                "Task %s completed: %d branches, точное число отзывов — в БД",
                task_id, len(branches),
            )

    except Exception as e:
        logger.exception("Task %s crashed: %s", task_id, e)
        async with AsyncSessionLocal() as session:
            t = await session.get(SearchTask, task_id)
            if t:
                t.status = TaskStatus.failed
                t.error_message = f"{type(e).__name__}: {e}"
                t.completed_at = datetime.now(tz=timezone.utc)
                await session.commit()
     