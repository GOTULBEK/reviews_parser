from __future__ import annotations

import asyncio
import csv
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

from app.core.config import settings

# NOTE: We keep the required columns first, then add everything else we have.

PLACE_COLUMNS: tuple[str, ...] = (
    # --- required ---
    "place_id",
    "name",
    "category",
    "parent_brand",
    "is_branch",
    "address",
    "city",
    "latitude",
    "longitude",
    "rating_avg",
    "reviews_count",
    "source",
    # --- extra (best-effort, may be empty) ---
    "place_url",
    "rating_distribution_json",
    "company_name",
    "scraped_at",
    "task_id",
)

REVIEW_COLUMNS: tuple[str, ...] = (
    # --- required ---
    "review_id",
    "place_id",
    "review_text",
    "stars",
    "review_date",
    "language",
    "source",
    # --- optional-if-exists (your list) ---
    "review_title",
    "author_name",
    "author_reviews_count",
    "likes_count",
    "owner_reply",
    "owner_reply_date",
    "photos_count",
    # --- extra (best-effort) ---
    "review_url",
    "is_rated",
    "date_edited",
    "hiding_reason",
    "raw_json",
    "task_id",
)

_places_lock = asyncio.Lock()
_reviews_lock = asyncio.Lock()


def _dataset_dir() -> Path:
    # settings.dataset_dir may not exist yet in older configs → default here too.
    d = getattr(settings, "dataset_dir", None) or "datasets"
    return Path(d)


def _to_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)
    return str(value)


def _append_row_sync(path: Path, fieldnames: tuple[str, ...], row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    # newline="" is required by csv module to avoid blank lines on some platforms.
    file_exists = path.exists()
    write_header = (not file_exists) or (path.stat().st_size == 0)

    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow({k: _to_cell(v) for k, v in row.items()})


async def append_place_row(row: dict[str, Any]) -> None:
    path = _dataset_dir() / "places.csv"
    async with _places_lock:
        await asyncio.to_thread(_append_row_sync, path, PLACE_COLUMNS, row)


async def append_review_row(row: dict[str, Any]) -> None:
    path = _dataset_dir() / "reviews.csv"
    async with _reviews_lock:
        await asyncio.to_thread(_append_row_sync, path, REVIEW_COLUMNS, row)


def build_place_row(*, task_id: str, city: str, branch_data: dict[str, Any]) -> dict[str, Any]:
    gis_branch_id = branch_data.get("gis_branch_id")
    company_name = (branch_data.get("company_name") or "").strip() or None
    address = (branch_data.get("address") or "").strip() or None

    return {
        "place_id": gis_branch_id,
        "name": company_name,
        "category": None,
        "parent_brand": company_name,
        "is_branch": True,
        "address": address,
        "city": city,
        "latitude": None,
        "longitude": None,
        "rating_avg": branch_data.get("rating"),
        "reviews_count": branch_data.get("total_reviews"),
        "source": "2gis",
        "place_url": branch_data.get("url"),
        "rating_distribution_json": branch_data.get("rating_distribution"),
        "company_name": company_name,
        "scraped_at": datetime.utcnow().isoformat(),
        "task_id": task_id,
    }


def _raw_get(raw: dict[str, Any] | None, *keys: str) -> Any:
    cur: Any = raw or {}
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def build_review_row(*, task_id: str, place_id: int, review: dict[str, Any]) -> dict[str, Any]:
    raw = review.get("raw") if isinstance(review.get("raw"), dict) else None

    # Best-effort extractions from raw payload (schema may change).
    lang = (
        _raw_get(raw, "language")
        or _raw_get(raw, "lang")
        or _raw_get(raw, "meta", "language")
    )

    photos = _raw_get(raw, "photos")
    if isinstance(photos, list):
        photos_count = len(photos)
    else:
        photos_count = _raw_get(raw, "photos_count") or _raw_get(raw, "images_count")

    return {
        "review_id": review.get("gis_review_id"),
        "place_id": place_id,
        "review_text": review.get("text"),
        "stars": review.get("rating"),
        "review_date": review.get("date_created"),
        "language": lang,
        "source": "2gis",
        # optional-if-exists
        "review_title": _raw_get(raw, "title") or _raw_get(raw, "review_title"),
        "author_name": review.get("user_name") or _raw_get(raw, "user", "name"),
        "author_reviews_count": _raw_get(raw, "user", "reviews_count") or _raw_get(raw, "user", "reviewsCount"),
        "likes_count": _raw_get(raw, "likes_count") or _raw_get(raw, "likesCount") or _raw_get(raw, "likes"),
        "owner_reply": review.get("official_answer_text"),
        "owner_reply_date": review.get("official_answer_date"),
        "photos_count": photos_count,
        # extra
        "review_url": review.get("review_url"),
        "is_rated": review.get("is_rated"),
        "date_edited": review.get("date_edited"),
        "hiding_reason": review.get("hiding_reason"),
        "raw_json": raw,
        "task_id": task_id,
    }

