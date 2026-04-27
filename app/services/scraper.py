from __future__ import annotations

import asyncio
import json
import logging
import random
import re
from datetime import datetime
from typing import Any
from urllib.parse import quote

import httpx
from bs4 import BeautifulSoup

from app.core.config import settings

logger = logging.getLogger(__name__)

REVIEWS_API_BASE = "https://public-api.reviews.2gis.com/3.0"
SITE_BASE = "https://2gis.kz"

_HEADERS_JSON = {
    "User-Agent": settings.user_agent,
    "Accept": "application/json",
    "Referer": "https://2gis.kz/",
}

_HEADERS_HTML = {
    "User-Agent": settings.user_agent,
    "Accept": "text/html,application/xhtml+xml",
    "Referer": "https://2gis.kz/",
}


# ---------------------------------------------------------------------------
# Step 1 — text search → list of branch IDs
# ---------------------------------------------------------------------------

async def search_branches(
    client: httpx.AsyncClient, query: str, city: str, max_branches: int
) -> list[dict]:
    """
    Ищет филиалы по текстовому запросу через HTML страницы поиска 2ГИС.

    ХРУПКИЙ КОМПОНЕНТ: зависит от разметки 2ГИС. Если поиск вернет 0, см. README —
    скорее всего 2ГИС поменял рендеринг или страница требует JS-гидрации.

    Единственная зацепка — регулярка на паттерн `/firm/<digits>` в HTML.
    Дубли убираются, порядок первого появления сохраняется (соответствует выдаче).
    """
    seen: set[int] = set()
    ordered: list[int] = []

    # 2GIS search pages:
    # - page 1: /{city}/search/{query}
    # - page N: /{city}/search/{query}/page/{N}/
    safe_query = quote(query, safe="")
    page = 1
    while True:
        if len(ordered) >= max_branches:
            break

        search_url = (
            f"{SITE_BASE}/{city}/search/{safe_query}"
            if page == 1
            else f"{SITE_BASE}/{city}/search/{safe_query}/page/{page}/"
        )

        logger.info("Search request: %s", search_url)

        try:
            res = await client.get(search_url, headers=_HEADERS_HTML)
        except httpx.RequestError as e:
            logger.error("Search HTTP error: %s", e)
            break

        if res.status_code != 200:
            # 404/410 usually means "page does not exist" (end of pagination).
            if res.status_code in (404, 410):
                logger.info("Search pagination ended at page=%d (HTTP %d)", page, res.status_code)
            else:
                logger.error("Search HTTP %d: %s", res.status_code, res.text[:200])
            break

        before = len(ordered)
        for m in re.finditer(r"/firm/(\d+)", res.text):
            fid = int(m.group(1))
            if fid not in seen:
                seen.add(fid)
                ordered.append(fid)
                if len(ordered) >= max_branches:
                    break

        # Stop when page is empty (no new firm IDs).
        if len(ordered) == before:
            break

        page += 1

    ordered = ordered[:max_branches]
    return [
        {
            "gis_branch_id": fid,
            "firm_url": f"{SITE_BASE}/{city}/firm/{fid}",
            "city": city,
        }
        for fid in ordered
    ]


# ---------------------------------------------------------------------------
# Step 2 — per-branch address (HTML scrape)
# ---------------------------------------------------------------------------

def _normalize_firm_url(firm_url: str) -> str:
    """Отрезает /tab/* и query/fragment — для запросов HTML нужен голый URL фирмы."""
    url = re.sub(r"/tab/.*$", "", firm_url)
    return re.split(r"[?#]", url)[0].rstrip("/")


async def _fetch_firm_soup(client: httpx.AsyncClient, firm_url: str) -> BeautifulSoup | None:
    """Загружает firm-страницу и парсит в BeautifulSoup. None при любой ошибке."""
    page_url = _normalize_firm_url(firm_url)
    try:
        res = await client.get(page_url, headers=_HEADERS_HTML)
    except httpx.RequestError as e:
        logger.warning("Firm page fetch error (%s): %s", page_url, e)
        return None

    if res.status_code != 200:
        logger.warning("Firm page HTTP %d: %s", res.status_code, page_url)
        return None

    return BeautifulSoup(res.text, "html.parser")


def _extract_address_from_soup(soup: BeautifulSoup) -> str:
    """
    Структурная зацепка — <a href="/<city>/geo/<id>">. CSS-классы игнорируем
    (CSS-in-JS хеши меняются при каждом билде).
    """
    geo_link = soup.find("a", href=re.compile(r"^/[^/]+/geo/\d+"))
    if not geo_link:
        return "Адрес не найден"

    outer_span = geo_link.find_parent("span")
    container = outer_span.find_parent("span") if outer_span else None
    if not container:
        return geo_link.get_text(strip=True).replace("\xa0", " ")

    parts: list[str] = []
    for span in container.find_all("span", recursive=False):
        # Пропускаем счетчик "NN филиал"
        if span.find("a", href=re.compile(r"/branches/\d+")):
            continue
        t = span.get_text(strip=True).replace("\xa0", " ")
        if t:
            parts.append(t)

    district_div = container.find_next_sibling("div")
    if district_div:
        d_text = district_div.get_text(strip=True).replace("\xa0", " ")
        if "район" in d_text.lower() or re.search(r"\d{6}", d_text):
            parts.append(d_text)

    return ", ".join(parts) if parts else "Адрес не найден"


def _extract_name_from_soup(soup: BeautifulSoup) -> str | None:
    """
    Извлекает название фирмы. Каскад источников от чистых к грязным.
    Если все дают None — возвращаем None, вызывающий код выберет заглушку.
    """
    # 1. og:title — обычно чистая строка вида "Underground gym, фитнес-клуб"
    og = soup.find("meta", attrs={"property": "og:title"}) or soup.find(
        "meta", attrs={"name": "og:title"}
    )
    if og and og.get("content"):
        value = og["content"].strip()
        if value:
            return _strip_2gis_suffix(value)

    # 2. JSON-LD schema.org Organization/LocalBusiness
    for script in soup.find_all("script", type="application/ld+json"):
        if not script.string:
            continue
        try:
            data = json.loads(script.string)
        except (json.JSONDecodeError, TypeError):
            continue
        candidates = data if isinstance(data, list) else [data]
        for c in candidates:
            if isinstance(c, dict) and c.get("name"):
                name = str(c["name"]).strip()
                if name:
                    return name

    # 3. h1 — часто есть, но может содержать хлебные крошки
    h1 = soup.find("h1")
    if h1:
        text = h1.get_text(strip=True)
        if text:
            return text

    # 4. <title> — самый грязный вариант: "NAME в Городе на адресе — отзывы — 2GIS"
    if soup.title and soup.title.string:
        return _strip_2gis_suffix(soup.title.string.strip())

    return None


def _strip_2gis_suffix(value: str) -> str:
    """Убирает хвост ' — 2GIS' и обрезает по первому ' — ' если заголовок длинный."""
    value = re.sub(r"\s*[—-]\s*2G(I|Г)S\s*$", "", value, flags=re.IGNORECASE).strip()
    # Если осталось что-то вроде "Underground gym — отзывы, телефон, адрес" — берем до первого ' — '
    # Но только если после обрезки осталось что-то осмысленное (>=3 символа)
    parts = re.split(r"\s+[—-]\s+", value, maxsplit=1)
    if len(parts) == 2 and len(parts[0]) >= 3:
        return parts[0]
    return value


async def scrape_branch_address(client: httpx.AsyncClient, firm_url: str) -> str:
    """Обертка для обратной совместимости: одна сетевая загрузка + извлечение адреса."""
    soup = await _fetch_firm_soup(client, firm_url)
    if soup is None:
        return "Адрес не найден"
    return _extract_address_from_soup(soup)


async def scrape_branch_preview(
    client: httpx.AsyncClient, gis_branch_id: int, firm_url: str
) -> dict:
    """
    Легковесный превью: одна HTML-загрузка, возвращает имя + адрес без отзывов.
    Используется для экрана выбора перед полным скрапом.
    """
    soup = await _fetch_firm_soup(client, firm_url)
    if soup is None:
        return {
            "gis_branch_id": gis_branch_id,
            "firm_url": _normalize_firm_url(firm_url),
            "name": None,
            "address": "Адрес не найден",
        }

    return {
        "gis_branch_id": gis_branch_id,
        "firm_url": _normalize_firm_url(firm_url),
        "name": _extract_name_from_soup(soup),
        "address": _extract_address_from_soup(soup),
    }


# ---------------------------------------------------------------------------
# Step 3 — reviews API
# ---------------------------------------------------------------------------

async def fetch_rating_distribution(client: httpx.AsyncClient, gis_branch_id: int) -> dict:
    url = f"{REVIEWS_API_BASE}/branches/{gis_branch_id}/reviews/stats"
    try:
        res = await client.get(
            url,
            params={"key": settings.twogis_reviews_api_key, "locale": "ru_RU"},
            headers=_HEADERS_JSON,
        )
        if res.status_code == 200:
            return res.json().get("ratings", {}) or {}
    except httpx.RequestError as e:
        logger.warning("Rating dist error: %s", e)
    return {}


async def fetch_reviews_batch(
    client: httpx.AsyncClient, gis_branch_id: int, rated: str, offset: int
) -> tuple[list[dict], dict, str | None]:
    url = f"{REVIEWS_API_BASE}/branches/{gis_branch_id}/reviews"
    params = {
        "limit": 50,
        "offset": offset,
        "is_advertiser": "false",
        "fields": (
            "meta.providers,meta.branch_rating,meta.branch_reviews_count,"
            "meta.total_count,reviews.hiding_reason,reviews.emojis,reviews.trust_factors"
        ),
        "without_my_first_review": "false",
        "rated": rated,
        "sort_by": "friends",
        "key": settings.twogis_reviews_api_key,
        "locale": "ru_RU",
    }
    try:
        res = await client.get(url, params=params, headers=_HEADERS_JSON)
    except httpx.RequestError as e:
        return [], {}, str(e)

    if res.status_code != 200:
        return [], {}, f"HTTP {res.status_code}"

    data = res.json()
    return data.get("reviews", []), data.get("meta", {}), None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_iso_datetime(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        # 2ГИС: "2025-09-16T14:37:07.248414+07:00"
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None


def _parse_rating(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def build_review_url(gis_branch_id: int, gis_review_id: str) -> str:
    """Публичный URL отзыва для редиректа с фронта."""
    return f"{SITE_BASE}/reviews/{gis_branch_id}/review/{gis_review_id}"


# ---------------------------------------------------------------------------
# Step 4 — aggregate scrape of one branch
# ---------------------------------------------------------------------------

async def scrape_branch(client: httpx.AsyncClient, gis_branch_id: int, firm_url: str) -> dict:
    """
    Скрапит адрес + распределение рейтинга + все отзывы одного филиала.
    Возвращает словарь, готовый для upsert в БД.
    """
    address, distribution = await asyncio.gather(
        scrape_branch_address(client, firm_url),
        fetch_rating_distribution(client, gis_branch_id),
    )

    all_reviews: list[dict] = []
    company_name: str | None = None
    final_rating: float | None = None
    final_total: int | None = None

    for rated in ("true", "false"):
        offset = 0
        while True:
            reviews, meta, err = await fetch_reviews_batch(client, gis_branch_id, rated, offset)
            if err:
                logger.warning("Branch %s rated=%s offset=%s: %s", gis_branch_id, rated, offset, err)
                break

            # Первая порция — забираем сводные метрики
            if final_rating is None:
                try:
                    br = meta.get("branch_rating")
                    final_rating = float(br) if br is not None else None
                except (ValueError, TypeError):
                    final_rating = None
                try:
                    brc = meta.get("branch_reviews_count")
                    final_total = int(brc) if brc is not None else None
                except (ValueError, TypeError):
                    final_total = None

            total_for_filter = meta.get("total_count", 0)
            if total_for_filter == 0 or not reviews:
                break

            for rev in reviews:
                off_ans = rev.get("official_answer") or {}
                if not company_name and off_ans.get("org_name"):
                    company_name = off_ans.get("org_name")

                review_id = str(rev.get("id") or "")
                if not review_id:
                    # Без ID отзыва — пропускаем, нечем дедуплицировать
                    continue

                all_reviews.append({
                    "gis_review_id": review_id,
                    "user_name": (rev.get("user") or {}).get("name") or "Аноним",
                    "rating": _parse_rating(rev.get("rating")),
                    "text": rev.get("text") or "",
                    "official_answer_text": off_ans.get("text") or None,
                    "official_answer_date": _parse_iso_datetime(off_ans.get("date_created")),
                    "hiding_reason": rev.get("hiding_reason") or None,
                    "is_rated": rated == "true",
                    "date_created": _parse_iso_datetime(rev.get("date_created")),
                    "date_edited": _parse_iso_datetime(rev.get("date_edited")),
                    "review_url": build_review_url(gis_branch_id, review_id),
                    "raw": rev,
                })

            offset += 50
            if offset >= total_for_filter:
                break

            await asyncio.sleep(random.uniform(settings.rate_limit_sleep_min, settings.rate_limit_sleep_max))

    return {
        "gis_branch_id": gis_branch_id,
        "company_name": company_name,
        "address": address,
        "rating": final_rating,
        "total_reviews": final_total,
        "rating_distribution": distribution,
        "url": firm_url,
        "reviews": all_reviews,
    }
