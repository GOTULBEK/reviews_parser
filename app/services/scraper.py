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
from app.services.cities import KZ_CITY_SLUGS, normalize_city_slug

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

async def _get_with_retry(
    client: httpx.AsyncClient, url: str, headers: dict, attempts: int = 3
) -> httpx.Response | None:
    """GET с ретраями на сетевые сбои и 5xx.

    Без ретраев одна транзиентная ошибка (reset/timeout) на 1-й странице поиска
    обнуляла всю выдачу. Возвращает Response (любой статус) или None, если все
    попытки упали на сетевом уровне.
    """
    last_exc: Exception | None = None
    for i in range(attempts):
        try:
            res = await client.get(url, headers=headers)
        except httpx.RequestError as e:
            last_exc = e
            logger.warning("Search GET failed (attempt %d/%d): %r", i + 1, attempts, e)
        else:
            # 5xx — транзиентная серверная ошибка, имеет смысл повторить.
            if res.status_code >= 500 and i < attempts - 1:
                logger.warning("Search GET HTTP %d (attempt %d/%d), retrying", res.status_code, i + 1, attempts)
            else:
                return res
        if i < attempts - 1:
            await asyncio.sleep(0.5 * (i + 1))
    if last_exc is not None:
        logger.error("Search GET gave up after %d attempts: %r", attempts, last_exc)
    return None

def _parse_search_total(html: str) -> int | None:
    """Достаёт заявленное 2ГИС число результатов (`"total":NN`) со страницы поиска.

    2ГИС знает реальное количество (напр. 83), но отдаёт через HTML максимум ~60.
    Нужно, чтобы понять, есть ли смысл добирать остаток deep-под-запросами.
    """
    m = re.search(r'"total":(\d+)', html)
    return int(m.group(1)) if m else None


def _parse_rubric_facets(html: str, limit: int) -> list[str]:
    """Извлекает названия рубрик-фасетов (`"rubrics":[{... "count":N}]`) из HTML поиска.

    Эти рубрики 2ГИС считает по текущему запросу (напр. Банкоматы=63, Банки=12).
    Возвращаем их имена — для уточняющих под-запросов "{query} {рубрика}", чтобы
    обойти лимит ~60 фирм на запрос. По убыванию count, без дублей, не более limit.
    """
    facets: dict[str, int] = {}
    for m in re.finditer(r'"rubrics":\[', html):
        start = m.end() - 1
        depth = 0
        i = start
        # Ручной матчинг скобок: вложенный JSON нельзя взять регуляркой.
        while i < len(html):
            ch = html[i]
            if ch == "[":
                depth += 1
            elif ch == "]":
                depth -= 1
                if depth == 0:
                    break
            i += 1
        chunk = html[start : i + 1]
        if '"count"' not in chunk or len(chunk) > 8000:
            continue
        try:
            arr = json.loads(chunk)
        except (json.JSONDecodeError, ValueError):
            continue
        for x in arr:
            if isinstance(x, dict) and x.get("count") and x.get("name"):
                name = str(x["name"]).strip()
                if name:
                    facets[name] = max(facets.get(name, 0), int(x["count"]))
    return [name for name, _ in sorted(facets.items(), key=lambda kv: kv[1], reverse=True)][:limit]


async def _collect_query_firm_ids(
    client: httpx.AsyncClient,
    query: str,
    city: str,
    *,
    seen: set[int],
    ordered: list[int],
    max_branches: int,
    unlimited: bool,
    page_cap: int,
    map_center: tuple[float, float] | None = None,
    max_pages: int | None = None,
) -> str | None:
    """Пагинирует один текстовый запрос, добавляя НОВЫЕ firm-id в seen/ordered.

    Возвращает HTML первой страницы (для разбора фасетов/total) или None.
    seen/ordered — общие на серию запросов, поэтому дедуп работает между ними.

    map_center=(lon, lat): сдвигает вьюпорт карты (`?m=lon,lat/zoom`). 2ГИС ранжирует
    выдачу относительно вьюпорта, поэтому из разных центров приходят разные топ-60 —
    так geo-sweep обходит лимит ~60 фирм на запрос.

    ВАЖНО: условие останова — по ЛОКАЛЬНОЙ выдаче этого запроса (а не по общему
    ordered). Иначе center, чья 1-я страница уже целиком в seen, оборвётся сразу и
    не дойдёт до своих уникальных фирм, лежащих на глубоких страницах вьюпорта.
    """
    safe_query = quote(query, safe="")
    suffix = (
        f"?m={map_center[0]}%2C{map_center[1]}%2F{settings.deep_search_zoom}"
        if map_center is not None
        else ""
    )
    page1_html: str | None = None
    local_seen: set[int] = set()
    page = 1
    while True:
        if not unlimited and len(ordered) >= max_branches:
            break
        if max_pages is not None and page > max_pages:
            break
        if page > page_cap:
            logger.warning(
                "Search hit page hard-cap (%d) for city=%s query=%r — stopping with %d results",
                page_cap, city, query, len(ordered),
            )
            break

        # 2GIS search pages:
        # - page 1: /{city}/search/{query}
        # - page N: /{city}/search/{query}/page/{N}/
        search_url = (
            f"{SITE_BASE}/{city}/search/{safe_query}"
            if page == 1
            else f"{SITE_BASE}/{city}/search/{safe_query}/page/{page}/"
        ) + suffix

        logger.info("Search request: %s", search_url)

        res = await _get_with_retry(client, search_url, _HEADERS_HTML)
        if res is None:
            # Все попытки упали на сетевом уровне. Если это была не первая страница,
            # отдаём, что уже собрали; на первой — пусто (и наверху будет 503).
            break

        if "/museum" in str(res.url):
            logger.error("2GIS Captcha (museum) triggered for %s!", search_url)
            raise RuntimeError("2GIS Captcha triggered (Bot detection). Try again later.")

        if res.status_code != 200:
            # 404/410 usually means "page does not exist" (end of pagination).
            if res.status_code in (404, 410):
                logger.info("Search pagination ended at page=%d (HTTP %d)", page, res.status_code)
            else:
                logger.error("Search HTTP %d: %s", res.status_code, res.text[:200])
            break

        if page == 1:
            page1_html = res.text

        before_local = len(local_seen)
        for m in re.finditer(r"/firm/(\d+)", res.text):
            fid = int(m.group(1))
            local_seen.add(fid)
            if fid not in seen:
                seen.add(fid)
                ordered.append(fid)
                if not unlimited and len(ordered) >= max_branches:
                    break

        # Stop when THIS query/viewport yields no new firm IDs of its own.
        if len(local_seen) == before_local:
            break

        page += 1

    return page1_html


def _sweep_centers(bbox: tuple[float, float, float, float], grid: int) -> list[tuple[float, float]]:
    """Раскладывает по bbox города сетку grid×grid центров карты для geo-sweep.

    Точки берём в центрах ячеек (отступ от краёв), по убыванию близости к центру
    города — центральные вьюпорты плотнее по фирмам, поэтому добор оттуда полезнее
    и срабатывает раньше при early-stop по total.
    """
    min_lon, min_lat, max_lon, max_lat = bbox
    if grid < 1:
        grid = 1
    mid_lon = (min_lon + max_lon) / 2
    mid_lat = (min_lat + max_lat) / 2
    pts: list[tuple[float, float]] = []
    for i in range(grid):
        for j in range(grid):
            lon = round(min_lon + (i + 0.5) / grid * (max_lon - min_lon), 6)
            lat = round(min_lat + (j + 0.5) / grid * (max_lat - min_lat), 6)
            pts.append((lon, lat))
    pts.sort(key=lambda p: (p[0] - mid_lon) ** 2 + (p[1] - mid_lat) ** 2)
    return pts


async def search_branches(
    client: httpx.AsyncClient,
    query: str,
    city: str,
    max_branches: int,
    *,
    deep: bool = False,
    bbox: tuple[float, float, float, float] | None = None,
) -> list[dict]:
    """
    Ищет филиалы по текстовому запросу через HTML страницы поиска 2ГИС.

    ХРУПКИЙ КОМПОНЕНТ: зависит от разметки 2ГИС. Если поиск вернет 0, см. README —
    скорее всего 2ГИС поменял рендеринг или страница требует JS-гидрации.

    Единственная зацепка — регулярка на паттерн `/firm/<digits>` в HTML.
    Дубли убираются, порядок первого появления сохраняется (соответствует выдаче).

    deep=True: 2ГИС отдаёт максимум ~60 фирм на запрос (5 страниц × 12), хотя реальных
    совпадений бывает больше (см. `"total"`). Чтобы добрать остаток БЕЗ API-ключа:
      • geo-sweep (если известен bbox города): повторяем запрос из сетки центров карты
        (`?m=lon,lat/zoom`) — у каждого вьюпорта свой топ-60, объединение покрывает весь
        набор. Это основной механизм, обычно добирает до реального total.
      • рубричный добор (фолбэк, если bbox нет): под-запросы "{query} {рубрика}" по
        фасетам рубрик со страницы поиска.
    Полнота best-effort, но покрытие сильно выше базовых 60. Стоит лишних запросов к 2ГИС.
    """
    seen: set[int] = set()
    ordered: list[int] = []

    # max_branches <= 0 → "без лимита": собираем всё, что 2ГИС отдаёт по запросу,
    # пока не закончится пагинация (404/410). page-бэкстоп страхует от зацикливания.
    unlimited = max_branches <= 0
    page_cap = settings.search_max_pages_hard_cap

    page1_html = await _collect_query_firm_ids(
        client, query, city,
        seen=seen, ordered=ordered, max_branches=max_branches,
        unlimited=unlimited, page_cap=page_cap,
    )

    # Deep: добираем остаток сверх базовых ~60. Имеет смысл, только если хотим больше,
    # чем уже набрали (unlimited или лимит ещё не выбран).
    if deep and page1_html and (unlimited or len(ordered) < max_branches):
        total = _parse_search_total(page1_html)

        def _need_more() -> bool:
            if not unlimited and len(ordered) >= max_branches:
                return False
            # total известен и уже добрали всё — нет смысла слать ещё запросы.
            if total is not None and len(ordered) >= total:
                return False
            return True

        # --- Шаг 1: geo-sweep по сетке центров карты (основной механизм). ---
        if bbox is not None and _need_more():
            centers = _sweep_centers(bbox, settings.deep_search_grid)
            logger.info(
                "Deep geo-sweep city=%s query=%r: base=%d, total≈%s, %d center(s)",
                city, query, len(ordered), total, len(centers),
            )
            sem = asyncio.Semaphore(settings.deep_search_concurrency)

            async def _one_center(ctr: tuple[float, float]) -> None:
                if not _need_more():
                    return
                async with sem:
                    if not _need_more():
                        return
                    await _collect_query_firm_ids(
                        client, query, city,
                        seen=seen, ordered=ordered, max_branches=max_branches,
                        unlimited=unlimited, page_cap=page_cap,
                        map_center=ctr, max_pages=settings.deep_search_center_pages,
                    )

            await asyncio.gather(*(_one_center(c) for c in centers))

        # --- Шаг 2: рубричный добор. Дополняет sweep (рубрики ловят фирмы, которые
        # geo-выдача оставила за топом вьюпортов), а при отсутствии bbox — заменяет его.
        if _need_more():
            rubrics = _parse_rubric_facets(page1_html, settings.deep_search_max_rubrics)
            if rubrics:
                logger.info(
                    "Deep rubric top-up city=%s query=%r: have=%d, total≈%s, %d rubric(s)",
                    city, query, len(ordered), total, len(rubrics),
                )
            for rub in rubrics:
                if not _need_more():
                    break
                await _collect_query_firm_ids(
                    client, f"{query} {rub}", city,
                    seen=seen, ordered=ordered, max_branches=max_branches,
                    unlimited=unlimited, page_cap=page_cap,
                )

    if not unlimited:
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
    res = await _get_with_retry(client, page_url, _HEADERS_HTML, attempts=2)
    if res is None:
        logger.warning("Firm page fetch failed after retries: %s", page_url)
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


def _extract_name_and_categories_from_soup(soup: BeautifulSoup) -> tuple[str | None, str | None, list[str]]:
    """
    Извлекает название фирмы, первичную категорию (legacy) и список всех рубрик.
    Рубрики берутся из <span class="_3yxk2u"> — элементы на странице фирмы 2ГИС.
    """
    _ADDR_KEYWORDS = ["улица", "ул.", "проспект", "пр.", "микрорайон", "мкр.", "шоссе", "тракт", "переулок", "квартал", "район", "д."]

    def _is_address_like(s: str) -> bool:
        return any(kw in s.lower() for kw in _ADDR_KEYWORDS) or any(c.isdigit() for c in s)

    # ---- Extract all rubrics from DOM (<span class="_3yxk2u">) ----
    rubric_spans = soup.find_all("span", class_="_3yxk2u")
    categories: list[str] = []
    for span in rubric_spans:
        text = span.get_text(strip=True)
        if text and not _is_address_like(text):
            categories.append(text)

    # ---- Extract name + fallback primary category from og:title ----
    og = soup.find("meta", attrs={"property": "og:title"}) or soup.find(
        "meta", attrs={"name": "og:title"}
    )
    if og and og.get("content"):
        value = og["content"].strip()
        if value:
            stripped = _strip_2gis_suffix(value)
            parts = [p.strip() for p in stripped.split(", ")]
            name = parts[0]
            category = parts[1] if len(parts) > 1 else None
            if category and _is_address_like(category):
                category = None
            # If we have rubrics from DOM, use first rubric as primary category
            if categories:
                category = categories[0]
            return name, category, categories

    # 2. JSON-LD
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
                    category = categories[0] if categories else None
                    return name, category, categories

    # 3. h1
    h1 = soup.find("h1")
    if h1:
        text = h1.get_text(strip=True)
        if text:
            category = categories[0] if categories else None
            return text, category, categories

    # 4. <title>
    if soup.title and soup.title.string:
        stripped = _strip_2gis_suffix(soup.title.string.strip())
        parts = [p.strip() for p in stripped.split(", ")]
        name = parts[0]
        category = parts[1] if len(parts) > 1 else None
        if category and _is_address_like(category):
            category = None
        if categories:
            category = categories[0]
        return name, category, categories

    return None, None, categories


# Keep old name as alias for backward-compat callers
def _extract_name_and_category_from_soup(soup: BeautifulSoup) -> tuple[str | None, str | None]:
    name, category, _ = _extract_name_and_categories_from_soup(soup)
    return name, category



def _strip_2gis_suffix(value: str) -> str:
    """Убирает хвост ' — 2GIS' и обрезает по первому ' — ' если заголовок длинный."""
    value = re.sub(r"\s*[—-]\s*2G(I|Г)S\s*$", "", value, flags=re.IGNORECASE).strip()
    # Если осталось что-то вроде "Underground gym — отзывы, телефон, адрес" — берем до первого ' — '
    # Но только если после обрезки осталось что-то осмысленное (>=3 символа)
    parts = re.split(r"\s+[—-]\s+", value, maxsplit=1)
    if len(parts) == 2 and len(parts[0]) >= 3:
        return parts[0]
    return value


def _extract_city_from_soup(soup: BeautifulSoup) -> str | None:
    """Достаёт slug города из ссылок страницы фирмы (`/{city}/firm|geo/{id}`).

    Работает, т.к. 2ГИС редиректит firm-URL с любым городом на правильный, и все
    ссылки на странице уже содержат настоящий город. Нужно для city='all', где
    город каждого филиала заранее неизвестен.
    """
    for a in soup.find_all("a", href=re.compile(r"^/[a-z-]+/(?:firm|geo)/\d+")):
        m = re.match(r"^/([a-z-]+)/(?:firm|geo)/\d+", a.get("href", ""))
        if not m:
            continue
        # 2ГИС использует иную транслитерацию для городов на «-й» (kostanaj/semej),
        # приводим к каноническому slug каталога перед проверкой.
        slug = normalize_city_slug(m.group(1))
        if slug in KZ_CITY_SLUGS:
            return slug
    return None


async def scrape_branch_info(client: httpx.AsyncClient, firm_url: str) -> tuple[str | None, str | None, str, list[str], str | None]:
    """Обертка для одной сетевой загрузки + извлечение имени, категорий, адреса, города."""
    soup = await _fetch_firm_soup(client, firm_url)
    if soup is None:
        return None, None, "Адрес не найден", [], None
    name, category, categories = _extract_name_and_categories_from_soup(soup)
    return name, category, _extract_address_from_soup(soup), categories, _extract_city_from_soup(soup)


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
            "category": None,
            "address": "Адрес не найден",
        }

    name, category = _extract_name_and_category_from_soup(soup)
    return {
        "gis_branch_id": gis_branch_id,
        "firm_url": _normalize_firm_url(firm_url),
        "name": name,
        "category": category,
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
    (name, category, address, categories, city), distribution = await asyncio.gather(
        scrape_branch_info(client, firm_url),
        fetch_rating_distribution(client, gis_branch_id),
    )

    all_reviews: list[dict] = []
    company_name: str | None = name
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
        "category": category,
        "categories": categories,
        "address": address,
        "rating": final_rating,
        "total_reviews": final_total,
        "rating_distribution": distribution,
        "url": firm_url,
        "city": city,
        "reviews": all_reviews,
    }
