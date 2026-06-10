from __future__ import annotations

import asyncio
import logging
import re
import time

import httpx

from app.core.config import settings

logger = logging.getLogger(__name__)

# Тип bbox города: (min_lon, min_lat, max_lon, max_lat) в WGS84.
BBox = tuple[float, float, float, float]


def _bbox_from_wkt(wkt: str | None) -> BBox | None:
    """Считает охватывающий прямоугольник из WKT-полигона границ города 2ГИС.

    2ГИС отдаёт `bounds` как `POLYGON((lon lat, lon lat, ...))`. Нам не нужен сам
    полигон — только bbox, чтобы разложить по городу сетку центров карты для
    geo-sweep поиска (обход лимита ~60 фирм на запрос). Чётные числа — долготы,
    нечётные — широты.
    """
    if not wkt:
        return None
    nums = re.findall(r"-?\d+\.\d+", wkt)
    if len(nums) < 4:
        return None
    lons = [float(n) for n in nums[0::2]]
    lats = [float(n) for n in nums[1::2]]
    if not lons or not lats:
        return None
    return (min(lons), min(lats), max(lons), max(lats))

# ---------------------------------------------------------------------------
# Каталог городов Казахстана из 2ГИС.
#
# region_id и человекочитаемое имя приходят из 2ГИС Catalog API
# (`/2.0/region/list?country_code_filter=kz`). НО API не отдаёт URL-slug
# (`almaty`, `astana`, ...), который нужен для построения ссылок вида
# `https://2gis.kz/{slug}/search/...`. Slug — это статичная транслитерация,
# поэтому держим её здесь как источник истины, а имена/статистику освежаем
# из живого API.
#
# Все 19 slug-ов проверены: каждый `https://2gis.kz/{slug}` отдаёт HTTP 200.
# ---------------------------------------------------------------------------

# region_id -> URL slug
_SLUG_BY_REGION_ID: dict[str, str] = {
    "196": "aktau",
    "167": "aktobe",
    "67": "almaty",
    "68": "astana",
    "168": "atyrau",
    "242": "zhezkazgan",
    "84": "karaganda",
    "201": "kokshetau",
    "203": "kostanay",
    "240": "kyzylorda",
    "111": "pavlodar",
    "170": "petropavlovsk",
    "169": "semey",
    "221": "taraz",
    "232": "turkestan",
    "162": "uralsk",
    "91": "ust-kamenogorsk",
    "161": "shymkent",
    "252": "ekibastuz",
}

# Статичный фолбэк-каталог (используется, если живой запрос к 2ГИС не удался).
# name — на момент написания; живой API при успехе перезапишет.
_STATIC_NAMES: dict[str, str] = {
    "196": "Актау",
    "167": "Актобе",
    "67": "Алматы",
    "68": "Астана",
    "168": "Атырау",
    "242": "Жезказган",
    "84": "Караганда",
    "201": "Кокшетау",
    "203": "Костанай",
    "240": "Кызылорда",
    "111": "Павлодар",
    "170": "Петропавловск",
    "169": "Семей",
    "221": "Тараз",
    "232": "Туркестан",
    "162": "Уральск",
    "91": "Усть-Каменогорск",
    "161": "Шымкент",
    "252": "Экибастуз",
}


def _static_catalog() -> list[dict]:
    return [
        {
            "id": rid,
            "slug": _SLUG_BY_REGION_ID[rid],
            "name": _STATIC_NAMES[rid],
            "branch_count": None,
            "bbox": None,
        }
        for rid in _SLUG_BY_REGION_ID
    ]


# In-memory кэш (каталог меняется крайне редко).
_cache: list[dict] | None = None
_cache_ts: float = 0.0
_lock = asyncio.Lock()


async def _fetch_live_catalog() -> list[dict]:
    """Тянет список регионов Казахстана из 2ГИС Catalog API и сшивает со slug-картой.

    Возвращает только города, для которых известен URL-slug. Города, появившиеся
    в API, но отсутствующие в `_SLUG_BY_REGION_ID`, логируются и пропускаются —
    их нельзя превратить в рабочий URL без проверенного slug.
    """
    url = f"{settings.twogis_catalog_base}/2.0/region/list"
    params = {
        "key": settings.twogis_web_api_key,
        "country_code_filter": "kz",
        "page_size": 300,
        # items.bounds — WKT-полигон границ города; из него берём bbox для geo-sweep.
        "fields": "items.statistics,items.bounds",
    }
    headers = {
        "User-Agent": settings.user_agent,
        "Accept": "application/json",
        "Referer": "https://2gis.kz/",
    }
    timeout = httpx.Timeout(settings.request_timeout_seconds, connect=10)
    async with httpx.AsyncClient(timeout=timeout) as client:
        res = await client.get(url, params=params, headers=headers)
        res.raise_for_status()
        data = res.json()

    if data.get("meta", {}).get("code") != 200:
        raise RuntimeError(f"2GIS region/list returned meta={data.get('meta')}")

    items = data.get("result", {}).get("items", [])
    out: list[dict] = []
    for it in items:
        rid = str(it.get("id"))
        slug = _SLUG_BY_REGION_ID.get(rid)
        if not slug:
            logger.warning(
                "2GIS region '%s' (id=%s) has no known URL slug — skipping. "
                "Add it to _SLUG_BY_REGION_ID in cities.py.",
                it.get("name"), rid,
            )
            continue
        out.append({
            "id": rid,
            "slug": slug,
            "name": it.get("name") or _STATIC_NAMES.get(rid),
            "branch_count": (it.get("statistics") or {}).get("branch_count"),
            "bbox": _bbox_from_wkt(it.get("bounds")),
        })

    if not out:
        raise RuntimeError("2GIS region/list returned no usable Kazakhstan cities")

    # Стабильный порядок по имени.
    out.sort(key=lambda c: c["name"] or c["slug"])
    return out


async def get_cities(force_refresh: bool = False) -> list[dict]:
    """Возвращает каталог городов Казахстана: [{id, slug, name, branch_count}].

    Освежается из 2ГИС не чаще, чем раз в `cities_cache_ttl_seconds`.
    При ошибке живого запроса отдаёт статичный фолбэк (без branch_count).
    """
    global _cache, _cache_ts

    now = time.monotonic()
    if not force_refresh and _cache is not None and (now - _cache_ts) < settings.cities_cache_ttl_seconds:
        return _cache

    async with _lock:
        now = time.monotonic()
        if not force_refresh and _cache is not None and (now - _cache_ts) < settings.cities_cache_ttl_seconds:
            return _cache
        try:
            catalog = await _fetch_live_catalog()
            logger.info("Loaded %d Kazakhstan cities from 2GIS catalog", len(catalog))
        except Exception as e:
            logger.warning("Failed to fetch live 2GIS city catalog (%s); using static fallback", e)
            catalog = _cache or _static_catalog()
        _cache = catalog
        _cache_ts = time.monotonic()
        return _cache


async def list_city_slugs() -> list[str]:
    """Список URL-slug-ов всех городов Казахстана."""
    return [c["slug"] for c in await get_cities()]


async def get_city_bbox(slug: str) -> BBox | None:
    """bbox (min_lon, min_lat, max_lon, max_lat) города или None, если неизвестен.

    Нужен для geo-sweep поиска: раскладываем по bbox сетку центров карты, чтобы
    обойти лимит 2ГИС ~60 фирм на текстовый запрос. None → sweep недоступен,
    поиск откатывается к рубричному добору.
    """
    for c in await get_cities():
        if c["slug"] == slug:
            return c.get("bbox")
    return None


async def is_valid_city(slug: str) -> bool:
    """True, если slug — известный город Казахстана (или служебное значение 'all')."""
    if slug == ALL_CITIES:
        return True
    return slug in {c["slug"] for c in await get_cities()}


# Служебное значение city для «искать по всем городам».
ALL_CITIES = "all"

# Все известные slug-и городов Казахстана (для валидации/извлечения из URL).
KZ_CITY_SLUGS = set(_SLUG_BY_REGION_ID.values())

# 2ГИС во ВНУТРЕННИХ ссылках firm/geo транслитерирует «-й» как «-j», тогда как
# наш каталог-slug использует «-y». Это касается единственных двух городов из 19,
# чьё имя кончается на «й»: Костанай и Семей. Без нормализации
# `_extract_city_from_soup` не узнаёт город (slug нет в KZ_CITY_SLUGS) → city=NULL
# → филиал выпадает из /cities и показывается как «all».
_SLUG_ALIASES: dict[str, str] = {
    "kostanaj": "kostanay",
    "semej": "semey",
}


def normalize_city_slug(slug: str | None) -> str | None:
    """Приводит slug из ссылок 2ГИС к каноническому slug каталога."""
    if not slug:
        return None
    return _SLUG_ALIASES.get(slug, slug)
