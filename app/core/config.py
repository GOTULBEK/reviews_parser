from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False, extra="ignore")

    app_env: str = ""
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/twogis"
    twogis_reviews_api_key: str = "6e7e1929-4ea9-4a5d-8c05-d601860389bd"
    # Публичный web-ключ 2ГИС для Catalog API (список городов/регионов).
    twogis_web_api_key: str = "c7f1a769-c8a5-4636-b14d-d8c987808a12"
    twogis_catalog_base: str = "https://catalog.api.2gis.com"
    cities_cache_ttl_seconds: int = 86400  # каталог городов меняется крайне редко
    
    # --- New Settings for Claude ---
    anthropic_api_key: str | None = None
    claude_model: str = "claude-haiku-4-5-20251001"
    max_reviews_to_analyze: int = 1000 # Guardrail for token limits

    max_concurrent_branches: int = 5
    # Сколько городов опрашивать параллельно при city="all". Держим низким:
    # 2ГИС троттлит (ConnectTimeout), если ударить всеми 19 городами разом.
    max_concurrent_cities: int = 4
    max_branches_per_search: int = 50
    # Бэкстоп от зацикливания пагинации при max_results=0 (без лимита).
    # ~12 фирм/страница → 200 страниц ≈ 2400 фирм на город. 2ГИС обычно отдаёт меньше.
    search_max_pages_hard_cap: int = 200
    # Deep-search: 2ГИС отдаёт максимум ~60 фирм на текстовый запрос. Основной способ
    # обойти лимит — geo-sweep: раскладываем по bbox города сетку center-точек карты
    # (?m=lon,lat/zoom) и повторяем запрос из каждой, объединяя результаты — у каждого
    # вьюпорта свой топ выдачи, объединение покрывает весь набор. Рубричный добор
    # (под-запросы "{query} {рубрика}") остаётся фолбэком, если bbox города неизвестен.
    deep_search_max_rubrics: int = 16
    # Размер сетки geo-sweep: N → N×N центров карты по bbox города. Больше — полнее,
    # но дороже по запросам к 2ГИС (и выше риск капчи). 4×4=16 центров + рубричный
    # добор обычно дотягивают до реального total; early-stop по total режет лишнее.
    deep_search_grid: int = 4
    # Макс. страниц пагинации на один center-запрос sweep (уникальные для вьюпорта
    # фирмы лежат вглубь его выдачи, поэтому страницу-две мало — нужна вся выдача).
    deep_search_center_pages: int = 6
    # Зум карты для center-точек sweep (чем меньше, тем шире вьюпорт).
    deep_search_zoom: int = 12
    # Сколько center-запросов sweep слать параллельно (2ГИС троттлит большие залпы).
    deep_search_concurrency: int = 5
    request_timeout_seconds: int = 15
    rate_limit_sleep_min: float = 1.0
    rate_limit_sleep_max: float = 2.0
    default_city_slug: str = "almaty"
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    )
    cors_origins: str = "http://localhost:3000,http://localhost:3001"

    # Dataset logging (CSV, UTF-8). Relative paths are resolved from CWD.
    dataset_dir: str = "datasets"

    # Auth / JWT
    secret_key: str = "change-me-in-production-use-a-long-random-string"
    access_token_expire_hours: int = 24

settings = Settings()