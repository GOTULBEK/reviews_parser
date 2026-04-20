from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False, extra="ignore")

    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/twogis"

    twogis_reviews_api_key: str = "6e7e1929-4ea9-4a5d-8c05-d601860389bd"

    max_concurrent_branches: int = 5
    max_branches_per_search: int = 50
    request_timeout_seconds: int = 15
    rate_limit_sleep_min: float = 1.0
    rate_limit_sleep_max: float = 2.0

    default_city_slug: str = "almaty"

    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    # Keep as string to avoid pydantic-settings JSON-decoding for complex types.
    # Format: comma-separated origins, e.g. "http://localhost:3000,http://localhost:3001"
    cors_origins: str = "http://localhost:3000,http://localhost:3001"

settings = Settings()
