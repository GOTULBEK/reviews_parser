from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False, extra="ignore")

    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/twogis"
    twogis_reviews_api_key: str = "6e7e1929-4ea9-4a5d-8c05-d601860389bd"
    
    # --- New Settings for Claude ---
    anthropic_api_key: str | None = None
    claude_model: str = "claude-3-haiku-20240307" # Haiku is fast/cheap for parsing, Sonnet 3.5 is better for deep insights
    max_reviews_to_analyze: int = 1000 # Guardrail for token limits

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
    cors_origins: str = "http://localhost:3000,http://localhost:3001"

settings = Settings()