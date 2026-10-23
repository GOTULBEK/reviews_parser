import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.db.database import Base, engine
from app.db.schema import ensure_added_columns
from app.api.v1.router import api_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await ensure_added_columns(conn)
    yield
    await engine.dispose()

app = FastAPI(
    title="2GIS Reviews Scraper",
    version="2.1.0",
    description=(
        "Workflow:\n"
        "1. POST /search/preview — найти кандидатов\n"
        "2. POST /search/scrape — запустить сбор по выбранным\n"
        "3. GET /tasks/{id} — опрос статуса\n"
        "4. Dashboard endpoints: /overview /branches /reviews /problems /actions\n"
    ),
    lifespan=lifespan,
)

# Origin сравнивается точной строкой, а браузер шлёт его без завершающего слэша —
# срезаем, иначе "http://host:3000/" в CORS_ORIGINS молча ничего не разрешит.
cors_origins = [o for o in (s.strip().rstrip("/") for s in settings.cors_origins.split(",")) if o]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix="/api")