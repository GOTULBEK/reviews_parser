import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.db.database import Base, engine
from app.api.v1.router import api_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
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

cors_origins = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)