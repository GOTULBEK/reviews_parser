from fastapi import APIRouter

from .endpoints.search import router as search_router
from .endpoints.tasks import router as tasks_router
from .endpoints.dashboard import router as dashboard_router
from .endpoints.system import router as system_router

api_router = APIRouter()

api_router.include_router(search_router, prefix="/search", tags=["search"])
api_router.include_router(tasks_router, prefix="/tasks", tags=["tasks"])
api_router.include_router(dashboard_router, prefix="/tasks", tags=["dashboard"])
api_router.include_router(system_router, tags=["meta", "reviews", "branches"])
