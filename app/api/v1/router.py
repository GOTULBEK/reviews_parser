from fastapi import APIRouter, Depends

from app.api.dependencies import require_admin, require_task_access
from .endpoints.auth import router as auth_router
from .endpoints.search import router as search_router
from .endpoints.cities import router as cities_router
from .endpoints.tasks import router as tasks_router
from .endpoints.dashboard import router as dashboard_router
from .endpoints.system import router as system_router

api_router = APIRouter()

# Public: login + admin-only register
api_router.include_router(auth_router, tags=["auth"])

# Admin-only: search / scrape
api_router.include_router(
    search_router,
    prefix="/search",
    tags=["search"],
    dependencies=[Depends(require_admin)],
)

# Admin-only: city catalog for the search dropdown
api_router.include_router(
    cities_router,
    tags=["cities"],
    dependencies=[Depends(require_admin)],
)

# Task access: admin always, customer only for their own task_id
api_router.include_router(
    tasks_router,
    prefix="/tasks",
    tags=["tasks"],
    dependencies=[Depends(require_task_access)],
)
api_router.include_router(
    dashboard_router,
    prefix="/tasks",
    tags=["dashboard"],
    dependencies=[Depends(require_task_access)],
)

# Admin-only: single resource lookups; health is public (handled inside system_router)
api_router.include_router(system_router, tags=["meta", "reviews", "branches"])
