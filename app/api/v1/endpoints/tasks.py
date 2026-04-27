from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from app.db.database import get_session
from app.models.tasks import SearchTask, TaskStatus, SearchTaskBranch
from app.models.core import Branch
from app.schemas.tasks import (
    TaskStatusResponse, 
    TaskResultResponse, 
    BranchWithReviewsResponse, 
    ReviewResponse, 
    BranchResponse
)

router = APIRouter()

async def _require_task(task_id: UUID, session: AsyncSession) -> SearchTask:
    task = await session.get(SearchTask, task_id)
    if not task:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Task not found")
    if task.status not in (TaskStatus.completed, TaskStatus.failed):
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"Task is still {task.status.value}",
        )
    return task

@router.get("/{task_id}", response_model=TaskStatusResponse)
async def get_task_status(task_id: UUID, session: AsyncSession = Depends(get_session)):
    task = await session.get(SearchTask, task_id)
    if not task:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Task not found")

    reviews_total_stmt = (
        select(func.coalesce(func.sum(Branch.total_reviews), 0))
        .select_from(Branch)
        .join(SearchTaskBranch, SearchTaskBranch.branch_id == Branch.id)
        .where(SearchTaskBranch.task_id == task_id)
    )
    reviews_total = (await session.execute(reviews_total_stmt)).scalar_one()

    return TaskStatusResponse(
        task_id=task.id,
        status=task.status.value,
        query=task.query,
        city=task.city,
        total_branches_found=task.total_branches_found,
        branches_completed=task.branches_completed,
        total_reviews_collected=task.total_reviews_collected,
        reviews_total=int(reviews_total or 0),
        reviews_parsed=task.total_reviews_collected,
        error_message=task.error_message,
        created_at=task.created_at,
        started_at=task.started_at,
        completed_at=task.completed_at,
    )

@router.get(
    "/{task_id}/results",
    response_model=TaskResultResponse,
    deprecated=True,
    summary="[DEPRECATED] Полный JSON",
)
async def get_task_results(
    task_id: UUID,
    include_reviews: bool = Query(True),
    session: AsyncSession = Depends(get_session),
):
    task = await _require_task(task_id, session)

    stmt = (
        select(Branch)
        .join(SearchTaskBranch, SearchTaskBranch.branch_id == Branch.id)
        .where(SearchTaskBranch.task_id == task_id)
    )
    if include_reviews:
        stmt = stmt.options(selectinload(Branch.reviews))

    branches = (await session.execute(stmt)).scalars().unique().all()

    out: list[BranchWithReviewsResponse] = []
    for b in branches:
        if include_reviews:
            payload = BranchWithReviewsResponse.model_validate(b)
            payload.reviews = [ReviewResponse.model_validate(r) for r in b.reviews]
        else:
            base = BranchResponse.model_validate(b)
            payload = BranchWithReviewsResponse(**base.model_dump(), reviews=[])
        out.append(payload)

    return TaskResultResponse(
        task_id=task.id,
        status=task.status.value,
        query=task.query,
        city=task.city,
        branches=out,
        total_reviews=task.total_reviews_collected,
    )
