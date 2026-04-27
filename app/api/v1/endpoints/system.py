from uuid import UUID
from fastapi import Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.db.database import get_session
from app.models.core import Review, Branch
from app.schemas.tasks import ReviewResponse, BranchResponse

from fastapi import APIRouter
router = APIRouter()

@router.get("/health", tags=["meta"])
async def health():
    return {"status": "ok"}
# Single-resource retrieval
# ---------------------------------------------------------------------------

@router.get("/reviews/{review_uuid}", response_model=ReviewResponse, tags=["reviews"])
async def get_review(review_uuid: UUID, session: AsyncSession = Depends(get_session)):
    review = await session.get(Review, review_uuid)
    if not review:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Review not found")
    return ReviewResponse.model_validate(review)


@router.get("/branches/{branch_uuid}", response_model=BranchResponse, tags=["branches"])
async def get_branch(branch_uuid: UUID, session: AsyncSession = Depends(get_session)):
    branch = await session.get(Branch, branch_uuid)
    if not branch:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Branch not found")
    return BranchResponse.model_validate(branch)


@router.get(
    "/branches/{branch_uuid}/reviews",
    response_model=list[ReviewResponse],
    tags=["branches"],
)
async def list_branch_reviews(
    branch_uuid: UUID,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    session: AsyncSession = Depends(get_session),
):
    stmt = (
        select(Review)
        .where(Review.branch_id == branch_uuid)
        .order_by(Review.date_created.desc().nulls_last())
        .limit(limit)
        .offset(offset)
    )
    result = await session.execute(stmt)
    reviews = result.scalars().all()
    return [ReviewResponse.model_validate(r) for r in reviews]
