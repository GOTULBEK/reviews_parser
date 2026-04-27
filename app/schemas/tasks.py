from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, ConfigDict
from .common import BranchIdStr

class SearchTaskResponse(BaseModel):
    task_id: UUID
    status: str
    query: str | None = None
    city: str

class TaskStatusResponse(BaseModel):
    task_id: UUID
    status: str
    query: str | None = None
    city: str
    total_branches_found: int
    branches_completed: int
    total_reviews_collected: int
    reviews_total: int
    reviews_parsed: int
    error_message: str | None = None
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None

class ReviewResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    gis_review_id: str
    branch_id: UUID
    user_name: str | None = None
    rating: int | None = None
    text: str | None = None
    official_answer_text: str | None = None
    official_answer_date: datetime | None = None
    hiding_reason: str | None = None
    is_rated: bool
    date_created: datetime | None = None
    date_edited: datetime | None = None
    review_url: str

class BranchResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    gis_branch_id: BranchIdStr
    name: str | None = None
    address: str | None = None
    rating: float | None = None
    total_reviews: int | None = None
    url: str
    rating_distribution: dict | None = None

class BranchWithReviewsResponse(BranchResponse):
    reviews: list[ReviewResponse] = []

class TaskResultResponse(BaseModel):
    task_id: UUID
    status: str
    query: str | None = None
    city: str
    branches: list[BranchWithReviewsResponse]
    total_reviews: int
