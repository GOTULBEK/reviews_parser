from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, field_validator


# ---------------------------------------------------------------------------
# gis_branch_id transmitted as string (IEEE-754 / JS Number safety)
# ---------------------------------------------------------------------------

def _coerce_branch_id_to_str(v) -> str:
    if isinstance(v, int):
        return str(v)
    if isinstance(v, str):
        if not v.isdigit():
            raise ValueError("gis_branch_id must be numeric digits")
        return v
    raise TypeError(f"gis_branch_id must be int or str, got {type(v).__name__}")


BranchIdStr = Annotated[str, BeforeValidator(_coerce_branch_id_to_str)]


# ---------------------------------------------------------------------------
# Preview / Scrape requests
# ---------------------------------------------------------------------------

class PreviewRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=512)
    city: str = Field(default="almaty", min_length=1, max_length=64)
    max_results: int = Field(default=20, ge=1, le=100)


class BranchPreviewItem(BaseModel):
    gis_branch_id: BranchIdStr
    firm_url: str
    name: str | None = None
    address: str | None = None


class PreviewResponse(BaseModel):
    query: str
    city: str
    count: int
    branches: list[BranchPreviewItem]


class ScrapeRequest(BaseModel):
    city: str = Field(..., min_length=1, max_length=64)
    gis_branch_ids: list[BranchIdStr] = Field(..., min_length=1, max_length=100)
    query: str | None = Field(default=None, max_length=512)

    @field_validator("gis_branch_ids")
    @classmethod
    def _dedupe(cls, v: list[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for x in v:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out


# ---------------------------------------------------------------------------
# Task lifecycle
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Legacy /results retrieval (kept for backward compat)
# ---------------------------------------------------------------------------

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


# ===========================================================================
# Dashboard tab endpoints (v1)
# ===========================================================================

Sentiment = Literal["pos", "neg", "neu", "unknown"]


# ---------------------------------------------------------------------------
# /tasks/{id}/overview
# ---------------------------------------------------------------------------

class KPIs(BaseModel):
    avg_rating: float | None = None   # None если нет отзывов с оценкой
    branches_total: int
    reviews_total: int
    negative_pct: float                # 0..100
    replies_pct: float                 # 0..100


class SentimentBreakdown(BaseModel):
    positive_pct: float
    negative_pct: float
    neutral_pct: float


class BranchRatingSummary(BaseModel):
    branch_id: UUID
    name: str | None = None
    rating: float | None = None
    total_reviews: int


class RatingBucket(BaseModel):
    count: int
    pct: int


class RatingDistribution(BaseModel):
    total_rated: int
    stars: dict[str, RatingBucket]
    one_two: RatingBucket | None = None


class OverviewBranchItem(BaseModel):
    branch_id: UUID
    gis_branch_id: BranchIdStr
    name: str
    city: str
    address: str
    district: str | None = None
    lat: float | None = None
    lng: float | None = None
    url: str | None = None


class TopMention(BaseModel):
    """
    Тема, выявленная в отзывах. `label` — канонический термин (лемма или
    биграмма) из реальных отзывов. `examples` — до 3 коротких цитат, где
    термин встречается. Формы одного слова ('очередь' / 'очереди') сливаются
    через лемматизацию, опечатки — через edit-distance.
    """
    label: str
    mentions: int
    examples: list[str] = []


class OverviewResponse(BaseModel):
    task_id: UUID
    status: str
    query: str | None = None
    city: str
    kpis: KPIs
    sentiment: SentimentBreakdown
    rating_distribution: RatingDistribution
    branches: list[OverviewBranchItem]
    branch_ratings: list[BranchRatingSummary]
    top_problems: list[TopMention] = []
    top_praise: list[TopMention] = []
    analytics_note: str | None = None


# ---------------------------------------------------------------------------
# /tasks/{id}/branches
# ---------------------------------------------------------------------------

class BranchListItem(BaseModel):
    id: UUID
    gis_branch_id: BranchIdStr
    name: str | None = None
    address: str | None = None
    rating: float | None = None
    total_reviews: int | None = None
    url: str
    rating_distribution: dict | None = None
    replies_pct: float | None = None         # computed from reviews
    top_tags: list[str] = []                 # NLP not implemented


class BranchesListResponse(BaseModel):
    task_id: UUID
    status: str
    count: int
    branches: list[BranchListItem]


# ---------------------------------------------------------------------------
# /tasks/{id}/reviews
# ---------------------------------------------------------------------------

class ReviewListItem(BaseModel):
    id: UUID
    gis_review_id: str
    branch_id: UUID
    branch_name: str | None = None
    rating: int | None = None
    text: str | None = None
    official_answer_text: str | None = None
    official_answer_date: datetime | None = None
    date_created: datetime | None = None
    review_url: str
    sentiment: Sentiment                     # derived from rating


class ReviewsListResponse(BaseModel):
    task_id: UUID
    status: str
    count: int                               # total matching filters
    limit: int
    offset: int
    reviews: list[ReviewListItem]


# ---------------------------------------------------------------------------
# /tasks/{id}/problems  — stub (NLP not implemented)
# ---------------------------------------------------------------------------

class ProblemItem(BaseModel):
    key: str
    title: str
    mentions: int
    quotes: list[str] = []
    recommendation: str | None = None
    kpi_hint: str | None = None


class ProblemsResponse(BaseModel):
    task_id: UUID
    status: str
    items: list[ProblemItem] = []
    analytics_note: str | None = None


# ---------------------------------------------------------------------------
# /tasks/{id}/actions  — stub (NLP not implemented)
# ---------------------------------------------------------------------------

class PriorityItem(BaseModel):
    level: int
    title: str
    items: list[str] = []


class InsightItem(BaseModel):
    label: str
    value: str
    subtext: str | None = None


class ActionsResponse(BaseModel):
    task_id: UUID
    status: str
    priorities: list[PriorityItem] = []
    insights: list[InsightItem] = []
    analytics_note: str | None = None