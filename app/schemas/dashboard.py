from datetime import datetime
from typing import Literal
from uuid import UUID
from pydantic import BaseModel
from .common import BranchIdStr, SourceType

Sentiment = Literal["pos", "neg", "neu", "unknown"]

class KPIs(BaseModel):
    avg_rating: float | None = None
    branches_total: int
    reviews_total: int
    negative_pct: float
    replies_pct: float

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
    source: SourceType
    name: str
    city: str
    address: str
    district: str | None = None
    lat: float | None = None
    lng: float | None = None
    url: str | None = None

class TopMention(BaseModel):
    label: str
    mentions: int
    examples: list[str] = []

class ReviewDynamicsPoint(BaseModel):
    month: str | None = None
    date: str | None = None
    all: int
    pos: int
    neg: int
    neu: int | None = None

class ReviewDynamics(BaseModel):
    range: str
    range_days: int | None = None
    timezone: str
    granularity: Literal["day", "month"] | None = None
    points: list[ReviewDynamicsPoint]

class BranchKPIs(BaseModel):
    avg_rating: float | None = None
    reviews_total: int
    negative_pct: float
    replies_pct: float

class OverviewBranchAnalytics(BaseModel):
    kpis: BranchKPIs
    rating_distribution: RatingDistribution
    review_dynamics: ReviewDynamics
    top_praise: list[TopMention] = []
    top_problems: list[TopMention] = []

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
    review_dynamics: ReviewDynamics | None = None
    by_branch: dict[str, OverviewBranchAnalytics] | None = None

class BranchListItem(BaseModel):
    id: UUID
    gis_branch_id: BranchIdStr
    source: SourceType
    name: str | None = None
    address: str | None = None
    rating: float | None = None
    total_reviews: int | None = None
    url: str
    rating_distribution: dict | None = None
    replies_pct: float | None = None
    top_tags: list[str] = []

class BranchesListResponse(BaseModel):
    task_id: UUID
    status: str
    count: int
    branches: list[BranchListItem]

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
    sentiment: Sentiment

class ReviewsListResponse(BaseModel):
    task_id: UUID
    status: str
    count: int
    limit: int
    offset: int
    reviews: list[ReviewListItem]

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

class CompareCompetitorItem(BaseModel):
    rank: int
    branch_id: UUID | None = None
    is_target: bool
    name: str
    address: str | None = None
    rating: float | None = None
    reviews_total: int
    negative_pct: float
    replies_pct: float
    dynamics: float | None = None
    categories: list[str] = []

class CompareKPIs(BaseModel):
    rank_in_district: int
    total_competitors: int
    best_rating: float | None = None
    best_rating_diff_from_avg: float | None = None
    negative_pct: float
    negative_pct_avg: float
    replies_pct: float
    replies_rank: int

class CompareStrengthItem(BaseModel):
    label: str
    value: str
    subtext: str | None = None
    meter_pct: int

class CompareResponse(BaseModel):
    task_id: UUID
    status: str
    kpis: CompareKPIs
    competitors: list[CompareCompetitorItem]
    strengths: list[CompareStrengthItem]


RecommendationIcon = Literal["shield", "headphones", "volume", "warning", "trend", "star"]
RecommendationTone = Literal["red", "orange", "green"]


class RecommendationItem(BaseModel):
    icon: RecommendationIcon
    tone: RecommendationTone
    title: str
    body: str


class RecommendationsResponse(BaseModel):
    task_id: UUID
    status: str
    items: list[RecommendationItem] = []
    analytics_note: str | None = None


TopicTone = Literal["pos", "neg", "neu"]


class TopicBarItem(BaseModel):
    label: str
    positive: int
    negative: int


class TopicListItem(BaseModel):
    label: str
    sentiment: TopicTone
    mentions: int


class TopicTrend(BaseModel):
    label: str
    description: str


class TopicTimeSeriesPoint(BaseModel):
    month: str
    positive: int
    negative: int


class TopicTimeSeries(BaseModel):
    label: str
    monthly: list[TopicTimeSeriesPoint] = []


class MonthlyAvgRatingPoint(BaseModel):
    month: str
    avg_rating: float | None = None


class TopicsModuleResponse(BaseModel):
    task_id: UUID
    status: str
    period_days: int | None = None
    reviews_total: int
    topics_count: int
    topic_bars: list[TopicBarItem] = []
    top_positive: list[TopicListItem] = []
    top_negative: list[TopicListItem] = []
    frequent_phrases: list[str] = []
    fastest_growing_negative: TopicTrend | None = None
    strongest_positive: TopicTrend | None = None
    monthly_avg_rating: list[MonthlyAvgRatingPoint] = []
    topic_timeseries: list[TopicTimeSeries] = []
    analytics_note: str | None = None


ReplyPriority = Literal["urgent", "high", "medium", "low"]


class RepliesKpis(BaseModel):
    answered_count: int
    answered_pct: int
    avg_response_hours: float | None = None
    negatives_replied_pct: int
    overdue_sla_count: int


class ReplyQueueItem(BaseModel):
    id: UUID
    branch_id: UUID
    branch_name: str | None = None
    user_name: str | None = None
    rating: int | None = None
    text: str | None = None
    date_created: datetime | None = None
    review_url: str
    sentiment: Sentiment
    priority: ReplyPriority
    overdue_sla: bool
    age_hours: float | None = None


class ReplyTemplate(BaseModel):
    title: str
    text: str


class RepliesModuleResponse(BaseModel):
    task_id: UUID
    status: str
    sla_hours: int
    unanswered_count: int
    urgent_count: int
    kpis: RepliesKpis
    queue: list[ReplyQueueItem] = []
    templates: list[ReplyTemplate] = []
    analytics_note: str | None = None

