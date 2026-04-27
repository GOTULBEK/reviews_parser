import enum
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    DateTime,
    Enum as SAEnum,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    ForeignKey
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.database import Base

class TaskStatus(str, enum.Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"

class SearchTask(Base):
    __tablename__ = "search_tasks"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    query: Mapped[str | None] = mapped_column(String(512))
    city: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[TaskStatus] = mapped_column(
        SAEnum(TaskStatus, name="task_status"), nullable=False, default=TaskStatus.pending
    )
    error_message: Mapped[str | None] = mapped_column(Text)

    total_branches_found: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    branches_completed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_reviews_collected: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

class SearchTaskBranch(Base):
    __tablename__ = "search_task_branches"
    __table_args__ = (UniqueConstraint("task_id", "branch_id", name="uq_task_branch"),)

    task_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("search_tasks.id", ondelete="CASCADE"), primary_key=True
    )
    branch_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("branches.id", ondelete="CASCADE"), primary_key=True
    )

class TaskTopicsCache(Base):
    __tablename__ = "task_topics_cache"
    __table_args__ = (UniqueConstraint("task_id", "days", name="uq_task_days_topics"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    task_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("search_tasks.id", ondelete="CASCADE"), nullable=False, index=True
    )
    days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    
    top_problems: Mapped[list[dict] | None] = mapped_column(JSONB)
    top_praise: Mapped[list[dict] | None] = mapped_column(JSONB)
    
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
