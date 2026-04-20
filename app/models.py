import enum
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Enum as SAEnum,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .database import Base


class TaskStatus(str, enum.Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"


class Company(Base):
    """
    Организация (бренд). Одна компания = много филиалов.
    Уникальность по имени — слабое допущение, но у 2ГИС org_name обычно уникален
    внутри нормализованной выдачи.
    """
    __tablename__ = "companies"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(512), unique=True, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    branches: Mapped[list["Branch"]] = relationship(
        back_populates="company", cascade="all, delete-orphan"
    )


class Branch(Base):
    """
    Конкретная точка/филиал. gis_branch_id — это ID из /firm/<id> в URL 2ГИС.
    """
    __tablename__ = "branches"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    gis_branch_id: Mapped[int] = mapped_column(
        BigInteger, unique=True, nullable=False, index=True
    )
    company_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str | None] = mapped_column(String(512))
    address: Mapped[str | None] = mapped_column(String(1024))
    rating: Mapped[float | None] = mapped_column(Float)
    total_reviews: Mapped[int | None] = mapped_column(Integer)
    url: Mapped[str] = mapped_column(String(1024), nullable=False)
    rating_distribution: Mapped[dict | None] = mapped_column(JSONB)
    scraped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    company: Mapped[Company] = relationship(back_populates="branches")
    reviews: Mapped[list["Review"]] = relationship(
        back_populates="branch", cascade="all, delete-orphan"
    )


class Review(Base):
    """
    Отзыв. gis_review_id — строковый ID из 2ГИС (UUID-подобный).
    raw хранит полный исходный JSON отзыва для дебага и форензики.
    """
    __tablename__ = "reviews"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    gis_review_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    branch_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("branches.id", ondelete="CASCADE"), nullable=False, index=True
    )

    user_name: Mapped[str | None] = mapped_column(String(255))
    rating: Mapped[int | None] = mapped_column(Integer)  # None = "Без оценки"
    text: Mapped[str | None] = mapped_column(Text)

    official_answer_text: Mapped[str | None] = mapped_column(Text)
    official_answer_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    hiding_reason: Mapped[str | None] = mapped_column(Text)
    is_rated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    date_created: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    date_edited: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    review_url: Mapped[str] = mapped_column(String(1024), nullable=False)
    raw: Mapped[dict | None] = mapped_column(JSONB)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    branch: Mapped[Branch] = relationship(back_populates="reviews")


class SearchTask(Base):
    """
    Задача на поиск и сбор. Хранит статус, прогресс, параметры запроса.
    Живет и после завершения — служит журналом.
    """
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
    """
    Связь многих-ко-многим: задача собрала эти филиалы.
    Повторный запуск задачи с тем же запросом создаст новую задачу, но филиалы
    будут теми же (если они не изменились в 2ГИС).
    """
    __tablename__ = "search_task_branches"
    __table_args__ = (UniqueConstraint("task_id", "branch_id", name="uq_task_branch"),)

    task_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("search_tasks.id", ondelete="CASCADE"), primary_key=True
    )
    branch_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("branches.id", ondelete="CASCADE"), primary_key=True
    )
