from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.database import Base

class Company(Base):
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
    __tablename__ = "reviews"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    gis_review_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    branch_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("branches.id", ondelete="CASCADE"), nullable=False, index=True
    )

    user_name: Mapped[str | None] = mapped_column(String(255))
    rating: Mapped[int | None] = mapped_column(Integer)  
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
