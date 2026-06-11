"""SQLAlchemy models for PostgreSQL (API keys, catalogue, certifications)."""
from datetime import datetime
from sqlalchemy import String, Boolean, Integer, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from typing import Optional


class Base(DeclarativeBase):
    """Base for all models."""
    pass


class ApiKey(Base):
    __tablename__ = "api_keys"

    key_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(512), nullable=False)
    is_admin: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    last_used_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class DataCatalogue(Base):
    __tablename__ = "data_catalogue"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    document: Mapped[dict] = mapped_column(JSONB, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)


class TableCertification(Base):
    __tablename__ = "table_certifications"

    id: Mapped[str] = mapped_column(String(256), primary_key=True)
    layer: Mapped[str] = mapped_column(String(64), nullable=False)
    table_name: Mapped[str] = mapped_column(String(256), nullable=False)
    certified: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    certified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    certified_by: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)


class QueryTracker(Base):
    __tablename__ = "query_tracker"

    table_name: Mapped[str] = mapped_column(String(256), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    query_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_query_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
