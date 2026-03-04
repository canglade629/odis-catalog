"""SQLAlchemy models for PostgreSQL (API keys, catalogue, jobs, certifications)."""
from datetime import datetime
from sqlalchemy import String, Boolean, Integer, Float, Text, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from typing import Any, Optional


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


class Job(Base):
    __tablename__ = "jobs"

    job_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    job_name: Mapped[str] = mapped_column(String(512), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    total_tasks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    completed_tasks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_tasks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    progress_percent: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    user_id: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)


class JobTask(Base):
    __tablename__ = "job_tasks"

    job_id: Mapped[str] = mapped_column(String(64), ForeignKey("jobs.job_id", ondelete="CASCADE"), primary_key=True)
    task_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    pipeline_name: Mapped[str] = mapped_column(String(256), nullable=False)
    layer: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_seconds: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stats: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)


class JobLog(Base):
    __tablename__ = "job_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(64), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    level: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    logger_name: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    task_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)


class QueryTracker(Base):
    __tablename__ = "query_tracker"

    table_name: Mapped[str] = mapped_column(String(256), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    query_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_query_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
