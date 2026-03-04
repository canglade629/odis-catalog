"""Async database session for PostgreSQL."""
import logging
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from app.core.config import get_settings
from app.db.models import Base

logger = logging.getLogger(__name__)

_engine = None
_async_session_factory = None


def init_engine():
    """Create async engine and session factory. Call once at app startup."""
    global _engine, _async_session_factory
    settings = get_settings()
    url = settings.resolved_database_url
    _engine = create_async_engine(
        url,
        echo=False,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )
    _async_session_factory = async_sessionmaker(
        _engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )
    logger.info("Database engine initialized")


def get_engine():
    """Return the async engine. Call init_engine() first."""
    if _engine is None:
        init_engine()
    return _engine


def async_session_factory():
    """Return the async session factory."""
    if _async_session_factory is None:
        init_engine()
    return _async_session_factory


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency: yield an async session."""
    factory = async_session_factory()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
