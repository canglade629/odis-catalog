"""API keys repository (PostgreSQL)."""
from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import ApiKey


class ApiKeyRepository:
    async def create(
        self,
        session: AsyncSession,
        key_hash: str,
        user_id: str,
        is_admin: bool = False,
    ) -> None:
        row = ApiKey(
            key_hash=key_hash,
            user_id=user_id,
            is_admin=is_admin,
            created_at=datetime.utcnow(),
            last_used_at=None,
            active=True,
        )
        session.add(row)
        await session.flush()

    async def delete_by_user_id(self, session: AsyncSession, user_id: str) -> int:
        result = await session.execute(delete(ApiKey).where(ApiKey.user_id == user_id))
        return result.rowcount or 0

    async def get_by_hash(self, session: AsyncSession, key_hash: str) -> Optional[Dict[str, Any]]:
        result = await session.execute(select(ApiKey).where(ApiKey.key_hash == key_hash))
        row = result.scalars().first()
        if not row:
            return None
        return {
            "user_id": row.user_id,
            "is_admin": row.is_admin,
            "created_at": row.created_at,
            "last_used_at": row.last_used_at,
            "active": row.active,
        }

    async def update_last_used(self, session: AsyncSession, key_hash: str) -> None:
        result = await session.execute(select(ApiKey).where(ApiKey.key_hash == key_hash))
        row = result.scalars().first()
        if row:
            row.last_used_at = datetime.utcnow()
            await session.flush()

    async def set_active(self, session: AsyncSession, key_hash: str, active: bool) -> bool:
        result = await session.execute(select(ApiKey).where(ApiKey.key_hash == key_hash))
        row = result.scalars().first()
        if not row:
            return False
        row.active = active
        await session.flush()
        return True

    async def delete(self, session: AsyncSession, key_hash: str) -> bool:
        result = await session.execute(delete(ApiKey).where(ApiKey.key_hash == key_hash))
        return (result.rowcount or 0) > 0

    async def list_all(self, session: AsyncSession) -> List[Dict[str, Any]]:
        result = await session.execute(select(ApiKey))
        rows = result.scalars().all()
        return [
            {
                "hash": r.key_hash,
                "user_id": r.user_id,
                "is_admin": r.is_admin,
                "created_at": r.created_at,
                "last_used_at": r.last_used_at,
                "active": r.active,
            }
            for r in rows
        ]


api_key_repo = ApiKeyRepository()
