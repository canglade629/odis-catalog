"""Data catalogue repository (PostgreSQL)."""
from datetime import datetime
from typing import Dict, Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import DataCatalogue

CATALOGUE_ID = "silver_tables"


class CatalogueRepository:
    async def get(self, session: AsyncSession, doc_id: str = CATALOGUE_ID) -> Optional[Dict[str, Any]]:
        result = await session.execute(select(DataCatalogue).where(DataCatalogue.id == doc_id))
        row = result.scalars().first()
        if not row:
            return None
        return row.document

    async def set(
        self,
        session: AsyncSession,
        document: Dict[str, Any],
        doc_id: str = CATALOGUE_ID,
    ) -> None:
        result = await session.execute(select(DataCatalogue).where(DataCatalogue.id == doc_id))
        row = result.scalars().first()
        now = datetime.utcnow()
        if row:
            row.document = document
            row.updated_at = now
        else:
            session.add(
                DataCatalogue(
                    id=doc_id,
                    document=document,
                    updated_at=now,
                )
            )
        await session.flush()


catalogue_repo = CatalogueRepository()
