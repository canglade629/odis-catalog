"""Data catalogue repository (PostgreSQL)."""
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import DataCatalogue

CATALOGUE_META_ID = "_catalogue_meta"
LEGACY_CATALOGUE_ID = "silver_tables"


class CatalogueRepository:
    async def get(self, session: AsyncSession, doc_id: str) -> Optional[Dict[str, Any]]:
        result = await session.execute(select(DataCatalogue).where(DataCatalogue.id == doc_id))
        row = result.scalars().first()
        if not row:
            return None
        return row.document

    async def list_table_rows(self, session: AsyncSession) -> List[Tuple[str, Dict[str, Any]]]:
        """List all per-table catalogue rows (excluding global meta row)."""
        result = await session.execute(
            select(DataCatalogue).where(
                DataCatalogue.id.notin_([CATALOGUE_META_ID, LEGACY_CATALOGUE_ID])
            )
        )
        rows = result.scalars().all()
        return [(row.id, row.document or {}) for row in rows]

    async def get_table_row(self, session: AsyncSession, table_name: str) -> Optional[Dict[str, Any]]:
        """Get one table catalogue row by table id."""
        return await self.get(session, table_name)

    async def get_meta_row(self, session: AsyncSession) -> Optional[Dict[str, Any]]:
        """Get global catalogue meta row."""
        return await self.get(session, CATALOGUE_META_ID)

    async def set(
        self,
        session: AsyncSession,
        document: Dict[str, Any],
        doc_id: str,
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

    async def upsert_meta_row(self, session: AsyncSession, document: Dict[str, Any]) -> None:
        """Upsert global catalogue meta row."""
        await self.set(session, document, doc_id=CATALOGUE_META_ID)


catalogue_repo = CatalogueRepository()
