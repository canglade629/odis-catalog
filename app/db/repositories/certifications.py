"""Table certifications repository (PostgreSQL)."""
from datetime import datetime
from typing import Optional, Dict, List

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import TableCertification


def _doc_id(layer: str, table_name: str) -> str:
    return f"{layer}_{table_name}"


class CertificationRepository:
    async def set(
        self,
        session: AsyncSession,
        layer: str,
        table_name: str,
        certified_by: str,
    ) -> None:
        doc_id = _doc_id(layer, table_name)
        result = await session.execute(select(TableCertification).where(TableCertification.id == doc_id))
        row = result.scalars().first()
        now = datetime.utcnow()
        if row:
            row.certified = True
            row.certified_at = now
            row.certified_by = certified_by
        else:
            session.add(
                TableCertification(
                    id=doc_id,
                    layer=layer,
                    table_name=table_name,
                    certified=True,
                    certified_at=now,
                    certified_by=certified_by,
                )
            )
        await session.flush()

    async def delete(self, session: AsyncSession, layer: str, table_name: str) -> bool:
        doc_id = _doc_id(layer, table_name)
        result = await session.execute(delete(TableCertification).where(TableCertification.id == doc_id))
        return (result.rowcount or 0) > 0

    async def get(
        self,
        session: AsyncSession,
        layer: str,
        table_name: str,
    ) -> Optional[Dict]:
        doc_id = _doc_id(layer, table_name)
        result = await session.execute(select(TableCertification).where(TableCertification.id == doc_id))
        row = result.scalars().first()
        if not row or not row.certified:
            return None
        return {
            "layer": row.layer,
            "table_name": row.table_name,
            "certified": row.certified,
            "certified_at": row.certified_at.isoformat() if row.certified_at else None,
            "certified_by": row.certified_by,
        }

    async def is_certified(self, session: AsyncSession, layer: str, table_name: str) -> bool:
        doc_id = _doc_id(layer, table_name)
        result = await session.execute(
            select(TableCertification).where(
                TableCertification.id == doc_id,
                TableCertification.certified == True,
            )
        )
        return result.scalars().first() is not None

    async def list_all(self, session: AsyncSession) -> List[Dict]:
        result = await session.execute(select(TableCertification).where(TableCertification.certified == True))
        rows = result.scalars().all()
        return [
            {
                "layer": r.layer,
                "table_name": r.table_name,
                "certified": r.certified,
                "certified_at": r.certified_at.isoformat() if r.certified_at else None,
                "certified_by": r.certified_by,
            }
            for r in rows
        ]


certification_repo = CertificationRepository()
