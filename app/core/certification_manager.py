"""Table Certification Manager (PostgreSQL-backed)."""
from datetime import datetime
from typing import Optional, Dict, List

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.repositories.certifications import certification_repo


async def certify_table(
    layer: str,
    table_name: str,
    admin_id: str,
    session: AsyncSession,
) -> Dict:
    """Certify a table for public use."""
    await certification_repo.set(session, layer, table_name, admin_id)
    return {
        "layer": layer,
        "table_name": table_name,
        "certified": True,
        "certified_at": datetime.utcnow().isoformat(),
        "certified_by": admin_id,
    }


async def uncertify_table(
    layer: str,
    table_name: str,
    session: AsyncSession,
) -> bool:
    """Remove certification from a table."""
    return await certification_repo.delete(session, layer, table_name)


async def is_table_certified(
    layer: str,
    table_name: str,
    session: AsyncSession,
) -> bool:
    """Check if a table is certified."""
    return await certification_repo.is_certified(session, layer, table_name)


async def get_certification_status(
    layer: str,
    table_name: str,
    session: AsyncSession,
) -> Optional[Dict]:
    """Get detailed certification status for a table."""
    return await certification_repo.get(session, layer, table_name)


async def get_all_certifications(session: AsyncSession) -> List[Dict]:
    """Get all table certifications."""
    return await certification_repo.list_all(session)
