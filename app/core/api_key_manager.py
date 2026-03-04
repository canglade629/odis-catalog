"""API Key management (PostgreSQL-backed)."""
import secrets
from datetime import datetime
from typing import Optional, Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.repositories.api_keys import api_key_repo

def generate_api_key() -> str:
    """Generate a secure API key with sk_live_ prefix."""
    random_part = secrets.token_urlsafe(32)
    return f"sk_live_{random_part}"


def hash_api_key(api_key: str) -> str:
    """Hash an API key using SHA-256."""
    import hashlib
    return hashlib.sha256(api_key.encode()).hexdigest()


async def create_api_key(user_id: str, session: AsyncSession) -> Dict[str, Any]:
    """
    Generate a new API key and store it in PostgreSQL.
    Replaces any existing key for this user.
    """
    deleted = await api_key_repo.delete_by_user_id(session, user_id)
    api_key = generate_api_key()
    hashed_key = hash_api_key(api_key)
    await api_key_repo.create(session, hashed_key, user_id)
    return {
        "api_key": api_key,
        "user_id": user_id,
        "created_at": datetime.utcnow().isoformat(),
        "replaced": deleted > 0,
    }


async def validate_api_key(api_key: str, session: AsyncSession) -> Optional[Dict[str, Any]]:
    """Validate an API key against PostgreSQL. Updates last_used_at on success."""
    hashed_key = hash_api_key(api_key)
    data = await api_key_repo.get_by_hash(session, hashed_key)
    if not data or not data.get("active", False):
        return None
    try:
        await api_key_repo.update_last_used(session, hashed_key)
    except Exception:
        pass
    return {
        "user_id": data["user_id"],
        "created_at": data["created_at"],
        "last_used_at": data["last_used_at"],
    }


async def revoke_api_key(api_key: str, session: AsyncSession) -> bool:
    """Revoke an API key by setting active=False."""
    hashed_key = hash_api_key(api_key)
    return await api_key_repo.set_active(session, hashed_key, False)


async def delete_api_key(api_key: str, session: AsyncSession) -> bool:
    """Permanently delete an API key."""
    hashed_key = hash_api_key(api_key)
    return await api_key_repo.delete(session, hashed_key)


async def list_api_keys(session: AsyncSession) -> list[Dict[str, Any]]:
    """List all API keys (metadata only, no plaintext)."""
    return await api_key_repo.list_all(session)
