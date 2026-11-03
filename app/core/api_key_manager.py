"""API Key management (Firestore-backed)."""
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any

from google.cloud import firestore

from app.db.repositories.api_keys import api_key_repo

# last_used_at is informational; refreshing it at most this often saves one Firestore write per request.
LAST_USED_REFRESH_INTERVAL = timedelta(minutes=5)


def generate_api_key() -> str:
    """Generate a secure API key with sk_live_ prefix."""
    random_part = secrets.token_urlsafe(32)
    return f"sk_live_{random_part}"


def hash_api_key(api_key: str) -> str:
    """Hash an API key using SHA-256."""
    import hashlib
    return hashlib.sha256(api_key.encode()).hexdigest()


def _needs_last_used_refresh(last_used_at: Any) -> bool:
    if not isinstance(last_used_at, datetime):
        return True
    if last_used_at.tzinfo is None:
        last_used_at = last_used_at.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - last_used_at >= LAST_USED_REFRESH_INTERVAL


async def create_api_key(
    user_id: str, db: firestore.AsyncClient, *, is_admin: bool = False
) -> Dict[str, Any]:
    """
    Generate a new API key and store it in Firestore.
    Replaces any existing key for this user.
    By default creates a read-only user; set is_admin=True for admin access.
    """
    api_key = generate_api_key()
    hashed_key = hash_api_key(api_key)
    replaced = await api_key_repo.replace_for_user(db, hashed_key, user_id, is_admin=is_admin)
    return {
        "api_key": api_key,
        "user_id": user_id,
        "is_admin": is_admin,
        "created_at": datetime.utcnow().isoformat(),
        "replaced": replaced > 0,
    }


async def validate_api_key(api_key: str, db: firestore.AsyncClient) -> Optional[Dict[str, Any]]:
    """Validate an API key against Firestore. Refreshes last_used_at on success."""
    hashed_key = hash_api_key(api_key)
    data = await api_key_repo.get_by_hash(db, hashed_key)
    if not data or not data.get("active", False):
        return None
    if _needs_last_used_refresh(data.get("last_used_at")):
        try:
            await api_key_repo.update_last_used(db, hashed_key)
        except Exception:
            pass
    return {
        "user_id": data["user_id"],
        "is_admin": data.get("is_admin", False),
        "created_at": data["created_at"],
        "last_used_at": data["last_used_at"],
    }


async def revoke_api_key(api_key: str, db: firestore.AsyncClient) -> bool:
    """Revoke an API key by setting active=False."""
    hashed_key = hash_api_key(api_key)
    return await api_key_repo.set_active(db, hashed_key, False)


async def delete_api_key(api_key: str, db: firestore.AsyncClient) -> bool:
    """Permanently delete an API key."""
    hashed_key = hash_api_key(api_key)
    return await api_key_repo.delete(db, hashed_key)


async def list_api_keys(db: firestore.AsyncClient) -> list[Dict[str, Any]]:
    """List all API keys (metadata only, no plaintext)."""
    return await api_key_repo.list_all(db)
