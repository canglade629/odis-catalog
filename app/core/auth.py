"""API Key authentication middleware (PostgreSQL-backed)."""
import secrets
from typing import NamedTuple

from fastapi import HTTPException, Security, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.api_key_manager import validate_api_key
from app.db.session import get_db

security = HTTPBearer(auto_error=False)


class AuthenticatedUser(NamedTuple):
    """Current user from API key or admin secret."""
    user_id: str
    is_admin: bool


async def verify_api_key(
    credentials: HTTPAuthorizationCredentials = Security(security),
    session: AsyncSession = Depends(get_db),
) -> str:
    """
    Verify API key from Authorization Bearer token.

    Returns:
        The user_id associated with the validated API key.
    """
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key is missing. Please provide Authorization: Bearer header.",
        )
    user_data = await validate_api_key(credentials.credentials, session)
    if not user_data:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or inactive API key",
        )
    return user_data["user_id"]


async def verify_admin_secret(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> bool:
    """Verify admin secret for admin endpoints."""
    from app.core.config import get_settings
    settings = get_settings()
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Admin secret is missing. Please provide Authorization: Bearer header.",
        )
    if not secrets.compare_digest(credentials.credentials, settings.admin_secret):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid admin secret",
        )
    return True


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Security(security),
    session: AsyncSession = Depends(get_db),
) -> AuthenticatedUser:
    """
    Verify API key or admin secret and return current user (user_id + is_admin).
    Use this for routes that need to know both identity and admin status.
    """
    from app.core.config import get_settings
    settings = get_settings()
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required. Please provide Authorization: Bearer header.",
        )
    token = credentials.credentials
    if secrets.compare_digest(token, settings.admin_secret):
        return AuthenticatedUser(user_id="admin", is_admin=True)
    user_data = await validate_api_key(token, session)
    if not user_data:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API key or admin secret",
        )
    return AuthenticatedUser(
        user_id=user_data["user_id"],
        is_admin=user_data.get("is_admin", False),
    )


async def verify_admin_secret_or_admin_key(
    credentials: HTTPAuthorizationCredentials = Security(security),
    session: AsyncSession = Depends(get_db),
) -> bool:
    """
    Verify admin access: either ADMIN_SECRET or a valid API key with is_admin=True.
    """
    from app.core.config import get_settings
    settings = get_settings()
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Admin secret or API key is missing. Please provide Authorization: Bearer header.",
        )
    token = credentials.credentials
    if secrets.compare_digest(token, settings.admin_secret):
        return True
    user_data = await validate_api_key(token, session)
    if user_data and user_data.get("is_admin") is True:
        return True
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Admin access required. Use the admin secret or an API key created with is_admin=True.",
    )


async def verify_api_key_or_admin(
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> str:
    """
    Verify authentication and return user_id (for backward compatibility).
    Prefer get_current_user when you need is_admin.
    """
    return current_user.user_id
