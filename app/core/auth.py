"""API Key authentication middleware (PostgreSQL-backed)."""
import secrets
from fastapi import HTTPException, Security, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.api_key_manager import validate_api_key
from app.db.session import get_db

security = HTTPBearer(auto_error=False)


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


# User ID that is treated as admin when using an API key (for admin view in UI).
ADMIN_USER_ID = "admin"


async def verify_admin_secret_or_admin_key(
    credentials: HTTPAuthorizationCredentials = Security(security),
    session: AsyncSession = Depends(get_db),
) -> bool:
    """
    Verify admin access: either ADMIN_SECRET or a valid API key with user_id 'admin'.
    Use this on admin routes so that the same API key created for user_id 'admin' grants admin view.
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
    if user_data and user_data.get("user_id") == ADMIN_USER_ID:
        return True
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Admin access required. Use the admin secret or an API key created for user_id 'admin'.",
    )


async def verify_api_key_or_admin(
    credentials: HTTPAuthorizationCredentials = Security(security),
    session: AsyncSession = Depends(get_db),
) -> str:
    """
    Verify either API key OR admin secret.
    Returns user_id for API keys, or "admin" for admin secret.
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
        return "admin"
    user_data = await validate_api_key(token, session)
    if not user_data:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API key or admin secret",
        )
    return user_data["user_id"]
