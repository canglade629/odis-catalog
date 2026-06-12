"""Main FastAPI application."""
import logging
from pathlib import Path

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from datetime import datetime
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.core.auth import get_current_user, AuthenticatedUser
from app.core.config import get_settings
from app.core.models import HealthResponse
from app.core.rate_limiter import limiter
from app.api.routes import data, admin, docs
from app.utils.sql_executor import initialize_sql_executor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Odace Data API",
    description="Data catalogue and query API",
    version="1.0.0"
)

# Add rate limiter state and exception handler
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Configure CORS with security-validated origins
settings = get_settings()
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files directory (relative to this file so it works in Docker/Coolify)
STATIC_DIR = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Include routers
app.include_router(data.router)
app.include_router(admin.router)
app.include_router(docs.router)


@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the main UI."""
    index_path = STATIC_DIR / "index.html"
    try:
        if index_path.exists():
            return HTMLResponse(content=index_path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("Could not serve index.html: %s", e)
    return HTMLResponse(
        content="<h1>Odace Data API</h1><p><a href='/docs'>/docs</a> – API documentation</p><p><a href='/health'>/health</a> – Health check</p>",
        status_code=200,
    )


@app.get("/api/me")
async def get_me(current_user: AuthenticatedUser = Depends(get_current_user)):
    """Return current user info (user_id, is_admin) for the given API key or admin secret."""
    return {"user_id": current_user.user_id, "is_admin": current_user.is_admin}


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow(),
        version="1.0.0"
    )


@app.on_event("startup")
async def startup_event():
    """Startup event handler."""
    settings = get_settings()
    logger.info("Starting Odace Data API")
    logger.info("Environment: %s", settings.environment)
    logger.info("S3 Bucket: %s", settings.scw_bucket_name)
    try:
        from app.db.session import init_engine
        init_engine()
        logger.info("Database (PostgreSQL) initialized")
    except Exception as e:
        logger.warning("Database init skipped or failed: %s", e)

    try:
        initialize_sql_executor()
        logger.info("SQL executor initialized")
    except Exception as e:
        logger.warning("SQL executor init failed: %s", e)
    
    logger.info("Startup completed")


@app.on_event("shutdown")
async def shutdown_event():
    """Shutdown event handler."""
    logger.info("Shutting down Odace Data API")


if __name__ == "__main__":
    import uvicorn
    logger.info("Starting Uvicorn on 0.0.0.0:8080")
    uvicorn.run(app, host="0.0.0.0", port=8080)

