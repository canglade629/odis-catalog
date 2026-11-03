"""Main FastAPI application."""
import logging
from contextlib import asynccontextmanager
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logger.info("Starting ODAS API")
    logger.info("Environment: %s", settings.environment)
    logger.info(
        "BigQuery datasets: %s_{bronze,silver,gold} (%s)",
        settings.bigquery_dataset_prefix,
        settings.bigquery_location,
    )
    logger.info("Firestore database: %s", settings.firestore_database)
    try:
        from app.db.firestore import get_firestore_client
        get_firestore_client()
    except Exception as e:
        logger.warning("Firestore client init failed: %s", e)
    logger.info("Startup completed")
    yield
    logger.info("Shutting down ODAS API")


# Create FastAPI app
app = FastAPI(
    title="ODAS API",
    description="ODAS (Open Data Accueil Solidaire) data catalogue and query API",
    version="1.0.0",
    lifespan=lifespan,
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

# Static files directory (relative to this file so it works in the container)
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
        content="<h1>ODAS API</h1><p><a href='/docs'>/docs</a> – API documentation</p><p><a href='/health'>/health</a> – Health check</p>",
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


if __name__ == "__main__":
    import os
    import uvicorn
    port = int(os.environ.get("PORT", "8080"))
    logger.info("Starting Uvicorn on 0.0.0.0:%d", port)
    uvicorn.run(app, host="0.0.0.0", port=port)

