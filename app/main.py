"""Main FastAPI application."""
import logging
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from datetime import datetime
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.core.config import get_settings
from app.core.models import HealthResponse
from app.core.rate_limiter import limiter
from app.api.routes import bronze, silver, gold, pipeline, files, data, jobs, admin, docs

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Odace Data Pipeline API",
    description="Platform-agnostic data pipeline for bronze/silver/gold layers",
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
app.include_router(bronze.router)
app.include_router(silver.router)
app.include_router(gold.router)
app.include_router(pipeline.router)
app.include_router(files.router)
app.include_router(data.router)
app.include_router(jobs.router)
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
        content="<h1>Odace Data Pipeline API</h1><p><a href='/docs'>/docs</a> – API documentation</p><p><a href='/health'>/health</a> – Health check</p>",
        status_code=200,
    )


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
    logger.info("Starting Odace Data Pipeline API")
    logger.info("Environment: %s", settings.environment)
    logger.info("S3 Bucket: %s", settings.scw_bucket_name)
    try:
        from app.db.session import init_engine
        init_engine()
        logger.info("Database (PostgreSQL) initialized")
    except Exception as e:
        logger.warning("Database init skipped or failed: %s", e)
    
    # Load pipelines from YAML configuration
    from app.core.config_loader import get_config_loader
    from app.core.pipeline_registry import register_pipelines_from_yaml, get_registry
    from app.core.models import PipelineLayer
    
    try:
        config_loader = get_config_loader()
        register_pipelines_from_yaml(config_loader)
        
        # Log registered pipelines (bronze only; silver/gold run via DBT)
        registry = get_registry()
        bronze_pipelines = registry.list_pipelines(layer=PipelineLayer.BRONZE)
        gold_pipelines = registry.list_pipelines(layer=PipelineLayer.GOLD)
        
        logger.info(f"Registered {len(bronze_pipelines)} bronze pipelines")
        logger.info(f"Silver/Gold: run via DBT project (dbt/)")
        logger.info(f"Bronze pipelines: {[p.name for p in bronze_pipelines]}")
        
    except Exception as e:
        logger.error(f"Error loading pipelines from YAML: {e}", exc_info=True)
        logger.warning("Continuing without YAML-configured pipelines")


@app.on_event("shutdown")
async def shutdown_event():
    """Shutdown event handler."""
    logger.info("Shutting down Odace Data Pipeline API")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)

