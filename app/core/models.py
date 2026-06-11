"""Pydantic models for API responses."""
from pydantic import BaseModel
from datetime import datetime


class HealthResponse(BaseModel):
    """Health check response."""
    status: str = "healthy"
    timestamp: datetime
    version: str = "1.0.0"

