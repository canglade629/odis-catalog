"""Configuration management using Pydantic Settings."""
from functools import lru_cache
from pydantic_settings import BaseSettings
from typing import Optional, List


class Settings(BaseSettings):
    """Application settings."""

    # Google Cloud
    gcp_project_id: Optional[str] = None  # Defaults to the project of the runtime credentials
    firestore_database: str = "(default)"  # Only the (default) database gets the Firestore free quota
    # The layers are the BigQuery datasets <prefix>_bronze, <prefix>_silver and <prefix>_gold
    bigquery_dataset_prefix: str = "odas"
    bigquery_location: str = "europe-west1"
    # Caps on what the API may spend, shared by every user: BigQuery bills the bytes a query processes
    # (1 TiB free per month and billing account), exports are billed as outbound data transfer
    query_max_bytes_billed: int = 5 * 1024**3
    query_daily_bytes_budget: int = 20 * 1024**3
    export_daily_bytes_budget: int = 5 * 1024**3

    # API Configuration
    admin_secret: str  # No default - must be explicitly set
    environment: str = "development"
    
    # CORS Configuration
    # Comma-separated list of allowed origins, or "*" for development only
    cors_origins: str = "*"
    
    # Application settings
    log_level: str = "INFO"
    metadata_cache_ttl_seconds: int = 300
    
    # SIAE API Configuration
    siae_api_base_url: str = "https://emplois.inclusion.beta.gouv.fr/api/v1"
    siae_api_rate_limit: int = 12  # requests per minute
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"  # Ignore extra fields in .env that are not in the model
    
    def __init__(self, **kwargs):
        """Initialize settings with security validation."""
        super().__init__(**kwargs)
        self._validate_security()
    
    def _validate_security(self):
        """Validate security-critical configuration."""
        # Check for insecure admin secret
        insecure_secrets = ["changeme", "admin", "secret", "password", "test", ""]
        if self.admin_secret.lower() in insecure_secrets:
            raise ValueError(
                f"ADMIN_SECRET is set to an insecure value '{self.admin_secret}'. "
                "Please set a strong, unique secret in your environment variables."
            )
        
        # Warn if using wildcard CORS in production
        if self.environment == "production" and self.cors_origins == "*":
            raise ValueError(
                "CORS wildcard (*) is not allowed in production. "
                "Set CORS_ORIGINS to specific allowed domains."
            )
    
    @property
    def allowed_origins(self) -> List[str]:
        """Parse and return list of allowed CORS origins."""
        if self.cors_origins == "*":
            # Only allowed in development
            if self.environment == "development":
                return ["*"]
            else:
                raise ValueError("CORS wildcard not allowed in production")
        
        # Split comma-separated origins and strip whitespace
        origins = [origin.strip() for origin in self.cors_origins.split(",")]
        return [origin for origin in origins if origin]  # Filter empty strings


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()

