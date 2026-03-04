"""Configuration management using Pydantic Settings."""
import os
import yaml
from functools import lru_cache
from pydantic_settings import BaseSettings
from typing import Optional, List, Dict, Any


class Settings(BaseSettings):
    """Application settings."""

    # Scaleway S3 (primary storage)
    scw_object_storage_endpoint: str = "https://s3.fr-par.scw.cloud"
    scw_region: str = "fr-par"
    scw_bucket_name: str = "odis-s3"
    scw_access_key: str = ""
    scw_secret_key: str = ""
    
    # PostgreSQL
    pg_db_host: Optional[str] = None
    pg_db_port: Optional[int] = None
    pg_db_name: Optional[str] = None
    pg_db_user: Optional[str] = None
    pg_db_pwd: Optional[str] = None
    database_url: Optional[str] = None  # Overrides PG_* if set
    
    # API Configuration
    admin_secret: str  # No default - must be explicitly set
    environment: str = "development"
    
    # CORS Configuration
    # Comma-separated list of allowed origins, or "*" for development only
    cors_origins: str = "*"
    
    # Application settings
    log_level: str = "INFO"
    
    # SIAE API Configuration
    siae_api_base_url: str = "https://emplois.inclusion.beta.gouv.fr/api/v1"
    siae_api_rate_limit: int = 12  # requests per minute
    
    # Open Data API Configuration (data.gouv.fr)
    open_data_api_base_url: str = "https://tabular-api.data.gouv.fr/api"
    open_data_api_rate_limit: int = 100  # requests per second
    open_data_sources_config: str = "datasources/open_data_sources.yaml"
    
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
    
    def _get_database_url(self) -> str:
        """Build async PostgreSQL URL from PG_* or use DATABASE_URL."""
        if self.database_url:
            url = self.database_url
            if url.startswith("postgresql://"):
                url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
            elif not url.startswith("postgresql+asyncpg://"):
                url = f"postgresql+asyncpg://{url.split('://', 1)[-1]}"
            return url
        if self.pg_db_host and self.pg_db_name and self.pg_db_user and self.pg_db_pwd is not None:
            port = self.pg_db_port or 5432
            from urllib.parse import quote_plus
            pwd = quote_plus(self.pg_db_pwd)
            return f"postgresql+asyncpg://{self.pg_db_user}:{pwd}@{self.pg_db_host}:{port}/{self.pg_db_name}"
        raise ValueError("Set DATABASE_URL or PG_DB_HOST, PG_DB_NAME, PG_DB_USER, PG_DB_PWD")

    @property
    def resolved_database_url(self) -> str:
        """Resolved async database URL for SQLAlchemy (postgresql+asyncpg://)."""
        return self._get_database_url()

    @property
    def sync_database_url(self) -> str:
        """Sync database URL for use in background threads (postgresql://)."""
        url = self.resolved_database_url
        if "+asyncpg" in url:
            url = url.replace("postgresql+asyncpg://", "postgresql://", 1)
        return url

    @property
    def s3_bucket_url(self) -> str:
        """Full S3 bucket URL (Scaleway)."""
        return f"s3://{self.scw_bucket_name}"

    @property
    def raw_path(self) -> str:
        """Path to raw data in S3."""
        return f"{self.s3_bucket_url}/raw"

    @property
    def bronze_path(self) -> str:
        """Path to bronze (Delta) layer in S3."""
        return f"{self.s3_bucket_url}/bronze"

    @property
    def silver_path(self) -> str:
        """Path to silver (Parquet) layer in S3."""
        return f"{self.s3_bucket_url}/silver"

    @property
    def gold_path(self) -> str:
        """Path to gold layer in S3."""
        return f"{self.s3_bucket_url}/gold"

    @property
    def delta_path(self) -> str:
        """Alias: base path for Delta tables (bronze)."""
        return self.bronze_path

    def get_raw_path(self, domain: str) -> str:
        """Get path to raw data for a specific domain."""
        return f"{self.raw_path}/{domain}"

    def get_bronze_path(self, table: str) -> str:
        """Get path to bronze Delta table."""
        return f"{self.bronze_path}/{table}"

    def get_silver_path(self, table: str) -> str:
        """Get path to silver table (Parquet file on S3)."""
        return f"{self.silver_path}/{table}.parquet"

    def get_gold_path(self, table: str) -> str:
        """Get path to gold table."""
        return f"{self.gold_path}/{table}"

    def get_checkpoint_path(self) -> str:
        """Get path to checkpoint Delta table."""
        return f"{self.bronze_path}/checkpoints"
    
    def load_open_data_sources(self) -> List[Dict[str, Any]]:
        """
        Load Open Data sources from YAML configuration.
        
        Returns:
            List of resource configurations with resource_id, name, and description
        """
        config_path = self.open_data_sources_config
        
        # Handle both absolute and relative paths
        if not os.path.isabs(config_path):
            # Try to find the file relative to the workspace root
            workspace_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            config_path = os.path.join(workspace_root, config_path)
        
        if not os.path.exists(config_path):
            return []
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                return config.get('resources', [])
        except Exception as e:
            print(f"Warning: Could not load open data sources config: {e}")
            return []


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()

