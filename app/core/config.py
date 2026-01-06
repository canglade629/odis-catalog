"""Configuration management using Pydantic Settings."""
import os
import yaml
from functools import lru_cache
from pydantic_settings import BaseSettings
from typing import Optional, List, Dict, Any


class Settings(BaseSettings):
    """Application settings."""
    
    # GCP Configuration
    gcp_project_id: str = "icc-project-472009"
    gcs_bucket: str = "jaccueille"
    gcs_raw_prefix: str = "raw"
    gcs_delta_prefix: str = "delta"
    
    # API Configuration
    admin_secret: str  # No default - must be explicitly set
    environment: str = "development"
    
    # CORS Configuration
    # Comma-separated list of allowed origins, or "*" for development only
    cors_origins: str = "https://odace-pipeline-588398598428.europe-west1.run.app"
    
    # Optional Database URL for metadata
    database_url: Optional[str] = None
    
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
    
    @property
    def gcs_bucket_url(self) -> str:
        """Full GCS bucket URL."""
        return f"gs://{self.gcs_bucket}"
    
    @property
    def raw_path(self) -> str:
        """Path to raw data in GCS."""
        return f"{self.gcs_bucket_url}/{self.gcs_raw_prefix}"
    
    @property
    def delta_path(self) -> str:
        """Path to Delta tables in GCS."""
        return f"{self.gcs_bucket_url}/{self.gcs_delta_prefix}"
    
    def get_raw_path(self, domain: str) -> str:
        """Get path to raw data for a specific domain."""
        return f"{self.raw_path}/{domain}"
    
    def get_bronze_path(self, table: str) -> str:
        """Get path to bronze Delta table."""
        return f"{self.delta_path}/bronze/{table}"
    
    def get_silver_path(self, table: str) -> str:
        """Get path to silver Delta table."""
        return f"{self.delta_path}/silver/{table}"
    
    def get_silver_v2_path(self, table: str) -> str:
        """Get path to silver_v2 Delta table."""
        return f"{self.delta_path}/silver_v2/{table}"
    
    def get_gold_path(self, table: str) -> str:
        """Get path to gold Delta table."""
        return f"{self.delta_path}/gold/{table}"
    
    def get_checkpoint_path(self) -> str:
        """Get path to checkpoint Delta table."""
        return f"{self.delta_path}/checkpoints"
    
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

