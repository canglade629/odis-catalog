"""Bronze pipeline for transport data (gares and lignes)."""
import pandas as pd
import re
from app.pipelines.base import BaseBronzePipeline
from app.pipelines.base_api import BaseAPIBronzePipeline
from app.core.pipeline_registry import register_pipeline
import logging

logger = logging.getLogger(__name__)


@register_pipeline(layer="bronze", name="gares")
class BronzeGaresPipeline(BaseBronzePipeline):
    """Ingests gares (train stations) CSV data from raw folder into bronze layer."""
    
    def get_name(self) -> str:
        return "bronze_gares"
    
    def get_source_path(self) -> str:
        return self.settings.get_raw_path("transport/gares")
    
    def get_target_table(self) -> str:
        return "gares"
    
    def read_source_file(self, file_path: str) -> pd.DataFrame:
        """Read CSV file from S3 with auto-delimiter detection."""
        logger.info(f"Reading gares CSV file: {file_path}")
        
        # Download file to memory
        file_content = self.s3.download_file(file_path)
        
        # Try to auto-detect delimiter (common ones: comma, semicolon, tab)
        try:
            df = pd.read_csv(
                pd.io.common.BytesIO(file_content),
                header=0,
                sep=None,  # Auto-detect delimiter
                engine='python',
                encoding='utf-8-sig',  # utf-8-sig strips BOM automatically
                on_bad_lines='skip'  # Skip problematic lines
            )
            logger.info(f"Auto-detected delimiter successfully")
        except Exception as e:
            logger.warning(f"Auto-detection failed: {e}, trying semicolon delimiter")
            # Fallback to semicolon
            df = pd.read_csv(
                pd.io.common.BytesIO(file_content),
                header=0,
                sep=';',
                encoding='utf-8-sig',
                on_bad_lines='skip'
            )
        
        # Strip BOM from column names if present (extra safety)
        df.columns = df.columns.str.replace('^\ufeff', '', regex=True)
        
        # Convert all columns to string to avoid Delta Lake null type issues
        for col in df.columns:
            df[col] = df[col].astype(str)
        
        logger.info(f"Read {len(df)} rows with {len(df.columns)} columns")
        return df
    
    def transform(self, df: pd.DataFrame, file_path: str) -> pd.DataFrame:
        """Normalize column names."""
        # Normalize column names: lowercase, replace non-alphanumeric with underscores
        normalized_cols = [re.sub(r'[^a-zA-Z0-9]', '_', c).lower() for c in df.columns]
        df.columns = normalized_cols
        
        # Add ingestion timestamp
        df = super().transform(df, file_path)
        
        logger.info(f"Normalized columns: {list(df.columns)}")
        return df

@register_pipeline(layer="bronze", name="lignes")
class BronzeLignesPipeline(BaseAPIBronzePipeline):
    """Ingests lignes (train lines) data from Open Data API into bronze layer."""
    
    # SNCF Lignes resource ID from data.gouv.fr
    RESOURCE_ID = "2f204d3f-4274-42fb-934f-4a73954e0c4e"
    
    def __init__(self):
        """Initialize with Open Data API configuration."""
        super().__init__()
        # Override rate limiter for Open Data API (100 req/sec)
        from app.pipelines.base_api import RateLimiter
        self.rate_limiter = RateLimiter(
            max_requests=self.settings.open_data_api_rate_limit,
            time_window=1  # 1 second window
        )
    
    def get_name(self) -> str:
        return "bronze_lignes"
    
    def get_source_path(self) -> str:
        return f"api://open_data/{self.RESOURCE_ID}"
    
    def get_target_table(self) -> str:
        return "lignes"
    
    def get_api_endpoint(self) -> str:
        """Get the API endpoint for SNCF lignes resource."""
        return f"/resources/{self.RESOURCE_ID}/data/"
    
    def get_api_params(self) -> dict:
        """Get query parameters for API request."""
        return {'page_size': 100}
    
    async def fetch_all_data(self):
        """Fetch all data from Open Data API."""
        import httpx
        import asyncio
        
        base_url = self.settings.open_data_api_base_url
        endpoint = self.get_api_endpoint()
        url = f"{base_url}{endpoint}"
        params = self.get_api_params()
        
        all_records = []
        page = 1
        
        async with httpx.AsyncClient() as client:
            self.client = client
            
            while True:
                page_params = {**params, "page": page}
                logger.info(f"Fetching page {page}...")
                response_data = await self.fetch_page(url, page_params)
                
                records = response_data.get("data", [])
                if not records:
                    break
                
                all_records.extend(records)
                
                meta = response_data.get("meta", {})
                total = meta.get("total", "unknown")
                logger.info(f"Fetched {len(all_records)} / {total} records")
                
                if not response_data.get("links", {}).get("next"):
                    break
                
                page += 1
        
        logger.info(f"Fetched {len(all_records)} total records")
        return all_records
    
    def transform(self, df: pd.DataFrame, file_path: str) -> pd.DataFrame:
        """Normalize column names and convert types to match Databricks schema."""
        import json
        
        # Normalize column names: lowercase, replace non-alphanumeric with underscores
        normalized_cols = [re.sub(r'[^a-zA-Z0-9]', '_', c).lower() for c in df.columns]
        df.columns = normalized_cols
        
        # Convert code_ligne to string to match Databricks schema
        if 'code_ligne' in df.columns:
            df['code_ligne'] = df['code_ligne'].astype(str)
        
        # Fix geo_shape_coordinates - convert nested lists to JSON string for Delta compatibility
        if 'geo_shape_coordinates' in df.columns:
            def serialize_coords(x):
                if x is None:
                    return None
                try:
                    # Check if it's a scalar NaN (not an array)
                    if isinstance(x, float) and pd.isna(x):
                        return None
                    return json.dumps(x)
                except (TypeError, ValueError):
                    return None
            
            df['geo_shape_coordinates'] = df['geo_shape_coordinates'].apply(serialize_coords)
        
        # Add ingestion timestamp
        df = super().transform(df, file_path)
        
        logger.info(f"Normalized columns: {list(df.columns)}")
        return df

