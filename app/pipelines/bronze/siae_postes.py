"""Bronze pipeline for SIAE postes data - reads from cached JSON files."""
import pandas as pd
import json
from app.pipelines.base import BaseBronzePipeline
from app.core.pipeline_registry import register_pipeline
import logging

logger = logging.getLogger(__name__)


@register_pipeline(layer="bronze", name="siae_postes")
class BronzeSIAEPostesPipeline(BaseBronzePipeline):
    """
    Ingests SIAE job positions data from cached JSON files.
    
    Reads from gs://jaccueille/raw/api/siae_postes/ instead of making API calls.
    """
    
    def get_name(self) -> str:
        return "bronze_siae_postes"
    
    def get_source_path(self) -> str:
        return "gs://jaccueille/raw/api/siae_postes/"
    
    def get_target_table(self) -> str:
        return "siae_postes"
    
    def read_source_file(self, file_path: str) -> pd.DataFrame:
        """Read cached JSON file from GCS."""
        logger.info(f"Reading cached SIAE postes from: {file_path}")
        
        # Download file content
        file_content = self.gcs.download_file(file_path)
        
        # Parse JSON
        data = json.loads(file_content.decode('utf-8'))
        
        logger.info(f"Loaded {len(data)} SIAE postes from cache")
        
        # Flatten nested JSON to DataFrame
        df = pd.json_normalize(
            data,
            sep='_',
            max_level=2
        )
        
        logger.info(f"Normalized to {len(df)} rows with {len(df.columns)} columns")
        return df
    
    def transform(self, df: pd.DataFrame, file_path: str) -> pd.DataFrame:
        """Add ingestion timestamp."""
        from datetime import datetime
        import re
        
        # Extract timestamp from filename (e.g., siae_postes_20251203_150005.json)
        filename = file_path.split('/')[-1]
        match = re.search(r'_(\d{8}_\d{6})', filename)
        
        if match:
            timestamp_str = match.group(1)
            try:
                ingestion_timestamp = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                logger.info(f"Extracted timestamp from filename: {ingestion_timestamp}")
            except ValueError:
                logger.warning(f"Could not parse timestamp: {timestamp_str}")
                ingestion_timestamp = datetime.utcnow()
        else:
            logger.warning(f"No timestamp found in filename: {filename}")
            ingestion_timestamp = datetime.utcnow()
        
        df['ingestion_timestamp'] = ingestion_timestamp
        
        return df
