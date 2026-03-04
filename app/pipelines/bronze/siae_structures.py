"""Bronze pipeline for SIAE structures data - reads from CSV files."""
import pandas as pd
from app.pipelines.base import BaseBronzePipeline
from app.core.pipeline_registry import register_pipeline
import logging

logger = logging.getLogger(__name__)


@register_pipeline(layer="bronze", name="siae_structures")
class BronzeSIAEStructuresPipeline(BaseBronzePipeline):
    """
    Ingests SIAE structures data from CSV files.
    
    Reads from gs://jaccueille/raw/api/siae_structures/ (CSV format).
    """
    
    def get_name(self) -> str:
        return "bronze_siae_structures"
    
    def get_source_path(self) -> str:
        return "gs://jaccueille/raw/api/siae_structures/"
    
    def get_target_table(self) -> str:
        return "siae_structures"
    
    def read_source_file(self, file_path: str) -> pd.DataFrame:
        """Read CSV file from S3."""
        logger.info(f"Reading SIAE structures CSV from: {file_path}")
        
        # Download file content
        file_content = self.s3.download_file(file_path)
        
        # Parse CSV with explicit dtypes to ensure proper string handling
        import io
        dtype_dict = {
            'id': str,
            'siret': str,
            'nom': str,
            'commune': str,
            'code_postal': str,
            'code_insee': str,
            'adresse': str,
            'complement_adresse': str,
            'telephone': str,
            'courriel': str,
            'site_web': str,
            'description': str,
            'source': str,
            'date_maj': str,
            'lien_source': str,
            'horaires_accueil': str,
            'accessibilite_lieu': str,
            'reseaux_porteurs': str
        }
        
        df = pd.read_csv(
            io.BytesIO(file_content),
            dtype=dtype_dict,
            keep_default_na=False,
            na_values=['']
        )
        
        # Convert longitude and latitude to float (keeping them as numeric)
        if 'longitude' in df.columns:
            df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        if 'latitude' in df.columns:
            df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        
        logger.info(f"Loaded {len(df)} SIAE structures from CSV with {len(df.columns)} columns")
        return df
    
    def transform(self, df: pd.DataFrame, file_path: str) -> pd.DataFrame:
        """Add ingestion timestamp."""
        from datetime import datetime
        import re
        
        # Extract date from filename (e.g., structures-inclusion-2025-12-01.csv)
        filename = file_path.split('/')[-1]
        match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
        
        if match:
            date_str = match.group(1)
            try:
                ingestion_timestamp = datetime.strptime(date_str, '%Y-%m-%d')
                logger.info(f"Extracted timestamp from filename: {ingestion_timestamp}")
            except ValueError:
                logger.warning(f"Could not parse date: {date_str}")
                ingestion_timestamp = datetime.utcnow()
        else:
            logger.warning(f"No date found in filename: {filename}")
            ingestion_timestamp = datetime.utcnow()
        
        df['ingestion_timestamp'] = ingestion_timestamp
        
        return df
