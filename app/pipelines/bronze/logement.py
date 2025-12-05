"""Bronze pipeline for logement (housing) data."""
import pandas as pd
import re
from datetime import datetime
from app.pipelines.base import BaseBronzePipeline
from app.core.pipeline_registry import register_pipeline
import logging

logger = logging.getLogger(__name__)


@register_pipeline(layer="bronze", name="logement")
class BronzeLogementPipeline(BaseBronzePipeline):
    """
    Ingests logement CSV data with timestamp extraction into bronze layer.
    
    Supports 2024 multi-source data with automatic classification based on filename:
    - pred-app-mef-dhup.csv: appartement, toutes typologies
    - pred-app12-mef-dhup.csv: appartement, T1 et T2
    - pred-app3-mef-dhup.csv: appartement, T3 et plus
    - pred-mai-mef-dhup.csv: maison, toutes typologies
    """
    
    # Mapping of filename patterns to housing characteristics
    FILENAME_MAPPING = {
        'pred-app-mef-dhup': {
            'type_bien': 'appartement',
            'segment_typologie': 'toutes typologies',
            'surface_ref': 52.0,
            'surface_piece_moy': 22.2
        },
        'pred-app12-mef-dhup': {
            'type_bien': 'appartement',
            'segment_typologie': 'T1 et T2',
            'surface_ref': 37.0,
            'surface_piece_moy': 23.0
        },
        'pred-app3-mef-dhup': {
            'type_bien': 'appartement',
            'segment_typologie': 'T3 et plus',
            'surface_ref': 72.0,
            'surface_piece_moy': 21.2
        },
        'pred-mai-mef-dhup': {
            'type_bien': 'maison',
            'segment_typologie': 'toutes typologies',
            'surface_ref': 92.0,
            'surface_piece_moy': 22.4
        }
    }
    
    def get_name(self) -> str:
        return "bronze_logement"
    
    def get_source_path(self) -> str:
        return self.settings.get_raw_path("logement")
    
    def get_target_table(self) -> str:
        return "logement"
    
    def read_source_file(self, file_path: str) -> pd.DataFrame:
        """Read CSV file from GCS with encoding fallback."""
        logger.info(f"Reading CSV file: {file_path}")
        
        # Download file to memory
        file_content = self.gcs.download_file(file_path)
        
        # Try UTF-8 first, then fallback to Latin-1 (ISO-8859-1)
        try:
            df = pd.read_csv(
                pd.io.common.BytesIO(file_content),
                delimiter=';',
                header=0,
                encoding='utf-8'
            )
            logger.info(f"Read CSV with UTF-8 encoding")
        except UnicodeDecodeError:
            logger.warning(f"UTF-8 decoding failed, trying Latin-1")
            df = pd.read_csv(
                pd.io.common.BytesIO(file_content),
                delimiter=';',
                header=0,
                encoding='latin1'
            )
            logger.info(f"Read CSV with Latin-1 encoding")
        
        logger.info(f"Read {len(df)} rows with {len(df.columns)} columns")
        return df
    
    def _identify_file_type(self, filename: str) -> dict:
        """
        Identify the housing type and characteristics from filename.
        
        Args:
            filename: Name of the file (e.g., 'pred-app-mef-dhup.csv')
            
        Returns:
            Dictionary with type_bien, segment_typologie, surface_ref, surface_piece_moy
        """
        # Remove extension and any trailing parts after .csv
        base_filename = filename.lower().replace('.csv', '')
        
        # Check each pattern
        for pattern, characteristics in self.FILENAME_MAPPING.items():
            if pattern in base_filename:
                logger.info(f"Identified file type from pattern '{pattern}': {characteristics}")
                return characteristics
        
        # Default fallback (should not happen with proper file naming)
        logger.warning(f"Could not identify file type from filename: {filename}. Using default values.")
        return {
            'type_bien': 'appartement',
            'segment_typologie': 'toutes typologies',
            'surface_ref': 52.0,
            'surface_piece_moy': 22.2
        }
    
    def transform(self, df: pd.DataFrame, file_path: str) -> pd.DataFrame:
        """
        Transform with timestamp extraction and 2024 data enrichment.
        
        For 2024 data files (pred-*.csv), adds:
        - annee: 2024
        - type_bien: appartement or maison
        - segment_typologie: toutes typologies, T1 et T2, or T3 et plus
        - surface_ref: reference surface in m²
        - surface_piece_moy: average surface per room in m²
        
        Args:
            df: Input DataFrame
            file_path: Path to the source file
            
        Returns:
            Transformed DataFrame with additional columns
        """
        # Extract filename
        filename = file_path.split('/')[-1]
        
        # Try to extract timestamp from filename pattern: *_YYYYMMDD_HHMMSS.csv
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
        
        # Check if this is a 2024 data file (pred-*.csv pattern)
        if filename.startswith('pred-'):
            logger.info(f"Processing 2024 logement file: {filename}")
            
            # Add year column
            df['annee'] = 2024
            
            # Identify and add housing characteristics
            file_characteristics = self._identify_file_type(filename)
            df['type_bien'] = file_characteristics['type_bien']
            df['segment_typologie'] = file_characteristics['segment_typologie']
            df['surface_ref'] = file_characteristics['surface_ref']
            df['surface_piece_moy'] = file_characteristics['surface_piece_moy']
            
            logger.info(f"Added 2024 columns: annee=2024, type_bien={file_characteristics['type_bien']}, "
                       f"segment_typologie={file_characteristics['segment_typologie']}, "
                       f"surface_ref={file_characteristics['surface_ref']}, "
                       f"surface_piece_moy={file_characteristics['surface_piece_moy']}")
        else:
            # Legacy files - add NULL columns for compatibility
            logger.info(f"Processing legacy logement file: {filename}")
            df['annee'] = None
            df['type_bien'] = None
            df['segment_typologie'] = None
            df['surface_ref'] = None
            df['surface_piece_moy'] = None
        
        return df
