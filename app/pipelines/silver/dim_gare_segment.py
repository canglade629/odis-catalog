"""Silver pipeline for dim_gare_segment - Gare segment reference table."""
import pandas as pd
import hashlib
from datetime import datetime
from app.pipelines.base import BasePipeline
from app.core.pipeline_registry import register_pipeline
import logging

logger = logging.getLogger(__name__)


@register_pipeline(
    layer="silver",
    name="dim_gare_segment",
    dependencies=[],
    description_fr="Table de référence des segments de gares (A=National, B=Régional, C=Local) avec 3 catégories hardcodées."
)
class DimGareSegmentPipeline(BasePipeline):
    """Generate hardcoded reference table for gare segments."""
    
    def get_name(self) -> str:
        return "dim_gare_segment"
    
    def get_target_table(self) -> str:
        return "dim_gare_segment"
    
    def run(self, force: bool = False) -> dict:
        """Generate and write the reference data."""
        logger.info("Generating gare segment reference data")
        logger.info(f"Force mode: {force}")
        
        # Define the 3 segments
        reference_data = [
            {
                'gare_segment_code': 'A',
                'gare_segment_label': "Gare d'intérêt national",
                'gare_segment_description': "Gares dont la fréquentation voyageurs nationaux/internationaux est ≥ 250 000 par an ou représente 100% du trafic"
            },
            {
                'gare_segment_code': 'B',
                'gare_segment_label': "Gare d'intérêt régional",
                'gare_segment_description': "Gares non classées A mais dont la fréquentation totale est ≥ 100 000 voyageurs annuels"
            },
            {
                'gare_segment_code': 'C',
                'gare_segment_label': "Gare d'intérêt local",
                'gare_segment_description': "Toutes les autres gares voyageurs"
            }
        ]
        
        # Create DataFrame
        df = pd.DataFrame(reference_data)
        
        # Add surrogate key (MD5 hash of segment code)
        df['gare_segment_sk'] = df['gare_segment_code'].apply(
            lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()
        )
        
        # Add job tracking metadata as struct
        now = datetime.utcnow()
        now_str = now.isoformat() + 'Z'
        job_metadata_struct = {
            'job_insert_id': 'dim_gare_segment',
            'job_insert_date_utc': now_str,
            'job_modify_id': 'dim_gare_segment',
            'job_modify_date_utc': now_str
        }
        df['job_metadata'] = [job_metadata_struct.copy() for _ in range(len(df))]
        
        # Reorder columns to match expected schema
        df = df[[
            'gare_segment_sk',
            'gare_segment_code',
            'gare_segment_label',
            'gare_segment_description',
            'job_metadata'
        ]]
        
        logger.info(f"Generated {len(df)} gare segment records")
        
        # Write to Delta Lake (silver layer)
        target_path = self.settings.get_silver_path(self.get_target_table())
        logger.info(f"Writing to {target_path}")
        
        self.delta_ops.write_delta(
            df=df,
            table_path=target_path,
            mode="overwrite",  # Always overwrite as this is reference data
            schema_mode="overwrite"
        )
        
        logger.info(f"Successfully wrote {len(df)} rows to {self.get_target_table()}")
        
        return {
            "status": "success",
            "rows_processed": len(df),
            "message": f"Successfully generated {len(df)} gare segment records"
        }


