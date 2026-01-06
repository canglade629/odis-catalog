"""Silver pipeline for ref_logement_profil - Reference table for housing profiles."""
import pandas as pd
from datetime import datetime
from app.pipelines.base import BasePipeline
from app.core.pipeline_registry import register_pipeline
import logging

logger = logging.getLogger(__name__)


@register_pipeline(
    layer="silver",
    name="ref_logement_profil",
    dependencies=[],
    description_fr="Table de référence des profils de biens immobiliers (type, typologie, surfaces de référence)"
)
class RefLogementProfilPipeline(BasePipeline):
    """
    Creates reference table for housing profiles (types and typologies).
    
    This is a static reference table that defines the characteristics
    for each housing segment used in the 2024 logement data.
    """
    
    def get_name(self) -> str:
        return "ref_logement_profil"
    
    def get_target_table(self) -> str:
        return "ref_logement_profil"
    
    def run(self, force: bool = False) -> dict:
        """Generate and write the reference data."""
        logger.info("Generating housing profile reference data")
        logger.info(f"Force mode: {force}")
        
        # Define the reference data matching the FILENAME_MAPPING from logement.py
        reference_data = [
            {
                'source_filename': 'pred-app-mef-dhup.csv',
                'annee': 2024,
                'logement_type': 'appartement',
                'typologie': 'toutes typologies',
                'surface': 52.0,
                'surface_piece_moy': 22.2
            },
            {
                'source_filename': 'pred-app12-mef-dhup.csv',
                'annee': 2024,
                'logement_type': 'appartement',
                'typologie': 'T1 et T2',
                'surface': 37.0,
                'surface_piece_moy': 23.0
            },
            {
                'source_filename': 'pred-app3-mef-dhup.csv',
                'annee': 2024,
                'logement_type': 'appartement',
                'typologie': 'T3 et plus',
                'surface': 72.0,
                'surface_piece_moy': 21.2
            },
            {
                'source_filename': 'pred-mai-mef-dhup.csv',
                'annee': 2024,
                'logement_type': 'maison',
                'typologie': 'toutes typologies',
                'surface': 92.0,
                'surface_piece_moy': 22.4
            }
        ]
        
        # Create DataFrame
        df = pd.DataFrame(reference_data)
        
        # Add surrogate key (MD5 hash of source_filename)
        import hashlib
        df['logement_profil_sk'] = df['source_filename'].apply(
            lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()
        )
        
        # Add job tracking metadata as struct (to match SQL pipelines which use JSON_OBJECT)
        # Delta Lake stores this as a struct type, not a JSON string
        now = datetime.utcnow()
        now_str = now.isoformat() + 'Z'
        job_metadata_struct = {
            'job_insert_id': 'ref_logement_profil',
            'job_insert_date_utc': now_str,
            'job_modify_id': 'ref_logement_profil',
            'job_modify_date_utc': now_str
        }
        # Create a list of identical dicts (one per row) - Delta Lake will store as struct
        df['job_metadata'] = [job_metadata_struct.copy() for _ in range(len(df))]
        
        # Reorder columns: SK first, then business columns, then metadata
        column_order = ['logement_profil_sk', 'source_filename', 'annee', 'logement_type', 
                        'typologie', 'surface', 'surface_piece_moy', 'job_metadata']
        df = df[column_order]
        
        logger.info(f"Generated {len(df)} housing profile records")
        
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
            "message": f"Successfully generated {len(df)} housing profile records"
        }

