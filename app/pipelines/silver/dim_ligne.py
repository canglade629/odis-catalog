"""Silver pipeline for dim_ligne - Railway lines dimension (SQL-based)."""
from app.pipelines.silver.base_v2 import SQLSilverV2Pipeline
from app.core.pipeline_registry import register_pipeline
import logging

logger = logging.getLogger(__name__)


@register_pipeline(
    layer="silver",
    name="dim_ligne",
    dependencies=["bronze.lignes"],
    description_fr="Table de dimension des lignes ferroviaires avec tronçons, catégories (TGV/non-TGV) et coordonnées de début/fin."
)
class DimLignePipeline(SQLSilverV2Pipeline):
    """Transform lignes data into normalized dim_ligne dimension table using SQL."""
    
    def get_name(self) -> str:
        return "dim_ligne"
    
    def get_target_table(self) -> str:
        return "dim_ligne"
    
    def get_sql_query(self) -> str:
        """SQL query to transform bronze lignes data."""
        return """
            WITH deduplicated AS (
                SELECT *,
                    CASE WHEN catlig = 'Ligne à grande vitesse' THEN TRUE ELSE FALSE END AS is_tgv,
                    ROW_NUMBER() OVER (PARTITION BY code_ligne ORDER BY ingestion_timestamp DESC) AS rn
                FROM bronze_lignes
                WHERE code_ligne IS NOT NULL
            )
            SELECT 
                MD5(code_ligne) AS ligne_sk,
                code_ligne AS ligne_code,
                lib_ligne AS ligne_label,
                catlig AS categorie,
                is_tgv,
                geo_shape_coordinates,
                JSON_OBJECT(
                    'job_insert_id', 'dim_ligne',
                    'job_insert_date_utc', CURRENT_TIMESTAMP,
                    'job_modify_id', 'dim_ligne',
                    'job_modify_date_utc', CURRENT_TIMESTAMP,
                    'ingestion_timestamp', ingestion_timestamp
                ) AS job_metadata
            FROM deduplicated
            WHERE rn = 1
        """
