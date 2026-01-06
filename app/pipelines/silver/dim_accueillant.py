"""Silver pipeline for dim_accueillant - Host locations dimension (SQL-based)."""
from app.pipelines.silver.base_v2 import SQLSilverV2Pipeline
from app.core.pipeline_registry import register_pipeline
import logging

logger = logging.getLogger(__name__)


@register_pipeline(
    layer="silver",
    name="dim_accueillant",
    dependencies=["bronze.accueillants", "silver.dim_commune"],
    description_fr="Table de dimension des structures d'accueil avec enrichissement géographique (FK vers dim_commune) et coordonnées GPS."
)
class DimAccueillantPipeline(SQLSilverV2Pipeline):
    """Transform accueillants data into normalized dim_accueillant dimension table using SQL."""
    
    def get_name(self) -> str:
        return "dim_accueillant"
    
    def get_target_table(self) -> str:
        return "dim_accueillant"
    
    def get_sql_query(self) -> str:
        """SQL query to transform bronze accueillants data with geographic enrichment and deduplication."""
        return """
            WITH deduplicated_bronze AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY TRIM(Ville), TRIM(Code_postal), CAST(Latitude AS DOUBLE), CAST(Longitude AS DOUBLE), statut
                        ORDER BY ingestion_timestamp DESC
                    ) AS rn
                FROM bronze_accueillants
                WHERE Ville IS NOT NULL AND Ville != '' AND Ville != 'nan'
            ),
            accueillants_clean AS (
                SELECT 
                    COALESCE(statut, 'Sans statut') AS statut,
                    TRIM(Ville) AS ville,
                    TRIM(Code_postal) AS code_postal,
                    CAST(Latitude AS DOUBLE) AS latitude,
                    CAST(Longitude AS DOUBLE) AS longitude
                FROM deduplicated_bronze
                WHERE rn = 1
                  AND Latitude BETWEEN -90 AND 90
                  AND Longitude BETWEEN -180 AND 180
            ),
            with_commune AS (
                SELECT 
                    a.*,
                    c.commune_sk,
                    b.ingestion_timestamp
                FROM accueillants_clean a
                LEFT JOIN silver_dim_commune c
                    ON SUBSTRING(a.code_postal, 1, 5) = c.commune_insee_code
                LEFT JOIN deduplicated_bronze b
                    ON TRIM(b.Ville) = a.ville
                    AND TRIM(b.Code_postal) = a.code_postal
                    AND CAST(b.Latitude AS DOUBLE) = a.latitude
                    AND CAST(b.Longitude AS DOUBLE) = a.longitude
                    AND b.rn = 1
            )
            SELECT 
                MD5(CONCAT(CAST(latitude AS VARCHAR), CAST(longitude AS VARCHAR), statut)) AS accueillant_sk,
                commune_sk,
                statut,
                latitude,
                longitude,
                JSON_OBJECT(
                    'job_insert_id', 'dim_accueillant',
                    'job_insert_date_utc', CURRENT_TIMESTAMP,
                    'job_modify_id', 'dim_accueillant',
                    'job_modify_date_utc', CURRENT_TIMESTAMP,
                    'ingestion_timestamp', ingestion_timestamp
                ) AS job_metadata
            FROM with_commune
        """
