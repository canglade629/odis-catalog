"""Silver pipeline for dim_gare - Train stations dimension (SQL-based)."""
from app.pipelines.silver.base_v2 import SQLSilverV2Pipeline
from app.core.pipeline_registry import register_pipeline
import logging

logger = logging.getLogger(__name__)


@register_pipeline(
    layer="silver",
    name="dim_gare",
    dependencies=["bronze.gares", "silver.dim_commune", "silver.dim_gare_segment"],
    description_fr="Table de dimension des gares voyageurs avec enrichissement géographique (FK vers dim_commune et dim_gare_segment), codes trigramme et coordonnées GPS."
)
class DimGarePipeline(SQLSilverV2Pipeline):
    """Transform gares data into normalized dim_gare dimension table using SQL."""
    
    def get_name(self) -> str:
        return "dim_gare"
    
    def get_target_table(self) -> str:
        return "dim_gare"
    
    def get_sql_query(self) -> str:
        """SQL query to transform bronze gares data with geographic enrichment."""
        return """
            WITH deduplicated AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY trigramme ORDER BY ingestion_timestamp DESC) AS rn
                FROM bronze_gares
                WHERE trigramme IS NOT NULL
                  AND trigramme != 'nan'
                  AND trigramme != ''
            ),
            segment_cleaned AS (
                SELECT 
                    *,
                    -- Apply priority logic: A > B > C
                    -- Handle cases like 'B;A;A', 'B;A', 'C;B', 'C;C', 'A;A', etc.
                    CASE 
                        WHEN segment_s__drg LIKE '%A%' THEN 'A'
                        WHEN segment_s__drg LIKE '%B%' THEN 'B'
                        ELSE 'C'
                    END AS cleaned_segment_code
                FROM deduplicated
                WHERE rn = 1
            ),
            parsed_coords AS (
                SELECT 
                    *,
                    CASE 
                        WHEN position_g_ographique IS NOT NULL 
                             AND position_g_ographique != 'nan' 
                             AND position_g_ographique != '' 
                        THEN 
                            CAST(TRIM(SPLIT(position_g_ographique, ',')[1]) AS DOUBLE)
                        ELSE 0.0
                    END AS latitude,
                    CASE 
                        WHEN position_g_ographique IS NOT NULL 
                             AND position_g_ographique != 'nan' 
                             AND position_g_ographique != '' 
                        THEN 
                            CAST(TRIM(SPLIT(position_g_ographique, ',')[2]) AS DOUBLE)
                        ELSE 0.0
                    END AS longitude
                FROM segment_cleaned
            ),
            with_dedup_timestamp AS (
                SELECT 
                    p.*,
                    d.ingestion_timestamp
                FROM parsed_coords p
                LEFT JOIN deduplicated d ON p.trigramme = d.trigramme AND d.rn = 1
            )
            SELECT 
                MD5(CAST(d.trigramme AS VARCHAR)) AS gare_sk,
                COALESCE(CAST(c.commune_sk AS VARCHAR), '') AS commune_sk,
                MD5(d.cleaned_segment_code) AS gare_segment_sk,
                COALESCE(CAST(d.trigramme AS VARCHAR), '') AS gare_code,
                COALESCE(CAST(d.nom AS VARCHAR), '') AS gare_label,
                COALESCE(d.latitude, 0.0) AS latitude,
                COALESCE(d.longitude, 0.0) AS longitude,
                JSON_OBJECT(
                    'job_insert_id', 'dim_gare',
                    'job_insert_date_utc', CURRENT_TIMESTAMP,
                    'job_modify_id', 'dim_gare',
                    'job_modify_date_utc', CURRENT_TIMESTAMP,
                    'ingestion_timestamp', d.ingestion_timestamp
                ) AS job_metadata
            FROM with_dedup_timestamp d
            LEFT JOIN silver_dim_commune c ON d.code_commune = c.commune_insee_code
        """
