"""Silver pipeline for fact_zone_attraction - Urban attraction zones fact table (SQL-based)."""
from app.pipelines.silver.base_v2 import SQLSilverV2Pipeline
from app.core.pipeline_registry import register_pipeline
import logging

logger = logging.getLogger(__name__)


@register_pipeline(
    layer="silver",
    name="fact_zone_attraction",
    dependencies=["bronze.zones_attraction", "silver.dim_commune"],
    description_fr="Table de faits des aires d'attraction des villes 2020 avec FKs vers communes (commune et pôle) et catégories d'attraction."
)
class FactZoneAttractionPipeline(SQLSilverV2Pipeline):
    """Transform zones_attraction data into normalized fact_zone_attraction fact table using SQL."""
    
    def get_name(self) -> str:
        return "fact_zone_attraction"
    
    def get_target_table(self) -> str:
        return "fact_zone_attraction"
    
    def get_sql_query(self) -> str:
        """SQL query to transform bronze zones_attraction data with dual FK enrichment and deduplication."""
        return """
            WITH deduplicated_bronze AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY CODGEO, AAV2020 ORDER BY ingestion_timestamp DESC) AS rn
                FROM bronze_zones_attraction
                WHERE AAV2020 != '000' AND CODGEO IS NOT NULL
            ),
            cleaned_bronze AS (
                SELECT
                    CODGEO, LIBGEO, AAV2020,
                    REGEXP_REPLACE(LIBAAV2020, '(?i)\\\\s*\\\\(partie française\\\\)', '') AS LIBAAV2020_CLEAN,
                    CATEAAV2020, DEP, REG
                FROM deduplicated_bronze
                WHERE rn = 1
            ),
            name_cleaned AS (
                SELECT
                    CODGEO, LIBGEO, AAV2020,
                    CASE 
                        WHEN REPLACE(LIBAAV2020_CLEAN, 'œ', 'oe') ILIKE 'Hesdin%' THEN 'Hesdin'
                        WHEN REPLACE(LIBAAV2020_CLEAN, 'œ', 'oe') ILIKE 'Cugand%' THEN 'Cugand'
                        ELSE REPLACE(LIBAAV2020_CLEAN, 'œ', 'oe')
                    END AS LIBAAV2020,
                    CATEAAV2020, DEP, REG
                FROM cleaned_bronze
            ),
            name_cleaned_with_timestamp AS (
                SELECT
                    n.*,
                    d.ingestion_timestamp
                FROM name_cleaned n
                LEFT JOIN deduplicated_bronze d ON n.CODGEO = d.CODGEO AND d.rn = 1
            ),
            with_pole_match AS (
                SELECT 
                    b.CODGEO, b.AAV2020 AS CODEAAV,
                    c_pole.commune_insee_code AS CODGEOAAV,
                    b.LIBAAV2020,
                    b.CATEAAV2020 AS CATEAAV,
                    b.DEP, b.REG,
                    b.ingestion_timestamp
                FROM name_cleaned_with_timestamp b
                INNER JOIN silver_dim_commune c_pole
                    ON REGEXP_REPLACE(UPPER(b.LIBAAV2020), '[^A-Z0-9]', '') = 
                       REGEXP_REPLACE(UPPER(REPLACE(c_pole.commune_label, 'œ', 'oe')), '[^A-Z0-9]', '')
                WHERE b.CODGEO NOT LIKE CONCAT('%', c_pole.commune_insee_code, '%')
            )
            SELECT 
                MD5(CONCAT(z.CODGEO, z.CODEAAV, z.CODGEOAAV)) AS zone_attraction_sk,
                c1.commune_sk,
                c2.commune_sk AS commune_pole_sk,
                z.CODEAAV AS zone_attraction_code,
                z.LIBAAV2020 AS zone_attraction_label,
                z.CATEAAV AS zone_attraction_categorie,
                JSON_OBJECT(
                    'job_insert_id', 'fact_zone_attraction',
                    'job_insert_date_utc', CURRENT_TIMESTAMP,
                    'job_modify_id', 'fact_zone_attraction',
                    'job_modify_date_utc', CURRENT_TIMESTAMP,
                    'ingestion_timestamp', z.ingestion_timestamp
                ) AS job_metadata
            FROM with_pole_match z
            JOIN silver_dim_commune c1 ON z.CODGEO = c1.commune_insee_code
            JOIN silver_dim_commune c2 ON z.CODGEOAAV = c2.commune_insee_code
        """
