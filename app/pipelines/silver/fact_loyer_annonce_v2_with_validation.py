"""Silver pipeline for fact_loyer_annonce - Housing rental price announcements fact table (SQL-based)."""
from app.pipelines.silver.base_v2 import SQLSilverV2Pipeline
from app.core.pipeline_registry import register_pipeline
import logging

logger = logging.getLogger(__name__)


@register_pipeline(
    layer="silver",
    name="fact_loyer_annonce",
    dependencies=["bronze.logement", "silver.dim_commune"],
    description_fr="Table de faits des loyers d'annonce au m² par commune avec bornes de prédiction, niveau d'observation et indicateurs de qualité. FK vers dim_commune."
)
class FactLoyerAnnoncePipeline(SQLSilverV2Pipeline):
    """Transform logement data into normalized fact_loyer_annonce fact table using SQL."""
    
    def get_name(self) -> str:
        return "fact_loyer_annonce"
    
    def get_target_table(self) -> str:
        return "fact_loyer_annonce"
    
    def get_sql_query(self) -> str:
        """SQL query to transform bronze logement data - FULLY NORMALIZED (no lib_* columns)."""
        return """
            WITH merged_data AS (
                SELECT *, LPAD(CAST(INSEE_C AS VARCHAR), 5, '0') AS INSEE_C_MERGED
                FROM bronze_logement
            ),
            with_code_commune AS (
                SELECT *,
                    CASE
                        WHEN INSEE_C_MERGED LIKE '132%' THEN '13055'
                        WHEN INSEE_C_MERGED LIKE '693%' THEN '69123'
                        WHEN INSEE_C_MERGED LIKE '751%' THEN '75056'
                        ELSE INSEE_C_MERGED
                    END AS code_commune,
                    -- Replace comma with period for decimal parsing
                    CAST(REPLACE(CAST(loypredm2 AS VARCHAR), ',', '.') AS DOUBLE) AS loypredm2_clean,
                    CAST(REPLACE(CAST("lwr.IPm2" AS VARCHAR), ',', '.') AS DOUBLE) AS lwr_clean,
                    CAST(REPLACE(CAST("upr.IPm2" AS VARCHAR), ',', '.') AS DOUBLE) AS upr_clean,
                    -- Parse new quality indicator columns with NULL safety
                    -- These columns may not exist in all data sources (e.g., 2023 vs 2018 data)
                    TRY_CAST(REPLACE(CAST(R2adj AS VARCHAR), ',', '.') AS DOUBLE) AS r2adj_clean,
                    TRY_CAST(NBobs_maille AS INTEGER) AS nbobs_maille_clean,
                    TRY_CAST(NBobs_commune AS INTEGER) AS nbobs_commune_clean
                FROM merged_data
            ),
            with_row_number AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY code_commune ORDER BY ingestion_timestamp DESC) AS rn
                FROM with_code_commune
            )
            SELECT
                MD5(l.code_commune) AS row_sk,
                c.commune_sk AS commune_sk,
                CAST(l.loypredm2_clean AS DECIMAL(10,2)) AS loyer_m2_moy,
                CAST(l.lwr_clean AS DECIMAL(10,2)) AS loyer_m2_min,
                CAST(l.upr_clean AS DECIMAL(10,2)) AS loyer_m2_max,
                COALESCE(CAST(l.TYPPRED AS VARCHAR), '') AS maille_observation,
                -- Quality indicators (nullable - may not be available in all data sources)
                CASE 
                    WHEN l.r2adj_clean IS NOT NULL AND l.r2adj_clean >= 0 AND l.r2adj_clean <= 1
                    THEN CAST(l.r2adj_clean AS DECIMAL(5,4))
                    ELSE NULL
                END AS score_qualite,
                CASE 
                    WHEN l.nbobs_maille_clean IS NOT NULL AND l.nbobs_maille_clean >= 0
                    THEN l.nbobs_maille_clean
                    ELSE NULL
                END AS nb_observation_maille,
                CASE 
                    WHEN l.nbobs_commune_clean IS NOT NULL AND l.nbobs_commune_clean >= 0
                    THEN l.nbobs_commune_clean
                    ELSE NULL
                END AS nb_observation_commune,
                'fact_loyer_annonce' AS job_insert_id,
                CURRENT_TIMESTAMP AS job_insert_date_utc,
                'fact_loyer_annonce' AS job_modify_id,
                CURRENT_TIMESTAMP AS job_modify_date_utc
            FROM with_row_number l
            JOIN silver_dim_commune c ON l.code_commune = c.commune_code
            WHERE rn = 1
              AND l.loypredm2_clean IS NOT NULL
              AND l.loypredm2_clean > 0
              AND l.lwr_clean IS NOT NULL
              AND l.upr_clean IS NOT NULL
              AND l.lwr_clean < l.upr_clean
        """

