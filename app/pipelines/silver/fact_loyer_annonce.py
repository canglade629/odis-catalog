"""Silver pipeline for fact_loyer_annonce - Housing rental price announcements fact table (SQL-based)."""
from app.pipelines.silver.base_v2 import SQLSilverV2Pipeline
from app.core.pipeline_registry import register_pipeline
import logging

logger = logging.getLogger(__name__)


@register_pipeline(
    layer="silver",
    name="fact_loyer_annonce",
    dependencies=["bronze.logement", "silver.dim_commune", "silver.ref_logement_profil"],
    description_fr="Table de faits des loyers d'annonce au m² par commune avec bornes de prédiction, niveau d'observation et indicateurs de qualité. FK vers dim_commune et ref_logement_profil."
)
class FactLoyerAnnoncePipeline(SQLSilverV2Pipeline):
    """Transform logement data into normalized fact_loyer_annonce fact table using SQL."""
    
    def get_name(self) -> str:
        return "fact_loyer_annonce"
    
    def get_target_table(self) -> str:
        return "fact_loyer_annonce"
    
    def get_sql_query(self) -> str:
        """
        SQL query to transform bronze logement data.
        
        Uses source_filename and logement_profil_sk to link with ref_logement_profil table
        for housing type and typology information.
        """
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
                    -- Parse quality indicator columns (handle both old and new column names)
                    CAST(REPLACE(CAST(R2_adj AS VARCHAR), ',', '.') AS DOUBLE) AS r2adj_clean,
                    CAST(nbobs_mail AS INTEGER) AS nbobs_maille_clean,
                    CAST(nbobs_com AS INTEGER) AS nbobs_commune_clean
                FROM merged_data
            ),
            with_row_number AS (
                SELECT *,
                    -- For 2024 data with multiple segments, partition by commune + annee + source_id
                    -- For legacy data, just partition by commune
                    ROW_NUMBER() OVER (
                        PARTITION BY 
                            code_commune,
                            COALESCE(annee, 0),
                            COALESCE(source_id, '')
                        ORDER BY ingestion_timestamp DESC
                    ) AS rn
                FROM with_code_commune
            )
            SELECT
                c.commune_sk AS commune_sk,
                -- logement_profil_sk: MD5 hash of source_filename for FK to ref_logement_profil (positioned as 3rd column after commune_sk)
                CASE 
                    WHEN l.source_id IS NOT NULL THEN MD5(l.source_id)
                    ELSE NULL
                END AS logement_profil_sk,
                CAST(l.loypredm2_clean AS DECIMAL(10,2)) AS loyer_m2_moy,
                CAST(l.lwr_clean AS DECIMAL(10,2)) AS loyer_m2_min,
                CAST(l.upr_clean AS DECIMAL(10,2)) AS loyer_m2_max,
                COALESCE(CAST(l.TYPPRED AS VARCHAR), '') AS maille_observation,
                CAST(l.r2adj_clean AS DECIMAL(5,4)) AS score_qualite,
                l.nbobs_maille_clean AS nb_observation_maille,
                l.nbobs_commune_clean AS nb_observation_commune,
                JSON_OBJECT(
                    'job_insert_id', 'fact_loyer_annonce',
                    'job_insert_date_utc', CURRENT_TIMESTAMP,
                    'job_modify_id', 'fact_loyer_annonce',
                    'job_modify_date_utc', CURRENT_TIMESTAMP,
                    'ingestion_timestamp', l.ingestion_timestamp
                ) AS job_metadata
            FROM with_row_number l
            JOIN silver_dim_commune c ON l.code_commune = c.commune_insee_code
            WHERE rn = 1
              AND l.loypredm2_clean IS NOT NULL
              AND l.loypredm2_clean > 0
              AND l.lwr_clean IS NOT NULL
              AND l.upr_clean IS NOT NULL
              AND l.lwr_clean < l.upr_clean
        """
