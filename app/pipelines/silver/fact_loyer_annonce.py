"""Silver pipeline for fact_loyer_annonce - Housing rental price announcements fact table (SQL-based)."""
from app.pipelines.silver.base_v2 import SQLSilverV2Pipeline
from app.core.pipeline_registry import register_pipeline
import logging

logger = logging.getLogger(__name__)


@register_pipeline(
    layer="silver",
    name="fact_loyer_annonce",
    dependencies=["bronze.logement", "silver.dim_commune"],
    description_fr="Table de faits des loyers d'annonce au m² par commune avec bornes de prédiction, niveau d'observation et indicateurs de qualité. FK vers dim_commune. Inclut les données 2024 avec segmentation par type de bien et typologie."
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
        
        Handles both legacy data and 2024 data with new segmentation columns:
        - annee: Year of the data (2024 for new data)
        - type_bien: Type of housing (appartement, maison)
        - segment_typologie: Typology segment (toutes typologies, T1 et T2, T3 et plus)
        - surface_ref: Reference surface in m²
        - surface_piece_moy: Average surface per room in m²
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
                    -- For 2024 data with multiple segments, partition by commune + type_bien + segment_typologie
                    -- For legacy data, just partition by commune
                    ROW_NUMBER() OVER (
                        PARTITION BY 
                            code_commune,
                            COALESCE(annee, 0),
                            COALESCE(type_bien, ''),
                            COALESCE(segment_typologie, '')
                        ORDER BY ingestion_timestamp DESC
                    ) AS rn
                FROM with_code_commune
            )
            SELECT
                -- For 2024 data, include segmentation in the surrogate key
                CASE 
                    WHEN l.annee IS NOT NULL THEN 
                        MD5(CONCAT(
                            l.code_commune, '|',
                            CAST(l.annee AS VARCHAR), '|',
                            COALESCE(l.type_bien, ''), '|',
                            COALESCE(l.segment_typologie, '')
                        ))
                    ELSE MD5(l.code_commune)
                END AS row_sk,
                c.commune_sk AS commune_sk,
                CAST(l.loypredm2_clean AS DECIMAL(10,2)) AS loyer_m2_moy,
                CAST(l.lwr_clean AS DECIMAL(10,2)) AS loyer_m2_min,
                CAST(l.upr_clean AS DECIMAL(10,2)) AS loyer_m2_max,
                COALESCE(CAST(l.TYPPRED AS VARCHAR), '') AS maille_observation,
                CAST(l.r2adj_clean AS DECIMAL(5,4)) AS score_qualite,
                l.nbobs_maille_clean AS nb_observation_maille,
                l.nbobs_commune_clean AS nb_observation_commune,
                -- New 2024 columns
                CAST(l.annee AS INTEGER) AS annee,
                CAST(l.type_bien AS VARCHAR) AS type_bien,
                CAST(l.segment_typologie AS VARCHAR) AS segment_typologie,
                CAST(l.surface_ref AS DECIMAL(10,2)) AS surface_ref,
                CAST(l.surface_piece_moy AS DECIMAL(10,2)) AS surface_piece_moy,
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
