"""Silver pipeline for dim_siae_structure - SIAE structures dimension (SQL-based)."""
from app.pipelines.silver.base_v2 import SQLSilverV2Pipeline
from app.core.pipeline_registry import register_pipeline
import logging

logger = logging.getLogger(__name__)


@register_pipeline(
    layer="silver",
    name="dim_siae_structure",
    dependencies=["bronze.siae_structures", "bronze.geo", "silver.dim_commune"],
    description_fr="Table de dimension des structures d'insertion par l'activité économique (SIAE) avec enrichissement géographique (FK vers dim_commune) et informations de contact."
)
class DimSIAEStructurePipeline(SQLSilverV2Pipeline):
    """Transform SIAE structures data into normalized dim_siae_structure dimension table using SQL."""
    
    def get_name(self) -> str:
        return "dim_siae_structure"
    
    def get_target_table(self) -> str:
        return "dim_siae_structure"
    
    def get_sql_query(self) -> str:
        """SQL query to transform bronze SIAE structures data with geographic enrichment."""
        return """
            WITH with_commune_sk AS (
                SELECT s.*, c.commune_sk
                FROM bronze_siae_structures s
                LEFT JOIN silver_dim_commune c ON s.code_insee = c.commune_insee_code
            ),
            deduplicated AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY siret 
                        ORDER BY CASE WHEN commune_sk IS NOT NULL THEN 0 ELSE 1 END, 
                                 date_maj DESC
                    ) AS rn
                FROM with_commune_sk
                WHERE siret IS NOT NULL 
                  AND LENGTH(REGEXP_REPLACE(siret, '[^0-9]', '')) = 14
            )
            SELECT 
                MD5(id) AS siae_structure_sk,
                id AS siae_structure_bk,
                siret AS siae_structure_siret_code,
                nom AS siae_structure_label,
                description AS siae_structure_description,
                MD5(commune) AS commune_sk,
                code_postal,
                adresse,
                complement_adresse,
                longitude,
                latitude,
                COALESCE(telephone, '') AS telephone,
                COALESCE(courriel, '') AS courriel,
                COALESCE(site_web, '') AS site_web,
                JSON_OBJECT(
                    'job_insert_id', 'silver_siae_structures',
                    'job_insert_date_utc', CURRENT_TIMESTAMP,
                    'job_modify_id', 'silver_siae_structures',
                    'job_modify_date_utc', CURRENT_TIMESTAMP
                ) AS job_metadata
            FROM deduplicated
            WHERE rn = 1
        """
