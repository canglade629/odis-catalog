"""
Validate silver layer tables: row counts, surrogate keys, metadata, and foreign keys.

Uses MigrationValidator to run checks on the current silver layer only
(no comparison with another layer). Run after pipelines to verify data quality.
"""
import sys
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from app.utils.migration_validator import MigrationValidator
from app.core.config_loader import get_config_loader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Surrogate key and required columns per silver table (subset used for validation)
SILVER_VALIDATIONS = {
    "dim_commune": {
        "key": "commune_sk",
        "required_cols": ["commune_sk", "commune_insee_code", "commune_label", "departement_code"],
    },
    "dim_accueillant": {
        "key": "accueillant_sk",
        "required_cols": ["accueillant_sk"],
    },
    "dim_gare": {
        "key": "gare_sk",
        "required_cols": ["gare_sk"],
    },
    "dim_gare_segment": {
        "key": "gare_segment_sk",
        "required_cols": ["gare_segment_sk"],
    },
    "dim_ligne": {
        "key": "ligne_sk",
        "required_cols": ["ligne_sk"],
    },
    "dim_siae_structure": {
        "key": "siae_structure_sk",
        "required_cols": ["siae_structure_sk"],
    },
    "ref_logement_profil": {
        "key": "logement_profil_sk",
        "required_cols": ["logement_profil_sk"],
    },
    "fact_loyer_annonce": {
        "key": "row_sk",
        "required_cols": ["row_sk"],
    },
    "fact_zone_attraction": {
        "key": "zone_attraction_sk",
        "required_cols": ["zone_attraction_sk"],
    },
    "fact_siae_poste": {
        "key": "siae_poste_sk",
        "required_cols": ["siae_poste_sk"],
    },
}


def main():
    """Run validation on silver tables that exist in config."""
    logger.info("=" * 80)
    logger.info("Silver layer validation")
    logger.info("=" * 80)

    config_loader = get_config_loader()
    silver_configs = config_loader.load_layer_config("silver")
    silver_tables = [c.name for c in silver_configs]

    validator = MigrationValidator()

    for table in silver_tables:
        spec = SILVER_VALIDATIONS.get(table)
        if not spec:
            logger.info("Skipping %s (no validation spec)", table)
            continue
        logger.info("Validating %s...", table)
        validator.validate_unique_key(table, spec["key"], layer="silver")
        validator.validate_no_nulls(table, spec["required_cols"], layer="silver")
        validator.validate_metadata_columns(table, layer="silver")

    # FK checks for a few key tables
    logger.info("Validating foreign keys...")
    validator.validate_foreign_keys(
        "fact_loyer_annonce", "commune_sk", "dim_commune", "commune_sk",
        fact_layer="silver", dim_layer="silver",
    )
    validator.validate_foreign_keys(
        "fact_siae_poste", "siae_structure_sk", "dim_siae_structure", "siae_structure_sk",
        fact_layer="silver", dim_layer="silver",
    )

    validator.print_report()

    passed = sum(1 for r in validator.results if r.passed)
    total = len(validator.results)
    if passed == total:
        logger.info("All %s validations passed.", total)
        return 0
    logger.error("%s/%s validations failed.", total - passed, total)
    return 1


if __name__ == "__main__":
    sys.exit(main())
