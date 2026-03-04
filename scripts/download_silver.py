#!/usr/bin/env python3
"""
Download all silver layer tables from S3 (Scaleway) to local CSV or Parquet files.

Requires SCW_* env vars (or app config) to read from Scaleway S3.
Run from project root: python scripts/download_silver.py

Output: downloads/silver/<table_name>.csv (default; override with --output-dir).
"""
import argparse
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
import os
os.chdir(project_root)

from app.core.config import get_settings
from app.core.config_loader import get_config_loader
from app.utils.delta_ops import DeltaOperations
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Download silver tables from S3 (Scaleway) to local CSV/Parquet.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=project_root / "downloads" / "silver",
        help="Directory to write CSV files (default: downloads/silver)",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "parquet"],
        default="csv",
        help="Output format (default: csv)",
    )
    args = parser.parse_args()

    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Output directory: %s", out_dir)

    settings = get_settings()
    config_loader = get_config_loader()
    silver_configs = config_loader.load_layer_config("silver")

    if not silver_configs:
        logger.warning("No silver tables in config; using default list.")
        silver_tables = [
            "dim_commune", "dim_accueillant", "dim_gare_segment", "dim_gare", "dim_ligne",
            "dim_siae_structure", "ref_logement_profil", "fact_loyer_annonce",
            "fact_zone_attraction", "fact_siae_poste",
        ]
    else:
        silver_tables = [c.name for c in silver_configs]

    for table_name in silver_tables:
        path = settings.get_silver_path(table_name)
        try:
            if path.rstrip("/").endswith(".parquet"):
                df = DeltaOperations.read_parquet(path)
            else:
                df = DeltaOperations.read_delta(path)
            ext = "csv" if args.format == "csv" else "parquet"
            out_file = out_dir / f"{table_name}.{ext}"
            if args.format == "csv":
                df.to_csv(out_file, index=False, encoding="utf-8")
            else:
                df.to_parquet(out_file, index=False)
            logger.info("Downloaded %s: %s rows -> %s", table_name, len(df), out_file)
        except Exception as e:
            logger.error("Failed %s: %s", table_name, e)

    logger.info("Done. Files in %s", out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
