#!/usr/bin/env python3
"""
Keep only raw files that are actually used by bronze pipelines.
Removes everything else from raw_download (or given dir).

Pipeline usage (from app/pipelines/bronze):
- accueillants: raw/accueillants/* (any file; typically one xlsx)
- geo: raw/geo/* (communes CSV)
- zones_attraction: raw/zones_attraction/* (xlsx)
- logement: raw/logement/* (all 4 pred-* CSVs)
- gares: raw/transport/gares/* (CSV; NOT raw/api/gares)
- lignes: raw/api/lignes/* (latest JSON only - cache for API)
- siae_postes: raw/api/siae_postes/* (latest JSON only)
- siae_structures: raw/api/siae_structures/* (CSV)
"""
from pathlib import Path
import os

# Directory to prune (default: raw_download)
RAW_DIR = Path(os.environ.get("RAW_DOWNLOAD_DIR", "raw_download"))

# Files to KEEP (relative to RAW_DIR, with raw/ prefix as in download)
# For api/lignes and api/siae_postes we keep only the latest (pipeline uses sorted(files)[-1])
KEEP_FILES = {
    "raw/accueillants/accueillants.xlsx",
    "raw/api/siae_structures/structures-inclusion-2025-12-01.csv",
    "raw/geo/communes-france-2025.csv",
    "raw/logement/pred-app-mef-dhup.csv",
    "raw/logement/pred-app12-mef-dhup.csv",
    "raw/logement/pred-app3-mef-dhup.csv",
    "raw/logement/pred-mai-mef-dhup.csv",
    "raw/transport/gares/gares-de-voyageurs.csv",
    "raw/zones_attraction/AAV2020_au_01-01-2025.xlsx",
    # Latest only for API cache (pipeline uses sorted(...)[-1])
    "raw/api/lignes/lignes_20260106_103712.json",
    "raw/api/siae_postes/siae_postes_20251203_161228.json",
}


def prune_raw_dir(base: Path) -> tuple[int, int]:
    """Remove files not in KEEP_FILES. Returns (deleted_count, kept_count)."""
    base = base.resolve()
    if not base.is_dir():
        raise SystemExit(f"Not a directory: {base}")
    deleted = 0
    kept = 0
    for root, _dirs, files in os.walk(base, topdown=False):
        for f in files:
            path = Path(root) / f
            rel = path.relative_to(base)
            key = str(rel).replace("\\", "/")
            if key in KEEP_FILES:
                kept += 1
                continue
            path.unlink()
            deleted += 1
            print(f"Removed {key}")
    # Remove empty directories
    for root, dirs, _files in os.walk(base, topdown=False):
        for d in dirs:
            p = Path(root) / d
            if p.is_dir() and not any(p.iterdir()):
                p.rmdir()
                print(f"Removed empty dir {p.relative_to(base)}")
    return deleted, kept


if __name__ == "__main__":
    deleted, kept = prune_raw_dir(RAW_DIR)
    print(f"Done: kept {kept} files, removed {deleted} files.")
