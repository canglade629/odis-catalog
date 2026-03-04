#!/usr/bin/env python3
"""
Sync data catalogue from YAML file to PostgreSQL.

Reads config/data_catalogue.yaml and stores it in the data_catalogue table.
Run once after DB schema is applied, or to reset catalogue from YAML.
"""
import asyncio
import sys
import yaml
import logging
from pathlib import Path
from datetime import datetime, timezone

# Add project root for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_catalogue_yaml(catalogue_path: Path) -> dict:
    """Load the data catalogue YAML file."""
    if not catalogue_path.exists():
        raise FileNotFoundError(f"Catalogue file not found: {catalogue_path}")
    with open(catalogue_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


async def main():
    from app.core.config import get_settings
    from app.db.session import async_session_factory
    from app.db.repositories.catalogue import catalogue_repo

    possible_paths = [
        Path(__file__).resolve().parent.parent / "config" / "data_catalogue.yaml",
        Path("/app/config/data_catalogue.yaml"),
        Path("config/data_catalogue.yaml"),
    ]
    catalogue_path = None
    for p in possible_paths:
        if p.exists():
            catalogue_path = p
            break
    if not catalogue_path:
        logger.error("Catalogue file not found in any of: %s", possible_paths)
        return 1

    catalogue_data = load_catalogue_yaml(catalogue_path)
    sync_time = datetime.now(timezone.utc)
    document = {
        "tables": catalogue_data.get("tables", {}),
        "version": catalogue_data.get("version", "unknown"),
        "generated_at": catalogue_data.get("generated_at", ""),
        "last_synced": sync_time.isoformat(),
        "source_file": "data_catalogue.yaml",
    }

    try:
        get_settings()  # ensure config loaded
        factory = async_session_factory()
        async with factory() as session:
            await catalogue_repo.set(session, document)
            await session.commit()
        num = len(document.get("tables", {}))
        logger.info("Synced %d tables to PostgreSQL at %s", num, sync_time.isoformat())
        return 0
    except Exception as e:
        logger.error("Error syncing catalogue: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
