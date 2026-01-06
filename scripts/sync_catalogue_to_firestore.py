#!/usr/bin/env python3
"""
Sync data catalogue from YAML file to Firestore.

This script reads config/data_catalogue.yaml and stores it in Firestore
for fast access by the API. It can be run standalone or imported.
"""

import sys
import yaml
import logging
from pathlib import Path
from datetime import datetime, timezone
from google.cloud import firestore

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_catalogue_yaml(catalogue_path: Path) -> dict:
    """Load the data catalogue YAML file."""
    if not catalogue_path.exists():
        raise FileNotFoundError(f"Catalogue file not found: {catalogue_path}")
    
    logger.info(f"Loading catalogue from {catalogue_path}")
    with open(catalogue_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def sync_catalogue_to_firestore(catalogue_data: dict, db: firestore.Client) -> dict:
    """
    Sync catalogue data to Firestore.
    
    Args:
        catalogue_data: Parsed YAML data
        db: Firestore client
    
    Returns:
        Sync status dict with counts and timestamp
    """
    collection_ref = db.collection('data_catalogue')
    doc_ref = collection_ref.document('silver_tables')
    
    # Prepare document with metadata
    sync_time = datetime.now(timezone.utc)
    
    firestore_doc = {
        'tables': catalogue_data.get('tables', {}),
        'version': catalogue_data.get('version', 'unknown'),
        'generated_at': catalogue_data.get('generated_at', ''),
        'last_synced': sync_time,
        'source_file': 'data_catalogue.yaml'
    }
    
    # Write to Firestore
    doc_ref.set(firestore_doc)
    
    num_tables = len(catalogue_data.get('tables', {}))
    
    logger.info(f"✅ Synced {num_tables} tables to Firestore at {sync_time.isoformat()}")
    
    return {
        'status': 'success',
        'tables_synced': num_tables,
        'last_synced': sync_time.isoformat(),
        'version': catalogue_data.get('version', 'unknown')
    }


def main():
    """Main entry point for command-line execution."""
    try:
        # Find catalogue file
        possible_paths = [
            Path(__file__).parent.parent / "config" / "data_catalogue.yaml",
            Path("/app/config/data_catalogue.yaml"),
            Path("config/data_catalogue.yaml"),
        ]
        
        catalogue_path = None
        for path in possible_paths:
            if path.exists():
                catalogue_path = path
                break
        
        if not catalogue_path:
            logger.error(f"❌ Catalogue file not found in any of: {[str(p) for p in possible_paths]}")
            sys.exit(1)
        
        # Load YAML
        catalogue_data = load_catalogue_yaml(catalogue_path)
        
        # Initialize Firestore client
        db = firestore.Client()
        
        # Sync to Firestore
        result = sync_catalogue_to_firestore(catalogue_data, db)
        
        logger.info(f"🎉 Catalogue sync complete: {result}")
        return 0
    
    except Exception as e:
        logger.error(f"❌ Error syncing catalogue: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())


