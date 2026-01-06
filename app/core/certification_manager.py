"""Table Certification Manager for controlling access to Silver tables."""
from google.cloud import firestore
from datetime import datetime
from typing import Optional, Dict, List


CERTIFICATION_COLLECTION = "table_certifications"


async def certify_table(
    layer: str,
    table_name: str,
    admin_id: str,
    db: firestore.AsyncClient
) -> Dict:
    """
    Certify a table for public use.
    
    Args:
        layer: The data layer (e.g., 'silver')
        table_name: Name of the table to certify
        admin_id: ID of the admin certifying the table
        db: Firestore async client
        
    Returns:
        Dictionary with certification details
    """
    doc_id = f"{layer}_{table_name}"
    
    certification_data = {
        "layer": layer,
        "table_name": table_name,
        "certified": True,
        "certified_at": datetime.utcnow(),
        "certified_by": admin_id
    }
    
    # Set or update the certification document
    doc_ref = db.collection(CERTIFICATION_COLLECTION).document(doc_id)
    await doc_ref.set(certification_data)
    
    return {
        "layer": layer,
        "table_name": table_name,
        "certified": True,
        "certified_at": certification_data["certified_at"].isoformat(),
        "certified_by": admin_id
    }


async def uncertify_table(
    layer: str,
    table_name: str,
    db: firestore.AsyncClient
) -> bool:
    """
    Remove certification from a table.
    
    Args:
        layer: The data layer (e.g., 'silver')
        table_name: Name of the table to uncertify
        db: Firestore async client
        
    Returns:
        True if uncertified successfully
    """
    doc_id = f"{layer}_{table_name}"
    doc_ref = db.collection(CERTIFICATION_COLLECTION).document(doc_id)
    
    # Delete the certification document
    await doc_ref.delete()
    
    return True


async def is_table_certified(
    layer: str,
    table_name: str,
    db: firestore.AsyncClient
) -> bool:
    """
    Check if a table is certified.
    
    Args:
        layer: The data layer (e.g., 'silver')
        table_name: Name of the table to check
        db: Firestore async client
        
    Returns:
        True if table is certified, False otherwise
    """
    doc_id = f"{layer}_{table_name}"
    doc_ref = db.collection(CERTIFICATION_COLLECTION).document(doc_id)
    
    doc = await doc_ref.get()
    
    if not doc.exists:
        return False
    
    data = doc.to_dict()
    return data.get("certified", False)


async def get_certification_status(
    layer: str,
    table_name: str,
    db: firestore.AsyncClient
) -> Optional[Dict]:
    """
    Get detailed certification status for a table.
    
    Args:
        layer: The data layer (e.g., 'silver')
        table_name: Name of the table
        db: Firestore async client
        
    Returns:
        Dictionary with certification details or None if not certified
    """
    doc_id = f"{layer}_{table_name}"
    doc_ref = db.collection(CERTIFICATION_COLLECTION).document(doc_id)
    
    doc = await doc_ref.get()
    
    if not doc.exists:
        return None
    
    data = doc.to_dict()
    
    # Convert timestamp to ISO format if present
    if "certified_at" in data and data["certified_at"]:
        data["certified_at"] = data["certified_at"].isoformat()
    
    return data


async def get_all_certifications(db: firestore.AsyncClient) -> List[Dict]:
    """
    Get all table certifications.
    
    Args:
        db: Firestore async client
        
    Returns:
        List of certification dictionaries
    """
    certifications = []
    
    # Query all certification documents
    docs = db.collection(CERTIFICATION_COLLECTION).stream()
    
    async for doc in docs:
        data = doc.to_dict()
        
        # Convert timestamp to ISO format if present
        if "certified_at" in data and data["certified_at"]:
            data["certified_at"] = data["certified_at"].isoformat()
        
        certifications.append(data)
    
    return certifications


