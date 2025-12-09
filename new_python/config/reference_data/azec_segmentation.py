"""
AZEC Product Segmentation Configuration.

Hardcoded segmentation mapping for 47 AZEC construction products.
Based on REF_segmentation_azec.sas L40-43.

This provides SEGMENT mapping until full LOB table is available.
TYPE_PRODUIT remains NULL until CONSTRCU/RISTECCU/Typrd_2 tables are loaded.
"""

from typing import Dict, Any

# =========================================================================
# AZEC PRODUCTS - HARDCODED SEGMENTATION
# =========================================================================
# Source: REF_segmentation_azec.sas L40-43
# Total: 47 products for Construction market (cmarch="6")

AZEC_PRODUCTS_SEGMENTATION: Dict[str, Dict[str, Any]] = {
    # Product codes from SAS
    "A00": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product A00"},
    "A01": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product A01"},
    "AA1": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product AA1"},
    "AB1": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product AB1"},
    "AG1": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product AG1"},
    "AG8": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product AG8"},
    "AR1": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product AR1"},
    "ING": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product ING"},
    "PG1": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product PG1"},
    "AA6": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product AA6"},
    "AG6": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product AG6"},
    "AR2": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product AR2"},
    "AU1": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product AU1"},
    "AA4": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product AA4"},
    "A03": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product A03"},
    "AA3": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product AA3"},
    "AG3": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product AG3"},
    "AM3": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product AM3"},
    "MA1": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MA1"},
    "MA0": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MA0"},
    "MA2": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MA2"},
    "MA3": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MA3"},
    "MA4": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MA4"},
    "MA5": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MA5"},
    "MT0": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MT0"},
    "MR0": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MR0"},
    "AAC": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product AAC"},
    "GAV": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product GAV"},
    "GNC": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product GNC"},
    "MB1": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MB1"},
    "MB2": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MB2"},
    "MB3": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MB3"},
    "MED": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MED"},
    "MH0": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MH0"},
    "MP0": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MP0"},
    "MP1": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MP1"},
    "MP2": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MP2"},
    "MP3": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MP3"},
    "MP4": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MP4"},
    "MPG": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MPG"},
    "MPP": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MPP"},
    "MI0": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MI0"},
    "MI1": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MI1"},
    "MI2": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MI2"},
    "MI3": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product MI3"},
    "PI2": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Product PI2"},
    "DO0": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Dommages Ouvrages"},  # FIXED: Changed D00 to DO0
    "TRC": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Tous Risques Chantiers"},
    "CTR": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Construction"},
    "CNR": {"segment": "Construction", "cmarch": "6", "cseg": "2", "description": "Construction"},
}


def get_azec_segment(produit: str) -> str:
    """
    Get segment for AZEC product code.
    
    Args:
        produit: AZEC product code (e.g., "AA1", "DO0", etc.)  # FIXED: Changed D00 to DO0
    
    Returns:
        Segment name or "Unknown" if product not found
    
    Example:
        >>> get_azec_segment("AA1")
        'Construction'
        >>> get_azec_segment("UNKNOWN")
        'Unknown'
    """
    mapping = AZEC_PRODUCTS_SEGMENTATION.get(produit, {})
    return mapping.get("segment", "Unknown")


def get_azec_market(produit: str) -> str:
    """
    Get market code for AZEC product.
    
    Args:
        produit: AZEC product code
    
    Returns:
        Market code (cmarch) or None
    """
    mapping = AZEC_PRODUCTS_SEGMENTATION.get(produit, {})
    return mapping.get("cmarch")


def is_construction_product(produit: str) -> bool:
    """
    Check if product is a construction product.
    
    Args:
        produit: AZEC product code
    
    Returns:
        True if construction product, False otherwise
    """
    return produit in AZEC_PRODUCTS_SEGMENTATION
