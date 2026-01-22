"""
Enrichment transformations module.

Contains utilities for data enrichment operations:
- ISIC code enrichment (via base.isic_codification)
- DESTINAT (construction site destination) enrichment
- Client data enrichment
- Segmentation enrichment
"""

# ISIC codification is now in base module (isic_enrichment was removed as duplicate)
from utils.transformations.base.isic_codification import (
    assign_isic_codes
)

from .destinat_enrichment import (
    apply_destinat_consolidation_logic
)

__all__ = [
    # ISIC (from base.isic_codification)
    'assign_isic_codes',
    'apply_destinat_consolidation_logic'
]
