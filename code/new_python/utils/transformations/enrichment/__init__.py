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
    assign_isic_codes,
    join_isic_sui,
    map_naf_to_isic,
    join_isic_const_act,
    compute_destination_isic,
    join_isic_const_cht,
    finalize_isic_codes,
    join_isic_hazard_grades,
    drop_isic_temp_columns
)

from .destinat_enrichment import (
    calculate_destinat,
    apply_destinat_consolidation_logic
)

__all__ = [
    # ISIC (from base.isic_codification)
    'assign_isic_codes',
    'calculate_destinat',
    'apply_destinat_consolidation_logic'
]
