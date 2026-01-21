"""
Enrichment transformations module.

Contains utilities for data enrichment operations:
- ISIC code enrichment
- DESTINAT (construction site destination) enrichment
- Client data enrichment
- Segmentation enrichment
"""

# from .risk_enrichment import enrich_with_risk_data  # Module not yet implemented
from .isic_enrichment import (
    join_isic_reference_tables,
    assign_isic_codes,
    apply_isic_corrections,
    add_partenariat_berlitz_flags
)
from .destinat_enrichment import (
    calculate_destinat,
    apply_destinat_consolidation_logic
)

__all__ = [
    'join_isic_reference_tables',
    'assign_isic_codes',
    'apply_isic_corrections',
    'add_partenariat_berlitz_flags',
    'calculate_destinat',
    'apply_destinat_consolidation_logic'
]
