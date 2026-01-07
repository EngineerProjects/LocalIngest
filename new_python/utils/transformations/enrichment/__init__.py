"""
Enrichment transformations module.

Contains utilities for data enrichment operations:
- RISK data enrichment (Q45, Q46, QAN)
- ISIC code enrichment
- DESTINAT (construction site destination) enrichment
- Client data enrichment
"""

from .risk_enrichment import enrich_with_risk_data
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
    'enrich_with_risk_data',
    'join_isic_reference_tables',
    'assign_isic_codes',
    'apply_isic_corrections',
    'add_partenariat_berlitz_flags',
    'calculate_destinat',
    'apply_destinat_consolidation_logic'
]
