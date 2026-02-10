"""
Module de transformations d'enrichissement.

Contient des utilitaires pour les opérations d'enrichissement de données :
- Code ISIC (via base.isic_codification)
- DESTINAT (destination du chantier)
- Données client (CLIACT)
- Segmentation (SEGMENTPRDT, CONSTRCU)
- Risque (IRD, Q45, Q46)
"""

# La codification ISIC est maintenant dans le module base
from utils.transformations.base.isic_codification import (
    assign_isic_codes
)

from .destinat_enrichment import (
    apply_destinat_consolidation_logic
)

from .client_enrichment import (
    join_client_data
)

from .segmentation_enrichment import (
    enrich_segmentation
)

from .risk_enrichment import (
    enrich_with_risk_data
)

__all__ = [
    # ISIC
    'assign_isic_codes',
    
    # Destinat
    'apply_destinat_consolidation_logic',
    
    # Client
    'join_client_data',
    
    # Segmentation
    'enrich_segmentation',

    # Risk
    'enrich_with_risk_data'
]
