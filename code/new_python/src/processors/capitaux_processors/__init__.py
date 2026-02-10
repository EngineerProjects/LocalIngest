"""
Package des processeurs de Capitaux.

Contient les processeurs pour l'extraction et la consolidation des données de capitaux :
- AZCapitauxProcessor : Traite les données capitaux du canal AZ
- AZECCapitauxProcessor : Traite les données capitaux du canal AZEC
- CapitauxConsolidationProcessor : Consolide les données capitaux AZ + AZEC
"""

from .az_capitaux_processor import AZCapitauxProcessor
from .azec_capitaux_processor import AZECCapitauxProcessor
from .consolidation_processor import CapitauxConsolidationProcessor

__all__ = [
    "AZCapitauxProcessor",
    "AZECCapitauxProcessor",
    "CapitauxConsolidationProcessor",
]
