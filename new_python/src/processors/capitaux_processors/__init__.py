"""
Capitaux Processors Package.

Contains processors for capital data extraction and consolidation:
- AZCapitauxProcessor: Process AZ channel capital data
- AZECCapitauxProcessor: Process AZEC channel capital data
- CapitauxConsolidationProcessor: Consolidate AZ + AZEC capital data
"""

from .az_capitaux_processor import AZCapitauxProcessor
from .azec_capitaux_processor import AZECCapitauxProcessor
from .consolidation_processor import CapitauxConsolidationProcessor

__all__ = [
    "AZCapitauxProcessor",
    "AZECCapitauxProcessor",
    "CapitauxConsolidationProcessor",
]
