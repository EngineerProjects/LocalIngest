"""
Package des processeurs d'émissions.

Contient les processeurs pour les données de primes issues de One BI :
- EmissionsProcessor : Processeur principal pour les données de primes One BI (Bronze → Silver → Gold)
"""

from src.processors.emissions_processors.emissions_processor import EmissionsProcessor

__all__ = [
    'EmissionsProcessor',
]
