"""
Emissions Processors Package.

Contains processors for premium emissions data from One BI:
- EmissionsProcessor: Main processor for One BI premium data (Bronze → Silver → Gold)
"""

from src.processors.emissions_processors.emissions_processor import EmissionsProcessor

__all__ = [
    'EmissionsProcessor',
]
