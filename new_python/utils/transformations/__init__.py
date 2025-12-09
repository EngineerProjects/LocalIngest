"""
Business transformation functions for Construction Data Pipeline.

Organized into three categories:
- base: Core utilities, generic functions, specialized transformations
- operations: Business logic for all domains (PTF_MVT, Capitaux, Emissions)
- enrichment: Data enrichment functions (AZEC, Client)
"""

# Base transformations (core utilities + specialized)
from utils.transformations.base.column_operations import (
    lowercase_all_columns,
    apply_column_config,
    build_computed_expression,
    rename_columns,
)

from utils.transformations.base.generic_transforms import (
    apply_business_filters,
)

# Operations (business logic for all domains)
from utils.transformations.operations.business_logic import (
    extract_capitals,
    calculate_movements,
    calculate_exposures,
    calculate_azec_movements,
    calculate_azec_suspension,
)

from utils.transformations.operations.capital_operations import (
    extract_capitals_extended,
    normalize_capitals_to_100,
    apply_capitaux_business_rules,
    process_azec_capitals,
    aggregate_azec_pe_rd,
)

from utils.transformations.operations.indexation import (
    index_capitals,
    load_index_table,
)

from utils.transformations.operations.emissions_operations import (
    assign_distribution_channel,
    calculate_exercice_split,
    extract_guarantee_code,
    apply_emissions_filters,
    aggregate_by_policy_guarantee,
)

# Enrichment functions
from utils.transformations.enrichment.client_enrichment import (
    join_client_data,
)

__all__ = [
    # Base - Column operations
    'lowercase_all_columns',
    'apply_column_config',
    'build_computed_expression',
    'rename_columns',
    # Base - Generic transforms
    'apply_business_filters',
    # Operations - PTF_MVT business logic
    'extract_capitals',
    'calculate_movements',
    'calculate_exposures',
    'calculate_azec_movements',
    'calculate_azec_suspension',
    # Operations - Capitaux
    'extract_capitals_extended',
    'normalize_capitals_to_100',
    'apply_capitaux_business_rules',
    'process_azec_capitals',
    'aggregate_azec_pe_rd',
    'index_capitals',
    'load_index_table',
    # Operations - Emissions
    'assign_distribution_channel',
    'calculate_exercice_split',
    'extract_guarantee_code',
    'apply_emissions_filters',
    'aggregate_by_policy_guarantee',
    # Enrichment
    'join_client_data',
]

