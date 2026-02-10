"""
Business transformation functions for Construction Data Pipeline.

Organized into three categories:
- base: Core utilities, generic functions, specialized transformations
- operations: Business logic for all domains (PTF_MVT, Capitaux, Emissions)
- enrichment: Data enrichment functions (AZEC, Client)
"""

# ============================================================================
# BASE TRANSFORMATIONS
# ============================================================================

from utils.transformations.base.column_operations import (
    lowercase_all_columns,
    apply_column_config,
)

from utils.transformations.base.generic_transforms import (
    apply_business_filters,
    apply_conditional_transform,
    apply_transformations,
)


from utils.transformations.base.isic_codification import (
    assign_isic_codes,
    add_partenariat_berlitz_flags,
)

# ============================================================================
# OPERATIONS — BUSINESS LOGIC
# ============================================================================

from utils.transformations.operations.business_logic import (
    extract_capitals,
    calculate_az_movements,       # AZ
    calculate_exposures,          # AZ
    calculate_azec_movements,     # AZEC
    calculate_exposures_azec,     # AZEC (nouveau)
    calculate_azec_suspension,    # AZEC
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

# ============================================================================
# ENRICHMENT (non ISIC, car ISIC a été déplacé dans base)
# ============================================================================

from utils.transformations.enrichment.client_enrichment import (
    join_client_data,
)

from utils.transformations.enrichment.segmentation_enrichment import enrich_segmentation

# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    # Base - Column operations
    'lowercase_all_columns',
    'apply_column_config',

    # Base - Generic transforms
    'apply_business_filters',
    'apply_conditional_transform',
    'apply_transformations',

    # Base - ISIC codification
    'assign_isic_codes',
    'add_partenariat_berlitz_flags',


    # Operations - PTF_MVT
    'extract_capitals',
    'calculate_az_movements',      # AZ
    'calculate_exposures',         # AZ
    'calculate_azec_movements',    # AZEC
    'calculate_exposures_azec',    # AZEC
    'calculate_azec_suspension',   # AZEC

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
    'enrich_segmentation'
]