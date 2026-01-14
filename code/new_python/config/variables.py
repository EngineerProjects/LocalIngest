"""
Column transformation configurations for Construction Data Pipeline.

IMPORTANT: Most configurations have been migrated to JSON files in config/transformations/.
This file now contains only essential constants and imports for backward compatibility.

For new configurations, use:
- config/transformations/az_transformations.json - AZ transformation configs
- config/transformations/azec_transformations.json - AZEC transformation configs
- config/transformations/consolidation_mappings.json - Consolidation harmonization
- config/transformations/business_rules.json - Business rules and filters

To load these configs:
    from utils.loaders import get_default_loader
    loader = get_default_loader()
    az_config = loader.get_az_config()
"""

# =========================================================================
# BACKWARD COMPATIBILITY IMPORTS
# =========================================================================
# These are provided for backward compatibility with existing code.

try:
    from utils.loaders import get_default_loader
    _loader = get_default_loader()

    # Legacy variable names pointing to JSON configs
    AZ_COLUMN_CONFIG = _loader.get_az_config()['column_selection']
    CAPITAL_EXTRACTION_CONFIG = _loader.get_az_config()['capital_extraction']
    # COASSURANCE_CONFIG removed - now in az/azec_transformations.json computed_fields
    REVISION_CRITERIA_CONFIG = {'critere_revision': _loader.get_az_config()['revision_criteria']}
    BUSINESS_FILTERS_CONFIG = _loader.get_business_rules()['business_filters']

    AZEC_COLUMN_CONFIG = _loader.get_azec_config()['column_selection']
    AZEC_CAPITAL_MAPPING = _loader.get_azec_config()['capital_mapping']['mappings']
    AZEC_PRODUIT_LIST = _loader.get_azec_config()['product_list']['products']
    AZEC_MIGRATION_CONFIG = _loader.get_azec_config()['migration_handling']
    AZEC_DATE_STATE_UPDATES = _loader.get_azec_config()['date_state_updates']['updates']
    AZEC_MOVEMENT_CONFIG = _loader.get_azec_config()['movement_calculation']
    AZEC_SUSPENSION_CALC = _loader.get_azec_config()['suspension_calculation']

    CONSOLIDATION_AZ_HARMONIZATION = _loader.get_consolidation_config()['az_harmonization']
    CONSOLIDATION_AZEC_HARMONIZATION = _loader.get_consolidation_config()['azec_harmonization']

    MOVEMENT_COLUMN_MAPPING = _loader.get_consolidation_config()['movement_column_mapping']
    EXPOSURE_COLUMN_MAPPING = _loader.get_consolidation_config()['exposure_column_mapping']

except Exception as e:
    # Fallback if JSON configs are not available (e.g., during tests)
    import warnings
    warnings.warn(f"Could not load JSON configs: {e}. Using empty fallback configs.")

    AZ_COLUMN_CONFIG = {'passthrough': [], 'rename': {}, 'computed': {}, 'metadata': {}}
    CAPITAL_EXTRACTION_CONFIG = []
    # COASSURANCE_CONFIG removed - now in computed_fields
    REVISION_CRITERIA_CONFIG = {'critere_revision': {'mapping': {}, 'default': ''}}
    BUSINESS_FILTERS_CONFIG = {'az': {'filters': []}, 'azec': {'filters': []}}

    AZEC_COLUMN_CONFIG = {'passthrough': [], 'rename': {}, 'computed': {}, 'metadata': {}}
    AZEC_CAPITAL_MAPPING = []
    AZEC_PRODUIT_LIST = []
    AZEC_MIGRATION_CONFIG = {'vision_threshold': 202009, 'transformations': []}
    AZEC_DATE_STATE_UPDATES = []
    AZEC_MOVEMENT_CONFIG = {'nbafn': {}, 'nbres': {}, 'nbptf': {}}
    AZEC_SUSPENSION_CALC = {'column': 'nbj_susp_ytd', 'type': 'conditional', 'conditions': [], 'default': 0}

    CONSOLIDATION_AZ_HARMONIZATION = {'rename': {}, 'computed': {}}
    CONSOLIDATION_AZEC_HARMONIZATION = {'rename': {}, 'computed': {}}

    MOVEMENT_COLUMN_MAPPING = {'az': {}, 'azec': {}}
    EXPOSURE_COLUMN_MAPPING = {'az': {}, 'azec': {}}
