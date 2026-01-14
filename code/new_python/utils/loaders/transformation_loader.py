"""
Transformation Configuration Loader.

Loads and validates JSON transformation configurations for AZ, AZEC, and Consolidation processors.
Provides a unified interface for accessing transformation rules.
"""

import json
from pathlib import Path
from typing import Dict, Any, Optional, List


class TransformationLoader:
    """
    Load and validate transformation configurations from JSON files.

    This class provides a centralized way to load transformation configs
    that were previously hardcoded in variables.py.

    Example:
        >>> loader = TransformationLoader()
        >>> az_config = loader.get_az_config()
        >>> capital_extraction = az_config['capital_extraction']
    """

    def __init__(self, config_dir: Optional[Path] = None):
        """
        Initialize the transformation loader.

        Args:
            config_dir: Path to config/transformations directory.
                       If None, uses default location relative to this file.
        """
        if config_dir is None:
            # Default: /home/amiche/Downloads/code/new_python/config/transformations
            # From utils/loaders/transformation_loader.py: need 3 .parent calls
            self.config_dir = Path(__file__).parent.parent.parent / "config" / "transformations"
        else:
            self.config_dir = Path(config_dir)

        # Cache for loaded configs
        self._cache: Dict[str, Dict[str, Any]] = {}

    def _load_json(self, filename: str) -> Dict[str, Any]:
        """
        Load a JSON configuration file.

        Args:
            filename: Name of JSON file (e.g., 'az_transformations.json')

        Returns:
            Dictionary containing the configuration

        Raises:
            FileNotFoundError: If config file doesn't exist
            json.JSONDecodeError: If file is not valid JSON
        """
        if filename in self._cache:
            return self._cache[filename]

        file_path = self.config_dir / filename

        if not file_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {file_path}\n"
                f"Expected location: {self.config_dir}"
            )

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                config = json.load(f)

            # Cache the loaded config
            self._cache[filename] = config
            return config

        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Invalid JSON in {filename}: {e.msg}",
                e.doc,
                e.pos
            )

    def get_az_config(self) -> Dict[str, Any]:
        """
        Get AZ transformation configuration.

        Returns:
            Dictionary with keys:
            - column_selection: passthrough, rename, computed, metadata
            - capital_extraction: defaults, targets
            - business_filters: filters
            - coassurance: conditions for top_coass, coass, partcie
            - revision_criteria: mapping from cdgrev codes
            - movements: column_mapping
            - exposures: column_mapping
            - initialization: default column values
        """
        return self._load_json('az_transformations.json')

    def get_azec_config(self) -> Dict[str, Any]:
        """
        Get AZEC transformation configuration.

        Returns:
            Dictionary with keys:
            - column_selection: passthrough, rename, computed, metadata
            - date_state_updates: conditional updates for dates/states
            - migration_handling: vision_threshold, transformations
            - capital_mapping: branch-specific capital extraction
            - product_list: AZEC-specific product codes
            - movement_calculation: nbafn, nbres, nbptf logic
            - suspension_calculation: nbj_susp_ytd logic
            - business_filters: filters
            - initialization: default column values
        """
        return self._load_json('azec_transformations.json')

    def get_consolidation_config(self) -> Dict[str, Any]:
        """
        Get consolidation mapping configuration.

        Returns:
            Dictionary with keys:
            - az_harmonization: rename, computed
            - azec_harmonization: rename, computed (extensive mappings)
            - common_transformations: cdtre_cleanup, partnership_flag
            - movement_column_mapping: az, azec
            - exposure_column_mapping: az, azec
        """
        return self._load_json('consolidation_mappings.json')

    def get_business_rules(self) -> Dict[str, Any]:
        """
        Get business rules configuration.

        Returns:
            Dictionary with keys:
            - business_filters: az, azec
            - lta_types: long term agreement types
            
        Note:
            coassurance_config removed - now in az/azec_transformations.json computed_fields
        """
        return self._load_json('business_rules.json')

    def get_column_selection(self, source: str) -> Dict[str, Any]:
        """
        Get column selection configuration for a specific source.

        Args:
            source: 'az' or 'azec'

        Returns:
            Dictionary with passthrough, rename, computed, init/metadata
        """
        if source.lower() == 'az':
            return self.get_az_config()['column_selection']
        elif source.lower() == 'azec':
            return self.get_azec_config()['column_selection']
        else:
            raise ValueError(f"Unknown source: {source}. Must be 'az' or 'azec'")

    def get_capital_extraction(self, source: str) -> Dict[str, Any]:
        """
        Get capital extraction configuration.

        Args:
            source: 'az' or 'azec'

        Returns:
            For AZ: capital_extraction config
            For AZEC: capital_mapping config
        """
        if source.lower() == 'az':
            return self.get_az_config()['capital_extraction']
        elif source.lower() == 'azec':
            return self.get_azec_config()['capital_mapping']
        else:
            raise ValueError(f"Unknown source: {source}")

    def get_business_filters(self, source: str) -> List[Dict[str, Any]]:
        """
        Get business filters for a specific source.

        Args:
            source: 'az' or 'azec'

        Returns:
            List of filter configurations
        """
        if source.lower() == 'az':
            az_config = self.get_az_config()
            return az_config.get('business_filters', {}).get('filters', [])
        elif source.lower() == 'azec':
            azec_config = self.get_azec_config()
            return azec_config.get('business_filters', {}).get('filters', [])
        else:
            # Try business_rules.json
            business_rules = self.get_business_rules()
            filters = business_rules.get('business_filters', {}).get(source.lower(), {})
            return filters.get('filters', [])

    def get_harmonization_mapping(self, source: str) -> Dict[str, Any]:
        """
        Get schema harmonization mapping for consolidation.

        Args:
            source: 'az' or 'azec'

        Returns:
            Dictionary with rename and computed mappings
        """
        consolidation = self.get_consolidation_config()

        if source.lower() == 'az':
            return consolidation['az_harmonization']
        elif source.lower() == 'azec':
            return consolidation['azec_harmonization']
        else:
            raise ValueError(f"Unknown source: {source}")

    def get_movement_columns(self, source: str) -> Dict[str, str]:
        """
        Get movement column mapping for a specific source.

        Args:
            source: 'az' or 'azec'

        Returns:
            Dictionary mapping logical column names to actual column names
        """
        consolidation = self.get_consolidation_config()
        mapping = consolidation['movement_column_mapping']

        if source.lower() in mapping:
            return mapping[source.lower()]
        else:
            raise ValueError(f"Unknown source: {source}")

    def get_exposure_columns(self, source: str) -> Dict[str, str]:
        """
        Get exposure column mapping for a specific source.

        Args:
            source: 'az' or 'azec'

        Returns:
            Dictionary mapping logical column names to actual column names
        """
        consolidation = self.get_consolidation_config()
        mapping = consolidation['exposure_column_mapping']

        if source.lower() in mapping:
            return mapping[source.lower()]
        else:
            raise ValueError(f"Unknown source: {source}")

    def get_constants(self, constant_name: Optional[str] = None) -> Any:
        """
        Get business constants.

        Args:
            constant_name: Specific constant to retrieve (e.g., 'excluded_noint')
                          If None, returns all constants

        Returns:
            Constant value or dictionary of all constants
        """
        business_rules = self.get_business_rules()
        constants = business_rules.get('constants', {})

        if constant_name is None:
            return constants

        if constant_name not in constants:
            raise KeyError(f"Constant '{constant_name}' not found")

        return constants[constant_name]

    def reload(self):
        """
        Clear cache and force reload of all configurations.

        Useful during development when JSON files are being modified.
        """
        self._cache.clear()

    def validate_all(self) -> bool:
        """
        Validate that all required configuration files exist and are valid JSON.

        Returns:
            True if all configs are valid, raises exception otherwise

        Raises:
            FileNotFoundError: If any config file is missing
            json.JSONDecodeError: If any file has invalid JSON
        """
        required_files = [
            'az_transformations.json',
            'azec_transformations.json',
            'consolidation_mappings.json',
            'business_rules.json'
        ]

        for filename in required_files:
            self._load_json(filename)

        return True


# =========================================================================
# Convenience Functions
# =========================================================================

def get_loader(config_dir: Optional[Path] = None) -> TransformationLoader:
    """
    Get a TransformationLoader instance.

    Args:
        config_dir: Optional custom config directory

    Returns:
        TransformationLoader instance

    Example:
        >>> from config.loaders.transformation_loader import get_loader
        >>> loader = get_loader()
        >>> az_config = loader.get_az_config()
    """
    return TransformationLoader(config_dir)


# Singleton instance for convenient access
_default_loader: Optional[TransformationLoader] = None

def get_default_loader() -> TransformationLoader:
    """
    Get the default singleton TransformationLoader instance.

    Returns:
        Singleton TransformationLoader instance

    Example:
        >>> from config.loaders.transformation_loader import get_default_loader
        >>> loader = get_default_loader()
        >>> filters = loader.get_business_filters('az')
    """
    global _default_loader
    if _default_loader is None:
        _default_loader = TransformationLoader()
    return _default_loader
