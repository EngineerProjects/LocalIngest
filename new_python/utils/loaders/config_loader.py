"""
Configuration loader for Construction Data Pipeline.
Loads and manages YAML configuration with dot-notation access.
"""

import yaml
from pathlib import Path
from typing import Dict, Any, Optional


class ConfigLoader:
    """Load and manage pipeline configuration from YAML."""

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration loader.

        Args:
            config_path: Path to config.yml file. If None, uses default path.
        """
        if config_path is None:
            # Default to config/config.yml in project root
            project_root = Path(__file__).parent.parent
            config_path = project_root / "config" / "config.yml"

        self.config_path = Path(config_path)
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from YAML file.

        Returns:
            Configuration dictionary

        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If config file is malformed
        """
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        with open(self.config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        return config if config is not None else {}

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key (supports nested keys with dot notation).

        Args:
            key: Configuration key (e.g., 'datalake.base_path')
            default: Default value if key not found

        Returns:
            Configuration value or default

        Example:
            >>> config = ConfigLoader()
            >>> base_path = config.get('datalake.base_path')
            >>> log_level = config.get('logging.level', 'INFO')
        """
        keys = key.split('.')
        value = self.config

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    def set(self, key: str, value: Any) -> None:
        """
        Set configuration value (supports nested keys with dot notation).

        Args:
            key: Configuration key (e.g., 'datalake.base_path')
            value: Value to set

        Example:
            >>> config = ConfigLoader()
            >>> config.set('datalake.base_path', 'abfss://mycontainer@...')
        """
        keys = key.split('.')
        target = self.config

        # Navigate to the parent of the final key
        for k in keys[:-1]:
            if k not in target:
                target[k] = {}
            target = target[k]

        # Set the value
        target[keys[-1]] = value

    def get_all(self) -> Dict[str, Any]:
        """
        Get entire configuration dictionary.

        Returns:
            Complete configuration dict
        """
        return self.config

    def __repr__(self) -> str:
        """String representation of configuration."""
        pipeline_name = self.get('pipeline.name', 'Unknown')
        version = self.get('pipeline.version', 'N/A')
        return f"ConfigLoader(pipeline={pipeline_name}, version={version})"
