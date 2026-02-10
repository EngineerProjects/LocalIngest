"""
Chargeur de configuration pour le Pipeline de Données Construction.
Charge et gère la configuration YAML avec accès par notation pointée.
"""

import yaml
from pathlib import Path
from typing import Dict, Any, Optional


class ConfigLoader:
    """Charge et gère la configuration du pipeline depuis YAML."""

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialise le chargeur de configuration.

        Args:
            config_path: Chemin vers le fichier config.yml. Si None, utilise le chemin par défaut.
        """
        if config_path is None:
            # Défaut à config/config.yml à la racine du projet
            # CORRECTION : .parent.parent.parent pour remonter de utils/loaders/config_loader.py vers la racine
            project_root = Path(__file__).parent.parent.parent
            config_path = project_root / "config" / "config.yml"

        self.config_path = Path(config_path)
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """
        Charge la configuration depuis le fichier YAML.

        Returns:
            Dictionnaire de configuration

        Raises:
            FileNotFoundError: Si le fichier de config n'existe pas
            yaml.YAMLError: Si le fichier de config est mal formé
        """
        if not self.config_path.exists():
            raise FileNotFoundError(f"Fichier de config non trouvé : {self.config_path}")

        with open(self.config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        return config if config is not None else {}

    def get(self, key: str, default: Any = None) -> Any:
        """
        Obtient une valeur de configuration par clé (supporte les clés imbriquées avec notation pointée).

        Args:
            key: Clé de configuration (ex: 'datalake.base_path')
            default: Valeur par défaut si la clé n'est pas trouvée

        Returns:
            Valeur de configuration ou défaut

        Exemple:
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
        Définit une valeur de configuration (supporte les clés imbriquées avec notation pointée).

        Args:
            key: Clé de configuration (ex: 'datalake.base_path')
            value: Valeur à définir

        Exemple:
            >>> config = ConfigLoader()
            >>> config.set('datalake.base_path', 'abfss://mycontainer@...')
        """
        keys = key.split('.')
        target = self.config

        # Naviguer jusqu'au parent de la clé finale
        for k in keys[:-1]:
            if k not in target:
                target[k] = {}
            target = target[k]

        # Définir la valeur
        target[keys[-1]] = value

    def get_all(self) -> Dict[str, Any]:
        """
        Obtient le dictionnaire de configuration complet.

        Returns:
            Dict de configuration complet
        """
        return self.config

    def __repr__(self) -> str:
        """Représentation chaîne de la configuration."""
        pipeline_name = self.get('pipeline.name', 'Inconnu')
        version = self.get('pipeline.version', 'N/A')
        return f"ConfigLoader(pipeline={pipeline_name}, version={version})"
