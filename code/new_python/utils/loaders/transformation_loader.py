"""
Chargeur de Configuration des Transformations.

Charge et valide les configurations de transformation JSON pour les processeurs AZ, AZEC et Consolidation.
Fournit une interface unifiée pour accéder aux règles de transformation.
"""

import json
from pathlib import Path
from typing import Dict, Any, Optional, List


class TransformationLoader:
    """
    Charge et valide les configurations de transformation depuis des fichiers JSON.

    Cette classe fournit un moyen centralisé de charger les configurations de transformation
    qui étaient auparavant codées en dur dans variables.py.

    Exemple:
        >>> loader = TransformationLoader()
        >>> az_config = loader.get_az_config()
        >>> capital_extraction = az_config['capital_extraction']
    """

    def __init__(self, config_dir: Optional[Path] = None):
        """
        Initialise le chargeur de transformation.

        Args:
            config_dir: Chemin vers le répertoire config/transformations.
                       Si None, utilise l'emplacement par défaut relatif à ce fichier.
        """
        if config_dir is None:
            # Défaut : /home/amiche/Downloads/code/new_python/config/transformations
            # Depuis utils/loaders/transformation_loader.py : besoin de 3 appels .parent
            self.config_dir = Path(__file__).parent.parent.parent / "config" / "transformations"
        else:
            self.config_dir = Path(config_dir)

        # Cache pour les configurations chargées
        self._cache: Dict[str, Dict[str, Any]] = {}

    def _load_json(self, filename: str) -> Dict[str, Any]:
        """
        Charge un fichier de configuration JSON.

        Args:
            filename: Nom du fichier JSON (ex: 'az_transformations.json')

        Returns:
            Dictionnaire contenant la configuration

        Raises:
            FileNotFoundError: Si le fichier de configuration n'existe pas
            json.JSONDecodeError: Si le fichier n'est pas un JSON valide
        """
        if filename in self._cache:
            return self._cache[filename]

        file_path = self.config_dir / filename

        if not file_path.exists():
            raise FileNotFoundError(
                f"Fichier de configuration non trouvé : {file_path}\n"
                f"Emplacement attendu : {self.config_dir}"
            )

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                config = json.load(f)

            # Mettre en cache la configuration chargée
            self._cache[filename] = config
            return config

        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"JSON invalide dans {filename} : {e.msg}",
                e.doc,
                e.pos
            )

    def get_az_config(self) -> Dict[str, Any]:
        """
        Obtient la configuration de transformation AZ.

        Returns:
            Dictionnaire avec les clés :
            - column_selection: passthrough, rename, computed, metadata
            - capital_extraction: defaults, targets
            - business_filters: filters
            - coassurance: conditions pour top_coass, coass, partcie
            - revision_criteria: mappage des codes cdgrev
            - movements: column_mapping
            - exposures: column_mapping
            - initialization: valeurs de colonne par défaut
        """
        return self._load_json('az_transformations.json')

    def get_azec_config(self) -> Dict[str, Any]:
        """
        Obtient la configuration de transformation AZEC.

        Returns:
            Dictionnaire avec les clés :
            - column_selection: passthrough, rename, computed, metadata
            - date_state_updates: mises à jour conditionnelles pour dates/états
            - migration_handling: vision_threshold, transformations
            - capital_mapping: extraction de capitaux spécifique à la branche
            - product_list: codes produits spécifiques AZEC
            - movement_calculation: logique nbafn, nbres, nbptf
            - suspension_calculation: logique nbj_susp_ytd
            - business_filters: filtres
            - initialization: valeurs de colonne par défaut
        """
        return self._load_json('azec_transformations.json')

    def get_consolidation_config(self) -> Dict[str, Any]:
        """
        Obtient la configuration de mappage de consolidation.

        Returns:
            Dictionnaire avec les clés :
            - az_harmonization: rename, computed
            - azec_harmonization: rename, computed (mappages étendus)
            - common_transformations: cdtre_cleanup, partnership_flag
            - movement_column_mapping: az, azec
            - exposure_column_mapping: az, azec
        """
        return self._load_json('consolidation_mappings.json')

    def get_business_rules(self) -> Dict[str, Any]:
        """
        Obtient la configuration des règles métier.

        Returns:
            Dictionnaire avec les clés :
            - business_filters: az, azec
            - lta_types: types d'accords à long terme
            
        Note:
            coassurance_config supprimé - maintenant dans az/azec_transformations.json computed_fields
        """
        return self._load_json('business_rules.json')

    def get_column_selection(self, source: str) -> Dict[str, Any]:
        """
        Obtient la configuration de sélection de colonnes pour une source spécifique.

        Args:
            source: 'az' ou 'azec'

        Returns:
            Dictionnaire avec passthrough, rename, computed, init/metadata
        """
        if source.lower() == 'az':
            return self.get_az_config()['column_selection']
        elif source.lower() == 'azec':
            return self.get_azec_config()['column_selection']
        else:
            raise ValueError(f"Source inconnue : {source}. Doit être 'az' ou 'azec'")

    def get_capital_extraction(self, source: str) -> Dict[str, Any]:
        """
        Obtient la configuration d'extraction de capitaux.

        Args:
            source: 'az' ou 'azec'

        Returns:
            Pour AZ : config capital_extraction
            Pour AZEC : config capital_mapping
        """
        if source.lower() == 'az':
            return self.get_az_config()['capital_extraction']
        elif source.lower() == 'azec':
            return self.get_azec_config()['capital_mapping']
        else:
            raise ValueError(f"Source inconnue : {source}")

    def get_business_filters(self, source: str) -> List[Dict[str, Any]]:
        """
        Obtient les filtres métier pour une source spécifique.

        Args:
            source: 'az' ou 'azec'

        Returns:
            Liste des configurations de filtres
        """
        if source.lower() == 'az':
            az_config = self.get_az_config()
            return az_config.get('business_filters', {}).get('filters', [])
        elif source.lower() == 'azec':
            azec_config = self.get_azec_config()
            return azec_config.get('business_filters', {}).get('filters', [])
        else:
            # Essayer business_rules.json
            business_rules = self.get_business_rules()
            filters = business_rules.get('business_filters', {}).get(source.lower(), {})
            return filters.get('filters', [])

    def get_harmonization_mapping(self, source: str) -> Dict[str, Any]:
        """
        Obtient le mappage d'harmonisation de schéma pour la consolidation.

        Args:
            source: 'az' ou 'azec'

        Returns:
            Dictionnaire avec mappages rename et computed
        """
        consolidation = self.get_consolidation_config()

        if source.lower() == 'az':
            return consolidation['az_harmonization']
        elif source.lower() == 'azec':
            return consolidation['azec_harmonization']
        else:
            raise ValueError(f"Source inconnue : {source}")

    def get_movement_columns(self, source: str) -> Dict[str, str]:
        """
        Obtient le mappage de colonnes de mouvement pour une source spécifique.

        Args:
            source: 'az' ou 'azec'

        Returns:
            Dictionnaire mappant les noms de colonnes logiques aux noms réels
        """
        consolidation = self.get_consolidation_config()
        mapping = consolidation['movement_column_mapping']

        if source.lower() in mapping:
            return mapping[source.lower()]
        else:
            raise ValueError(f"Source inconnue : {source}")

    def get_exposure_columns(self, source: str) -> Dict[str, str]:
        """
        Obtient le mappage de colonnes d'exposition pour une source spécifique.

        Args:
            source: 'az' ou 'azec'

        Returns:
            Dictionnaire mappant les noms de colonnes logiques aux noms réels
        """
        consolidation = self.get_consolidation_config()
        mapping = consolidation['exposure_column_mapping']

        if source.lower() in mapping:
            return mapping[source.lower()]
        else:
            raise ValueError(f"Source inconnue : {source}")

    def get_constants(self, constant_name: Optional[str] = None) -> Any:
        """
        Obtient les constantes métier.

        Args:
            constant_name: Constant spécifique à récupérer (ex: 'excluded_noint')
                          Si None, retourne toutes les constantes

        Returns:
            Valeur de la constante ou dictionnaire de toutes les constantes
        """
        business_rules = self.get_business_rules()
        constants = business_rules.get('constants', {})

        if constant_name is None:
            return constants

        if constant_name not in constants:
            raise KeyError(f"Constante '{constant_name}' non trouvée")

        return constants[constant_name]

    def reload(self):
        """
        Vide le cache et force le rechargement de toutes les configurations.

        Utile pendant le développement lorsque les fichiers JSON sont modifiés.
        """
        self._cache.clear()

    def validate_all(self) -> bool:
        """
        Valide que tous les fichiers de configuration requis existent et sont des JSON valides.

        Returns:
            True si toutes les configs sont valides, lève une exception sinon

        Raises:
            FileNotFoundError: Si un fichier de config manque
            json.JSONDecodeError: Si un fichier contient du JSON invalide
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
# Fonctions de commodité
# =========================================================================

def get_loader(config_dir: Optional[Path] = None) -> TransformationLoader:
    """
    Obtient une instance de TransformationLoader.

    Args:
        config_dir: Répertoire de configuration personnalisé optionnel

    Returns:
        Instance TransformationLoader

    Exemple:
        >>> from config.loaders.transformation_loader import get_loader
        >>> loader = get_loader()
        >>> az_config = loader.get_az_config()
    """
    return TransformationLoader(config_dir)


# Instance singleton pour accès pratique
_default_loader: Optional[TransformationLoader] = None

def get_default_loader() -> TransformationLoader:
    """
    Obtient l'instance singleton par défaut de TransformationLoader.

    Returns:
        Instance singleton TransformationLoader

    Exemple:
        >>> from config.loaders.transformation_loader import get_default_loader
        >>> loader = get_default_loader()
        >>> filters = loader.get_business_filters('az')
    """
    global _default_loader
    if _default_loader is None:
        _default_loader = TransformationLoader()
    return _default_loader
