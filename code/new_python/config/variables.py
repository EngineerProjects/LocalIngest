# -*- coding: utf-8 -*-
"""
Module de configuration centralisée pour le pipeline Construction.

Ce module charge les configurations depuis les fichiers JSON et expose
des constantes pour garantir la rétrocompatibilité. Les clés obligatoires
génèrent une erreur si absentes (fail-fast), les clés optionnelles utilisent
des valeurs par défaut.

Fichiers JSON utilisés :
- config/transformations/az_transformations.json
- config/transformations/azec_transformations.json  
- config/transformations/consolidation_mappings.json
- config/transformations/business_rules.json

Utilisation :
    from utils.loaders import get_default_loader
    loader = get_default_loader()
    az_config = loader.get_az_config()
"""

from typing import Any, Dict, List, Optional
import warnings

# =============================================================================
# Fonctions utilitaires
# =============================================================================

def _safe_get(cfg: Dict[str, Any],
              path: List[str],
              default: Any = None,
              *,
              required: bool = False,
              cfg_name: str = "config",
              warn_if_default: bool = True) -> Any:
    """
    Récupère une valeur dans un dictionnaire imbriqué de façon sécurisée.
    
    Args:
        cfg: Dictionnaire de configuration
        path: Chemin vers la clé (ex: ["capital_mapping", "mappings"])
        default: Valeur par défaut si clé absente (et required=False)
        required: Si True, lève une erreur si la clé est absente
        cfg_name: Nom du fichier de config (pour messages d'erreur)
        warn_if_default: Émettre un avertissement si valeur par défaut utilisée
        
    Returns:
        La valeur trouvée ou la valeur par défaut
        
    Raises:
        RuntimeError si required=True et la clé est absente
    """
    d = cfg
    walked = []
    for key in path:
        walked.append(key)
        if not isinstance(d, dict) or key not in d:
            if required:
                raise RuntimeError(
                    f"[CONFIG] Clé obligatoire manquante '{'.'.join(path)}' dans {cfg_name}."
                )
            if warn_if_default:
                warnings.warn(
                    f"[CONFIG] Clé optionnelle manquante '{'.'.join(path)}' dans {cfg_name} "
                    f"→ valeur par défaut utilisée: {default!r}"
                )
            return default
        d = d[key]
    return d


# =============================================================================
# Chargement des configurations
# =============================================================================

try:
    from utils.loaders import get_default_loader
    _loader = get_default_loader()

    # Configuration AZ (agents et courtiers)
    _az_cfg = _loader.get_az_config()

    AZ_COLUMN_CONFIG = _safe_get(
        _az_cfg, ["column_selection"],
        default={'passthrough': [], 'rename': {}, 'computed': {}, 'metadata': {}},
        cfg_name="az_transformations.json",
        required=False
    )

    CAPITAL_EXTRACTION_CONFIG = _safe_get(
        _az_cfg, ["capital_extraction"],
        default=[],
        cfg_name="az_transformations.json",
        required=False
    )

    REVISION_CRITERIA_CONFIG = {
        'critere_revision': _safe_get(
            _az_cfg, ["revision_criteria"],
            default={},
            cfg_name="az_transformations.json",
            required=False
        )
    }

    # Filtres métier (conservé pour rétrocompatibilité)
    try:
        _br = _loader.get_business_rules()
        BUSINESS_FILTERS_CONFIG = _br.get('business_filters', {
            'az': {'filters': []}, 
            'azec': {'filters': []}
        })
    except Exception:
        BUSINESS_FILTERS_CONFIG = {'az': {'filters': []}, 'azec': {'filters': []}}

    # Configuration AZEC (construction)
    _azec_cfg = _loader.get_azec_config()

    AZEC_COLUMN_CONFIG = _safe_get(
        _azec_cfg, ["column_selection"],
        default={'passthrough': [], 'rename': {}, 'computed': {}, 'metadata': {}},
        cfg_name="azec_transformations.json",
        required=False
    )

    # OBLIGATOIRE : Mappages capitaux (SMP/LCI)
    AZEC_CAPITAL_MAPPING = _safe_get(
        _azec_cfg, ["capital_mapping", "mappings"],
        default=None,
        cfg_name="azec_transformations.json",
        required=True
    )
    if not AZEC_CAPITAL_MAPPING:
        raise RuntimeError(
            "[CONFIG] 'capital_mapping.mappings' présent mais vide dans azec_transformations.json. "
            "Ce mappage est obligatoire pour SMP/LCI."
        )

    AZEC_PRODUIT_LIST = _safe_get(
        _azec_cfg, ["product_list", "products"],
        default=[],
        cfg_name="azec_transformations.json",
        required=False
    )

    AZEC_MIGRATION_CONFIG = _safe_get(
        _azec_cfg, ["migration_handling"],
        default={'vision_threshold': 202009},
        cfg_name="azec_transformations.json",
        required=False
    )

    # Configuration consolidation
    _cons_cfg = _loader.get_consolidation_config()

    CONSOLIDATION_AZ_HARMONIZATION = _safe_get(
        _cons_cfg, ["az_harmonization"],
        default={'rename': {}, 'computed': {}},
        cfg_name="consolidation_mappings.json",
        required=False
    )

    CONSOLIDATION_AZEC_HARMONIZATION = _safe_get(
        _cons_cfg, ["azec_harmonization"],
        default={'rename': {}, 'computed': {}},
        cfg_name="consolidation_mappings.json",
        required=False
    )

except Exception as e:
    # Loader indisponible (ex: tests unitaires) → valeurs par défaut
    warnings.warn(f"[CONFIG] Impossible de charger les configs: {e}. Utilisation valeurs par défaut.")

    AZ_COLUMN_CONFIG = {'passthrough': [], 'rename': {}, 'computed': {}, 'metadata': {}}
    CAPITAL_EXTRACTION_CONFIG = []
    REVISION_CRITERIA_CONFIG = {'critere_revision': {}}
    BUSINESS_FILTERS_CONFIG = {'az': {'filters': []}, 'azec': {'filters': []}}

    AZEC_COLUMN_CONFIG = {'passthrough': [], 'rename': {}, 'computed': {}, 'metadata': {}}
    AZEC_CAPITAL_MAPPING = []  # Vide intentionnellement pour fail-fast si utilisé
    AZEC_PRODUIT_LIST = []
    AZEC_MIGRATION_CONFIG = {'vision_threshold': 202009}

    CONSOLIDATION_AZ_HARMONIZATION = {'rename': {}, 'computed': {}}
    CONSOLIDATION_AZEC_HARMONIZATION = {'rename': {}, 'computed': {}}
