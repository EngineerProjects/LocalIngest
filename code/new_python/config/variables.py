# -*- coding: utf-8 -*-
"""
- Most configurations have been migrated to JSON files in config/transformations/.
- This module now ONLY exposes constants for backward compatibility, loaded safely.
- Missing OPTIONAL keys DO NOT cascade; only the missing key is defaulted.
- Missing MANDATORY keys RAISE a clear RuntimeError (fail-fast, by design).

JSON files:
- config/transformations/az_transformations.json       → AZ transformation configs
- config/transformations/azec_transformations.json     → AZEC transformation configs
- config/transformations/consolidation_mappings.json   → Consolidation harmonization
- config/transformations/business_rules.json           → (legacy) business filters

Usage:
    from utils.loaders import get_default_loader
    loader = get_default_loader()
    az_config = loader.get_az_config()
"""

from typing import Any, Dict, List, Optional
import warnings

# ─────────────────────────────────────────────────────────────────────────────
# Helpers (granular, safe loading)
# ─────────────────────────────────────────────────────────────────────────────

def _safe_get(cfg: Dict[str, Any],
              path: List[str],
              default: Any = None,
              *,
              required: bool = False,
              cfg_name: str = "config",
              warn_if_default: bool = True) -> Any:
    """
    Safely traverse a nested dict to fetch a value with optional fail-fast.

    Args:
        cfg: The configuration dictionary.
        path: List of nested keys, e.g., ["capital_mapping","mappings"].
        default: Value to return if the key is missing (and required=False).
        required: If True, raise RuntimeError when the key is missing.
        cfg_name: Human-readable name for better error messages.
        warn_if_default: If True, emit a warning when default is used.

    Returns:
        The value found at path, or default.

    Raises:
        RuntimeError if required=True and the path is missing.
    """
    d = cfg
    walked = []
    for key in path:
        walked.append(key)
        if not isinstance(d, dict) or key not in d:
            if required:
                raise RuntimeError(
                    f"[CONFIG] Missing mandatory key '{'.'.join(path)}' in {cfg_name}."
                )
            if warn_if_default:
                warnings.warn(
                    f"[CONFIG] Missing optional key '{'.'.join(path)}' in {cfg_name} "
                    f"→ using default: {default!r}"
                )
            return default
        d = d[key]
    return d


# ─────────────────────────────────────────────────────────────────────────────
# Load all configs (granularly)
# ─────────────────────────────────────────────────────────────────────────────

try:
    from utils.loaders import get_default_loader
    _loader = get_default_loader()

    # ----------------------------
    # AZ configuration (optional)
    # ----------------------------
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

    # COASSURANCE_CONFIG is deprecated → use computed fields in JSON; keep placeholder:
    REVISION_CRITERIA_CONFIG = {
        'critere_revision': _safe_get(
            _az_cfg, ["revision_criteria"],
            default={},
            cfg_name="az_transformations.json",
            required=False
        )
    }

    # Business filters JSON n'est plus utilisé pour AZ/AZEC dans les processors.
    # On expose un placeholder vide pour rétro-compatibilité.
    try:
        _br = _loader.get_business_rules()
        BUSINESS_FILTERS_CONFIG = _br.get('business_filters', {'az': {'filters': []}, 'azec': {'filters': []}})
    except Exception as _:
        BUSINESS_FILTERS_CONFIG = {'az': {'filters': []}, 'azec': {'filters': []}}

    # ----------------------------
    # AZEC configuration (MANDATORY keys fail-fast)
    # ----------------------------
    _azec_cfg = _loader.get_azec_config()

    AZEC_COLUMN_CONFIG = _safe_get(
        _azec_cfg, ["column_selection"],
        default={'passthrough': [], 'rename': {}, 'computed': {}, 'metadata': {}},
        cfg_name="azec_transformations.json",
        required=False
    )

    # MANDATORY: capital_mapping.mappings → fail-fast if missing/empty
    AZEC_CAPITAL_MAPPING = _safe_get(
        _azec_cfg, ["capital_mapping", "mappings"],
        default=None,
        cfg_name="azec_transformations.json",
        required=True
    )
    if not AZEC_CAPITAL_MAPPING:
        # Double-protection: empty list is also invalid (stop pipeline)
        raise RuntimeError(
            "[CONFIG] 'capital_mapping.mappings' is present but empty in azec_transformations.json. "
            "This mapping is mandatory for SMP/LCI; please fix the JSON."
        )

    # Optional lists (safe defaults)
    AZEC_PRODUIT_LIST = _safe_get(
        _azec_cfg, ["product_list", "products"],
        default=[],
        cfg_name="azec_transformations.json",
        required=False
    )

    # Migration threshold (optional with sensible default)
    AZEC_MIGRATION_CONFIG = _safe_get(
        _azec_cfg, ["migration_handling"],
        default={'vision_threshold': 202009},
        cfg_name="azec_transformations.json",
        required=False
    )

    # ----------------------------
    # CONSOLIDATION configuration (optional, per-key safe)
    # ----------------------------
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
    # Loader completely unavailable (e.g., unit tests without JSON files):
    warnings.warn(f"[CONFIG] Could not initialize loader or load configs: {e}. Using fallback defaults.")

    # ---- AZ fallbacks ----
    AZ_COLUMN_CONFIG = {'passthrough': [], 'rename': {}, 'computed': {}, 'metadata': {}}
    CAPITAL_EXTRACTION_CONFIG = []
    REVISION_CRITERIA_CONFIG = {'critere_revision': {}}
    BUSINESS_FILTERS_CONFIG = {'az': {'filters': []}, 'azec': {'filters': []}}

    # ---- AZEC fallbacks (note: CAPITAL_MAPPING kept empty to fail earlier in join if used) ----
    AZEC_COLUMN_CONFIG = {'passthrough': [], 'rename': {}, 'computed': {}, 'metadata': {}}
    AZEC_CAPITAL_MAPPING = []   # Intentionally empty; processors should fail-fast if they rely on it.
    AZEC_PRODUIT_LIST = []
    AZEC_MIGRATION_CONFIG = {'vision_threshold': 202009}

    # ---- Consolidation fallbacks ----
    CONSOLIDATION_AZ_HARMONIZATION = {'rename': {}, 'computed': {}}
    CONSOLIDATION_AZEC_HARMONIZATION = {'rename': {}, 'computed': {}}
