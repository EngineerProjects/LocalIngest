# src/utils/ — Utilitaires (chaînes, géo, fuzzy)

from src.utils.string_utils import (
    detect_encoding,
    format_number,
    is_empty,
    is_not_empty,
    normalize_string,
    safe_str,
)
from src.utils.geo_utils import (
    compute_bbox,
    extract_all_coordinates,
    extract_department,
    is_in_bbox,
)
from src.utils.fuzzy_matcher import find_best_match, levenshtein_ratio

__all__ = [
    "detect_encoding",
    "format_number",
    "is_empty",
    "is_not_empty",
    "normalize_string",
    "safe_str",
    "compute_bbox",
    "extract_all_coordinates",
    "extract_department",
    "is_in_bbox",
    "find_best_match",
    "levenshtein_ratio",
]