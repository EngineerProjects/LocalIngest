# src/utils/ — Utilitaires (chaînes, géo, fuzzy)

from src.utils.string_utils import (
    detect_encoding,
    detect_separator,
    extract_postal_code,
    extract_street_number,
    format_number,
    is_empty,
    is_not_empty,
    normalize_contract,
    normalize_country,
    normalize_postal_code,
    normalize_string,
    parse_coordinate,
    safe_str,
)
from src.utils.geo_utils import (
    compute_bbox,
    extract_all_coordinates,
    extract_department,
    extract_region_code,
    is_in_bbox,
)
from src.utils.fuzzy_matcher import find_best_match, levenshtein_ratio

__all__ = [
    # string_utils
    "detect_encoding",
    "detect_separator",
    "extract_postal_code",
    "extract_street_number",
    "format_number",
    "is_empty",
    "is_not_empty",
    "normalize_contract",
    "normalize_country",
    "normalize_postal_code",
    "normalize_string",
    "parse_coordinate",
    "safe_str",
    # geo_utils
    "compute_bbox",
    "extract_all_coordinates",
    "extract_department",
    "extract_region_code",
    "is_in_bbox",
    # fuzzy_matcher
    "find_best_match",
    "levenshtein_ratio",
]
