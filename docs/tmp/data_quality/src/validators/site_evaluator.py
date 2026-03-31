from typing import Dict, List, Optional

import pandas as pd

from src.config import IssueCode, ValidationMode
from src.loaders.reference_loader import ReferenceLoader
from src.utils import (
    extract_postal_code,
    extract_region_code,
    extract_street_number,
    is_in_bbox,
    is_not_empty,
    normalize_country,
    parse_coordinate,
    safe_str,
)


def evaluate_site(
    row: pd.Series,
    ref_loader: ReferenceLoader,
    site_key: str,
) -> Dict:
    """
    Évalue un site selon la logique métier.

    Ordre obligatoire (chaque étape peut provoquer un STOP) :
        A) Pays          → P-01 (pays vide), R-01 (pays inconnu du référentiel)
        B) GPS           → G-02, G-03, G-06, G-04, P-02, G-05
        C) Adresse       → A-01, A-02  (uniquement si GPS non valide)
    """
    issue_codes: List[str] = []
    issue_details: List[Dict] = []

    country = normalize_country(row.get("PAYS_SITE"))
    postal_code = safe_str(row.get("CP_SITE"))
    full_address = safe_str(row.get("ADRESSE_SITE"))

    # =========================================================================
    # A. Contrôle Pays
    # =========================================================================

    # A.1 — Pays vide
    # (En pratique handle_country_filter a déjà remplacé les vides par FRANCE,
    #  mais on garde ce garde-fou au cas où.)
    if not country:
        issue_codes.append("P-01")
        _add_issue_detail(
            "P-01",
            IssueCode.get_label("P-01"),
            "pays",
            "Le pays n'est pas renseigné.",
            ["PAYS_SITE"],
            "",
            "Renseigner le pays.",
            issue_details,
        )
        return _build_result(site_key, ValidationMode.INCOMPLETE, issue_codes, issue_details)

    # A.2 — Pays inconnu du référentiel
    bbox_country = ref_loader.get_bbox_pays(country)
    if bbox_country is None:
        issue_codes.append("R-01")
        _add_issue_detail(
            "R-01",
            IssueCode.get_label("R-01"),
            "pays",
            f"Le pays '{country}' n'est pas présent dans le référentiel.",
            ["PAYS_SITE"],
            f"Pays={country}",
            "",
            issue_details,
        )
        return _build_result(site_key, ValidationMode.INCOMPLETE, issue_codes, issue_details)

    # =========================================================================
    # Extraction des champs dérivés (CP, numéro de rue, département)
    # =========================================================================

    # CP : priorité CP_SITE, sinon extraction regex depuis ADRESSE_SITE
    extracted_cp = extract_postal_code(full_address)
    if not postal_code and extracted_cp:
        postal_code = extracted_cp

    extracted_num = extract_street_number(full_address)
    dept_code = extract_region_code(country, postal_code) if postal_code else ""

    # =========================================================================
    # B. Contrôle GPS
    # =========================================================================
    gps_ctx = _read_gps(row)
    gps_result = _check_gps_path(
        gps_ctx, country, postal_code, dept_code, bbox_country, ref_loader,
        issue_codes, issue_details,
    )

    # =========================================================================
    # C. Contrôle Adresse (uniquement si le GPS n'a pas validé le site)
    # =========================================================================
    if gps_result["valid"]:
        control_mode = ValidationMode.GPS
    else:
        addr_ctx = _read_address(row, extracted_num)
        control_mode = _check_address_path(addr_ctx, issue_codes, issue_details)

    return _build_result(site_key, control_mode, issue_codes, issue_details)


# =============================================================================
# GPS
# =============================================================================

def _read_gps(row: pd.Series) -> Dict:
    """Lit les coordonnées GPS brutes."""
    lon_raw = row.get("COORD_X_SITE")
    lat_raw = row.get("COORD_Y_SITE")
    has_lon = is_not_empty(lon_raw)
    has_lat = is_not_empty(lat_raw)
    return {
        "lon_raw": lon_raw,
        "lat_raw": lat_raw,
        "has_lon": has_lon,
        "has_lat": has_lat,
        "has_any": has_lon or has_lat,
        "complete": has_lon and has_lat,
    }


def _check_gps_path(
    gps_ctx: Dict,
    country: str,
    postal_code: str,
    dept_code: str,
    bbox_country: list,
    ref_loader: ReferenceLoader,
    issue_codes: List[str],
    issue_details: List[Dict],
) -> Dict:
    """
    Vérifie le GPS selon la logique métier.

    Précondition : bbox_country est déjà résolu (R-01 a déjà été géré en amont).

    B.1 — GPS incomplet (une seule coord)  → G-02 → STOP
    B.2 — GPS non numérique / hors plage   → G-03 → STOP
    B.3 — GPS hors pays                    → G-06 (inversé) ou G-04 → STOP
    B.4 — GPS OK mais CP absent            → P-02 → STOP
    B.5 — GPS incohérent avec département  → G-05 → STOP
    OK  → valid=True
    """
    result = {"valid": False}

    # B.1 — GPS incomplet
    if not gps_ctx["complete"]:
        if gps_ctx["has_any"]:
            # Une seule coordonnée renseignée sur deux
            issue_codes.append("G-02")
            _add_issue_detail(
                "G-02",
                IssueCode.get_label("G-02"),
                "gps",
                "Une seule coordonnée GPS renseignée (X ou Y manquant).",
                ["COORD_X_SITE", "COORD_Y_SITE"],
                f"X={gps_ctx['lon_raw']}, Y={gps_ctx['lat_raw']}",
                "Compléter la coordonnée manquante.",
                issue_details,
            )
        # GPS totalement absent → pas d'erreur GPS, on passe à l'adresse
        return result

    # B.2 — GPS non numérique ou hors plage [-180,180] / [-90,90]
    lon = parse_coordinate(gps_ctx["lon_raw"], -180.0, 180.0)
    lat = parse_coordinate(gps_ctx["lat_raw"], -90.0, 90.0)

    if lon is None or lat is None:
        issue_codes.append("G-03")
        _add_issue_detail(
            "G-03",
            IssueCode.get_label("G-03"),
            "gps",
            "Les coordonnées GPS ne sont pas valides (non numériques ou hors limites).",
            ["COORD_X_SITE", "COORD_Y_SITE"],
            f"X={gps_ctx['lon_raw']}, Y={gps_ctx['lat_raw']}",
            "Vérifier et corriger les coordonnées GPS.",
            issue_details,
        )
        return result

    # B.3 — GPS dans le pays ?
    if not is_in_bbox(lon, lat, bbox_country):
        # Hors pays → tester si (Y, X) est valide (inversion possible)
        lon_inv, lat_inv = lat, lon
        if is_in_bbox(lon_inv, lat_inv, bbox_country):
            issue_codes.append("G-06")
            _add_issue_detail(
                "G-06",
                IssueCode.get_label("G-06"),
                "gps",
                f"GPS ({lon}, {lat}) hors {country}, mais ({lon_inv}, {lat_inv}) y est — coordonnées probablement inversées.",
                ["COORD_X_SITE", "COORD_Y_SITE"],
                f"X={lon}, Y={lat}",
                "Inverser X et Y.",
                issue_details,
            )
        else:
            issue_codes.append("G-04")
            _add_issue_detail(
                "G-04",
                IssueCode.get_label("G-04"),
                "gps",
                f"GPS ({lon}, {lat}) situé hors de {country}.",
                ["COORD_X_SITE", "COORD_Y_SITE", "PAYS_SITE"],
                f"X={lon}, Y={lat}, Pays={country}",
                "Vérifier les coordonnées GPS.",
                issue_details,
            )
        return result

    # B.4 — GPS dans le pays : CP requis pour aller plus loin
    if not postal_code:
        issue_codes.append("P-02")
        _add_issue_detail(
            "P-02",
            IssueCode.get_label("P-02"),
            "adresse",
            "Le GPS est dans le pays mais le code postal est absent.",
            ["CP_SITE"],
            "",
            "Renseigner le code postal.",
            issue_details,
        )
        return result

    # B.5 — Cohérence GPS / département (si bbox département disponible)
    if dept_code:
        bbox_region = ref_loader.get_bbox_region(country, dept_code)
        if bbox_region is not None and not is_in_bbox(lon, lat, bbox_region):
            issue_codes.append("G-05")
            _add_issue_detail(
                "G-05",
                IssueCode.get_label("G-05"),
                "gps",
                f"GPS ({lon}, {lat}) incohérent avec le code postal {postal_code} (département {dept_code}).",
                ["COORD_X_SITE", "COORD_Y_SITE", "CP_SITE"],
                f"X={lon}, Y={lat}, CP={postal_code}",
                "Vérifier la cohérence entre GPS et code postal.",
                issue_details,
            )
            return result

    # GPS entièrement valide
    result["valid"] = True
    return result


# =============================================================================
# Adresse
# =============================================================================

def _read_address(row: pd.Series, extracted_num: str) -> Dict:
    """
    Lit les champs adresse et détermine :
    - si au moins un champ d'adresse est renseigné
    - si un numéro de rue est disponible
    """
    street_number = safe_str(row.get("NUM_VOIE_SITE"))
    street_name   = safe_str(row.get("RUE_SITE"))
    street_full   = safe_str(row.get("NUM_RUE_SITE"))
    full_address  = safe_str(row.get("ADRESSE_SITE"))
    lieu_dit      = safe_str(row.get("LIEU_DIT_SITE"))

    address_present = any([
        is_not_empty(street_number),
        is_not_empty(street_name),
        is_not_empty(street_full),
        is_not_empty(full_address),
        is_not_empty(lieu_dit),
    ])

    # Numéro de rue : colonne dédiée OU extraction regex
    street_number_present = is_not_empty(street_number) or is_not_empty(extracted_num)

    return {
        "address_present": address_present,
        "street_number_present": street_number_present,
    }


def _check_address_path(
    addr_ctx: Dict,
    issue_codes: List[str],
    issue_details: List[Dict],
) -> str:
    """
    Contrôle adresse (déclenché uniquement si GPS non valide).

    A.1 — Aucune info adresse → A-01 → INCOMPLET
    A.2 — Adresse présente, no numéro → A-02 → INCOMPLET
    OK  → ADRESSE_OK
    """
    if not addr_ctx["address_present"]:
        issue_codes.append("A-01")
        _add_issue_detail(
            "A-01",
            IssueCode.get_label("A-01"),
            "adresse",
            "Aucune information d'adresse renseignée.",
            ["NUM_VOIE_SITE", "RUE_SITE", "NUM_RUE_SITE", "ADRESSE_SITE", "LIEU_DIT_SITE"],
            "",
            "Renseigner l'adresse du site.",
            issue_details,
        )
        return ValidationMode.INCOMPLETE

    if not addr_ctx["street_number_present"]:
        issue_codes.append("A-02")
        _add_issue_detail(
            "A-02",
            IssueCode.get_label("A-02"),
            "adresse",
            "L'adresse est renseignée mais le numéro de rue est absent.",
            ["NUM_VOIE_SITE", "ADRESSE_SITE"],
            "",
            "Ajouter le numéro de rue.",
            issue_details,
        )
        return ValidationMode.INCOMPLETE

    return ValidationMode.ADDRESS


# =============================================================================
# Helpers internes
# =============================================================================

def _build_result(
    site_key: str,
    control_mode: str,
    issue_codes: List[str],
    issue_details: List[Dict],
) -> Dict:
    """Construit le dictionnaire résultat d'un site."""
    final_codes = _ordered_unique_codes(issue_codes)
    return {
        "_SITE_KEY": site_key,
        "_CONTROL_MODE": control_mode,
        "_ISSUE_COUNT": len(final_codes),
        "_ISSUE_CODES": ", ".join(final_codes),
        "_ISSUE_SUMMARY": _build_issue_summary(final_codes),
        "_ISSUE_DETAILS": issue_details,
    }


def _add_issue_detail(
    code: str,
    libelle: str,
    categorie: str,
    description: str,
    champs: List[str],
    valeur: str,
    suggestion: str,
    issue_details: List[Dict],
) -> None:
    """Ajoute un détail d'anomalie à la liste."""
    issue_details.append(
        {
            "CODE_ANOMALIE": code,
            "LIBELLE": libelle,
            "CATEGORIE": categorie,
            "DESCRIPTION": description,
            "CHAMPS_CONCERNES": ", ".join(champs),
            "VALEUR_ACTUELLE": valeur,
            "SUGGESTION": suggestion,
        }
    )


def _ordered_unique_codes(codes: List[str]) -> List[str]:
    """Déduplique les codes en conservant l'ordre d'apparition."""
    seen = set()
    ordered = []
    for code in codes:
        if code and code not in seen:
            seen.add(code)
            ordered.append(code)
    return ordered


def _build_issue_summary(codes: List[str]) -> str:
    """Construit le libellé consolidé de l'anomalie (séparateur ' | ')."""
    return " | ".join(IssueCode.get_label(c) for c in codes)
