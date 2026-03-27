"""
Évaluation d'un site — logique métier en 7 étapes.

`evaluate_site` est le point d'entrée.
Chaque étape est isolée dans une sous-fonction pour faciliter la lecture et le debug.

Étape 1 : GPS renseigné et valide ?
Étape 2 : GPS dans la bbox pays ?
Étape 3 : GPS cohérent avec le code postal ?
      → Si l'une de ces étapes échoue : bascule sur adresse
Étape 4 : Adresse renseignée ?
Étape 5 : Numéro de voie présent ?
Étape 6 : Rue à vérifier manuellement ? (faible priorité)
Étape 7 : Cohérence département / pays ? (faible priorité)
"""

from typing import Dict, List, Optional

import pandas as pd

from src.config import Config, IssueCode, ValidationMode
from src.loaders.reference_loader import ReferenceLoader
from src.models import (
    build_control_status,
    build_issue_summary,
    get_worst_priority,
    ordered_unique_codes,
)
from src.utils import (
    extract_region_code,
    is_in_bbox,
    is_not_empty,
    normalize_country,
    safe_str,
)


# ─────────────────────────────────────────────────────────────────────────────
# Point d'entrée public
# ─────────────────────────────────────────────────────────────────────────────


def evaluate_site(
    row: pd.Series,
    ref_loader: ReferenceLoader,
    site_key: str,
) -> Dict:
    """
    Évalue un site selon la logique métier en 7 étapes.
    Retourne un dictionnaire prêt à être ajouté au DataFrame enrichi.
    """
    issue_codes: List[str] = []
    issue_details: List[Dict] = []

    country = normalize_country(row.get(Config.COL_COUNTRY))
    postal_code = safe_str(row.get(Config.COL_POSTAL_CODE))
    department_code = (
        extract_region_code(country, postal_code) if country and postal_code else ""
    )

    # ── Étape 0 : Moteur de rejet immédiat (Bouncer pays) ────────────────────
    if not country:
        issue_codes.append("P-01")
        _add_issue_detail(
            "P-01",
            "Pays manquant",
            "GRAVE",
            "pays",
            "Le pays n'est pas renseigné. Validation bloquée.",
            [Config.COL_COUNTRY],
            "",
            "Renseigner le pays.",
            issue_details,
            row,
        )
        final_codes = ordered_unique_codes(issue_codes)
        addr_ctx = _read_address(row)
        gps_ctx = _read_gps(row)
        return {
            "_SITE_KEY": site_key,
            "_CONTROL_MODE": ValidationMode.INCOMPLETE,
            "_CONTROL_STATUS": build_control_status(final_codes),
            "_PRIORITY": get_worst_priority(final_codes),
            "_ISSUE_COUNT": len(final_codes),
            "_ISSUE_CODES": ", ".join(final_codes),
            "_ISSUE_SUMMARY": build_issue_summary(final_codes),
            "_ISSUE_DETAILS": issue_details,
            "_HAS_GPS": gps_ctx["has_any"],
            "_GPS_IN_COUNTRY": None,
            "_GPS_MATCH_POSTAL_CODE": None,
            "_ADDRESS_PRESENT": addr_ctx["address_present"],
            "_STREET_NUMBER_PRESENT": addr_ctx["street_number_present"],
            "_DEPARTMENT_CODE": department_code,
            "_DEPARTMENT_COUNTRY_OK": None,
        }

    # ── Étapes 1-3 : chemin GPS ──────────────────────────────────────────────
    gps_ctx = _read_gps(row)
    gps_result = _check_gps_path(
        gps_ctx,
        country,
        postal_code,
        department_code,
        ref_loader,
        issue_codes,
        issue_details,
        row,
    )
    gps_path_valid = gps_result["valid"]

    # ── Étapes 4-6 : chemin adresse (si GPS n'a pas validé) ─────────────────
    addr_ctx = _read_address(row)
    if not gps_path_valid:
        control_mode = _check_address_path(
            row, addr_ctx, postal_code, issue_codes, issue_details
        )
    else:
        control_mode = ValidationMode.GPS

    # ── Étape 7 : cohérence département / pays (faible priorité) ────────────
    dept_ok = _check_department(
        country, postal_code, department_code, ref_loader, issue_codes, issue_details
    )

    # ── Consolidation ────────────────────────────────────────────────────────
    final_codes = ordered_unique_codes(issue_codes)

    return {
        "_SITE_KEY": site_key,
        "_CONTROL_MODE": control_mode,
        "_CONTROL_STATUS": build_control_status(final_codes),
        "_PRIORITY": get_worst_priority(final_codes),
        "_ISSUE_COUNT": len(final_codes),
        "_ISSUE_CODES": ", ".join(final_codes),
        "_ISSUE_SUMMARY": build_issue_summary(final_codes),
        "_ISSUE_DETAILS": issue_details,
        # Diagnostics détaillés (utiles pour le debug et les données enrichies)
        "_HAS_GPS": gps_ctx["has_any"],
        "_GPS_IN_COUNTRY": gps_result["in_country"],
        "_GPS_MATCH_POSTAL_CODE": gps_result["matches_postal_code"],
        "_ADDRESS_PRESENT": addr_ctx["address_present"],
        "_STREET_NUMBER_PRESENT": addr_ctx["street_number_present"],
        "_DEPARTMENT_CODE": department_code,
        "_DEPARTMENT_COUNTRY_OK": dept_ok,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Étape 1 — Lecture des coordonnées GPS
# ─────────────────────────────────────────────────────────────────────────────


def _read_gps(row: pd.Series) -> Dict:
    """Lit et pré-analyse les coordonnées GPS brutes de la ligne."""
    lon_raw = row.get(Config.COL_LONGITUDE)
    lat_raw = row.get(Config.COL_LATITUDE)
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


# ─────────────────────────────────────────────────────────────────────────────
# Étapes 2-3 — Contrôles GPS (bbox pays puis bbox code postal)
# ─────────────────────────────────────────────────────────────────────────────


def _check_gps_path(
    gps_ctx: Dict,
    country: str,
    postal_code: str,
    department_code: str,
    ref_loader: ReferenceLoader,
    issue_codes: List[str],
    issue_details: List[Dict],
    row: pd.Series,
) -> Dict:
    """
    Tente de valider le site via le GPS.

    Retourne un dict avec :
      valid               → True si le GPS valide le site
      in_country          → True/False/None
      matches_postal_code → True/False/None
    """
    result = {"valid": False, "in_country": None, "matches_postal_code": None}

    # Pas de GPS du tout → on laisse issue_codes vide (on bascule adresse silencieusement)
    if not gps_ctx["complete"]:
        if gps_ctx["has_any"]:
            issue_codes.append("G-02")
            _add_issue_detail(
                "G-02",
                "GPS incomplet",
                "LEGERE",
                "adresse",
                f"Une seule coordonnée GPS renseignée (X ou Y manquant).",
                [Config.COL_LONGITUDE, Config.COL_LATITUDE],
                f"Lon={gps_ctx['lon_raw']}, Lat={gps_ctx['lat_raw']}",
                "Compléter la coordonnée manquante.",
                issue_details,
                row,
            )
        return result

    # Étape 1 : les valeurs sont-elles numériquement valides ?
    lon = _parse_coordinate(gps_ctx["lon_raw"], -180.0, 180.0)
    lat = _parse_coordinate(gps_ctx["lat_raw"], -90.0, 90.0)
    if lon is None or lat is None:
        issue_codes.append("G-03")
        _add_issue_detail(
            "G-03",
            "GPS invalide",
            "GRAVE",
            "gps",
            f"Coordonnée GPS non numérique ou hors limites.",
            [Config.COL_LONGITUDE, Config.COL_LATITUDE],
            f"Lon={gps_ctx['lon_raw']}, Lat={gps_ctx['lat_raw']}",
            "Vérifier les coordonnées GPS.",
            issue_details,
            row,
        )
        return result



    # Étape 2 : bbox pays
    bbox_country = ref_loader.get_bbox_pays(country)
    if bbox_country is None:
        issue_codes.append("R-01")
        _add_issue_detail(
            "R-01",
            "Pays non couvert",
            "FAIBLE",
            "pays",
            f"Pays '{country}' non couvert par le référentiel.",
            [Config.COL_COUNTRY],
            f"Pays={country}",
            "",
            issue_details,
            row,
        )
        return result

    result["in_country"] = is_in_bbox(lon, lat, bbox_country)
    if not result["in_country"]:
        issue_codes.append("G-04")
        _add_issue_detail(
            "G-04",
            "GPS hors pays",
            "GRAVE",
            "gps",
            f"GPS ({lon}, {lat}) situé hors de {country}.",
            [Config.COL_LONGITUDE, Config.COL_LATITUDE, Config.COL_COUNTRY],
            f"Lon={lon}, Lat={lat}, Pays={country}",
            "Vérifier les coordonnées GPS.",
            issue_details,
            row,
        )
        return result

    # Étape 3 : bbox code postal (si référentiel disponible)
    if not postal_code:
        issue_codes.append("P-02")
        _add_issue_detail(
            "P-02",
            "Code postal manquant",
            "MOYENNE",
            "adresse",
            "Le code postal n'est pas renseigné.",
            [Config.COL_POSTAL_CODE],
            "",
            "Renseigner le code postal.",
            issue_details,
            row,
        )
        return result

    if department_code:
        bbox_region = ref_loader.get_bbox_region(country, department_code)
        if bbox_region is not None:
            result["matches_postal_code"] = is_in_bbox(lon, lat, bbox_region)
            if not result["matches_postal_code"]:
                issue_codes.append("G-05")
                _add_issue_detail(
                    "G-05",
                    "GPS incohérent avec le code postal",
                    "HAUTE",
                    "gps",
                    f"GPS ({lon}, {lat}) ne correspond pas au code postal {postal_code}.",
                    [Config.COL_LONGITUDE, Config.COL_LATITUDE, Config.COL_POSTAL_CODE],
                    f"Lon={lon}, Lat={lat}, CP={postal_code}, Dept={department_code}",
                    "Vérifier la cohérence GPS / code postal.",
                    issue_details,
                    row,
                )
                return result

    # GPS valide le site (avec ou sans vérification CP si pas de référentiel)
    result["valid"] = True
    return result




# ─────────────────────────────────────────────────────────────────────────────
# Étape 1 bis — Lecture des champs adresse
# ─────────────────────────────────────────────────────────────────────────────


def _read_address(row: pd.Series) -> Dict:
    """Lit et pré-analyse les champs adresse de la ligne."""
    address_present = any(
        is_not_empty(row.get(col))
        for col in [
            Config.COL_STREET_FULL,
            Config.COL_STREET_NAME,
            Config.COL_FULL_ADDRESS,
            Config.COL_LIEU_DIT,
        ]
    )
    street_number_present = is_not_empty(row.get(Config.COL_STREET_NUMBER))
    street_text_present = any(
        is_not_empty(row.get(col))
        for col in [
            Config.COL_STREET_FULL,
            Config.COL_STREET_NAME,
            Config.COL_FULL_ADDRESS,
        ]
    )
    return {
        "address_present": address_present,
        "street_number_present": street_number_present,
        "street_text_present": street_text_present,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Étapes 4-6 — Contrôles adresse
# ─────────────────────────────────────────────────────────────────────────────


def _check_address_path(
    row: pd.Series,
    addr_ctx: Dict,
    postal_code: str,
    issue_codes: List[str],
    issue_details: List[Dict],
) -> str:
    """
    Contrôle le chemin adresse si le GPS n'a pas validé le site.

    Retourne le ValidationMode correspondant.
    """
    # Étape 4 : adresse présente ?
    if not addr_ctx["address_present"]:
        issue_codes.append("A-01")
        _add_issue_detail(
            "A-01",
            "Adresse manquante",
            "GRAVE",
            "adresse",
            "Aucune information d'adresse renseignée.",
            [
                Config.COL_STREET_FULL,
                Config.COL_STREET_NAME,
                Config.COL_FULL_ADDRESS,
                Config.COL_LIEU_DIT,
            ],
            "",
            "Rencher l'adresse.",
            issue_details,
            row,
        )
        return ValidationMode.INCOMPLETE

    # Étape 5 : numéro de voie présent ?
    if addr_ctx["street_number_present"]:
        return ValidationMode.ADDRESS

    # Étape 6 : numéro manquant → alertes faible priorité
    issue_codes.append("A-02")
    _add_issue_detail(
        "A-02",
        "Numéro de voie manquant",
        "MOYENNE",
        "adresse",
        "L'adresse est renseignée mais le numéro de voie est absent.",
        [Config.COL_STREET_NUMBER],
        f"Numéro=, Rue={row.get(Config.COL_STREET_NAME, '')}",
        "Compléter le numéro de rue si l'information existe.",
        issue_details,
        row,
    )
    if addr_ctx["street_text_present"]:
        issue_codes.append("A-03")
        _add_issue_detail(
            "A-03",
            "Rue à vérifier manuellement",
            "FAIBLE",
            "adresse",
            "Le numéro de rue manque : la cohérence du nom de rue avec le département doit être vérifiée manuellement.",
            [Config.COL_STREET_NAME, Config.COL_STREET_FULL, Config.COL_POSTAL_CODE],
            f"Rue={row.get(Config.COL_STREET_NAME, '')}, CP={postal_code}",
            "Contrôler le nom de rue avec la source métier du département.",
            issue_details,
            row,
        )
    return ValidationMode.INCOMPLETE


# ─────────────────────────────────────────────────────────────────────────────
# Étape 7 — Cohérence département / pays (faible priorité)
# ─────────────────────────────────────────────────────────────────────────────


def _check_department(
    country: str,
    postal_code: str,
    department_code: str,
    ref_loader: ReferenceLoader,
    issue_codes: List[str],
    issue_details: List[Dict],
) -> Optional[bool]:
    """
    Vérifie que le code département (déduit du CP) correspond au pays.
    Actif uniquement pour la France.
    Retourne True/False/None.
    """
    if not country or not postal_code or country != "FRANCE":
        return None

    if not department_code:
        issue_codes.append("D-01")
        _add_issue_detail(
            "D-01",
            "Code département à vérifier",
            "FAIBLE",
            "pays",
            "Le code département ne peut pas être extrait du code postal.",
            [Config.COL_POSTAL_CODE],
            f"CP={postal_code}",
            "Vérifier le code postal.",
            issue_details,
            None,
        )
        return False

    bbox_region = ref_loader.get_bbox_region(country, department_code)
    if bbox_region is None:
        issue_codes.append("D-01")
        _add_issue_detail(
            "D-01",
            "Code département à vérifier",
            "FAIBLE",
            "pays",
            f"Le département '{department_code}' n'existe pas dans le référentiel.",
            [Config.COL_POSTAL_CODE],
            f"CP={postal_code}, Dept={department_code}",
            "Vérifier le code département.",
            issue_details,
            None,
        )
        return False

    return True


def _add_issue_detail(
    code: str,
    libelle: str,
    gravite: str,
    categorie: str,
    description: str,
    champs: List[str],
    valeur: str,
    suggestion: str,
    issue_details: List[Dict],
    row: pd.Series,
) -> None:
    """Ajoute un détail d'anomalie à la liste."""
    issue_details.append(
        {
            "CODE_ANOMALIE": code,
            "LIBELLE": libelle,
            "GRAVITE": gravite,
            "CATEGORIE": categorie,
            "DESCRIPTION": description,
            "CHAMPS_CONCERNES": ", ".join(champs),
            "VALEUR_ACTUELLE": valeur,
            "SUGGESTION": suggestion,
        }
    )


# ─────────────────────────────────────────────────────────────────────────────
# Utilitaire interne
# ─────────────────────────────────────────────────────────────────────────────


def _parse_coordinate(raw_value, minimum: float, maximum: float) -> Optional[float]:
    """Parse une coordonnée GPS en acceptant la virgule comme séparateur décimal."""
    try:
        value = float(str(raw_value).replace(",", "."))
    except (TypeError, ValueError):
        return None
    return value if minimum <= value <= maximum else None
