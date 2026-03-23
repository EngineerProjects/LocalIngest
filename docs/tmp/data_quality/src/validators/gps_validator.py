"""
Validation GPS — Bloc C : G-01 à G-07.

Appliquée quand au moins une coordonnée GPS est présente.

Ordre de validation :
    G-01 → GPS incomplet (une seule coordonnée) → BASCULE ADRESSE
    G-02 → Format longitude invalide            → BASCULE ADRESSE
    G-03 → Format latitude invalide             → BASCULE ADRESSE
    G-05 → GPS hors pays                        → BASCULE ADRESSE
      ↳ G-06 → Diagnostic inversion X/Y (INFO)
      ↳ G-07 → Diagnostic signe erroné (INFO)
    G-04 → GPS hors département/région          → LOCALISÉ quand même (LÉGÈRE)
"""

from typing import Any, Optional

import pandas as pd

from src.config import Config, Severity, SupportLevel
from src.loaders.reference_loader import ReferenceLoader
from src.models import Anomaly, AnomalyCollector
from src.utils import extract_region_code, is_in_bbox, is_not_empty, normalize_country, safe_str


def validate_gps(
    site_id: Any,
    contract_id: Any,
    row: pd.Series,
    ref_loader: ReferenceLoader,
    support_level: str,
    collector: AnomalyCollector,
) -> int:
    """
    Valide les coordonnées GPS.

    Returns:
        Nombre d'anomalies GPS ajoutées (0 = GPS valide)
    """
    anomalies_before = collector.count()

    lon_raw = row.get(Config.COL_LONGITUDE)
    lat_raw = row.get(Config.COL_LATITUDE)
    has_lon = is_not_empty(lon_raw)
    has_lat = is_not_empty(lat_raw)

    # G-01 : GPS incomplet
    if has_lon != has_lat:
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="G-01",
            severity=Severity.LEGERE,
            description="Une seule coordonnée GPS renseignée (X ou Y manquant).",
            fields_concerned=[Config.COL_LONGITUDE, Config.COL_LATITUDE],
            current_value=f"Lon={lon_raw}, Lat={lat_raw}",
        ))
        return collector.count() - anomalies_before

    if not has_lon and not has_lat:
        return 0  # Pas de GPS du tout

    # G-02 / G-03 : Format des coordonnées
    lon = _parse_coord(site_id, contract_id, lon_raw, "G-02", "Longitude", -180, 180, collector)
    lat = _parse_coord(site_id, contract_id, lat_raw, "G-03", "Latitude", -90, 90, collector)

    if lon is None or lat is None:
        return collector.count() - anomalies_before

    # --- Cohérence géographique ---
    country = normalize_country(row.get(Config.COL_COUNTRY))
    cp = safe_str(row.get(Config.COL_POSTAL_CODE))

    # G-05 : GPS vs pays (si support ≥ PARTIEL)
    if support_level in (SupportLevel.COMPLET, SupportLevel.PARTIEL):
        _check_gps_vs_country(site_id, contract_id, lon, lat, country, ref_loader, collector)

    # G-04 : GPS vs département (si support COMPLET)
    if support_level == SupportLevel.COMPLET:
        _check_gps_vs_region(
            site_id, contract_id, lon, lat, country, cp, ref_loader, collector
        )

    return collector.count() - anomalies_before


# =============================================================================
# HELPERS PRIVÉS
# =============================================================================

def _parse_coord(
    site_id, contract_id, raw_value, code, label, min_val, max_val, collector
) -> Optional[float]:
    """Parse et valide une coordonnée. Retourne None si invalide."""
    try:
        # Accepter virgule comme séparateur décimal
        val = float(str(raw_value).replace(",", "."))
        if not (min_val <= val <= max_val):
            collector.add(Anomaly(
                site_id=site_id,
                contract_id=contract_id,
                code=code,
                severity=Severity.GRAVE,
                description=f"{label} '{val}' hors limites (attendu entre {min_val} et {max_val}).",
                fields_concerned=[Config.COL_LONGITUDE if code == "G-02" else Config.COL_LATITUDE],
                current_value=str(raw_value),
            ))
            return None
        return val
    except (ValueError, TypeError):
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code=code,
            severity=Severity.GRAVE,
            description=f"{label} '{raw_value}' n'est pas numérique.",
            fields_concerned=[Config.COL_LONGITUDE if code == "G-02" else Config.COL_LATITUDE],
            current_value=str(raw_value),
        ))
        return None


def _check_gps_vs_country(
    site_id, contract_id, lon, lat, country, ref_loader, collector
) -> bool:
    """
    G-05 : vérifie que le GPS est dans la bbox du pays.
    Ajoute aussi G-06 ou G-07 si un diagnostic est trouvé.
    Retourne True si GPS dans le bon pays.
    """
    bbox_pays = ref_loader.get_bbox_pays(country)
    if bbox_pays is None:
        return True  # Pas de bbox connue → on ne peut pas vérifier

    if is_in_bbox(lon, lat, bbox_pays):
        return True  # GPS dans le bon pays ✅

    # G-06 : Inversion X/Y ?
    if is_in_bbox(lat, lon, bbox_pays):
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="G-06",
            severity=Severity.INFO,
            description=(
                f"Coordonnées probablement inversées. "
                f"En échangeant X et Y, le point ({lat}, {lon}) serait dans {country}."
            ),
            fields_concerned=[Config.COL_LONGITUDE, Config.COL_LATITUDE],
            current_value=f"Lon={lon}, Lat={lat}",
            suggestion=f"Inverser : Lon={lat}, Lat={lon}",
        ))
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="G-05",
            severity=Severity.GRAVE,
            description=f"GPS ({lon}, {lat}) situé hors de {country}.",
            fields_concerned=[Config.COL_LONGITUDE, Config.COL_LATITUDE, Config.COL_COUNTRY],
            current_value=f"Lon={lon}, Lat={lat}, Pays={country}",
        ))
        return False

    # G-07 : Signe erroné ?
    combos = [(-lon, lat), (lon, -lat), (-lon, -lat)]
    for test_lon, test_lat in combos:
        if is_in_bbox(test_lon, test_lat, bbox_pays):
            collector.add(Anomaly(
                site_id=site_id,
                contract_id=contract_id,
                code="G-07",
                severity=Severity.INFO,
                description=(
                    f"Signe probablement erroné. "
                    f"Avec ({test_lon}, {test_lat}), le point serait dans {country}."
                ),
                fields_concerned=[Config.COL_LONGITUDE, Config.COL_LATITUDE],
                current_value=f"Lon={lon}, Lat={lat}",
                suggestion=f"Essayer : Lon={test_lon}, Lat={test_lat}",
            ))
            break

    # G-05
    collector.add(Anomaly(
        site_id=site_id,
        contract_id=contract_id,
        code="G-05",
        severity=Severity.GRAVE,
        description=f"GPS ({lon}, {lat}) situé hors de {country}.",
        fields_concerned=[Config.COL_LONGITUDE, Config.COL_LATITUDE, Config.COL_COUNTRY],
        current_value=f"Lon={lon}, Lat={lat}, Pays={country}",
    ))
    return False


def _check_gps_vs_region(
    site_id, contract_id, lon, lat, country, cp, ref_loader, collector
) -> None:
    """G-04 : vérifie que le GPS est dans la région/zone déduite du CP si la règle pays existe."""
    if not cp:
        return

    region_code = extract_region_code(country, cp)
    if not region_code:
        return

    bbox_dept = ref_loader.get_bbox_region(country, region_code)
    if bbox_dept is None:
        return

    if not is_in_bbox(lon, lat, bbox_dept):
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="G-04",
            severity=Severity.LEGERE,
            description=(
                f"GPS ({lon}, {lat}) hors de la région/zone {region_code}. "
                f"Le CP {cp} indique cette zone."
            ),
            fields_concerned=[Config.COL_LONGITUDE, Config.COL_LATITUDE, Config.COL_POSTAL_CODE],
            current_value=f"Lon={lon}, Lat={lat}, Region={region_code}",
        ))
