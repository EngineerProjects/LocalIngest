"""
Règles GPS — Validation des coordonnées géographiques.

Appliquées quand le GPS existe (au moins une coordonnée non null) :
- G-01 : GPS incomplet (une seule coordonnée)
- G-02 : Format longitude invalide
- G-03 : Format latitude invalide
- G-04 : GPS incohérent avec le département
- G-05 : GPS hors du pays renseigné
- G-06 : Cause probable : X et Y inversés
- G-07 : Cause probable : signe erroné
"""

from typing import Any

import pandas as pd

from src.config import Config, Severity
from src.loader import ReferenceData
from src.models import Anomaly, AnomalyCollector
from src.utils import extract_department, is_in_bbox, is_not_empty, safe_str


def validate_gps(
    site_id: Any,
    contract_id: Any,
    row: pd.Series,
    ref_data: ReferenceData,
    collector: AnomalyCollector,
) -> int:
    """
    Valide les coordonnées GPS (format + cohérence).

    Returns:
        Nombre d'anomalies GPS détectées
    """
    anomalies_before = collector.count()

    lon_raw = row.get(Config.COL_LONGITUDE)
    lat_raw = row.get(Config.COL_LATITUDE)
    has_lon = is_not_empty(lon_raw)
    has_lat = is_not_empty(lat_raw)

    # G-01 : GPS incomplet (une seule coordonnée)
    if has_lon != has_lat:
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="G-01",
            severity=Severity.LEGERE,
            description="Une seule coordonnée GPS renseignée.",
            fields_concerned=[Config.COL_LONGITUDE, Config.COL_LATITUDE],
            current_value=f"Lon={lon_raw}, Lat={lat_raw}",
        ))
        return collector.count() - anomalies_before

    if not has_lon and not has_lat:
        return 0

    # G-02 / G-03 : Format des coordonnées
    lon = _parse_coordinate(site_id, contract_id, lon_raw, "G-02", "longitude", -180, 180, collector)
    lat = _parse_coordinate(site_id, contract_id, lat_raw, "G-03", "latitude", -90, 90, collector)

    if lon is None or lat is None:
        return collector.count() - anomalies_before

    # --- Cohérence GPS ---
    country = safe_str(row.get(Config.COL_COUNTRY)).upper()
    cp = safe_str(row.get(Config.COL_POSTAL_CODE))

    # G-05 : GPS vs pays
    _check_gps_vs_country(site_id, contract_id, lon, lat, country, ref_data, collector)

    # G-04 : GPS vs département
    _check_gps_vs_department(site_id, contract_id, lon, lat, cp, ref_data, collector)

    return collector.count() - anomalies_before


# =============================================================================
# HELPERS PRIVÉES
# =============================================================================

def _parse_coordinate(
    site_id, contract_id, raw_value, code, label, min_val, max_val, collector
) -> float | None:
    """Parse et valide une coordonnée. Retourne None si invalide."""
    try:
        val = float(raw_value)
        if not (min_val <= val <= max_val):
            collector.add(Anomaly(
                site_id=site_id,
                contract_id=contract_id,
                code=code,
                severity=Severity.GRAVE,
                description=f"{label.capitalize()} '{val}' hors limites ({min_val} à {max_val}).",
                fields_concerned=[Config.COL_LONGITUDE if "lon" in label else Config.COL_LATITUDE],
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
            description=f"{label.capitalize()} '{raw_value}' n'est pas numérique.",
            fields_concerned=[Config.COL_LONGITUDE if "lon" in label else Config.COL_LATITUDE],
            current_value=str(raw_value),
        ))
        return None


def _check_gps_vs_country(
    site_id, contract_id, lon, lat, country, ref_data, collector
) -> None:
    """Vérifie que le GPS est dans le pays renseigné (G-05, G-06, G-07)."""
    if not country:
        return

    bbox_pays = ref_data.get_bbox_pays(country)
    if not bbox_pays:
        return

    if is_in_bbox(lon, lat, bbox_pays):
        return  # OK

    # G-06 : Inversion X/Y ?
    if is_in_bbox(lat, lon, bbox_pays):
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="G-06",
            severity=Severity.INFO,
            description="Le GPS est valide si on inverse longitude et latitude.",
            fields_concerned=[Config.COL_LONGITUDE, Config.COL_LATITUDE],
            current_value=f"Lon={lon}, Lat={lat}",
            suggestion=f"Inverser : Lon={lat}, Lat={lon}",
        ))
        return

    # G-07 : Signe erroné ?
    if is_in_bbox(-lon, lat, bbox_pays) or is_in_bbox(lon, -lat, bbox_pays):
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="G-07",
            severity=Severity.INFO,
            description="Le GPS est valide si on corrige le signe d'une coordonnée.",
            fields_concerned=[Config.COL_LONGITUDE, Config.COL_LATITUDE],
            current_value=f"Lon={lon}, Lat={lat}",
        ))
        return

    # G-05 : GPS hors pays
    collector.add(Anomaly(
        site_id=site_id,
        contract_id=contract_id,
        code="G-05",
        severity=Severity.GRAVE,
        description=f"Le GPS ({lon}, {lat}) est hors du pays '{country}'.",
        fields_concerned=[Config.COL_LONGITUDE, Config.COL_LATITUDE, Config.COL_COUNTRY],
        current_value=f"Lon={lon}, Lat={lat}, Pays={country}",
    ))


def _check_gps_vs_department(
    site_id, contract_id, lon, lat, cp, ref_data, collector
) -> None:
    """Vérifie que le GPS est dans le département déduit du CP (G-04)."""
    if not cp:
        return

    dept = extract_department(cp)
    if not dept:
        return

    bbox_dept = ref_data.get_bbox_departement(dept)
    if not bbox_dept:
        return

    if not is_in_bbox(lon, lat, bbox_dept):
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="G-04",
            severity=Severity.GRAVE,
            description=f"Le GPS ({lon}, {lat}) est hors du département {dept}.",
            fields_concerned=[Config.COL_LONGITUDE, Config.COL_LATITUDE, Config.COL_POSTAL_CODE],
            current_value=f"Lon={lon}, Lat={lat}, Dept={dept}",
        ))
