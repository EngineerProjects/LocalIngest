"""
Règles Adresse — Validation par adresse (fallback quand GPS absent ou anomalé).

- A-01 : Code postal manquant
- A-02 : Ville manquante
- A-03 : Rue manquante
- A-04 : Code postal non trouvé dans la référence
- A-05 : Incohérence CP / Ville (fuzzy matching)
"""

from typing import Any

import pandas as pd

from src.config import Config, Severity
from src.loader import ReferenceData
from src.models import Anomaly, AnomalyCollector
from src.utils import (
    find_best_match,
    is_empty,
    is_not_empty,
    normalize_string,
    safe_str,
)


def validate_address(
    site_id: Any,
    contract_id: Any,
    row: pd.Series,
    ref_data: ReferenceData,
    collector: AnomalyCollector,
) -> int:
    """
    Valide l'adresse (CP, ville, rue).

    Returns:
        Nombre d'anomalies d'adresse détectées
    """
    anomalies_before = collector.count()

    cp = row.get(Config.COL_POSTAL_CODE)
    city = row.get(Config.COL_CITY)
    country = safe_str(row.get(Config.COL_COUNTRY)).upper()

    # A-01 : CP manquant
    if is_empty(cp):
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="A-01",
            severity=Severity.GRAVE,
            description="Le code postal n'est pas renseigné.",
            fields_concerned=[Config.COL_POSTAL_CODE],
        ))

    # A-02 : Ville manquante
    if is_empty(city):
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="A-02",
            severity=Severity.GRAVE,
            description="La ville n'est pas renseignée.",
            fields_concerned=[Config.COL_CITY],
        ))

    # A-03 : Rue manquante
    has_rue = any(
        is_not_empty(row.get(col))
        for col in [Config.COL_STREET_NAME, Config.COL_STREET_FULL, Config.COL_FULL_ADDRESS]
    )
    if not has_rue:
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="A-03",
            severity=Severity.LEGERE,
            description="Aucune information de rue/adresse renseignée.",
            fields_concerned=[Config.COL_STREET_NAME, Config.COL_STREET_FULL],
        ))

    # A-04 / A-05 : Cohérence CP / Ville
    if is_not_empty(cp) and ref_data.has_cp_reference(country):
        _check_cp_city(site_id, contract_id, cp, city, country, ref_data, collector)

    return collector.count() - anomalies_before


# =============================================================================
# HELPER PRIVÉE
# =============================================================================

def _check_cp_city(
    site_id, contract_id, cp, city, country, ref_data, collector
) -> None:
    """Vérifie la cohérence CP / Ville (A-04, A-05)."""
    cp_normalized = safe_str(cp).zfill(5)
    expected_cities = ref_data.get_cities_for_cp(cp_normalized)

    if not expected_cities:
        # A-04 : CP non trouvé
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="A-04",
            severity=Severity.GRAVE,
            description=f"Le code postal '{cp}' n'existe pas dans la référence ({country}).",
            fields_concerned=[Config.COL_POSTAL_CODE],
            current_value=str(cp),
            suggestion="Vérifier le code postal",
        ))
        return

    if is_empty(city):
        return  # Déjà signalé par A-02

    # A-05 : Vérification de la cohérence CP / Ville
    city_normalized = normalize_string(city)
    expected_normalized = [normalize_string(c) for c in expected_cities]

    if city_normalized in expected_normalized:
        return  # OK

    best_match_norm, score = find_best_match(city_normalized, expected_normalized)

    # Retrouver le nom original
    best_display = ""
    if best_match_norm:
        try:
            idx = expected_normalized.index(best_match_norm)
            best_display = expected_cities[idx]
        except ValueError:
            best_display = best_match_norm

    if score >= Config.FUZZY_THRESHOLD_HIGH:
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="A-05",
            severity=Severity.LEGERE,
            description="Probable faute de frappe sur la ville.",
            fields_concerned=[Config.COL_POSTAL_CODE, Config.COL_CITY],
            current_value=f"CP={cp}, Ville={city}",
            suggestion=f"Vouliez-vous dire '{best_display}' ?",
            similarity_score=round(score, 1),
        ))
    elif score >= Config.FUZZY_THRESHOLD_MEDIUM:
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="A-05",
            severity=Severity.LEGERE,
            description="Ville douteuse pour ce code postal.",
            fields_concerned=[Config.COL_POSTAL_CODE, Config.COL_CITY],
            current_value=f"CP={cp}, Ville={city}",
            suggestion=f"Ville similaire : '{best_display}'",
            similarity_score=round(score, 1),
        ))
    else:
        cities_sample = ", ".join(expected_cities[:3])
        if len(expected_cities) > 3:
            cities_sample += f"... (+{len(expected_cities) - 3})"
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="A-05",
            severity=Severity.GRAVE,
            description=f"La ville '{city}' ne correspond pas au CP {cp}.",
            fields_concerned=[Config.COL_POSTAL_CODE, Config.COL_CITY],
            current_value=f"CP={cp}, Ville={city}",
            suggestion=f"Communes attendues : {cities_sample}",
            similarity_score=round(score, 1),
        ))
