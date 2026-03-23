"""
Validation Adresse — Bloc D : A-01 à A-05.

Exécuté si GPS absent, incomplet, format invalide, ou hors pays.

Ordre de validation :
    A-01 → Code postal manquant     → GRAVE (bloquant)
    A-02 → Ville manquante          → GRAVE (bloquant)
    A-03 → Rue manquante            → LÉGÈRE (non bloquant)
    A-04 → CP inexistant en réf.    → GRAVE (bloquant, si réf. dispo)
    A-05 → Incohérence CP / Ville   → Variable selon score fuzzy
"""

from typing import Any

import pandas as pd

from src.config import Config, Severity
from src.loaders.reference_loader import ReferenceLoader
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
    ref_loader: ReferenceLoader,
    collector: AnomalyCollector,
) -> int:
    """
    Valide l'adresse (CP, ville, rue).

    Returns:
        Nombre d'anomalies d'adresse ajoutées (0 = adresse valide)
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

    # A-03 : Rue manquante (non bloquant)
    has_rue = any(
        is_not_empty(row.get(col))
        for col in [
            Config.COL_STREET_NAME,
            Config.COL_STREET_FULL,
            Config.COL_FULL_ADDRESS,
            Config.COL_LIEU_DIT,
        ]
    )
    if not has_rue:
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="A-03",
            severity=Severity.LEGERE,
            description="Aucune information de rue ou adresse renseignée.",
            fields_concerned=[Config.COL_STREET_NAME, Config.COL_STREET_FULL],
        ))

    # A-04 / A-05 : Cohérence CP / Ville (si référence disponible)
    if is_not_empty(cp) and ref_loader.has_cp_reference(country):
        _check_cp_city(site_id, contract_id, cp, city, country, ref_loader, collector)

    return collector.count() - anomalies_before


# =============================================================================
# HELPER PRIVÉ
# =============================================================================

def _check_cp_city(
    site_id, contract_id, cp, city, country, ref_loader, collector
) -> None:
    """Vérifie la cohérence CP / Ville (A-04, A-05)."""
    cp_normalized = safe_str(cp).zfill(5)
    expected_cities = ref_loader.get_cities_for_cp(country, cp_normalized)

    # A-04 : CP non trouvé dans la référence
    if not expected_cities:
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="A-04",
            severity=Severity.GRAVE,
            description=f"Le code postal '{cp}' n'existe pas dans la référence ({country}).",
            fields_concerned=[Config.COL_POSTAL_CODE],
            current_value=str(cp),
            suggestion="Vérifier le code postal.",
        ))
        return

    if is_empty(city):
        return  # Déjà signalé par A-02

    # A-05 : Cohérence CP / Ville via fuzzy matching
    city_normalized = normalize_string(city)

    if city_normalized in expected_cities:
        return  # Correspondance exacte ✅

    best_match, score = find_best_match(city_normalized, expected_cities)

    # Retrouver la casse d'origine du meilleur match
    best_display = best_match  # les villes sont déjà normalisées dans le loader

    # Construire un aperçu des communes attendues
    cities_sample = ", ".join(expected_cities[:3])
    if len(expected_cities) > 3:
        cities_sample += f" ... (+{len(expected_cities) - 3})"

    if score >= 90:
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="A-05",
            severity=Severity.LEGERE,
            description="Probable faute de frappe sur le nom de ville.",
            fields_concerned=[Config.COL_POSTAL_CODE, Config.COL_CITY],
            current_value=f"CP={cp}, Ville={city}",
            suggestion=f"Vouliez-vous dire '{best_display}' ?",
            similarity_score=round(score, 1),
        ))
    elif score >= 70:
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="A-05",
            severity=Severity.LEGERE,
            description="Ville douteuse pour ce code postal.",
            fields_concerned=[Config.COL_POSTAL_CODE, Config.COL_CITY],
            current_value=f"CP={cp}, Ville={city}",
            suggestion=f"Ville similaire trouvée : '{best_display}'. Communes du CP : {cities_sample}",
            similarity_score=round(score, 1),
        ))
    else:
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="A-05",
            severity=Severity.GRAVE,
            description=f"La ville '{city}' ne correspond pas au CP {cp}.",
            fields_concerned=[Config.COL_POSTAL_CODE, Config.COL_CITY],
            current_value=f"CP={cp}, Ville={city}",
            suggestion=f"Communes attendues pour ce CP : {cities_sample}",
            similarity_score=round(score, 1),
        ))
