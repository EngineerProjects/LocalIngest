"""
Prérequis — Blocs A et B.

Bloc A (indépendant, toujours exécuté) :
    P-07 : Données techniques manquantes

Bloc B (prérequis pays, bloquant pour la localisation) :
    P-01 : Pays manquant
    R-01 : Pays non supporté (INFO)

Retourne :
    (country_ok, support_level)
    country_ok=False si P-01 → stoppe la validation de localisation
"""

from typing import Any, Tuple

import pandas as pd

from src.config import Config, Severity, SupportLevel
from src.loaders.reference_loader import ReferenceLoader
from src.models import Anomaly, AnomalyCollector
from src.utils import is_empty, is_not_empty, safe_str


def check_prerequisites(
    site_id: Any,
    contract_id: Any,
    row: pd.Series,
    ref_loader: ReferenceLoader,
    collector: AnomalyCollector,
) -> Tuple[bool, str]:
    """
    Vérifie les prérequis d'un site.

    Returns:
        (country_ok, support_level)
        - country_ok=False → pays manquant, localisation impossible
        - support_level → niveau de support du pays (COMPLET/PARTIEL/NON_SUPPORTÉ)
    """

    # ----- Bloc A : P-07 (indépendant, toujours) -----
    _check_p07(site_id, contract_id, row, collector)

    # ----- Bloc B : P-01 -----
    country_raw = row.get(Config.COL_COUNTRY)
    if is_empty(country_raw):
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="P-01",
            severity=Severity.GRAVE,
            description="Le pays du site n'est pas renseigné.",
            fields_concerned=[Config.COL_COUNTRY],
        ))
        return False, SupportLevel.NON_SUPPORTE

    # ----- Bloc B : R-01 -----
    country = safe_str(country_raw).upper()
    support = ref_loader.get_support_level(country)

    if support == SupportLevel.NON_SUPPORTE:
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="R-01",
            severity=Severity.INFO,
            description=(
                f"Pays '{country}' non supporté : "
                "aucune référence géographique disponible."
            ),
            fields_concerned=[Config.COL_COUNTRY],
            current_value=country,
        ))

    return True, support


# =============================================================================
# HELPER PRIVÉ
# =============================================================================

def _check_p07(
    site_id: Any,
    contract_id: Any,
    row: pd.Series,
    collector: AnomalyCollector,
) -> None:
    """P-07 : au moins une donnée technique doit être renseignée et > 0."""
    for col in Config.COLS_TECHNICAL:
        if col in row.index and is_not_empty(row[col]):
            try:
                if float(row[col]) > 0:
                    return  # OK
            except (ValueError, TypeError):
                return  # Valeur non numérique mais présente → considérée OK

    collector.add(Anomaly(
        site_id=site_id,
        contract_id=contract_id,
        code="P-07",
        severity=Severity.GRAVE,
        description="Aucune donnée technique renseignée (capitaux, surface).",
        fields_concerned=Config.COLS_TECHNICAL,
    ))