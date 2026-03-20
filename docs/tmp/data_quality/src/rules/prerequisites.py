"""
Règles prérequises — Vérifications systématiques.

Appliquées à chaque site, indépendamment du chemin GPS/Adresse :
- P-01 : Pays manquant
- P-07 : Données techniques manquantes
"""

from typing import Any

import pandas as pd

from src.config import Config, Severity
from src.models import Anomaly, AnomalyCollector
from src.utils import is_empty, is_not_empty


def check_prerequisites(
    site_id: Any,
    contract_id: Any,
    row: pd.Series,
    collector: AnomalyCollector,
) -> None:
    """Vérifie les prérequis : pays et données techniques."""

    # P-01 : Pays manquant
    if is_empty(row.get(Config.COL_COUNTRY)):
        collector.add(Anomaly(
            site_id=site_id,
            contract_id=contract_id,
            code="P-01",
            severity=Severity.GRAVE,
            description="Le pays du site n'est pas renseigné.",
            fields_concerned=[Config.COL_COUNTRY],
        ))

    # P-07 : Données techniques manquantes
    for col in Config.COLS_TECHNICAL:
        if col in row.index and is_not_empty(row[col]):
            return  # Au moins une valeur technique → OK

    collector.add(Anomaly(
        site_id=site_id,
        contract_id=contract_id,
        code="P-07",
        severity=Severity.GRAVE,
        description="Aucune donnée technique renseignée (capitaux, surface).",
        fields_concerned=Config.COLS_TECHNICAL,
    ))
