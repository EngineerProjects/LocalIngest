"""
Modèles de données pour le contrôle qualité.

- Anomaly : représente une anomalie détectée
- AnomalyCollector : collecteur centralisé d'anomalies
"""

from dataclasses import dataclass
from typing import Any, List, Optional

import pandas as pd

from src.config import AnomalyCode, Severity


# =============================================================================
# ANOMALY
# =============================================================================

@dataclass
class Anomaly:
    """Représente une anomalie détectée."""

    site_id: Any
    contract_id: Any
    code: str
    severity: str
    description: str
    fields_concerned: List[str]
    current_value: str = ""
    suggestion: Optional[str] = None
    similarity_score: Optional[float] = None

    def to_dict(self) -> dict:
        return {
            "ID_SITE": self.site_id,
            "NU_CNT": self.contract_id,
            "CODE_ANOMALIE": self.code,
            "LIBELLE": AnomalyCode.get_label(self.code),
            "GRAVITE": self.severity,
            "CATEGORIE": AnomalyCode.get_category(self.code),
            "DESCRIPTION": self.description,
            "CHAMPS_CONCERNES": ", ".join(self.fields_concerned),
            "VALEUR_ACTUELLE": self.current_value,
            "SUGGESTION": self.suggestion or "",
            "SCORE_SIMILARITE": self.similarity_score,
        }


# =============================================================================
# ANOMALY COLLECTOR
# =============================================================================

class AnomalyCollector:
    """Collecteur d'anomalies pour tout le traitement."""

    def __init__(self):
        self.anomalies: List[Anomaly] = []

    def add(self, anomaly: Anomaly):
        self.anomalies.append(anomaly)

    def to_dataframe(self) -> pd.DataFrame:
        if not self.anomalies:
            return pd.DataFrame()
        return pd.DataFrame([a.to_dict() for a in self.anomalies])

    def count(self) -> int:
        return len(self.anomalies)

    def count_by_severity(self) -> dict:
        counts = {Severity.GRAVE: 0, Severity.LEGERE: 0, Severity.INFO: 0}
        for a in self.anomalies:
            counts[a.severity] = counts.get(a.severity, 0) + 1
        return counts

    def count_by_code(self) -> dict:
        counts = {}
        for a in self.anomalies:
            counts[a.code] = counts.get(a.code, 0) + 1
        return dict(sorted(counts.items(), key=lambda x: x[1], reverse=True))