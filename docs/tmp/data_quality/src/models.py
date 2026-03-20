"""
Modèles de données pour le contrôle qualité.

- Anomaly : représente une anomalie détectée (dataclass)
- AnomalyCollector : collecteur centralisé d'anomalies
"""

from dataclasses import dataclass
from typing import Any, List, Optional

import pandas as pd

from src.config import AnomalyCode, Severity


# =============================================================================
# ANOMALY — STRUCTURE DE DONNÉES
# =============================================================================

@dataclass
class Anomaly:
    """Représente une anomalie détectée."""

    site_id: Any                             # ID du site
    contract_id: Any                         # Numéro de contrat
    code: str                                # Code anomalie (ex: "G-01")
    severity: str                            # Gravité
    description: str                         # Description détaillée
    fields_concerned: List[str]              # Champs concernés
    current_value: str = ""                  # Valeur actuelle
    category: str = ""                       # Catégorie (prérequis, gps, adresse, localisation)
    suggestion: Optional[str] = None         # Suggestion de correction
    similarity_score: Optional[float] = None # Score fuzzy (si applicable)

    def to_dict(self) -> dict:
        """Convertit l'anomalie en dictionnaire."""
        return {
            "ID_SITE": self.site_id,
            "NU_CNT": self.contract_id,
            "CODE_ANOMALIE": self.code,
            "LIBELLE": AnomalyCode.get_label(self.code),
            "GRAVITE": self.severity,
            "CATEGORIE": self.category,
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
        """Ajoute une anomalie."""
        self.anomalies.append(anomaly)

    def add_many(self, anomalies: List[Anomaly]):
        """Ajoute plusieurs anomalies."""
        self.anomalies.extend(anomalies)

    def to_dataframe(self) -> pd.DataFrame:
        """Convertit toutes les anomalies en DataFrame."""
        if not self.anomalies:
            return pd.DataFrame()
        return pd.DataFrame([a.to_dict() for a in self.anomalies])

    def count(self) -> int:
        """Retourne le nombre total d'anomalies."""
        return len(self.anomalies)

    def count_by_severity(self) -> dict:
        """Compte les anomalies par gravité."""
        counts = {Severity.GRAVE: 0, Severity.LEGERE: 0, Severity.INFO: 0}
        for a in self.anomalies:
            counts[a.severity] = counts.get(a.severity, 0) + 1
        return counts

    def count_by_code(self) -> dict:
        """Compte les anomalies par code."""
        counts = {}
        for a in self.anomalies:
            counts[a.code] = counts.get(a.code, 0) + 1
        return dict(sorted(counts.items(), key=lambda x: x[1], reverse=True))
