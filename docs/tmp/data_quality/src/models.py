"""
Helpers métier pour consolider les anomalies par site.

Ici, on ne manipule plus des objets "1 anomalie = 1 ligne".
Le pipeline travaille avec des listes de codes d'anomalies par site,
puis construit un libellé unique et lisible.
"""

from typing import Iterable, List

from src.config import ControlStatus, IssueCode, Priority


_PRIORITY_ORDER = {
    Priority.NONE: 0,
    Priority.LOW: 1,
    Priority.MEDIUM: 2,
    Priority.HIGH: 3,
}


def ordered_unique_codes(codes: Iterable[str]) -> List[str]:
    """Déduplique les codes en conservant leur ordre d'apparition."""
    seen = set()
    ordered = []
    for code in codes:
        if not code or code in seen:
            continue
        seen.add(code)
        ordered.append(code)
    return ordered


def build_issue_labels(codes: Iterable[str]) -> List[str]:
    """Traduit une liste de codes en libellés métier courts."""
    return [IssueCode.get_label(code) for code in ordered_unique_codes(codes)]


def build_issue_summary(codes: Iterable[str]) -> str:
    """Construit le libellé consolidé affiché dans le rapport."""
    labels = build_issue_labels(codes)
    return " | ".join(labels)


def get_worst_priority(codes: Iterable[str]) -> str:
    """Retourne la priorité la plus haute rencontrée sur le site."""
    ordered = ordered_unique_codes(codes)
    if not ordered:
        return Priority.NONE

    return max(
        (IssueCode.get_priority(code) for code in ordered),
        key=lambda priority: _PRIORITY_ORDER.get(priority, 0),
    )


def build_control_status(codes: Iterable[str]) -> str:
    """Construit un statut de lecture rapide à partir de la priorité max."""
    priority = get_worst_priority(codes)
    if priority == Priority.NONE:
        return ControlStatus.OK
    if priority == Priority.HIGH:
        return ControlStatus.TO_FIX
    if priority == Priority.MEDIUM:
        return ControlStatus.TO_COMPLETE
    return ControlStatus.TO_REVIEW
