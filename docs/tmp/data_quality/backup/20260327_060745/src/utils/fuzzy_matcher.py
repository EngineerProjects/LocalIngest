"""
Utilitaires de matching flou (fuzzy matching).

- levenshtein_ratio : similarité entre deux chaînes (0 à 100)
- find_best_match : meilleure correspondance dans une liste
"""

from difflib import SequenceMatcher
from typing import List, Tuple


def levenshtein_ratio(s1: str, s2: str) -> float:
    """Ratio de similarité entre deux chaînes via SequenceMatcher (0 à 100)."""
    return SequenceMatcher(None, s1, s2).ratio() * 100


def find_best_match(target: str, candidates: List[str]) -> Tuple[str, float]:
    """
    Trouve la meilleure correspondance dans une liste de candidats.

    Returns:
        (meilleur_candidat, score)
    """
    if not candidates:
        return "", 0.0
    best_match = ""
    best_score = 0.0
    for candidate in candidates:
        score = levenshtein_ratio(target, candidate)
        if score > best_score:
            best_score = score
            best_match = candidate
    return best_match, best_score