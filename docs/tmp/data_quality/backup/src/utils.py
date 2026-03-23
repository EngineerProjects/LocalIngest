"""
Fonctions utilitaires générales.

- is_empty / is_not_empty : vérification de valeurs vides
- normalize_string : normalisation pour comparaison
- safe_str : conversion sûre en string
- detect_encoding : détection d'encodage d'un fichier
- format_number : formatage avec séparateurs de milliers
- extract_department : extraction code département depuis CP
- is_in_bbox : vérification point dans bounding box
- levenshtein_ratio / find_best_match : fuzzy matching
"""

from difflib import SequenceMatcher
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd


# =============================================================================
# VALEURS VIDES
# =============================================================================

def is_empty(value) -> bool:
    """Vérifie si une valeur est vide (None, NaN, chaîne vide)."""
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def is_not_empty(value) -> bool:
    """Vérifie si une valeur n'est pas vide."""
    return not is_empty(value)


# =============================================================================
# CHAÎNES
# =============================================================================

def normalize_string(s: str) -> str:
    """Normalise une chaîne pour comparaison (minuscules, trim, espaces multiples)."""
    if is_empty(s):
        return ""
    return " ".join(str(s).lower().strip().split())


def safe_str(value) -> str:
    """Convertit une valeur en string de manière sécurisée."""
    if is_empty(value):
        return ""
    return str(value).strip()


# =============================================================================
# FICHIERS
# =============================================================================

def detect_encoding(file_path: Path) -> str:
    """Détecte l'encodage d'un fichier en testant plusieurs encodages courants."""
    encodings_to_try = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]

    for encoding in encodings_to_try:
        try:
            with open(file_path, "r", encoding=encoding) as f:
                f.read(10000)
            return encoding
        except UnicodeDecodeError:
            continue

    return "utf-8"  # Fallback


def format_number(n: int) -> str:
    """Formate un nombre avec séparateurs de milliers (espace)."""
    return f"{n:,}".replace(",", " ")


# =============================================================================
# GÉO — DÉPARTEMENTS & BOUNDING BOXES
# =============================================================================

def extract_department(cp: str) -> str:
    """
    Extrait le code département d'un code postal français.
    Gère les cas spéciaux (Corse, DOM-TOM).
    """
    cp = safe_str(cp).zfill(5)
    if len(cp) < 2:
        return ""
    # DOM-TOM : 3 premiers chiffres
    if cp[:3] in ["971", "972", "973", "974", "976"]:
        return cp[:3]
    # Corse : 20 → 2A ou 2B
    if cp[:2] == "20":
        if len(cp) >= 3:
            return "2A" if cp[2] in "012" else "2B"
        return "20"
    # Cas général
    return cp[:2]


def is_in_bbox(lon: float, lat: float, bbox: List[float]) -> bool:
    """Vérifie si un point (lon, lat) est dans une bounding box [lon_min, lat_min, lon_max, lat_max]."""
    return bbox[0] <= lon <= bbox[2] and bbox[1] <= lat <= bbox[3]


# =============================================================================
# FUZZY MATCHING
# =============================================================================

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
