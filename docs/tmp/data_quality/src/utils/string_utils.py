"""
Utilitaires chaînes de caractères.

- is_empty / is_not_empty : vérification de valeurs vides
- normalize_string : normalisation pour comparaison (lowercase, trim)
- safe_str : conversion sûre en string
- detect_encoding : détection d'encodage d'un fichier
- format_number : formatage avec séparateurs de milliers
"""

from pathlib import Path

import pandas as pd


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
    return "utf-8"


def format_number(n: int) -> str:
    """Formate un nombre avec séparateurs de milliers (espace)."""
    return f"{int(n):,}".replace(",", " ")