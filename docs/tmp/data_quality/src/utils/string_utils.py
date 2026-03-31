"""
Utilitaires chaînes de caractères.

- is_empty / is_not_empty    : vérification de valeurs vides
- normalize_string           : normalise pour comparaison (lowercase, trim)
- normalize_country          : normalise les libellés pays (uppercase, trim)
- normalize_postal_code      : normalisation générique des codes postaux
- normalize_contract         : nettoie un numéro de contrat (zéros à gauche)
- safe_str                   : conversion sûre en string
- detect_encoding            : détection d'encodage d'un fichier
- detect_separator           : détection du séparateur CSV (;, |, ,)
- format_number              : formatage avec séparateurs de milliers
- extract_postal_code        : extrait un CP à 5 chiffres depuis une adresse
- extract_street_number      : extrait le numéro de rue depuis une adresse
- parse_coordinate           : convertit et valide une coordonnée GPS
"""

import re
from pathlib import Path
from typing import Optional

import pandas as pd


# ==============================================================================
# VÉRIFICATIONS VIDE / NON VIDE
# ==============================================================================

def is_empty(value) -> bool:
    """Vérifie si une valeur est vide (None, NaN, chaîne vide)."""
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def is_not_empty(value) -> bool:
    """Vérifie si une valeur n'est pas vide."""
    return not is_empty(value)


# ==============================================================================
# NORMALISATION CHAÎNES
# ==============================================================================

def safe_str(value) -> str:
    """Convertit une valeur en string de manière sécurisée."""
    if is_empty(value):
        return ""
    return str(value).strip()


def normalize_string(s: str) -> str:
    """Normalise une chaîne pour comparaison (minuscules, trim, espaces multiples)."""
    if is_empty(s):
        return ""
    return " ".join(str(s).lower().strip().split())


def normalize_country(value) -> str:
    """Normalise un libellé pays (majuscules, espaces compactés)."""
    if is_empty(value):
        return ""
    return " ".join(str(value).strip().split()).upper()


def normalize_postal_code(value, numeric_width: Optional[int] = None) -> str:
    """
    Normalise un code postal de façon générique.

    - trim + uppercase
    - suppression des espaces et tirets
    - padding si numérique et largeur connue
    """
    if is_empty(value):
        return ""

    code = str(value).strip().upper().replace(" ", "").replace("-", "")
    if code.endswith(".0") and code[:-2].isdigit():
        code = code[:-2]

    if numeric_width and code.isdigit():
        return code.zfill(numeric_width)

    return code


def normalize_contract(value) -> str:
    """
    Normalise un numéro de contrat.

    - Conserve les zéros à gauche
    - Supprime les espaces
    - Retire le suffixe '.0' si présent (artéfact pandas)
    - Retourne "" si vide
    """
    if is_empty(value):
        return ""

    value = str(value).strip()

    if value.endswith(".0") and value[:-2].isdigit():
        value = value[:-2]

    return value.strip()


# ==============================================================================
# FICHIERS — ENCODAGE ET SÉPARATEUR
# ==============================================================================

def detect_encoding(file_path: Path) -> str:
    """Détecte l'encodage d'un fichier en testant les encodages courants."""
    encodings_to_try = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]
    for encoding in encodings_to_try:
        try:
            with open(file_path, "r", encoding=encoding) as f:
                f.read(10000)
            return encoding
        except UnicodeDecodeError:
            continue
    return "utf-8"


def detect_separator(file_path: Path, encoding: str = "utf-8") -> str:
    """
    Détecte le séparateur CSV depuis la première ligne du fichier.

    Candidates : ';', '|', ','
    Retourne le candidat le plus fréquent sur la première ligne.
    """
    candidates = (";", "|", ",")
    try:
        with open(file_path, "r", encoding=encoding) as f:
            first_line = f.readline()
        return max(candidates, key=first_line.count)
    except Exception:
        return ";"


# ==============================================================================
# EXTRACTION DEPUIS ADRESSE
# ==============================================================================

_RE_POSTAL_CODE = re.compile(r"\b(\d{5})\b")
_RE_STREET_NUMBER = re.compile(r"^(\d{1,4})\s")


def extract_postal_code(text: str) -> str:
    """
    Extrait un code postal à 5 chiffres depuis un texte d'adresse.

    Priorité : CP_SITE (colonne dédiée) > cette extraction depuis ADRESSE_SITE.
    Exemple : "92 rue de Pologne, 59800 Lille" → "59800"

    Retourne "" si rien trouvé.
    """
    if is_empty(text):
        return ""
    match = _RE_POSTAL_CODE.search(str(text))
    return match.group(1) if match else ""


def extract_street_number(text: str) -> str:
    """
    Extrait le numéro de rue en début d'une adresse complète.

    Regex : 1 à 4 chiffres suivis d'un espace en début de chaîne.
    Exemple : "92 rue de Pologne, 59800 Lille" → "92"

    Retourne "" si rien trouvé.
    """
    if is_empty(text):
        return ""
    match = _RE_STREET_NUMBER.match(str(text).strip())
    return match.group(1) if match else ""


# ==============================================================================
# GPS
# ==============================================================================

def parse_coordinate(value, minimum: float, maximum: float) -> Optional[float]:
    """
    Convertit une valeur brute en coordonnée GPS et vérifie qu'elle est dans la plage.

    Gère la virgule comme séparateur décimal.
    Retourne None si la valeur est invalide ou hors plage.
    """
    if is_empty(value):
        return None
    try:
        coord = float(str(value).strip().replace(",", "."))
    except (ValueError, TypeError):
        return None

    if not (minimum <= coord <= maximum):
        return None

    return coord


# ==============================================================================
# FORMATAGE
# ==============================================================================

def format_number(n: int) -> str:
    """Formate un nombre avec séparateurs de milliers (espace)."""
    return f"{int(n):,}".replace(",", " ")
