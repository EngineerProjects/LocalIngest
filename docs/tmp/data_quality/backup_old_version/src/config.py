"""
Configuration centralisée du projet — version simplifiée.

La logique métier est volontairement limitée à quelques contrôles lisibles :
- présence du GPS ;
- cohérence GPS / pays ;
- cohérence GPS / code postal quand le référentiel existe ;
- présence de l'adresse ;
- présence du numéro de voie ;
- contrôles faibles priorité sur la rue et le département.
"""

from pathlib import Path
from typing import List, Optional


class Config:
    """Configuration globale du pipeline."""

    # ----- Chemins -----
    BASE_DIR = Path(__file__).resolve().parent.parent
    DATA_DIR = BASE_DIR / "data"
    INPUT_DIR = DATA_DIR / "inputs"
    REFERENCE_DIR = DATA_DIR / "references"
    OUTPUT_DIR = DATA_DIR / "outputs"

    # Fichier source
    INPUT_FILE = INPUT_DIR / "export_Saas_2026-03-03 1.csv"

    # Références
    BBOX_PAYS_GLOBAL_FILE = REFERENCE_DIR / "bbox_pays_global.json"
    CP_FILENAME = "codes_postaux.csv"
    GEOJSON_FILENAMES = [
        "departements.geojson",
        "provinces.geojson",
        "regions.geojson",
        "provincias.geojson",
    ]
    BBOX_REGIONS_FILENAME = "bbox_regions.json"

    # ----- Paramètres CSV source -----
    CSV_SEPARATOR = None
    CSV_ENCODING = None

    # ----- Filtre pays optionnel -----
    COUNTRY_FILTER: Optional[List[str]] = None

    # ----- Logs -----
    REFERENCE_LOG_DETAILS: bool = False

    # ----- Mapping des colonnes -----
    # Identification
    COL_SITE_ID = "ID_SITE"
    COL_CONTRACT_ID = "NU_CNT"
    COL_SITE_NAME = "NOM_SITE"
    COL_CLIENT_NAME = "NOM_CLI"

    # Filtre sites actifs
    COL_STOCK = "STOCK"
    COL_DT_RESIL = "DT_EFF_RESIL_CNT"

    # Adresse
    COL_COUNTRY = "PAYS_SITE"
    COL_POSTAL_CODE = "CP_SITE"
    COL_CITY = "VILLE_SITE"
    COL_STREET_FULL = "NUM_RUE_SITE"
    COL_STREET_NAME = "RUE_SITE"
    COL_STREET_NUMBER = "NUM_VOIE_SITE"
    COL_LIEU_DIT = "LIEU_DIT_SITE"
    COL_FULL_ADDRESS = "ADRESSE_SITE"

    # GPS
    COL_LONGITUDE = "COORD_X_SITE"
    COL_LATITUDE = "COORD_Y_SITE"
    COL_GEO_QUALITY = "QUALITE_GEOCODAGE"

    # Colonnes export métier
    BUSINESS_EXPORT_COLUMNS = [
        COL_CONTRACT_ID,
        COL_SITE_ID,
        COL_CLIENT_NAME,
        COL_SITE_NAME,
        COL_COUNTRY,
        COL_POSTAL_CODE,
        COL_CITY,
        COL_STREET_NUMBER,
        COL_STREET_NAME,
        COL_STREET_FULL,
        COL_FULL_ADDRESS,
        COL_LONGITUDE,
        COL_LATITUDE,
        "_CONTROL_MODE",
        "_CONTROL_STATUS",
        "_PRIORITY",
        "_ISSUE_COUNT",
        "_ISSUE_CODES",
        "_ISSUE_SUMMARY",
    ]

    ENRICHED_EXPORT_COLUMNS = BUSINESS_EXPORT_COLUMNS + [
        "_SITE_KEY",
        "_HAS_GPS",
        "_GPS_IN_COUNTRY",
        "_GPS_MATCH_POSTAL_CODE",
        "_ADDRESS_PRESENT",
        "_STREET_NUMBER_PRESENT",
        "_DEPARTMENT_CODE",
        "_DEPARTMENT_COUNTRY_OK",
    ]

    DETAIL_ANOMALY_COLUMNS = [
        COL_SITE_ID,
        COL_CONTRACT_ID,
        "CODE_ANOMALIE",
        "LIBELLE",
        "GRAVITE",
        "CATEGORIE",
        "DESCRIPTION",
        "CHAMPS_CONCERNES",
        "VALEUR_ACTUELLE",
        "SUGGESTION",
        "SCORE_SIMILARITE",
    ]


class SupportLevel:
    """Niveau de couverture des références."""

    COMPLET = "COMPLET"
    PARTIEL = "PARTIEL"
    NON_SUPPORTE = "NON_SUPPORTÉ"


class Priority:
    """Niveau de priorité métier pour la correction."""

    HIGH = "HAUTE"
    MEDIUM = "MOYENNE"
    LOW = "FAIBLE"
    NONE = "OK"


class ControlStatus:
    """Statut de lecture rapide dans le rapport."""

    OK = "OK"
    TO_FIX = "A_CORRIGER"
    TO_COMPLETE = "A_COMPLETER"
    TO_REVIEW = "A_VERIFIER"


class ValidationMode:
    """Chemin de validation retenu pour le site."""

    GPS = "GPS_OK"
    ADDRESS = "ADRESSE_OK"
    INCOMPLETE = "INCOMPLET"


class IssueCode:
    """Catalogue simplifié des anomalies par site."""

    DEFINITIONS = {
        "P-01": ("Pays manquant", Priority.HIGH),
        "P-02": ("Code postal manquant", Priority.MEDIUM),
        "R-01": ("Pays non couvert", Priority.LOW),
        "G-01": ("GPS manquant", Priority.MEDIUM),
        "G-02": ("GPS incomplet", Priority.MEDIUM),
        "G-03": ("GPS invalide", Priority.HIGH),
        "G-04": ("GPS hors pays", Priority.HIGH),
        "G-05": ("GPS incohérent avec le code postal", Priority.HIGH),
        "A-01": ("Adresse manquante", Priority.HIGH),
        "A-02": ("Numéro de voie manquant", Priority.MEDIUM),
        "A-03": ("Rue à vérifier manuellement", Priority.LOW),
        "D-01": ("Département / pays à vérifier", Priority.LOW),
    }

    @classmethod
    def get_label(cls, code: str) -> str:
        return cls.DEFINITIONS.get(code, ("Code inconnu", Priority.LOW))[0]

    @classmethod
    def get_priority(cls, code: str) -> str:
        return cls.DEFINITIONS.get(code, ("Code inconnu", Priority.LOW))[1]
