"""
Configuration centralisée du projet — Contrôle Qualité Proxima.

Contient :
- Config : chemins, colonnes, paramètres de validation
- Severity : niveaux de gravité
- AnomalyCode : définitions des 16 types d'anomalies

NOTE : Aucune donnée de référence n'est hardcodée ici.
       Tout vient des fichiers dans data/references/ chargés par ReferenceData.
"""

from pathlib import Path
from typing import List, Optional


# =============================================================================
# CONFIGURATION GLOBALE
# =============================================================================

class Config:
    """Configuration centralisée du projet."""

    # ----- Chemins -----
    BASE_DIR = Path(__file__).resolve().parent.parent
    DATA_DIR = BASE_DIR / "data"
    INPUT_DIR = DATA_DIR / "inputs"
    REFERENCE_DIR = DATA_DIR / "references"
    OUTPUT_DIR = DATA_DIR / "outputs"

    # Fichier source
    INPUT_FILE = INPUT_DIR / "export_Saas_2026-03-03 1.csv"

    # Fichiers de référence — SOURCES (à télécharger manuellement)
    GEOJSON_DEPT_FILE = REFERENCE_DIR / "departements_france.geojson"
    CP_REFERENCE_FILE = REFERENCE_DIR / "codes_postaux_france.csv"
    BBOX_PAYS_FILE = REFERENCE_DIR / "bbox_pays.json"

    # Fichiers de référence — GÉNÉRÉS (par setup.py)
    BBOX_DEPT_FILE = REFERENCE_DIR / "bbox_departements.json"

    # ----- Paramètres CSV source -----
    CSV_SEPARATOR = None   # Auto-détection (';' ou ',')
    CSV_ENCODING = None  # Auto-détection

    # ----- Filtre par pays (optionnel) -----
    # None = tous les pays, ["FRANCE"] = France uniquement
    # Permet de travailler sur un sous-ensemble pour le développement/test
    COUNTRY_FILTER: Optional[List[str]] = ["FRANCE"]

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

    # Colonnes techniques (capitaux)
    COLS_CAPITAL = [
        "CAPITAUX_MURS_BATIMENTS",
        "CAPITAUX_CONTENU",
        "CAPITAUX_MATERIEL",
        "CAPITAUX_MARCHANDISES",
        "CAPITAUX_TT",
        "CAPITAUX_ASS_MH_HLL_CARAV",
        "CAPITAUX_DEPENDANCE",
        "CAPITAUX_COMPL",
        "CAPITAUX_DD",
    ]

    # Colonnes techniques (surface)
    COLS_SURFACE = [
        "SURFACE",
        "SURFACE_DPDCE",
    ]

    # Toutes les colonnes techniques
    COLS_TECHNICAL = COLS_CAPITAL + COLS_SURFACE

    # ----- Paramètres de validation -----
    # Fuzzy matching seuils
    FUZZY_THRESHOLD_HIGH = 90     # Probable faute de frappe
    FUZZY_THRESHOLD_MEDIUM = 70   # Ville douteuse


# =============================================================================
# NIVEAUX DE GRAVITÉ
# =============================================================================

class Severity:
    """Niveaux de gravité des anomalies."""

    GRAVE = "GRAVE"
    LEGERE = "LÉGÈRE"
    INFO = "INFO"


# =============================================================================
# CODES ET DÉFINITIONS DES ANOMALIES
# =============================================================================

class AnomalyCode:
    """Codes et définitions des anomalies."""

    # Structure : code -> (libellé, gravité, catégorie)
    DEFINITIONS = {
        # ----- Toujours vérifiés (par site) -----
        "P-01": ("Pays manquant", Severity.GRAVE, "prérequis"),
        "P-07": ("Données techniques manquantes", Severity.GRAVE, "prérequis"),

        # ----- GPS (prioritaire quand il existe) -----
        "G-01": ("GPS incomplet (une seule coordonnée)", Severity.LEGERE, "gps"),
        "G-02": ("Format longitude invalide", Severity.GRAVE, "gps"),
        "G-03": ("Format latitude invalide", Severity.GRAVE, "gps"),
        "G-04": ("GPS incohérent avec le département", Severity.GRAVE, "gps"),
        "G-05": ("GPS hors du pays renseigné", Severity.GRAVE, "gps"),
        "G-06": ("Cause probable : X et Y inversés", Severity.INFO, "gps"),
        "G-07": ("Cause probable : signe erroné", Severity.INFO, "gps"),

        # ----- Adresse (fallback si GPS absent ou GPS avec anomalies) -----
        "A-01": ("Code postal manquant", Severity.GRAVE, "adresse"),
        "A-02": ("Ville manquante", Severity.GRAVE, "adresse"),
        "A-03": ("Rue manquante", Severity.LEGERE, "adresse"),
        "A-04": ("Code postal non trouvé dans la référence", Severity.GRAVE, "adresse"),
        "A-05": ("Incohérence CP / Ville", Severity.LEGERE, "adresse"),

        # ----- Site non localisable -----
        "L-01": ("Site non localisable (ni GPS ni adresse valide)", Severity.GRAVE, "localisation"),
    }

    @classmethod
    def get_label(cls, code: str) -> str:
        """Retourne le libellé d'un code anomalie."""
        return cls.DEFINITIONS.get(code, ("Inconnu", Severity.INFO, 0))[0]

    @classmethod
    def get_severity(cls, code: str) -> str:
        """Retourne la gravité d'un code anomalie."""
        return cls.DEFINITIONS.get(code, ("Inconnu", Severity.INFO, 0))[1]

    @classmethod
    def get_category(cls, code: str) -> str:
        """Retourne la catégorie d'un code anomalie."""
        return cls.DEFINITIONS.get(code, ("Inconnu", Severity.INFO, "inconnu"))[2]
