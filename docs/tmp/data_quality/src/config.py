"""
Configuration centralisée du projet — Contrôle Qualité Proxima v2.

Contient :
- Config : chemins, colonnes, paramètres de validation
- SupportLevel : niveaux de support par pays (COMPLET / PARTIEL / NON_SUPPORTÉ)
- Severity : niveaux de gravité
- AnomalyCode : définitions des anomalies
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

    # Fichier de référence global (fallback pour tous les pays)
    BBOX_PAYS_GLOBAL_FILE = REFERENCE_DIR / "bbox_pays_global.json"

    # Noms de fichiers attendus dans chaque dossier pays
    # Ex : data/references/FRANCE/codes_postaux.csv
    CP_FILENAME = "codes_postaux.csv"
    GEOJSON_FILENAMES = [
        "departements.geojson",
        "provinces.geojson",
        "regions.geojson",
        "provincias.geojson",
    ]
    # Fichier de bounding boxes généré (dans le dossier du pays)
    BBOX_REGIONS_FILENAME = "bbox_regions.json"

    # ----- Paramètres CSV source -----
    CSV_SEPARATOR = None    # Auto-détection (';' ou ',')
    CSV_ENCODING = None     # Auto-détection

    # ----- Filtre par pays (optionnel, surtout utile en debug) -----
    # None = automatique, tous les pays presents dans le fichier
    COUNTRY_FILTER: Optional[List[str]] = None

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
    FUZZY_THRESHOLD_HIGH = 90     # Probable faute de frappe
    FUZZY_THRESHOLD_MEDIUM = 70   # Ville douteuse


# =============================================================================
# NIVEAUX DE SUPPORT PAR PAYS
# =============================================================================

class SupportLevel:
    """Niveaux de support géographique par pays."""

    COMPLET = "COMPLET"         # Dossier pays avec codes_postaux.csv + geojson
    PARTIEL = "PARTIEL"         # Pas de dossier pays, mais présent dans bbox_pays_global
    NON_SUPPORTE = "NON_SUPPORTÉ"  # Absent de bbox_pays_global aussi


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

    DEFINITIONS = {
        # ----- Indépendants (toujours exécutés) -----
        "P-07": ("Données techniques manquantes", Severity.GRAVE, "prérequis"),

        # ----- Prérequis pays -----
        "P-01": ("Pays manquant", Severity.GRAVE, "prérequis"),
        "R-01": ("Pays non supporté (pas de référence géographique)", Severity.INFO, "référence"),

        # ----- GPS -----
        "G-01": ("GPS incomplet (une seule coordonnée)", Severity.LEGERE, "gps"),
        "G-02": ("Format longitude invalide", Severity.GRAVE, "gps"),
        "G-03": ("Format latitude invalide", Severity.GRAVE, "gps"),
        "G-04": ("GPS hors région/département", Severity.LEGERE, "gps"),
        "G-05": ("GPS hors du pays renseigné", Severity.GRAVE, "gps"),
        "G-06": ("Cause probable : X et Y inversés", Severity.INFO, "gps"),
        "G-07": ("Cause probable : signe erroné", Severity.INFO, "gps"),

        # ----- Adresse -----
        "A-01": ("Code postal manquant", Severity.GRAVE, "adresse"),
        "A-02": ("Ville manquante", Severity.GRAVE, "adresse"),
        "A-03": ("Rue manquante", Severity.LEGERE, "adresse"),
        "A-04": ("Code postal non trouvé dans la référence", Severity.GRAVE, "adresse"),
        "A-05": ("Incohérence CP / Ville", Severity.LEGERE, "adresse"),

        # ----- Localisation finale -----
        "L-01": ("Site non localisable (ni GPS ni adresse valide)", Severity.GRAVE, "localisation"),
    }

    @classmethod
    def get_label(cls, code: str) -> str:
        return cls.DEFINITIONS.get(code, ("Inconnu", Severity.INFO, "inconnu"))[0]

    @classmethod
    def get_severity(cls, code: str) -> str:
        return cls.DEFINITIONS.get(code, ("Inconnu", Severity.INFO, "inconnu"))[1]

    @classmethod
    def get_category(cls, code: str) -> str:
        return cls.DEFINITIONS.get(code, ("Inconnu", Severity.INFO, "inconnu"))[2]
