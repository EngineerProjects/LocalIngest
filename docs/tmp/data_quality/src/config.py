from pathlib import Path
from typing import Optional, List

# ==============================================================================
# CHEMINS
# ==============================================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "inputs"
REFERENCE_DIR = DATA_DIR / "references"
OUTPUT_DIR = DATA_DIR / "outputs"

INPUT_FILE = INPUT_DIR / "export_Saas_2026-03-03 1.csv"
DATAMART_FILE = INPUT_DIR / "ptf202602.csv"

BBOX_PAYS_GLOBAL_FILE = REFERENCE_DIR / "bbox_pays_global.json"
BBOX_REGIONS_FILENAME = "bbox_regions.json"
CP_FILENAME = "codes_postaux.csv"


# ==============================================================================
# COLONNES À LIRE
# ==============================================================================

COLUMNS_PROXIMA = [
    "ID_SITE", "NU_CNT", "NOM_SITE", "NOM_CLI",
    "STOCK", "DT_EFF_RESIL_CNT",
    "PAYS_SITE", "CP_SITE", "VILLE_SITE",
    "NUM_RUE_SITE", "RUE_SITE", "NUM_VOIE_SITE", "LIEU_DIT_SITE", "ADRESSE_SITE",
    "COORD_X_SITE", "COORD_Y_SITE", "QUALITE_GEOCODAGE",
    "CAPITAUX_MURS_BATIMENTS", "CAPITAUX_CONTENU", "CAPITAUX_MATERIEL",
    "CAPITAUX_MARCHANDISES", "CAPITAUX_TT", "CAPITAUX_ASS_MH_HLL_CARAV",
    "CAPITAUX_DEPENDANCE", "CAPITAUX_COMPL", "CAPITAUX_DD",
    "SURFACE", "SURFACE_DPDCE",
]

COLUMNS_DATAMART = ["NOPOL", "PTGST", "CDSITP"]


# ==============================================================================
# CODES ANOMALIES
# ==============================================================================

class IssueCode:
    """Catalogue des anomalies."""

    DEFINITIONS = {
        "P-01": "Pays manquant",
        "P-02": "Code postal manquant",
        "R-01": "Pays non trouvé dans le référentiel",
        "G-02": "Une seule coordonnée GPS (X ou Y) renseignée",
        "G-03": "Coordonnées GPS (X,Y) non valides",
        "G-04": "Coordonnées GPS (X,Y) hors pays",
        "G-05": "GPS (X,Y) incohérent avec le code postal",
        "G-06": "Coordonnées GPS (X,Y) potentiellement inversées",
        "A-01": "Adresse manquante",
        "A-02": "Numéro de rue manquant",
    }

    @classmethod
    def get_label(cls, code: str) -> str:
        return cls.DEFINITIONS.get(code, "Code inconnu")


# ==============================================================================
# MODES DE VALIDATION
# ==============================================================================

class ValidationMode:
    GPS = "GPS_OK"
    ADDRESS = "ADRESSE_OK"
    INCOMPLETE = "INCOMPLET"
