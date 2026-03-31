import json
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from src.config import (
    BBOX_PAYS_GLOBAL_FILE,
    BBOX_REGIONS_FILENAME,
    CP_FILENAME,
    REFERENCE_DIR,
)


class ReferenceLoader:
    """Charge les fichiers de référence (bbox pays, régions, codes postaux)."""

    def __init__(self):
        self.bbox_pays: Dict[str, list] = {}
        self.bbox_regions: Dict[str, Dict[str, list]] = {}
        self.codes_postaux: Dict[str, str] = {}
        self.supported_countries: List[str] = []

    def discover_supported_countries(self) -> List[str]:
        """
        Découvre les pays avec données de référence disponibles.

        Un pays est considéré supporté dès qu'il dispose d'un sous-dossier dans
        REFERENCE_DIR. Les fichiers bbox_regions.json et codes_postaux.csv sont
        optionnels : leur absence dégrade la précision du contrôle, mais ne bloque
        pas l'analyse.
        """
        if not REFERENCE_DIR.exists():
            print(f"⚠️ Dossier référentiel introuvable : {REFERENCE_DIR}")
            return []

        countries = []
        for item in REFERENCE_DIR.iterdir():
            if item.is_dir():
                country = item.name.upper()
                countries.append(country)
                missing = []
                if not (item / BBOX_REGIONS_FILENAME).exists():
                    missing.append(BBOX_REGIONS_FILENAME)
                if not (item / CP_FILENAME).exists():
                    missing.append(CP_FILENAME)
                if missing:
                    print(f"   ⚠️ {country} : fichiers optionnels absents → {', '.join(missing)}")

        self.supported_countries = countries
        return countries

    def load(self, countries: list = None) -> None:
        """Charge les références pour les pays demandés."""
        self._load_bbox_pays()

        if countries is None:
            countries = self.discover_supported_countries()

        for country in (countries or []):
            self._load_bbox_regions(country)
            self._load_codes_postaux(country)

    def _load_bbox_pays(self) -> None:
        """Charge les bounding boxes des pays (fichier global)."""
        path = BBOX_PAYS_GLOBAL_FILE
        if not path.exists():
            print(f"⚠️ bbox pays global non trouvé : {path}")
            return

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        for country, bbox in data.items():
            self.bbox_pays[country.upper()] = bbox

        print(f"   BBox pays chargés : {len(self.bbox_pays)} pays")

    def _load_bbox_regions(self, country: str) -> None:
        """Charge les bounding boxes des régions/départements (par pays)."""
        if country not in self.bbox_regions:
            self.bbox_regions[country] = {}

        bbox_file = REFERENCE_DIR / country / BBOX_REGIONS_FILENAME
        if not bbox_file.exists():
            return

        with open(bbox_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.bbox_regions[country].update(data)
        print(f"   BBox régions chargées pour {country} : {len(data)} régions")

    def _load_codes_postaux(self, country: str) -> None:
        """Charge le mapping code postal → ville (par pays)."""
        cp_file = REFERENCE_DIR / country / CP_FILENAME
        if not cp_file.exists():
            return

        df = pd.read_csv(cp_file, dtype=str)
        if "code_postal" in df.columns and "nom_commune" in df.columns:
            self.codes_postaux.update(
                dict(zip(df["code_postal"], df["nom_commune"]))
            )
            print(f"   Codes postaux chargés pour {country} : {len(df)}")

    def get_bbox_pays(self, country: str) -> Optional[list]:
        """Retourne la bbox d'un pays (None si non trouvé)."""
        return self.bbox_pays.get(country.upper())

    def get_bbox_region(self, country: str, region_code: str) -> Optional[list]:
        """Retourne la bbox d'une région/département (None si non trouvé)."""
        return self.bbox_regions.get(country.upper(), {}).get(region_code)

    def get_ville_from_cp(self, postal_code: str) -> Optional[str]:
        """Retourne le nom de la ville pour un code postal."""
        return self.codes_postaux.get(postal_code)

    def is_country_supported(self, country: str) -> bool:
        """Vérifie si le pays est dans les références."""
        return country.upper() in self.supported_countries
