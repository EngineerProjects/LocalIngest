"""
Chargement des données de référence — Architecture multi-pays v2.

Logique de support par pays :
  COMPLET     → dossier references/{PAYS}/ avec codes_postaux.csv + geojson
  PARTIEL     → pas de dossier pays, mais pays dans bbox_pays_global.json
  NON_SUPPORTÉ → absent de bbox_pays_global.json

Chargement au démarrage :
  1. Charger bbox_pays_global.json (fallback)
  2. Scanner les pays uniques dans les données source
  3. Pour chaque pays : détecter le niveau de support et charger les refs dispo
  4. Générer bbox_regions.json à la volée si le geojson est présent mais pas le json
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd

from src.config import Config, SupportLevel
from src.utils import (
    compute_bbox,
    detect_encoding,
    extract_all_coordinates,
    format_number,
    normalize_string,
    safe_str,
)


class ReferenceLoader:
    """
    Conteneur pour les données de référence multi-pays.

    Attributs après load() :
        support_levels        : dict pays_upper → SupportLevel
        cp_to_cities          : dict pays_upper → dict CP → [villes]
        countries_with_cp_ref : set de pays (upper) avec référence CP chargée
        bbox_regions          : dict pays_upper → dict code_region → {"name": ..., "bbox": [...]}
        bbox_pays_global      : dict pays_upper → {"bbox": [...]}
    """

    def __init__(self):
        self.support_levels: Dict[str, str] = {}
        self.cp_to_cities: Dict[str, Dict[str, List[str]]] = {}
        self.countries_with_cp_ref: Set[str] = set()
        self.bbox_regions: Dict[str, Dict[str, dict]] = {}
        self.bbox_pays_global: Dict[str, dict] = {}
        self._loaded = False

    # =========================================================================
    # CHARGEMENT PRINCIPAL
    # =========================================================================

    def load(self, source_countries: List[str]) -> None:
        """
        Charge les références pour les pays présents dans les données source.

        Args:
            source_countries: liste des pays uniques trouvés dans le CSV source
        """
        if self._loaded:
            return

        print("\n" + "=" * 60)
        print("   CHARGEMENT DES DONNÉES DE RÉFÉRENCE")
        print("=" * 60)

        # Étape 1 — Bounding boxes pays (fallback)
        self._load_bbox_pays_global()

        # Étape 2 — Pour chaque pays du fichier source
        unique_countries = [c.upper().strip() for c in source_countries if c]
        print(f"\n🌍 Pays détectés dans les données : {', '.join(sorted(unique_countries))}")

        for country in sorted(unique_countries):
            self._load_country(country)

        # Résumé
        print("\n📋 Niveaux de support :")
        print("-" * 45)
        for country, level in sorted(self.support_levels.items()):
            icon = {"COMPLET": "✅", "PARTIEL": "⚠️", "NON_SUPPORTÉ": "❌"}.get(level, "?")
            print(f"   {icon} {country:20} : {level}")

        self._loaded = True
        print("\n✅ Données de référence chargées")
        print("=" * 60)

    # =========================================================================
    # CHARGEMENT GLOBAL (bbox pays)
    # =========================================================================

    def _load_bbox_pays_global(self) -> None:
        """Charge bbox_pays_global.json — fallback pour tous les pays."""
        path = Config.BBOX_PAYS_GLOBAL_FILE
        if not path.exists():
            print(f"   ⚠️ bbox_pays_global.json non trouvé : {path}")
            return

        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        # Normaliser les clés en majuscules
        self.bbox_pays_global = {k.upper(): v for k, v in raw.items()}
        print(f"   ✅ bbox_pays_global : {len(self.bbox_pays_global)} pays")

    # =========================================================================
    # CHARGEMENT PAR PAYS
    # =========================================================================

    def _load_country(self, country: str) -> None:
        """Détecte le niveau de support d'un pays et charge ses références."""
        country_dir = self._find_country_dir(country)

        if country_dir is not None:
            # Dossier pays trouvé → tenter support COMPLET
            self._load_country_complet(country, country_dir)
        elif country in self.bbox_pays_global:
            # Pas de dossier mais bbox globale connue → PARTIEL
            self.support_levels[country] = SupportLevel.PARTIEL
        else:
            # Rien → NON SUPPORTÉ
            self.support_levels[country] = SupportLevel.NON_SUPPORTE

    def _find_country_dir(self, country: str) -> Optional[Path]:
        """
        Cherche le dossier références du pays (insensible à la casse).
        Ex : "FRANCE" → cherche FRANCE/, France/, france/ dans data/references/
        """
        ref_dir = Config.REFERENCE_DIR
        if not ref_dir.exists():
            return None

        for candidate in ref_dir.iterdir():
            if candidate.is_dir() and candidate.name.upper() == country:
                return candidate
        return None

    def _load_country_complet(self, country: str, country_dir: Path) -> None:
        """
        Tente de charger les refs complètes pour un pays.
        Charge CP et regions, puis décide COMPLET ou PARTIEL.
        """
        has_cp = self._load_cp_file(country, country_dir)
        has_regions = self._load_regions(country, country_dir)

        if has_cp and has_regions:
            self.support_levels[country] = SupportLevel.COMPLET
        elif country in self.bbox_pays_global:
            self.support_levels[country] = SupportLevel.PARTIEL
            if not has_cp:
                print(f"   ⚠️ {country} : codes_postaux.csv absent → support partiel")
            if not has_regions:
                print(f"   ⚠️ {country} : geojson absent → support partiel")
        else:
            self.support_levels[country] = SupportLevel.NON_SUPPORTE

    # =========================================================================
    # CHARGEMENT CODES POSTAUX
    # =========================================================================

    def _load_cp_file(self, country: str, country_dir: Path) -> bool:
        """
        Charge le fichier codes_postaux.csv du pays.
        Retourne True si chargé avec succès.
        """
        cp_file = country_dir / Config.CP_FILENAME
        if not cp_file.exists():
            return False

        encoding = detect_encoding(cp_file)
        df = pd.read_csv(cp_file, sep=";", dtype=str, encoding=encoding)

        # Nettoyer les noms de colonnes (retirer '#' artefact CSV)
        df.columns = [col.lstrip("#").strip() for col in df.columns]

        # Mapping flexible des colonnes CP / Ville
        rename_map = {}
        for col in df.columns:
            col_lower = col.lower()
            if "code_postal" in col_lower or "code postal" in col_lower:
                rename_map[col] = "code_postal"
            elif "nom_de_la_commune" in col_lower or "nom_commune" in col_lower or "commune" in col_lower:
                rename_map[col] = "nom_commune"
            elif "acheminement" in col_lower or "libelle" in col_lower:
                rename_map[col] = "nom_commune"

        if rename_map:
            df = df.rename(columns=rename_map)

        # Identifier colonnes CP et Ville
        cp_col = "code_postal" if "code_postal" in df.columns else df.columns[0]
        city_col = "nom_commune" if "nom_commune" in df.columns else df.columns[1]

        # Construire pays → CP → [villes]
        country_cp_map = self.cp_to_cities.setdefault(country, {})
        count_before = len(country_cp_map)
        for _, row in df.iterrows():
            cp = safe_str(row[cp_col]).zfill(5)
            city = normalize_string(row[city_col])
            if cp and city:
                if cp not in country_cp_map:
                    country_cp_map[cp] = []
                if city not in country_cp_map[cp]:
                    country_cp_map[cp].append(city)

        self.countries_with_cp_ref.add(country)
        added = len(country_cp_map) - count_before
        print(f"   ✅ {country} CP : {format_number(added)} codes postaux chargés")
        return True

    # =========================================================================
    # CHARGEMENT RÉGIONS / DÉPARTEMENTS (GeoJSON → bbox)
    # =========================================================================

    def _load_regions(self, country: str, country_dir: Path) -> bool:
        """
        Charge les bounding boxes des régions/départements.
        Utilise le JSON mis en cache s'il existe, sinon le génère depuis GeoJSON.
        Retourne True si des bounding boxes ont été chargées.
        """
        # Chercher d'abord le cache JSON généré
        bbox_cache = country_dir / Config.BBOX_REGIONS_FILENAME
        if bbox_cache.exists():
            with open(bbox_cache, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.bbox_regions[country] = data
            print(f"   ✅ {country} régions : {len(data)} bounding boxes (cache)")
            return True

        # Sinon, chercher un fichier GeoJSON
        geojson_path = self._find_geojson(country_dir)
        if geojson_path is None:
            return False

        data = self._generate_bbox_from_geojson(geojson_path)
        if not data:
            return False

        # Sauvegarder le cache pour les prochains runs
        with open(bbox_cache, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        self.bbox_regions[country] = data
        print(f"   ✅ {country} régions : {len(data)} bounding boxes (générées depuis GeoJSON)")
        return True

    def _find_geojson(self, country_dir: Path) -> Optional[Path]:
        """Cherche un fichier GeoJSON dans le dossier pays (noms connus ou premier .geojson)."""
        # Chercher les noms standards définis dans Config
        for filename in Config.GEOJSON_FILENAMES:
            candidate = country_dir / filename
            if candidate.exists():
                return candidate

        # Fallback : premier .geojson trouvé (insensible à la casse)
        for f in country_dir.iterdir():
            if f.suffix.lower() == ".geojson":
                return f

        return None

    def _generate_bbox_from_geojson(self, geojson_path: Path) -> Dict[str, dict]:
        """
        Génère un dict {code_region: {"name": ..., "bbox": [...]}} depuis un GeoJSON.
        Essaie plusieurs noms de propriétés courants pour code et nom.
        """
        with open(geojson_path, "r", encoding="utf-8") as f:
            geojson_data = json.load(f)

        features = geojson_data.get("features", [])
        bbox_dict = {}

        CODE_PROPS = ["code", "CODE", "id", "ID", "dep", "DEP", "num_dep"]
        NAME_PROPS = ["nom", "NOM", "name", "NAME", "libelle", "LIBELLE"]

        for feature in features:
            props = feature.get("properties", {})
            geometry = feature.get("geometry", {})

            # Trouver le code région
            code = None
            for key in CODE_PROPS:
                if key in props and props[key]:
                    code = str(props[key])
                    break
            if not code:
                continue

            # Trouver le nom région
            name = ""
            for key in NAME_PROPS:
                if key in props and props[key]:
                    name = str(props[key])
                    break

            coords = extract_all_coordinates(geometry)
            bbox = compute_bbox(coords)
            if bbox:
                bbox_dict[code] = {"name": name, "bbox": bbox}

        return dict(sorted(bbox_dict.items()))

    # =========================================================================
    # ACCESSEURS
    # =========================================================================

    def get_support_level(self, country: str) -> str:
        """Retourne le niveau de support d'un pays."""
        country_upper = country.upper().strip() if country else ""
        return self.support_levels.get(country_upper, SupportLevel.NON_SUPPORTE)

    def has_cp_reference(self, country: str) -> bool:
        """Vérifie si on a une référence CP pour ce pays."""
        return country.upper().strip() in self.countries_with_cp_ref

    def get_cities_for_cp(self, country: str, cp: str) -> List[str]:
        """Retourne la liste des villes normalisées pour un CP dans un pays donné."""
        country_upper = country.upper().strip() if country else ""
        cp_normalized = safe_str(cp).zfill(5)
        country_cp_map = self.cp_to_cities.get(country_upper, {})
        return country_cp_map.get(cp_normalized, [])

    def get_bbox_region(self, country: str, region_code: str) -> Optional[List[float]]:
        """
        Retourne la bbox d'une région/département pour un pays donné.
        Returns [lon_min, lat_min, lon_max, lat_max] ou None.
        """
        country_upper = country.upper().strip() if country else ""
        country_regions = self.bbox_regions.get(country_upper, {})
        entry = country_regions.get(region_code)
        if entry and "bbox" in entry:
            return entry["bbox"]
        return None

    def get_bbox_pays(self, country: str) -> Optional[List[float]]:
        """
        Retourne la bbox d'un pays depuis bbox_pays_global.
        Returns [lon_min, lat_min, lon_max, lat_max] ou None.
        """
        country_upper = country.upper().strip() if country else ""
        entry = self.bbox_pays_global.get(country_upper)
        if entry and "bbox" in entry:
            return entry["bbox"]
        return None

    def is_loaded(self) -> bool:
        return self._loaded
