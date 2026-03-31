"""
Chargement des données de référence.

Version simplifiée :
- une bbox pays suffit pour contrôler "GPS dans le pays ?"
- une bbox région/département permet en plus le contrôle "GPS cohérent avec le CP ?"
- les autres références restent optionnelles

On ne "skip" plus un pays uniquement parce qu'il manque un référentiel annexe.
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
    normalize_country,
    normalize_postal_code,
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
        postal_code_widths    : dict pays_upper → largeur numérique de CP observée
        bbox_regions          : dict pays_upper → dict code_region → {"name": ..., "bbox": [...]}
        bbox_pays_global      : dict pays_upper → {"bbox": [...]}
    """

    def __init__(self):
        self.support_levels: Dict[str, str] = {}
        self.cp_to_cities: Dict[str, Dict[str, List[str]]] = {}
        self.countries_with_cp_ref: Set[str] = set()
        self.postal_code_widths: Dict[str, Optional[int]] = {}
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

        # Étape 1 — Bounding boxes pays (obligatoire pour qu'un pays soit traitable)
        self._load_bbox_pays_global()

        # Étape 2 — Pays détectés dans les données source (déduplication sur la valeur normalisée)
        unique_countries = sorted({
            normalize_country(country)
            for country in source_countries
            if normalize_country(country)
        })

        # Étape 3 — Chargement par pays
        for country in unique_countries:
            self._load_country(country)

        # Résumé — séparation supportés / non supportés
        supported = sorted(
            c for c, lvl in self.support_levels.items()
            if lvl in (SupportLevel.COMPLET, SupportLevel.PARTIEL)
        )
        unsupported = sorted(
            c for c, lvl in self.support_levels.items()
            if lvl == SupportLevel.NON_SUPPORTE
        )

        print(f"\nPays détectés : {len(unique_countries)} au total")
        print("-" * 45)

        print(f"   ✅ Pays supportés ({len(supported)}) :")
        for country in supported:
            level = self.support_levels[country]
            print(f"      {country:22} : {level}")

        print(f"\n   ⛔ Pays sans bbox pays ({len(unsupported)}) :")
        print(f"      {', '.join(unsupported) if unsupported else '(aucun)'}")

        self._loaded = True
        print("\nDonnées de référence chargées")
        print("=" * 60)

    # =========================================================================
    # CHARGEMENT GLOBAL (bbox pays)
    # =========================================================================

    def _load_bbox_pays_global(self) -> None:
        """Charge bbox_pays_global.json — requis pour qu'un pays soit supporté."""
        path = Config.BBOX_PAYS_GLOBAL_FILE
        if not path.exists():
            print(f"   bbox_pays_global.json non trouvé : {path}")
            return

        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        self.bbox_pays_global = {
            normalize_country(country): data
            for country, data in raw.items()
            if normalize_country(country)
        }
        print(f"   bbox_pays_global : {len(self.bbox_pays_global)} pays")

    # =========================================================================
    # CHARGEMENT PAR PAYS
    # =========================================================================

    def _load_country(self, country: str) -> None:
        """
        Détecte le niveau de support d'un pays et charge ses références.

        Règles simplifiées :
          - COMPLET     : bbox pays + régions
          - PARTIEL     : bbox pays seule
          - NON_SUPPORTÉ: aucune bbox pays
        """
        has_country_bbox = country in self.bbox_pays_global
        country_dir = self._find_country_dir(country)

        has_cp = False
        has_regions = False

        if country_dir is None:
            if getattr(Config, 'REFERENCE_LOG_DETAILS', False):
                print(f"\n{country}")
                print(f"   Dossier de référence absent pour {country}")
        else:
            if getattr(Config, 'REFERENCE_LOG_DETAILS', False):
                print(f"\n{country}")
            has_cp = self._load_cp_file(country, country_dir)
            has_regions = self._load_regions(country, country_dir)

        if not has_country_bbox:
            self.support_levels[country] = SupportLevel.NON_SUPPORTE
            if getattr(Config, 'REFERENCE_LOG_DETAILS', False):
                print(f"   {country} : absent de bbox_pays_global.json → non supporté")
            return

        if has_regions:
            self.support_levels[country] = SupportLevel.COMPLET
        else:
            self.support_levels[country] = SupportLevel.PARTIEL
            if getattr(Config, 'REFERENCE_LOG_DETAILS', False):
                print(f"   {country} : bbox pays disponible, sans détail régional")

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

    # =========================================================================
    # CHARGEMENT CODES POSTAUX
    # =========================================================================

    def _load_cp_file(self, country: str, country_dir: Path) -> bool:
        """
        Charge le fichier codes_postaux.csv du pays.
        Retourne True si chargé avec succès et exploitable.
        """
        cp_file = country_dir / Config.CP_FILENAME
        if not cp_file.exists():
            return False

        try:
            encoding = detect_encoding(cp_file)
            df = pd.read_csv(cp_file, sep=";", dtype=str, encoding=encoding)
        except Exception as e:
            print(f"   {country} CP : erreur de lecture du fichier {cp_file.name} ({e})")
            return False

        if df.empty or len(df.columns) < 1:
            print(f"   {country} CP : fichier vide ou colonnes invalides")
            return False

        # Nettoyer les noms de colonnes (retirer '#' artefact CSV)
        df.columns = [col.lstrip("#").strip() for col in df.columns]

        cp_col = None
        city_col = None

        for col in df.columns:
            col_lower = col.lower()
            if cp_col is None and ("code_postal" in col_lower or "code postal" in col_lower):
                cp_col = col
            if city_col is None and (
                "nom_de_la_commune" in col_lower
                or "nom de la commune" in col_lower
                or "nom_commune" in col_lower
                or col_lower == "commune"
                or "acheminement" in col_lower
                or "libellé" in col_lower
                or "libelle" in col_lower
            ):
                city_col = col

        if cp_col is None:
            cp_col = df.columns[0]

        if city_col is None:
            if len(df.columns) > 1:
                city_col = df.columns[1]
            else:
                print(f"   {country} CP : colonne ville introuvable")
                return False

        raw_codes = [safe_str(value) for value in df[cp_col].tolist()]
        numeric_lengths = [len(code) for code in raw_codes if code.isdigit()]
        self.postal_code_widths[country] = max(numeric_lengths) if numeric_lengths else None

        # Construire pays → CP → [villes]
        country_cp_map = self.cp_to_cities.setdefault(country, {})
        count_before = len(country_cp_map)
        valid_rows = 0

        for _, row in df.iterrows():
            cp = self.normalize_postal_code(country, row.get(cp_col))
            city = normalize_string(row.get(city_col))

            if cp and city:
                valid_rows += 1
                if cp not in country_cp_map:
                    country_cp_map[cp] = []
                if city not in country_cp_map[cp]:
                    country_cp_map[cp].append(city)

        # Si rien d'exploitable n'a été chargé, on considère que le CP n'est pas valide
        if valid_rows == 0 or len(country_cp_map) == 0:
            self.cp_to_cities.pop(country, None)
            self.postal_code_widths[country] = None
            print(f"   {country} CP : aucune donnée exploitable trouvée")
            return False

        self.countries_with_cp_ref.add(country)
        added = len(country_cp_map) - count_before
        print(f"   {country} CP : {format_number(added)} codes postaux chargés")
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
        bbox_cache = country_dir / Config.BBOX_REGIONS_FILENAME

        # 1) Cache JSON
        if bbox_cache.exists():
            try:
                with open(bbox_cache, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                print(f"   {country} régions : erreur lecture cache {bbox_cache.name} ({e})")
                return False

            if not data:
                print(f"   {country} régions : cache vide")
                return False

            self.bbox_regions[country] = data
            print(f"   {country} régions : {len(data)} bounding boxes (cache)")
            return True

        # 2) GeoJSON
        geojson_path = self._find_geojson(country_dir)
        if geojson_path is None:
            return False

        data = self._generate_bbox_from_geojson(geojson_path)
        if not data:
            print(f"   {country} régions : aucune bbox générée depuis le GeoJSON")
            return False

        # 3) Sauvegarder le cache
        try:
            with open(bbox_cache, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"   {country} régions : impossible d'écrire le cache ({e})")

        self.bbox_regions[country] = data
        print(f"   {country} régions : {len(data)} bounding boxes (générées depuis GeoJSON)")
        return True

    def _find_geojson(self, country_dir: Path) -> Optional[Path]:
        """Cherche un fichier GeoJSON dans le dossier pays."""
        for filename in Config.GEOJSON_FILENAMES:
            candidate = country_dir / filename
            if candidate.exists():
                return candidate

        for f in country_dir.iterdir():
            if f.is_file() and f.suffix.lower() == ".geojson":
                return f

        return None

    def _generate_bbox_from_geojson(self, geojson_path: Path) -> Dict[str, dict]:
        """
        Génère un dict {code_region: {"name": ..., "bbox": [...]}} depuis un GeoJSON.
        Essaie plusieurs noms de propriétés courants pour code et nom.
        """
        try:
            with open(geojson_path, "r", encoding="utf-8") as f:
                geojson_data = json.load(f)
        except Exception:
            return {}

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
                    code = str(props[key]).strip()
                    break

            if not code:
                continue

            # Trouver le nom région
            name = ""
            for key in NAME_PROPS:
                if key in props and props[key]:
                    name = str(props[key]).strip()
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
        country_upper = normalize_country(country)
        return self.support_levels.get(country_upper, SupportLevel.NON_SUPPORTE)

    def has_cp_reference(self, country: str) -> bool:
        """Vérifie si on a une référence CP pour ce pays."""
        return normalize_country(country) in self.countries_with_cp_ref

    def normalize_postal_code(self, country: str, cp: str) -> str:
        """Normalise un CP selon la largeur de référence observée pour le pays."""
        country_upper = normalize_country(country)
        width = self.postal_code_widths.get(country_upper)
        return normalize_postal_code(cp, width)

    def get_cities_for_cp(self, country: str, cp: str) -> List[str]:
        """Retourne la liste des villes normalisées pour un CP dans un pays donné."""
        country_upper = normalize_country(country)
        cp_normalized = self.normalize_postal_code(country_upper, cp)
        country_cp_map = self.cp_to_cities.get(country_upper, {})
        return country_cp_map.get(cp_normalized, [])

    def get_bbox_region(self, country: str, region_code: str) -> Optional[List[float]]:
        """
        Retourne la bbox d'une région/département pour un pays donné.
        Returns [lon_min, lat_min, lon_max, lat_max] ou None.
        """
        country_upper = normalize_country(country)
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
        country_upper = normalize_country(country)
        entry = self.bbox_pays_global.get(country_upper)
        if entry and "bbox" in entry:
            return entry["bbox"]
        return None

    def has_country_bbox(self, country: str) -> bool:
        """Indique si une bbox pays est disponible."""
        return self.get_bbox_pays(country) is not None

    def has_region_reference(self, country: str) -> bool:
        """Indique si un référentiel régional est disponible pour ce pays."""
        country_upper = normalize_country(country)
        return country_upper in self.bbox_regions and bool(self.bbox_regions[country_upper])

    def is_loaded(self) -> bool:
        return self._loaded
