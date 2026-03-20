"""
Chargement des données source et des données de référence.

- load_source_data : chargement du CSV Proxima
- show_initial_stats : affichage des statistiques initiales
- ReferenceData : conteneur pour CP, bbox départements, bbox pays

NOTE : ReferenceData est la SEULE source de vérité pour les données de référence.
       Rien n'est hardcodé — tout vient des fichiers dans data/references/.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd

from src.config import Config
from src.utils import (
    detect_encoding,
    format_number,
    is_not_empty,
    normalize_string,
    safe_str,
)


# =============================================================================
# CHARGEMENT DU FICHIER SOURCE
# =============================================================================

def load_source_data(file_path: Path = None) -> pd.DataFrame:
    """
    Charge le fichier source Proxima.
    Détecte automatiquement l'encodage et le séparateur CSV.
    """
    file_path = file_path or Config.INPUT_FILE

    print(f"📂 Chargement du fichier source : {file_path.name}")

    # Détection de l'encodage
    if Config.CSV_ENCODING is None:
        encoding = detect_encoding(file_path)
        print(f"   Encodage détecté : {encoding}")
    else:
        encoding = Config.CSV_ENCODING

    # Détection du séparateur (si non spécifié)
    separator = Config.CSV_SEPARATOR
    if separator is None:
        with open(file_path, "r", encoding=encoding) as f:
            first_line = f.readline()
        # Celui qui produit le plus de colonnes est probablement le bon
        if first_line.count(";") > first_line.count(","):
            separator = ";"
        else:
            separator = ","
        print(f"   Séparateur détecté : '{separator}'")

    # Chargement — tout en string pour éviter les conversions automatiques
    df = pd.read_csv(
        file_path,
        sep=separator,
        encoding=encoding,
        dtype=str,
        low_memory=False,
    )

    print(f"✅ Fichier chargé : {format_number(len(df))} lignes, {len(df.columns)} colonnes")

    return df


# =============================================================================
# APERÇU ET STATISTIQUES INITIALES
# =============================================================================

def show_initial_stats(df: pd.DataFrame):
    """Affiche les statistiques initiales du fichier source."""

    print("\n" + "=" * 60)
    print("   STATISTIQUES INITIALES")
    print("=" * 60)

    print(f"\n📊 Volume : {format_number(len(df))} lignes")

    # Colonnes clés et leur taux de remplissage
    key_cols = [
        Config.COL_SITE_ID,
        Config.COL_COUNTRY,
        Config.COL_POSTAL_CODE,
        Config.COL_CITY,
        Config.COL_LONGITUDE,
        Config.COL_LATITUDE,
        Config.COL_STOCK,
    ]

    print("\n📋 Taux de remplissage des colonnes clés :")
    print("-" * 45)

    for col in key_cols:
        if col in df.columns:
            non_empty = df[col].apply(is_not_empty).sum()
            pct = (non_empty / len(df)) * 100
            print(f"   {col:25} : {pct:6.2f}% ({format_number(non_empty)})")
        else:
            print(f"   {col:25} : ⚠️ COLONNE ABSENTE")

    # Valeurs uniques pour le pays
    if Config.COL_COUNTRY in df.columns:
        pays_counts = df[Config.COL_COUNTRY].value_counts().head(10)
        print(f"\n🌍 Top 10 pays :")
        print("-" * 45)
        for pays, count in pays_counts.items():
            print(f"   {pays:25} : {format_number(count)}")

    print("\n" + "=" * 60)


# =============================================================================
# DONNÉES DE RÉFÉRENCE
# =============================================================================

class ReferenceData:
    """
    Conteneur pour les données de référence.

    Toutes les données viennent des fichiers dans data/references/.
    Rien n'est hardcodé.

    Attributs après load() :
    - cp_to_cities : dict CP -> [villes] (de tous les fichiers CP disponibles)
    - bbox_departements : dict code_dept -> {"name": ..., "bbox": [...]}
    - bbox_pays : dict pays_upper -> {"name": ..., "bbox": [...]}
    - valid_departments : set de codes départements valides (extrait de bbox_departements)
    - countries_with_cp_ref : set de pays (upper) qui ont une référence CP chargée
    """

    def __init__(self):
        self.cp_to_cities: Dict[str, List[str]] = {}
        self.bbox_departements: Dict[str, dict] = {}
        self.bbox_pays: Dict[str, dict] = {}
        self.valid_departments: Set[str] = set()
        self.countries_with_cp_ref: Set[str] = set()
        self._loaded = False

    def load(self):
        """Charge toutes les données de référence disponibles."""
        if self._loaded:
            return

        print("📂 Chargement des données de référence...")

        self._load_cp_references()
        self._load_bbox_departements()
        self._load_bbox_pays()

        self._loaded = True
        print("✅ Données de référence chargées")

    def _load_cp_references(self):
        """
        Charge les fichiers de codes postaux disponibles.
        Cherche tous les fichiers CSV dans data/references/ qui ressemblent
        à des bases CP et les charge.
        """
        # Pour l'instant on charge la France.
        # Extensible : ajouter ici d'autres fichiers CP par pays
        # par exemple codes_postaux_belgique.csv, etc.
        self._load_cp_file(
            file_path=Config.CP_REFERENCE_FILE,
            country_name="FRANCE",
        )

    def _load_cp_file(self, file_path: Path, country_name: str):
        """
        Charge un fichier de codes postaux pour un pays donné.

        Le fichier doit être un CSV avec au minimum :
        - Une colonne contenant le code postal
        - Une colonne contenant le nom de la commune/ville

        Les colonnes sont nettoyées et renommées automatiquement.
        """
        if not file_path.exists():
            print(f"   ⚠️ Fichier CP non trouvé : {file_path.name}")
            return

        # Détecter l'encodage et charger
        encoding = detect_encoding(file_path)
        df = pd.read_csv(
            file_path,
            sep=";",
            dtype=str,
            encoding=encoding,
        )

        # ----- Nettoyage des noms de colonnes -----
        # Retirer le '#' en début de nom (artefact fréquent dans les CSV)
        df.columns = [col.lstrip("#").strip() for col in df.columns]

        # Mapping de renommage pour les colonnes françaises connues
        rename_map = {}
        for col in df.columns:
            col_lower = col.lower()
            if "code_commune" in col_lower or "code commune" in col_lower:
                rename_map[col] = "code_commune_insee"
            elif "nom_de_la_commune" in col_lower or "nom_commune" in col_lower:
                rename_map[col] = "nom_commune"
            elif "code_postal" in col_lower or "code postal" in col_lower:
                rename_map[col] = "code_postal"
            elif "acheminement" in col_lower or "libelle" in col_lower or "libell" in col_lower:
                rename_map[col] = "libelle_acheminement"
            elif "ligne_5" in col_lower or "ligne 5" in col_lower:
                rename_map[col] = "complement_adresse"

        if rename_map:
            df = df.rename(columns=rename_map)

        print(f"   Colonnes chargées : {list(df.columns)}")

        # ----- Identifier les colonnes CP et Ville -----
        cp_col = None
        city_col = None

        for col in df.columns:
            col_lower = col.lower()
            if "code_postal" in col_lower:
                cp_col = col
            if "nom_commune" in col_lower or "commune" in col_lower:
                city_col = col

        if cp_col is None or city_col is None:
            print(f"   ⚠️ Colonnes CP/Ville non identifiées, fallback colonnes 0 et 1")
            cp_col = df.columns[0]
            city_col = df.columns[1]

        print(f"   CP col: {cp_col}, City col: {city_col}")

        # ----- Construire le dictionnaire CP -> [villes] -----
        for _, row in df.iterrows():
            cp = safe_str(row[cp_col]).zfill(5)
            city = normalize_string(row[city_col])

            if cp and city:
                if cp not in self.cp_to_cities:
                    self.cp_to_cities[cp] = []
                if city not in self.cp_to_cities[cp]:
                    self.cp_to_cities[cp].append(city)

        self.countries_with_cp_ref.add(country_name.upper())

        print(f"   ✅ {format_number(len(self.cp_to_cities))} codes postaux chargés ({country_name})")

    def _load_bbox_departements(self):
        """
        Charge les bounding boxes des départements.
        Extrait aussi la liste des départements valides.
        """
        if not Config.BBOX_DEPT_FILE.exists():
            print("   ⚠️ bbox_departements.json non trouvé (lancez setup d'abord)")
            return

        with open(Config.BBOX_DEPT_FILE, "r", encoding="utf-8") as f:
            self.bbox_departements = json.load(f)

        # Les départements valides viennent directement du fichier
        self.valid_departments = set(self.bbox_departements.keys())

        print(f"   ✅ {len(self.bbox_departements)} bounding boxes départements")
        print(f"      → {len(self.valid_departments)} départements valides reconnus")

    def _load_bbox_pays(self):
        """Charge les bounding boxes des pays."""
        if not Config.BBOX_PAYS_FILE.exists():
            print("   ⚠️ bbox_pays.json non trouvé")
            return

        with open(Config.BBOX_PAYS_FILE, "r", encoding="utf-8") as f:
            self.bbox_pays = json.load(f)

        print(f"   ✅ {len(self.bbox_pays)} bounding boxes pays")

    # -------------------------------------------------------------------------
    # Accesseurs
    # -------------------------------------------------------------------------

    def get_cities_for_cp(self, cp: str) -> List[str]:
        """Retourne la liste des villes pour un code postal."""
        cp_normalized = safe_str(cp).zfill(5)
        return self.cp_to_cities.get(cp_normalized, [])

    def has_cp_reference(self, country: str) -> bool:
        """Vérifie si on a une référence CP pour ce pays."""
        return country.upper().strip() in self.countries_with_cp_ref

    def is_valid_department(self, dept_code: str) -> bool:
        """Vérifie si un code département est valide (basé sur les données chargées)."""
        return dept_code in self.valid_departments

    def get_bbox_departement(self, dept_code: str) -> Optional[List[float]]:
        """
        Retourne la bounding box d'un département : [lon_min, lat_min, lon_max, lat_max].
        Retourne None si le département n'est pas trouvé.
        """
        entry = self.bbox_departements.get(dept_code)
        if entry and "bbox" in entry:
            return entry["bbox"]
        return None

    def get_bbox_pays(self, pays: str) -> Optional[List[float]]:
        """
        Retourne la bounding box d'un pays : [lon_min, lat_min, lon_max, lat_max].
        Retourne None si le pays n'est pas trouvé.
        """
        pays_normalized = pays.upper().strip() if pays else ""
        entry = self.bbox_pays.get(pays_normalized)
        if entry and "bbox" in entry:
            return entry["bbox"]
        return None
