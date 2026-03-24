"""
Chargement des données source.

- load_source_data : chargement du CSV Proxima avec auto-détection encodage/séparateur
- show_initial_stats : statistiques initiales du fichier chargé
"""

from pathlib import Path

import pandas as pd

from src.config import Config
from src.utils import detect_encoding, format_number, is_not_empty, normalize_country


def load_source_data(file_path: Path = None) -> pd.DataFrame:
    """
    Charge le fichier source Proxima.
    Détecte automatiquement l'encodage et le séparateur CSV.
    Tout est chargé en string pour éviter les conversions automatiques.
    """
    file_path = file_path or Config.INPUT_FILE

    print(f"Chargement du fichier source : {file_path.name}")

    # Détection de l'encodage
    encoding = Config.CSV_ENCODING or detect_encoding(file_path)
    print(f"   Encodage : {encoding}")

    # Détection du séparateur
    separator = Config.CSV_SEPARATOR
    if separator is None:
        with open(file_path, "r", encoding=encoding) as f:
            first_line = f.readline()
        separator = ";" if first_line.count(";") > first_line.count(",") else ","
        print(f"   Séparateur : '{separator}'")

    df = pd.read_csv(
        file_path,
        sep=separator,
        encoding=encoding,
        dtype=str,
        low_memory=False,
    )

    # Normaliser le pays dès le chargement pour éviter les doublons FRANCE/France.
    if Config.COL_COUNTRY in df.columns:
        df[Config.COL_COUNTRY] = df[Config.COL_COUNTRY].apply(normalize_country)
        df[Config.COL_COUNTRY] = df[Config.COL_COUNTRY].replace("", pd.NA)

    print(f"Fichier chargé : {format_number(len(df))} lignes, {len(df.columns)} colonnes")
    return df


def show_initial_stats(df: pd.DataFrame) -> None:
    """Affiche les statistiques initiales du fichier source."""

    print("\n" + "=" * 60)
    print("   STATISTIQUES INITIALES")
    print("=" * 60)

    total_lignes = len(df)
    print(f"\nVolume : {format_number(total_lignes)} lignes")

    # Doublons ID_SITE
    if Config.COL_SITE_ID in df.columns:
        unique_sites = df[Config.COL_SITE_ID].dropna().nunique()
        total_with_id = df[Config.COL_SITE_ID].notna().sum()
        doublons = int(total_with_id) - unique_sites
        print(f"ID_SITE uniques : {format_number(unique_sites)}", end="")
        if doublons > 0:
            print(f"  ⚠️  ({format_number(doublons)} doublons détectés)", end="")
        print()

    key_cols = [
        Config.COL_SITE_ID,
        Config.COL_COUNTRY,
        Config.COL_POSTAL_CODE,
        Config.COL_CITY,
        Config.COL_LONGITUDE,
        Config.COL_LATITUDE,
        Config.COL_STOCK,
    ]

    print("\nTaux de remplissage des colonnes clés :")
    print("-" * 45)
    for col in key_cols:
        if col in df.columns:
            non_empty = df[col].apply(is_not_empty).sum()
            pct = (non_empty / len(df)) * 100
            print(f"   {col:25} : {pct:6.2f}% ({format_number(non_empty)})")
        else:
            print(f"   {col:25} : COLONNE ABSENTE")

    if Config.COL_COUNTRY in df.columns:
        pays_counts = df[Config.COL_COUNTRY].value_counts().head(10)
        print(f"\nTop 10 pays :")
        print("-" * 45)
        for pays, count in pays_counts.items():
            print(f"   {str(pays):25} : {format_number(count)}")

    print("\n" + "=" * 60)