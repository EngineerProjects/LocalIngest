"""
Chargement des données source.

- load_source_data : chargement du CSV Proxima avec auto-détection encodage/séparateur
- show_initial_stats : statistiques initiales du fichier chargé
"""

from pathlib import Path

import pandas as pd

from src.config import Config
from src.utils import detect_encoding, format_number, is_not_empty


def load_source_data(file_path: Path = None) -> pd.DataFrame:
    """
    Charge le fichier source Proxima.
    Détecte automatiquement l'encodage et le séparateur CSV.
    Tout est chargé en string pour éviter les conversions automatiques.
    """
    file_path = file_path or Config.INPUT_FILE

    print(f"📂 Chargement du fichier source : {file_path.name}")

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

    print(f"✅ Fichier chargé : {format_number(len(df))} lignes, {len(df.columns)} colonnes")
    return df


def show_initial_stats(df: pd.DataFrame) -> None:
    """Affiche les statistiques initiales du fichier source."""

    print("\n" + "=" * 60)
    print("   STATISTIQUES INITIALES")
    print("=" * 60)

    print(f"\n📊 Volume : {format_number(len(df))} lignes")

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

    if Config.COL_COUNTRY in df.columns:
        pays_counts = df[Config.COL_COUNTRY].value_counts().head(10)
        print(f"\n🌍 Top 10 pays :")
        print("-" * 45)
        for pays, count in pays_counts.items():
            print(f"   {str(pays):25} : {format_number(count)}")

    print("\n" + "=" * 60)