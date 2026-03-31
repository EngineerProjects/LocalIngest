from pathlib import Path

import pandas as pd

from src.config import (
    COLUMNS_PROXIMA,
    COLUMNS_DATAMART,
    INPUT_FILE,
    DATAMART_FILE,
)
from src.utils import (
    detect_encoding,
    detect_separator,
    format_number,
    normalize_country,
)


def load_source_data(file_path: Path = None) -> pd.DataFrame:
    """Charge le fichier source Proxima avec uniquement les colonnes utiles."""
    file_path = file_path or INPUT_FILE

    print(f"Chargement : {file_path.name}")

    encoding = detect_encoding(file_path)
    print(f"   Encodage : {encoding}")

    separator = detect_separator(file_path, encoding)
    print(f"   Séparateur : '{separator}'")

    df = pd.read_csv(
        file_path,
        sep=separator,
        usecols=COLUMNS_PROXIMA,
        encoding=encoding,
        dtype=str,
        low_memory=False,
    )

    if "PAYS_SITE" in df.columns:
        df["PAYS_SITE"] = df["PAYS_SITE"].apply(normalize_country)
        df["PAYS_SITE"] = df["PAYS_SITE"].replace("", pd.NA)

    print(
        f"Fichier chargé : {format_number(len(df))} lignes, {len(df.columns)} colonnes"
    )
    return df


def load_datamart(file_path: Path = None) -> pd.DataFrame:
    """Charge le fichier datamart PTF avec uniquement les colonnes utiles."""
    file_path = file_path or DATAMART_FILE

    if not file_path.exists():
        print(f"⚠️ Datamart non trouvé : {file_path}")
        return pd.DataFrame()

    print(f"Chargement datamart : {file_path.name}")

    encoding = detect_encoding(file_path)
    separator = detect_separator(file_path, encoding)

    df = pd.read_csv(
        file_path,
        sep=separator,
        usecols=COLUMNS_DATAMART,
        encoding=encoding,
        dtype=str,
        low_memory=False,
    )

    print(f"Datamart chargé : {format_number(len(df))} lignes")
    return df


def show_initial_stats(df: pd.DataFrame) -> None:
    """Affiche les statistiques initiales du fichier source."""
    print("\n" + "=" * 60)
    print("   STATISTIQUES INITIALES")
    print("=" * 60)

    total = len(df)
    print(f"\nVolume : {format_number(total)} lignes")

    if "ID_SITE" in df.columns:
        unique = df["ID_SITE"].dropna().nunique()
        print(f"ID_SITE uniques : {format_number(unique)}")

    key_cols = [
        "ID_SITE",
        "NU_CNT",
        "PAYS_SITE",
        "CP_SITE",
        "VILLE_SITE",
        "COORD_X_SITE",
        "COORD_Y_SITE",
    ]

    print("\nTaux de remplissage :")
    print("-" * 45)
    for col in key_cols:
        if col in df.columns:
            non_empty = df[col].notna().sum()
            pct = (non_empty / len(df)) * 100
            print(f"   {col:25} : {pct:6.2f}%")

    print("=" * 60)
