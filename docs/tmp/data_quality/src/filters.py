"""
Filtres de données — Étape 1.

- filter_active_sites : STOCK=1, pas de date de résiliation
- filter_by_country   : filtre optionnel par pays (configurable dans Config)
"""

from typing import List, Optional

import pandas as pd

from src.config import Config
from src.utils import format_number, is_empty, normalize_country


def filter_active_sites(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filtre les sites actifs.

    Critères :
        - STOCK = 1
        - DT_EFF_RESIL_CNT est vide (pas de date de résiliation)

    Returns:
        DataFrame filtré contenant uniquement les sites actifs.
    """
    print("\n" + "=" * 60)
    print("   ÉTAPE 1 — FILTRE SITES ACTIFS")
    print("=" * 60)

    initial_count = len(df)

    # Condition 1 : STOCK = 1
    mask_stock = df[Config.COL_STOCK].astype(str).str.strip() == "1"

    # Condition 2 : DT_EFF_RESIL_CNT est vide
    mask_resil = df[Config.COL_DT_RESIL].apply(is_empty)

    mask_active = mask_stock & mask_resil
    df_active = df[mask_active].copy()

    filtered_count = len(df_active)
    removed_count = initial_count - filtered_count

    print(f"\n📊 Résultats du filtre :")
    print(f"   Lignes initiales     : {format_number(initial_count)}")
    print(f"   Sites actifs retenus : {format_number(filtered_count)}")
    print(f"   Sites exclus         : {format_number(removed_count)}")
    print(f"   Taux de rétention    : {(filtered_count / initial_count) * 100:.2f}%")

    excluded_stock = (~mask_stock).sum()
    excluded_resil = (~mask_resil).sum()
    print(f"\n📋 Détail des exclusions :")
    print(f"   STOCK ≠ 1            : {format_number(excluded_stock)}")
    print(f"   Date résiliation     : {format_number(excluded_resil)}")

    print("\n" + "=" * 60)
    return df_active


def filter_by_country(
    df: pd.DataFrame,
    countries: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Filtre optionnel par pays.

    Args:
        df: DataFrame à filtrer
        countries: Liste de pays à garder (en majuscules).
                   Si None, renvoie le DataFrame tel quel.

    Returns:
        DataFrame filtré.
    """
    if countries is None:
        return df

    print("\n" + "=" * 60)
    print("   FILTRE PAR PAYS")
    print("=" * 60)

    initial_count = len(df)
    countries_upper = {
        normalized
        for country in countries
        for normalized in [normalize_country(country)]
        if normalized
    }

    mask = df[Config.COL_COUNTRY].apply(
        lambda x: normalize_country(x) in countries_upper
    )
    df_filtered = df[mask].copy()

    print(f"\n📊 Filtre pays : {', '.join(sorted(countries_upper))}")
    print(f"   Lignes avant filtre  : {format_number(initial_count)}")
    print(f"   Lignes retenues      : {format_number(len(df_filtered))}")
    print(f"   Lignes exclues       : {format_number(initial_count - len(df_filtered))}")

    print("\n" + "=" * 60)
    return df_filtered
