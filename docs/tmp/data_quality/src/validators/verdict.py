"""
Orchestrateur unique du contrôle par site.

Chaque site est évalué par `evaluate_site` et produit une ligne de résultat.
La sortie est un DataFrame avec 1 ligne par site (données enrichies).
"""

import sys

import pandas as pd
from tqdm import tqdm

from src.config import Config, ValidationMode
from src.loaders.reference_loader import ReferenceLoader
from src.utils import format_number, safe_str
from src.validators.site_evaluator import evaluate_site


def check_localisation(
    df: pd.DataFrame,
    ref_loader: ReferenceLoader,
) -> pd.DataFrame:
    """
    Exécute le contrôle complet sur tous les sites actifs.

    Returns:
        - DataFrame enrichi : 1 ligne par site
    """
    print("\n" + "=" * 60)
    print("   ÉTAPE 3 — CONTRÔLES SIMPLIFIÉS PAR SITE")
    print("=" * 60)
    sys.stdout.flush()

    site_results = []

    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Contrôle", file=sys.stdout):
        site_key = _build_site_key(row, idx)
        site_result = evaluate_site(row, ref_loader, site_key)
        site_results.append(site_result)

    enriched_cols = pd.DataFrame(site_results, index=df.index)
    df_enriched = pd.concat([df, enriched_cols], axis=1)

    _print_summary(df_enriched)
    return df_enriched


def _build_site_key(row: pd.Series, idx: int) -> str:
    """Construit une clé site stable à partir du contrat et de l'ID site."""
    contract_id = safe_str(row.get(Config.COL_CONTRACT_ID))
    site_id = safe_str(row.get(Config.COL_SITE_ID))

    if contract_id and site_id:
        return f"{contract_id}::{site_id}"
    if contract_id:
        return f"{contract_id}::ROW_{idx}"
    if site_id:
        return f"NO_CONTRACT::{site_id}"
    return f"ROW_{idx}"


def _print_summary(df: pd.DataFrame) -> None:
    """Affiche un résumé court du contrôle."""
    gps_ok = int((df["_CONTROL_MODE"] == ValidationMode.GPS).sum())
    address_ok = int((df["_CONTROL_MODE"] == ValidationMode.ADDRESS).sum())
    incomplete = int((df["_CONTROL_MODE"] == ValidationMode.INCOMPLETE).sum())
    with_issues = int((df["_ISSUE_COUNT"] > 0).sum())

    sys.stdout.flush()
    print("\nRésultats :")
    print(f"   Sites actifs analysés : {format_number(len(df))}")
    print(f"   Sites validés par GPS : {format_number(gps_ok)}")
    print(f"   Sites validés adresse : {format_number(address_ok)}")
    print(f"   Sites incomplets      : {format_number(incomplete)}")
    print(f"   Sites avec anomalies  : {format_number(with_issues)}")
    print("=" * 60)
