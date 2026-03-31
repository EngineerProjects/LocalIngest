"""
Point d'entrée principal — Contrôle Qualité Proxima.

Pipeline:
    Étape 0 → Chargement fichier source + stats initiales
    Étape 1 → Jointure datamart + filtre contrats actifs
    Étape 2 → Filtre pays (selon références disponibles)
    Étape 3 → Chargement des références
    Étape 4 → Contrôles par site
    Étape 5 → Génération du rapport Excel
"""

from datetime import datetime
from pathlib import Path

import pandas as pd

from src.filters import filter_active_contracts, handle_country_filter
from src.loaders import (
    ReferenceLoader,
    load_datamart,
    load_source_data,
    show_initial_stats,
)
from src.reporter import compute_global_stats, generate_excel_report
from src.utils import format_number
from src.validators import check_localisation


def run_quality_control(
    input_file: Path = None, datamart_file: Path = None
) -> pd.DataFrame:
    """Exécute le contrôle qualité."""
    print("=" * 70)
    print("   CONTRÔLE QUALITÉ PROXIMA")
    print("=" * 70)
    start_time = datetime.now()

    # ===== ÉTAPE 0 — Chargement source =====
    df = load_source_data(input_file)
    show_initial_stats(df)
    nb_total_fichier = len(df)

    # ===== ÉTAPE 1 — Jointure datamart + filtre contrats actifs =====
    df_datamart = load_datamart(datamart_file)
    df_active = filter_active_contracts(df, df_datamart)
    nb_inactifs = nb_total_fichier - len(df_active)

    # ===== ÉTAPE 2 — Filtre pays (selon références disponibles) =====
    ref_loader = ReferenceLoader()
    supported_countries = ref_loader.discover_supported_countries()
    print(f"\nPays avec références disponibles : {', '.join(supported_countries)}")

    df_active = handle_country_filter(df_active, supported_countries)

    if df_active.empty:
        print("\n⚠️ Aucune donnée à analyser après filtrage.")
        return pd.DataFrame()

    # ===== ÉTAPE 3 — Références =====
    unique_countries = df_active["PAYS_SITE"].dropna().unique().tolist()
    ref_loader.load(unique_countries)

    # ===== ÉTAPE 4 — Contrôles par site =====
    df_enriched = check_localisation(df_active, ref_loader)

    # ===== ÉTAPE 5 — Rapport Excel =====
    stats = compute_global_stats(
        df_enriched,
        nb_total_fichier=nb_total_fichier,
        nb_inactifs=nb_inactifs,
    )
    output_path = generate_excel_report(df_enriched, stats, output_path=None)

    duration = (datetime.now() - start_time).total_seconds()
    sites_with_issues = int((df_enriched["_ISSUE_COUNT"] > 0).sum())

    print("\n" + "=" * 70)
    print("   CONTRÔLE QUALITÉ TERMINÉ")
    print("=" * 70)
    print(f"\nDurée totale          : {duration:.1f} secondes")
    print(f"Sites dans le fichier : {format_number(nb_total_fichier)}")
    print(f"Sites inactifs         : {format_number(nb_inactifs)}")
    print(f"Sites actifs analysés : {format_number(len(df_enriched))}")
    print(f"Sites avec anomalies  : {format_number(sites_with_issues)}")
    print(f"Rapport               : {output_path}")
    print("=" * 70)

    return df_enriched


if __name__ == "__main__":
    run_quality_control()
