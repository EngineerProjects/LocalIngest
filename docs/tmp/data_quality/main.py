"""
Point d'entrée principal — Contrôle Qualité Proxima simplifié.

Pipeline :
    Étape 0 → Chargement fichier source + stats initiales
    Étape 1 → Filtre sites actifs + filtre pays optionnel
    Étape 2 → Chargement des références utiles
    Étape 3 → Contrôles simplifiés par site (1 ligne par site)
    Étape 4 → Génération du rapport Excel
"""

from datetime import datetime
from pathlib import Path

import pandas as pd

from src.config import Config
from src.filters import filter_active_sites, filter_by_country
from src.loaders import ReferenceLoader, load_source_data, show_initial_stats
from src.reporter import compute_global_stats, generate_excel_report
from src.utils import format_number
from src.validators.verdict import check_localisation


def run_quality_control(input_file: Path = None) -> pd.DataFrame:
    """Exécute le contrôle qualité simplifié."""
    print("=" * 70)
    print("   CONTRÔLE QUALITÉ PROXIMA — VERSION SIMPLIFIÉE")
    print("=" * 70)
    start_time = datetime.now()

    # ===== ÉTAPE 0 — Chargement source =====
    df = load_source_data(input_file)
    show_initial_stats(df)

    # ===== ÉTAPE 1 — Filtre sites actifs =====
    df_active = filter_active_sites(df)
    df_active = filter_by_country(df_active, Config.COUNTRY_FILTER)

    # ===== ÉTAPE 2 — Références =====
    unique_countries = (
        df_active[Config.COL_COUNTRY]
        .dropna()
        .unique()
        .tolist()
    )

    ref_loader = ReferenceLoader()
    ref_loader.load(unique_countries)

    # ===== ÉTAPE 3 — Contrôles par site (1 ligne par site) =====
    df_enriched = check_localisation(df_active, ref_loader)

    # ===== ÉTAPE 4 — Rapport Excel =====
    stats = compute_global_stats(df_enriched)
    output_path = generate_excel_report(df_enriched, stats, output_path=None)

    duration = (datetime.now() - start_time).total_seconds()
    sites_with_issues = int((df_enriched["_ISSUE_COUNT"] > 0).sum())

    print("\n" + "=" * 70)
    print("   CONTRÔLE QUALITÉ TERMINÉ")
    print("=" * 70)
    print(f"\nDurée totale         : {duration:.1f} secondes")
    print(f"Sites actifs         : {format_number(len(df_enriched))}")
    print(f"Sites avec anomalies : {format_number(sites_with_issues)}")
    print(f"Rapport              : {output_path}")
    print("=" * 70)

    return df_enriched


if __name__ == "__main__":
    run_quality_control()
