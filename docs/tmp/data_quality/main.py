"""
Point d'entrée principal — Contrôle Qualité Proxima v2.

Pipeline :
    Étape 0 → Chargement fichier source + stats initiales
    Étape 1 → Filtre sites actifs + filtre pays optionnel
    Étape 2 → Chargement des références (multi-pays, support levels)
    Étape 3 → Validation de localisation (cascade GPS → Adresse)
    Étape 4 → Génération rapport Excel
"""

from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd

from src.config import Config
from src.filters import filter_active_sites, filter_by_country
from src.loaders import ReferenceLoader, load_source_data, show_initial_stats
from src.models import AnomalyCollector
from src.reporter import compute_global_stats, generate_excel_report
from src.utils import format_number
from src.validators.verdict import check_localisation


def run_quality_control(
    input_file: Path = None,
) -> Tuple[pd.DataFrame, AnomalyCollector, dict]:
    """
    Exécute le contrôle qualité complet.

    Returns:
        - df_enriched : DataFrame enrichi avec les flags d'anomalies
        - collector   : Collecteur contenant toutes les anomalies détectées
        - stats       : Dictionnaire des statistiques globales
    """
    print("=" * 70)
    print("   CONTRÔLE QUALITÉ PROXIMA v2 — DÉMARRAGE")
    print("=" * 70)
    start_time = datetime.now()

    collector = AnomalyCollector()

    # ===== ÉTAPE 0 — Chargement source =====
    df = load_source_data(input_file)
    show_initial_stats(df)

    # ===== ÉTAPE 1 — Filtre sites actifs =====
    df_active = filter_active_sites(df)
    df_active = filter_by_country(df_active, Config.COUNTRY_FILTER)

    # ===== ÉTAPE 2 — Références multi-pays =====
    # On charge uniquement les pays présents dans les données filtrées
    unique_countries = (
        df_active[Config.COL_COUNTRY]
        .dropna()
        .unique()
        .tolist()
    )

    ref_loader = ReferenceLoader()
    ref_loader.load(unique_countries)

    # ===== ÉTAPE 3 — Validation de localisation =====
    df_enriched = check_localisation(df_active, ref_loader, collector)

    # ===== ÉTAPE 4 — Rapport Excel =====
    stats = compute_global_stats(df_enriched, collector)
    output_path = generate_excel_report(df_enriched, collector, stats)

    # Résumé final
    duration = (datetime.now() - start_time).total_seconds()

    print("\n" + "=" * 70)
    print("   CONTRÔLE QUALITÉ TERMINÉ")
    print("=" * 70)
    print(f"\nDurée totale         : {duration:.1f} secondes")
    print(f"Sites analysés        : {format_number(len(df_enriched))}")
    print(f"Anomalies détectées   : {format_number(collector.count())}")
    print(f"Rapport               : {output_path}")
    print("=" * 70)

    return df_enriched, collector, stats


# =============================================================================
# EXÉCUTION
# =============================================================================

if __name__ == "__main__":
    run_quality_control()