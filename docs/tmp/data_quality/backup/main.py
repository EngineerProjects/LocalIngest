"""
Point d'entrée principal — Contrôle Qualité Proxima.

Orchestre le pipeline complet :
    Setup   → Prépare les données de référence (bbox départements, etc.)
    Étape 0 → Filtre sites actifs + filtre pays (optionnel)
    Étape 1 → Vérification de localisation (cascade GPS → adresse)
    Étape 2 → Agrégation + Rapport Excel
"""

from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd

from src.config import Config
from src.filters import filter_active_sites, filter_by_country
from src.loader import ReferenceData, load_source_data, show_initial_stats
from src.models import AnomalyCollector
from src.reporter import aggregate_anomalies_by_site, compute_global_stats, generate_excel_report
from src.rules.localisation import check_localisation
from src.setup import run_setup
from src.utils import format_number


def run_quality_control(
    input_file: Path = None,
) -> Tuple[pd.DataFrame, AnomalyCollector, dict]:
    """
    Exécute le contrôle qualité complet.

    Returns:
        - df_enriched : DataFrame source enrichi avec les flags d'anomalies
        - collector : Collecteur contenant toutes les anomalies
        - stats : Dictionnaire des statistiques globales
    """
    print("=" * 70)
    print("   CONTRÔLE QUALITÉ PROXIMA — DÉMARRAGE")
    print("=" * 70)
    start_time = datetime.now()

    # ===== SETUP =====
    setup_ok = run_setup()
    if not setup_ok:
        raise RuntimeError("Setup échoué — fichiers de référence manquants.")

    # ===== INITIALISATION =====
    collector = AnomalyCollector()

    # 1. Charger les données de référence
    ref_data = ReferenceData()
    ref_data.load()

    # 2. Charger le fichier source
    df = load_source_data(input_file)
    show_initial_stats(df)

    # 3. Étape 0 — Filtrer les sites actifs
    df_active = filter_active_sites(df)

    # 3b. Filtre par pays (optionnel, configurable dans Config.COUNTRY_FILTER)
    df_active = filter_by_country(df_active, Config.COUNTRY_FILTER)

    # ===== CONTRÔLE DE LOCALISATION =====

    # 4. Cascade GPS → Adresse
    #    - GPS existe → valider GPS → si anomalies, aussi vérifier adresse
    #    - GPS absent → valider adresse → si KO, site non localisable
    check_localisation(df_active, ref_data, collector)

    # ===== RÉSULTATS =====

    # 5. Agrégation des résultats
    df_enriched = aggregate_anomalies_by_site(df_active, collector)

    # 6. Calcul des statistiques globales
    stats = compute_global_stats(df_enriched, collector)

    # 7. Génération du rapport Excel
    output_path = generate_excel_report(df_enriched, collector, stats)

    # Résumé final
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print("\n" + "=" * 70)
    print("   CONTRÔLE QUALITÉ TERMINÉ")
    print("=" * 70)
    print(f"\n⏱️ Durée totale : {duration:.1f} secondes")
    print(f"📊 Sites analysés : {format_number(len(df_enriched))}")
    print(f"🔍 Anomalies détectées : {format_number(collector.count())}")
    print(f"📁 Rapport : {output_path}")
    print("=" * 70)

    return df_enriched, collector, stats


# =============================================================================
# EXÉCUTION
# =============================================================================

if __name__ == "__main__":
    df_result, anomalies, statistics = run_quality_control()
