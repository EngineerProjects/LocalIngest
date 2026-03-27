"""
Génération du rapport Excel — Étape 4.

Trois onglets :
    Résumé          → statistiques globales + répartition par code + répartition par pays
    Détail anomalies → une ligne par anomalie (toutes les colonnes)
    Données enrichies → fichier source + colonnes _IS_LOCALIZED, _ANOMALY_CODES, etc.
"""

from datetime import datetime
from pathlib import Path

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill

from src.config import AnomalyCode, Config, Severity
from src.models import AnomalyCollector
from src.utils import format_number


# =============================================================================
# STATISTIQUES GLOBALES
# =============================================================================

def compute_global_stats(
    df: pd.DataFrame,
    collector: AnomalyCollector,
) -> dict:
    """Calcule les statistiques globales pour le rapport."""

    # Localisation
    count_gps = int((df["_LOCALIZATION_MODE"] == "GPS").sum()) if "_LOCALIZATION_MODE" in df else 0
    count_addr = int((df["_LOCALIZATION_MODE"] == "ADRESSE").sum()) if "_LOCALIZATION_MODE" in df else 0
    count_none = int((df["_LOCALIZATION_MODE"] == "AUCUN").sum()) if "_LOCALIZATION_MODE" in df else 0

    # Par pays
    stats_par_pays = {}
    if Config.COL_COUNTRY in df.columns and "_SUPPORT_LEVEL" in df.columns:
        for pays, grp in df.groupby(Config.COL_COUNTRY):
            stats_par_pays[str(pays)] = {
                "nb_sites": len(grp),
                "support": grp["_SUPPORT_LEVEL"].iloc[0] if "_SUPPORT_LEVEL" in grp else "?",
                "localises": int(grp["_IS_LOCALIZED"].sum()) if "_IS_LOCALIZED" in grp else 0,
                "avec_anomalie": int(grp["_HAS_ANOMALY"].sum()) if "_HAS_ANOMALY" in grp else 0,
            }

    return {
        "date_execution": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_lignes": len(df),
        "localises_gps": count_gps,
        "localises_adresse": count_addr,
        "non_localisables": count_none,
        "lignes_sans_anomalies": int((~df["_HAS_ANOMALY"]).sum()) if "_HAS_ANOMALY" in df else 0,
        "lignes_avec_anomalies": int(df["_HAS_ANOMALY"].sum()) if "_HAS_ANOMALY" in df else 0,
        "total_anomalies": collector.count(),
        "anomalies_par_gravite": collector.count_by_severity(),
        "anomalies_par_code": collector.count_by_code(),
        "stats_par_pays": stats_par_pays,
    }


# =============================================================================
# GÉNÉRATION DU RAPPORT EXCEL
# =============================================================================

def generate_excel_report(
    df: pd.DataFrame,
    collector: AnomalyCollector,
    stats: dict,
    output_path: Path = None,
) -> Path:
    """
    Génère le rapport Excel avec 3 onglets.

    Returns:
        Chemin vers le fichier généré.
    """
    print("\n" + "=" * 60)
    print("   GÉNÉRATION DU RAPPORT EXCEL")
    print("=" * 60)

    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Config.OUTPUT_DIR / f"rapport_anomalies_{timestamp}.xlsx"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    wb = Workbook()

    # Styles communs
    header_font = Font(bold=True, color="FFFFFF")
    header_fill_blue = PatternFill(start_color="2E5FA3", end_color="2E5FA3", fill_type="solid")
    header_fill_dark = PatternFill(start_color="3B3B3B", end_color="3B3B3B", fill_type="solid")
    title_font = Font(bold=True, size=13)

    # =========================================================
    # ONGLET 1 : RÉSUMÉ
    # =========================================================
    ws = wb.active
    ws.title = "Résumé"

    row = 1

    def write_title(text, r):
        ws.cell(row=r, column=1, value=text).font = title_font
        return r + 1

    def write_kv(label, value, r, bold_val=False):
        ws.cell(row=r, column=1, value=label)
        cell = ws.cell(row=r, column=2, value=value)
        if bold_val:
            cell.font = Font(bold=True)
        return r + 1

    def write_header(cols, r, fill=None):
        fill = fill or header_fill_blue
        for c, text in enumerate(cols, 1):
            cell = ws.cell(row=r, column=c, value=text)
            cell.font = header_font
            cell.fill = fill
        return r + 1

    # Section A — Statistiques globales
    row = write_title("SECTION A — Statistiques globales", row)
    row += 1
    kv_data = [
        ("Date d'exécution",           stats["date_execution"]),
        ("Fichier source",              Config.INPUT_FILE.name),
        ("Total sites analysés",        format_number(stats["total_lignes"])),
        ("",                            ""),
        ("Sites localisés via GPS",     format_number(stats["localises_gps"])),
        ("Sites localisés via Adresse", format_number(stats["localises_adresse"])),
        ("Sites NON localisables",      format_number(stats["non_localisables"])),
        ("",                            ""),
        ("Sites sans aucune anomalie",  format_number(stats["lignes_sans_anomalies"])),
        ("Sites avec anomalie(s)",      format_number(stats["lignes_avec_anomalies"])),
        ("Total anomalies détectées",   format_number(stats["total_anomalies"])),
        ("",                            ""),
        ("Anomalies GRAVES",            format_number(stats["anomalies_par_gravite"].get(Severity.GRAVE, 0))),
        ("Anomalies LÉGÈRES",           format_number(stats["anomalies_par_gravite"].get(Severity.LEGERE, 0))),
        ("Anomalies INFO",              format_number(stats["anomalies_par_gravite"].get(Severity.INFO, 0))),
    ]
    for label, value in kv_data:
        row = write_kv(label, value, row)

    row += 2

    # Section B — Répartition par code
    row = write_title("SECTION B — Répartition par code anomalie", row)
    row += 1
    row = write_header(["Code", "Libellé", "Occurrences", "Gravité"], row)
    for code, count in stats["anomalies_par_code"].items():
        ws.cell(row=row, column=1, value=code)
        ws.cell(row=row, column=2, value=AnomalyCode.get_label(code))
        ws.cell(row=row, column=3, value=count)
        ws.cell(row=row, column=4, value=AnomalyCode.get_severity(code))
        row += 1

    row += 2

    # Section C — Répartition par pays
    row = write_title("SECTION C — Répartition par pays", row)
    row += 1
    row = write_header(
        ["Pays", "Nb sites", "Support", "Localisés", "Avec anomalie"], row,
        fill=header_fill_dark,
    )
    for pays, s in sorted(stats["stats_par_pays"].items()):
        ws.cell(row=row, column=1, value=pays)
        ws.cell(row=row, column=2, value=s["nb_sites"])
        ws.cell(row=row, column=3, value=s["support"])
        ws.cell(row=row, column=4, value=s["localises"])
        ws.cell(row=row, column=5, value=s["avec_anomalie"])
        row += 1

    # Largeurs colonnes
    ws.column_dimensions["A"].width = 35
    ws.column_dimensions["B"].width = 50
    ws.column_dimensions["C"].width = 15
    ws.column_dimensions["D"].width = 12
    ws.column_dimensions["E"].width = 15

    # =========================================================
    # ONGLET 2 : DÉTAIL DES ANOMALIES
    # =========================================================
    ws2 = wb.create_sheet("Détail anomalies")
    df_anomalies = collector.to_dataframe()

    if not df_anomalies.empty:
        df_anomalies = df_anomalies.copy()
        df_anomalies = df_anomalies.where(pd.notna(df_anomalies), None)

        for col_idx, col_name in enumerate(df_anomalies.columns, 1):
            cell = ws2.cell(row=1, column=col_idx, value=col_name)
            cell.font = header_font
            cell.fill = header_fill_blue

        for row_idx, row_data in enumerate(df_anomalies.itertuples(index=False), 2):
            for col_idx, value in enumerate(row_data, 1):
                ws2.cell(row=row_idx, column=col_idx, value=value)

        # Largeurs auto
        col_widths = [15, 15, 15, 40, 10, 15, 60, 40, 30, 40, 12]
        for i, w in enumerate(col_widths, 1):
            ws2.column_dimensions[ws2.cell(row=1, column=i).column_letter].width = w

    # =========================================================
    # ONGLET 3 : DONNÉES ENRICHIES
    # =========================================================
    ws3 = wb.create_sheet("Données enrichies")

    # Colonnes à exporter : identité + localisation + flags
    priority_cols = [
        Config.COL_SITE_ID,
        Config.COL_CONTRACT_ID,
        Config.COL_COUNTRY,
        Config.COL_POSTAL_CODE,
        Config.COL_CITY,
        Config.COL_LONGITUDE,
        Config.COL_LATITUDE,
        "_IS_LOCALIZED",
        "_LOCALIZATION_MODE",
        "_HAS_ANOMALY",
        "_ANOMALY_COUNT",
        "_ANOMALY_CODES",
        "_WORST_SEVERITY",
        "_SUPPORT_LEVEL",
    ]
    cols = [c for c in priority_cols if c in df.columns]
    df_export = df[cols].copy()
    df_export = df_export.where(pd.notna(df_export), None)

    # En-têtes
    for col_idx, col_name in enumerate(df_export.columns, 1):
        cell = ws3.cell(row=1, column=col_idx, value=col_name)
        cell.font = header_font
        cell.fill = header_fill_blue

    # Données (limité à 1M lignes pour Excel)
    max_rows = min(len(df_export), 1000_000)
    if len(df_export) > max_rows:
        print(f"Limitation à {format_number(max_rows)} lignes (onglet Données enrichies)")

    for row_idx, row_data in enumerate(df_export.head(max_rows).itertuples(index=False), 2):
        for col_idx, value in enumerate(row_data, 1):
            ws3.cell(row=row_idx, column=col_idx, value=value)

    ws3.column_dimensions["A"].width = 20
    ws3.column_dimensions["B"].width = 15

    # Sauvegarder
    wb.save(output_path)

    print(f"\nRapport généré : {output_path}")
    print(f"   Résumé                : statistiques globales + par pays")
    print(f"   Détail anomalies      : {format_number(len(df_anomalies))} lignes")
    print(f"   Données enrichies     : {format_number(max_rows)} lignes")

    return output_path
