#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Date Format Audit — Real Formats Only

But :
    - Lister pour chaque file_group (ayant des colonnes DateType dans SCHEMA_REGISTRY)
      le format RÉEL observé des colonnes, sans comparer à une config "expected".
    - Nettoyer les NULLs façon SAS avant détection.
    - Supporter les fichiers mensuels avec {vision}.

Affichage :
    Par file_group :
        - Path
        - Colonnes DateType existantes dans le CSV
        - Pour chaque colonne : sample (non null nettoyé) + format détecté

Usage :
    spark-submit scripts/date_format_audit.py
"""

import json
import re
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, trim, regexp_replace, when, lit
)
from pyspark.sql.types import StringType, DateType

from config.schemas import SCHEMA_REGISTRY


# ======================================================================================
# SPARK
# ======================================================================================
spark = (
    SparkSession.builder
    .appName("DateFormatAuditRealOnly")
    .getOrCreate()
)

# ======================================================================================
# LECTURE reading_config.json (recherche vers le haut)
# ======================================================================================
script_path = Path(__file__).resolve()
current_dir = script_path.parent

config_path = None
for _ in range(3):
    test_path = current_dir / "config" / "reading_config.json"
    if test_path.exists():
        config_path = test_path
        break
    current_dir = current_dir.parent

if not config_path:
    raise FileNotFoundError("❌ Impossible de trouver config/reading_config.json")

with open(config_path, "r") as f:
    reading_config = json.load(f)

# ======================================================================================
# PARAMS DATALAKE
# ======================================================================================
VISION = "202512"  # <- ajuste si nécessaire
YEAR = VISION[:4]
MONTH = VISION[4:6]

BASE = (
    "abfss://shared@azfrdatalab.dfs.core.windows.net/"
    "ABR/P4D/ADC/DATAMARTS/CONSTRUCTION"
)

bronze_month_path = f"{BASE}/bronze/{YEAR}/{MONTH}"
bronze_ref_path = f"{BASE}/bronze/ref"


# ======================================================================================
# UTILS : Clean SAS NULLs
# ======================================================================================
def _clean_sas_nulls(df: DataFrame) -> DataFrame:
    """
    Nettoie les valeurs SAS nulles dans tous les champs string :
      - "." → NULL
      - "" → NULL
      - " " / espaces longs / U+00A0 → NULL
    """
    string_cols = [c for c, t in df.dtypes if t == "string"]
    if not string_cols:
        return df

    exprs = []
    for c in df.columns:
        if c in string_cols:
            tmp = trim(regexp_replace(col(c), u"\u00A0", " "))
            cleaned = when(
                (tmp == ".") | (tmp == "") | tmp.isNull(),
                lit(None).cast(StringType())
            ).otherwise(tmp).alias(c)
            exprs.append(cleaned)
        else:
            exprs.append(col(c))

    return df.select(*exprs)


# ======================================================================================
# UTILS : Extraire les colonnes DateType depuis SCHEMA_REGISTRY
# ======================================================================================
def get_date_columns(schema):
    """Retourne les colonnes de type DateType du schema."""
    if schema is None:
        return []
    return [f.name for f in schema.fields if isinstance(f.dataType, DateType)]


def build_filegroup_date_map():
    """Construit {file_group -> [date columns]} en partant du SCHEMA_REGISTRY"""
    out = {}
    for fg, schema in SCHEMA_REGISTRY.items():
        cols = get_date_columns(schema)
        if cols:
            out[fg] = cols
    return out


# ======================================================================================
# UTILS : Lecture CSV en ne gardant que les colonnes DateType
# ======================================================================================
def read_date_columns_only(path, sep, date_cols):
    """
    Lit le CSV (sans schéma explicite), restreint aux seules colonnes DateType,
    puis nettoie les valeurs SAS NULLs sur les colonnes string.
    """
    df = spark.read.csv(path, sep=sep, header=True, inferSchema=False)
    existing = [c for c in date_cols if c in df.columns]
    if not existing:
        return None, []

    df = df.select(*existing)
    df = _clean_sas_nulls(df)
    return df, existing


# ======================================================================================
# DÉTECTION DE FORMAT — patterns enrichis (ISO, slash, tirets, SAS, période)
# ======================================================================================
SAS_MONTHS = "(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)"

# l’ordre compte : on met les patterns les plus spécifiques d’abord
PATTERN_TO_FORMAT = [
    # SAS Datetime : 14MAY1991:00:00:00
    (rf"^\d{{2}}{SAS_MONTHS}\d{{4}}:\d{{2}}:\d{{2}}:\d{{2}}$", "ddMMMyyyy:HH:mm:ss"),
    # ISO
    (r"^\d{4}-\d{2}-\d{2}$", "yyyy-MM-dd"),
    # Slash
    (r"^\d{2}/\d{2}/\d{4}$", "dd/MM/yyyy or MM/dd/yyyy"),
    (r"^\d{4}/\d{2}/\d{2}$", "yyyy/MM/dd"),
    # Compact
    (r"^\d{8}$", "yyyyMMdd"),
    # Tirets
    (r"^\d{2}-\d{2}-\d{4}$", "dd-MM-yyyy or MM-dd-yyyy"),
    # Période YYYYMM
    (r"^\d{6}$", "yyyyMM"),
]


def detect_date_format(value: str) -> str:
    """
    Détecte un format probable à partir d’une valeur de sample (string).
    Retourne "NULL" si value est None/empty, "UNKNOWN" si aucun pattern match.
    """
    if not value:
        return "NULL"
    v = value.strip().upper()  # upper pour les mois 'Jan'/'JAN'
    for pattern, fmt in PATTERN_TO_FORMAT:
        if re.match(pattern, v):
            return fmt
    return "UNKNOWN"


# ======================================================================================
# AUDIT D’UN FILE_GROUP : affiche les formats observés (sans "expected")
# ======================================================================================
def resolve_pattern(file_patterns, vision):
    """
    Pour ce script, on ne prend que le premier pattern et on applique {vision}.
    """
    if not file_patterns:
        return None
    return file_patterns[0].replace("{vision}", vision)


def audit_file_group_real_formats(fg, date_map):
    """
    Affiche les formats RÉELS pour chaque colonne date du file_group fg.
    Ne fait AUCUNE comparaison avec "expected".
    """
    schema = SCHEMA_REGISTRY.get(fg)
    date_cols = date_map.get(fg, [])
    if not date_cols:
        return {}

    cfg = reading_config["file_groups"].get(fg)
    if not cfg:
        return {}

    sep = cfg.get("read_options", {}).get("sep", "|")
    loc = cfg.get("location_type", "monthly")
    pattern = resolve_pattern(cfg.get("file_patterns", []), VISION)

    if not pattern:
        print(f"\n⚠️  {fg}: aucun pattern de fichier")
        return {}

    path = f"{bronze_month_path}/{pattern}" if loc == "monthly" else f"{bronze_ref_path}/{pattern}"

    # Header FG
    print("\n" + "=" * 120)
    print(f"📁 FILE GROUP : {fg}")
    print(f"📍 Path       : {path}")
    print(f"📋 Date cols  : {date_cols}")
    print("=" * 120)

    try:
        df, existing = read_date_columns_only(path, sep, date_cols)
    except Exception as e:
        print(f"❌ Erreur de lecture : {e}")
        return {}

    if df is None or not existing:
        print("❌ Aucune des colonnes date du schema n’est présente dans le CSV")
        return {}

    print(f"➡️  Colonnes présentes (DateType selon schema) : {existing}")

    results = {}

    for c in existing:
        row = (
            df.filter(col(c).isNotNull())
              .select(c)
              .limit(1)
              .collect()
        )
        if not row:
            results[c] = {"sample": None, "format": "ALL NULL"}
            print(f"• {c:20} | ALL NULL")
            continue

        value = row[0][0]
        fmt = detect_date_format(value)
        results[c] = {"sample": value, "format": fmt}
        print(f"• {c:20} | sample={str(value):20} | detected={fmt}")

    return results


# ======================================================================================
# MAIN
# ======================================================================================
if __name__ == "__main__":
    print("\n" + "="*120)
    print("🔬 DATE FORMAT AUDIT — REAL FORMATS ONLY")
    print("="*120)

    print("\n📌 Construction de la map file_group → colonnes DateType (depuis SCHEMA_REGISTRY)")
    date_map = build_filegroup_date_map()
    print(f"➡️  {len(date_map)} file_groups trouvés avec au moins une colonne DateType.\n")

    all_results = {}
    for fg in date_map.keys():
        res = audit_file_group_real_formats(fg, date_map)
        all_results[fg] = res

    print("\n" + "="*120)
    print("🏁 Audit terminé — Formats réels listés ci-dessus (pas de comparaison)")
    print("="*120)

    spark.stop()