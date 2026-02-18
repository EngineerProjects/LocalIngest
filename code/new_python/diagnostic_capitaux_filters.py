# -*- coding: utf-8 -*-
"""
Diagnostic des colonnes de filtre Capitaux AZ.

Objectif:
- Comprendre pourquoi le filtre cdri != 'X' élimine 100% des lignes
- Vérifier présence, NULLs, valeurs distinctes
- Tester filtres en mode Spark strict vs SAS-like (trim/upper + NULL conservés)

Exécution :
  python diagnostic_capitaux_filters.py
"""

import os
import sys

# -----------------------------------------------------------------------------
# 0) Ensure project root is in PYTHONPATH
# -----------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "."))  # adjust if needed: ".."
sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, trim, upper

# -----------------------------------------------------------------------------
# 1) ConfigLoader import (supports both code layouts)
# -----------------------------------------------------------------------------
ConfigLoader = None
try:
    # Old/working layout (you showed it in the working script)
    from utils.loaders.config_loader import ConfigLoader  # type: ignore
except Exception:
    try:
        # Alternative layout (your failing import)
        from config.config_loader import ConfigLoader  # type: ignore
    except Exception as e:
        raise ImportError(
            "Impossible d'importer ConfigLoader.\n"
            "Essaye l'un des modules:\n"
            " - utils.loaders.config_loader\n"
            " - config.config_loader\n"
            f"Détails: {e}"
        )

from src.reader import BronzeReader

# -----------------------------------------------------------------------------
# 2) Helpers
# -----------------------------------------------------------------------------
def resolve_col(df, name: str):
    """Return the actual column name in df matching `name` case-insensitively, else None."""
    for c in df.columns:
        if c.lower() == name.lower():
            return c
    return None

def sas_not_equal(cname: str, value: str):
    """SAS-like: keep NULLs + trim/upper before comparing."""
    return col(cname).isNull() | (upper(trim(col(cname))) != value)

def sas_in(cname: str, values):
    """SAS-like IN: keep NULLs + trim/upper."""
    vals = [v.upper() for v in values]
    return col(cname).isNull() | upper(trim(col(cname))).isin(vals)

# -----------------------------------------------------------------------------
# 3) Run
# -----------------------------------------------------------------------------
config = ConfigLoader()  # if your ConfigLoader requires a path, pass it here
vision = config.get("runtime.vision_", "202601")

spark = (
    SparkSession.builder
    .appName("DiagCapitauxFilters")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

reading_config_path = config.get("config_files.reading_config", "config/reading_config.json")

reader = BronzeReader(spark, config, reading_config_path)

# NOTE: You read ipf here; if CAPITAUX uses another file_group upstream, adjust accordingly.
df = reader.read_file_group("ipf", vision)

total = df.count()
print("\n" + "=" * 70)
print("DIAGNOSTIC CAPITAUX AZ — Données brutes IPF")
print(f"Vision: {vision}")
print(f"Total lignes : {total:,}")
print("=" * 70)

# -----------------------------------------------------------------------------
# 4) Column presence + mapping
# -----------------------------------------------------------------------------
filter_columns = ["cdri", "cdsitp", "cdnatp", "cdprod", "noint", "cmarch", "csegt"]

print("\n--- Colonnes présentes dans le DataFrame (case-insensitive) ---")
resolved = {}
for fc in filter_columns:
    rc = resolve_col(df, fc)
    resolved[fc] = rc
    print(f"  {fc:12s} : {'✓ TROUVÉ -> ' + rc if rc else '✗ ABSENT'}")

# -----------------------------------------------------------------------------
# 5) Distinct values + NULL stats for each column
# -----------------------------------------------------------------------------
print("\n--- Valeurs distinctes par colonne de filtre (top 20) ---")
for fc in filter_columns:
    rc = resolved.get(fc)
    if not rc:
        print(f"\n  {fc}: COLONNE ABSENTE !")
        continue

    print(f"\n  {fc} (colonne réelle: '{rc}'):")
    (
        df.groupBy(col(rc).alias("value"))
        .agg(count("*").alias("count"))
        .orderBy(col("count").desc())
        .show(20, truncate=False)
    )

    null_count = df.filter(col(rc).isNull()).count()
    non_null_count = total - null_count
    print(f"  NULLs     : {null_count:,} ({(100*null_count/total):.1f}%)")
    print(f"  Non-NULLs : {non_null_count:,} ({(100*non_null_count/total):.1f}%)")

# -----------------------------------------------------------------------------
# 6) Filter tests: Spark strict vs SAS-like
# -----------------------------------------------------------------------------
print("\n" + "=" * 70)
print("TEST FILTRES (Spark strict vs SAS-like)")
print("=" * 70)

tests = []

if resolved["cdri"]:
    c = resolved["cdri"]
    tests += [
        ("cdri != 'X' (Spark)", col(c) != "X"),
        ("cdri != 'X' (SAS-like)", sas_not_equal(c, "X")),
        ("cdri is NULL", col(c).isNull()),
    ]
else:
    print("cdri absent -> filtre cdri invalide (=> peut expliquer le 0 lignes)")

if resolved["cdsitp"]:
    c = resolved["cdsitp"]
    tests += [
        ("cdsitp != '5' (Spark)", col(c) != "5"),
        ("cdsitp != '5' (SAS-like)", sas_not_equal(c, "5")),
        ("cdsitp is NULL", col(c).isNull()),
    ]

if resolved["cdnatp"]:
    c = resolved["cdnatp"]
    tests += [
        ("cdnatp in (R,O,T) (Spark)", col(c).isin("R", "O", "T")),
        ("cdnatp in (R,O,T) (SAS-like)", sas_in(c, ["R", "O", "T"])),
        ("cdnatp is NULL", col(c).isNull()),
    ]

if resolved["cmarch"]:
    c = resolved["cmarch"]
    tests += [
        ("cmarch == '6' (Spark)", col(c) == "6"),
        ("cmarch == '6' (trim/upper)", upper(trim(col(c))) == "6"),
    ]

if resolved["csegt"]:
    c = resolved["csegt"]
    tests += [
        ("csegt == '2' (Spark)", col(c) == "2"),
        ("csegt == '2' (trim/upper)", upper(trim(col(c))) == "2"),
    ]

for label, condition in tests:
    cnt = df.filter(condition).count()
    pct = 100 * cnt / total if total else 0.0
    print(f"  {label:35s} → {cnt:>10,} lignes ({pct:.1f}%)")

spark.stop()
print("\n✓ Diagnostic terminé")
