"""
Script de diagnostic pour identifier le problème d'ordre des colonnes
dans rf_fr1_prm_dtl_midcorp_m.csv

Le problème : mt_ht_cts et mt_cms_cts sont NULL dans toutes les lignes
Cause probable : L'ordre des colonnes dans le CSV ne correspond pas au schéma défini
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Initialiser Spark
spark = SparkSession.builder.appName("ColumnOrderDiag").getOrCreate()

# Configuration
from utils.loaders.config_loader import ConfigLoader
config = ConfigLoader("config/config.yml")

# Construire le chemin vers le fichier
import os
container = os.getenv('DATALAKE_CONTAINER') or config.get('environment.container')
data_root = config.get('datalake.data_root')
file_path = f"{container}{data_root}/bronze/ref/rf_fr1_prm_dtl_midcorp_m.csv"

print("=" * 80)
print("DIAGNOSTIC ORDRE DES COLONNES")
print("=" * 80)
print(f"\nFichier : {file_path}")

# 1. Lire SANS schéma pour voir l'ordre réel des colonnes dans le header
print("\n1. LECTURE SANS SCHÉMA (inferSchema=true)")
df_infer = spark.read.option("sep", "|") \
    .option("header", "true") \
    .option("encoding", "LATIN9") \
    .option("inferSchema", "true") \
    .csv(file_path)

print(f"\nNombre de colonnes dans le CSV : {len(df_infer.columns)}")
print("\nOrdre des colonnes dans le CSV :")
for i, col in enumerate(df_infer.columns, 1):
    print(f"  {i:2d}. {col}")

print("\nSchéma inféré :")
df_infer.printSchema()

# 2. Afficher quelques lignes pour voir les valeurs
print("\n2. EXEMPLES DE DONNÉES (5 premières lignes)")
df_infer.show(5, truncate=False)

# 3. Vérifier si les colonnes de montants existent et ont des valeurs
if 'mt_ht_cts' in df_infer.columns:
    print("\n3. STATISTIQUES mt_ht_cts")
    from pyspark.sql.functions import count, sum as _sum
    stats = df_infer.select(
        count("*").alias("total"),
        count("mt_ht_cts").alias("non_null"),
        _sum("mt_ht_cts").alias("somme")
    ).collect()[0]
    print(f"Total lignes : {stats['total']:,}")
    print(f"mt_ht_cts non-null : {stats['non_null']:,}")
    print(f"Somme mt_ht_cts : {stats['somme']:,.2f}" if stats['somme'] else "Somme : NULL")
else:
    print("\n⚠️  COLONNE mt_ht_cts N'EXISTE PAS DANS LE CSV!")

if 'mt_cms_cts' in df_infer.columns:
    print("\n4. STATISTIQUES mt_cms_cts")
    stats2 = df_infer.select(
        count("mt_cms_cts").alias("non_null"),
        _sum("mt_cms_cts").alias("somme")
    ).collect()[0]
    print(f"mt_cms_cts non-null : {stats2['non_null']:,}")
    print(f"Somme mt_cms_cts : {stats2['somme']:,.2f}" if stats2['somme'] else "Somme : NULL")
else:
    print("\n⚠️  COLONNE mt_cms_cts N'EXISTE PAS DANS LE CSV!")

# 4. Comparer avec le schéma défini dans column_definitions.py
print("\n" + "=" * 80)
print("COMPARAISON AVEC LE SCHÉMA DÉFINI")
print("=" * 80)

from config.column_definitions import RF_FR1_PRM_DTL_MIDCORP_M_SCHEMA
print("\nSchéma défini dans column_definitions.py :")
print(RF_FR1_PRM_DTL_MIDCORP_M_SCHEMA)

# Extraire les colonnes du schéma string
import re
schema_cols = []
for part in RF_FR1_PRM_DTL_MIDCORP_M_SCHEMA.strip().split(','):
    match = re.search(r'(\w+)\s+\w+', part.strip())
    if match:
        schema_cols.append(match.group(1))

print(f"\nNombre de colonnes dans le schéma : {len(schema_cols)}")
print("\nOrdre des colonnes dans le schéma :")
for i, col in enumerate(schema_cols, 1):
    print(f"  {i:2d}. {col}")

# 5. Identifier les différences
print("\n" + "=" * 80)
print("DIFFÉRENCES D'ORDRE")
print("=" * 80)

csv_cols = df_infer.columns
if len(csv_cols) != len(schema_cols):
    print(f"\n⚠️  NOMBRE DE COLONNES DIFFÉRENT!")
    print(f"   CSV : {len(csv_cols)} colonnes")
    print(f"   Schéma : {len(schema_cols)} colonnes")

mismatches = []
for i in range(min(len(csv_cols), len(schema_cols))):
    if csv_cols[i].lower() != schema_cols[i].lower():
        mismatches.append((i+1, csv_cols[i], schema_cols[i]))

if mismatches:
    print(f"\n⚠️  {len(mismatches)} COLONNES DANS LE MAUVAIS ORDRE:")
    print("\n  Pos | CSV                | Schéma")
    print("  " + "-" * 50)
    for pos, csv_col, schema_col in mismatches:
        print(f"  {pos:2d}  | {csv_col:18s} | {schema_col}")
else:
    print("\n✅ L'ordre des colonnes correspond!")

print("\n" + "=" * 80)
print("FIN DU DIAGNOSTIC")
print("=" * 80)

spark.stop()
