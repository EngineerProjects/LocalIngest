"""
Script de diagnostic pour identifier pourquoi les primes sont à zéro
dans le pipeline Emissions.

Ce script vérifie :
1. Les valeurs de mt_ht_cts dans la source
2. Les valeurs après filtres
3. Les valeurs après agrégation
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, countDistinct

# Initialiser Spark
spark = SparkSession.builder.appName("EmissionsDiag").getOrCreate()

# Configuration
vision = "202601"
config_path = "config/config.yml"
reading_config_path = "config/reading_config.json"

# Charger les utili Aires
from utils.loaders.config_loader import ConfigLoader
from src.reader import BronzeReader

# Charger la config
config = ConfigLoader(config_path)

# Lecture des données
reader = BronzeReader(spark, config, reading_config_path)
df = reader.read_file_group('rf_fr1_prm_dtl_midcorp_m', 'ref')

print("=" * 80)
print("DIAGNOSTIC EMISSIONS - Primes à Zéro")
print("=" * 80)

# 1. Statistiques sur les données brutes
print("\n1. STATISTIQUES DONNÉES BRUTES")
print(f"Total lignes : {df.count():,}")
print(f"Colonnes : {df.columns}")
df.printSchema()

# 2. Vérifier les valeurs de mt_ht_cts
print("\n2. VALEURS DE mt_ht_cts")
df_stats = df.select(
    count("*").alias("total_lines"),
    count("mt_ht_cts").alias("non_null_mt_ht"),
    _sum("mt_ht_cts").alias("sum_mt_ht"),
    countDistinct("mt_ht_cts").alias("distinct_values")
).collect()[0]

print(f"Total lignes : {df_stats['total_lines']:,}")
print(f"mt_ht_cts non-null : {df_stats['non_null_mt_ht']:,}")
print(f"Somme mt_ht_cts : {df_stats['sum_mt_ht']:,.2f}" if df_stats['sum_mt_ht'] else "Somme mt_ht_cts : NULL")
print(f"Valeurs distinctes : {df_stats['distinct_values']:,}")

#  3. Exemples de valeurs
print("\n3. EXEMPLES DE VALEURS (10 premières lignes)")
df.select("dt_cpta_cts", "cd_marche", "mt_ht_cts", "mt_cms_cts").show(10, truncate=False)

# 4. Après mise en minuscules
print("\n4. APRÈS MISE EN MINUSCULES")
from utils.transformations import lowercase_all_columns
df_lower = lowercase_all_columns(df)
df_lower_stats = df_lower.select(
    count("*").alias("total_lines"),
    count("mt_ht_cts").alias("non_null_mt_ht"),
    _sum("mt_ht_cts").alias("sum_mt_ht")
).collect()[0]

print(f"Total lignes : {df_lower_stats['total_lines']:,}")
print(f"mt_ht_cts non-null : {df_lower_stats['non_null_mt_ht']:,}")
print(f"Somme mt_ht_cts : {df_lower_stats['sum_mt_ht']:,.2f}" if df_lower_stats['sum_mt_ht'] else "Somme mt_ht_cts : NULL")

# 5. Après filtre marché
print("\n5. APRÈS FILTRE MARCHÉ (cd_marche='6')")
df_market = df_lower.filter(col('cd_marche') == '6')
df_market_stats = df_market.select(
    count("*").alias("total_lines"),
    count("mt_ht_cts").alias("non_null_mt_ht"),
    _sum("mt_ht_cts").alias("sum_mt_ht")
).collect()[0]

print(f"Total lignes : {df_market_stats['total_lines']:,}")
print(f"mt_ht_cts non-null : {df_market_stats['non_null_mt_ht']:,}")
print(f"Somme mt_ht_cts : {df_market_stats['sum_mt_ht']:,.2f}" if df_market_stats['sum_mt_ht'] else "Somme mt_ht_cts : NULL")

# 6. Après filtre date
print("\n6. APRÈS FILTRE DATE (dt_cpta_cts <= '202601')")
df_date = df_market.filter(col('dt_cpta_cts') <= vision)
df_date_stats = df_date.select(
    count("*").alias("total_lines"),
    count("mt_ht_cts").alias("non_null_mt_ht"),
    _sum("mt_ht_cts").alias("sum_mt_ht")
).collect()[0]

print(f"Total lignes : {df_date_stats['total_lines']:,}")
print(f"mt_ht_cts non-null : {df_date_stats['non_null_mt_ht']:,}")
print(f"Somme mt_ht_cts : {df_date_stats['sum_mt_ht']:,.2f}" if df_date_stats['sum_mt_ht'] else "Somme mt_ht_cts : NULL")

# 7. Exemples après tous les filtres
print("\n7. EXEMPLES APRÈS FILTRES (10 premières lignes)")
df_date.select("dt_cpta_cts", "cd_marche", "mt_ht_cts", "mt_cms_cts", "nu_cnt_prm").show(10, truncate=False)

print("\n" + "=" * 80)
print("FIN DU DIAGNOSTIC")
print("=" * 80)

spark.stop()
