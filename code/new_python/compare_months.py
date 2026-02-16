"""
Script pour comparer le volume de données par mois entre Python et SAS
et identifier pourquoi les montants sont différents.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, countDistinct

# Initialiser Spark
spark = SparkSession.builder.appName("CompareMonths").getOrCreate()

from utils.loaders.config_loader import ConfigLoader
config = ConfigLoader("config/config.yml")

# Lire les données Bronze
from src.reader import BronzeReader
reader = BronzeReader(spark, config)

print("=" * 80)
print("COMPARAISON PYTHON VS SAS - Volume par Mois")
print("=" * 80)

# Lire les données brutes
vision = "202601"
df = reader.read_file_group('rf_fr1_prm_dtl_midcorp_m', vision)

print(f"\n1. TOTAL DONNÉES BRUTES")
print(f"Total lignes : {df.count():,}")

# Filtrer marché Construction
df = df.filter(col('cd_marche') == '6')
print(f"\n2. APRÈS FILTRE MARCHÉ (cd_marche='6')")
print(f"Total lignes : {df.count():,}")

# Analyser la distribution par mois
print(f"\n3. DISTRIBUTION PAR MOIS (dt_cpta_cts)")
df.groupBy('dt_cpta_cts') \
  .agg(
      count('*').alias('nb_lignes'),
      _sum('mt_ht_cts').alias('sum_mt_ht'),
      countDistinct('nu_cnt_prm').alias('nb_polices')
  ) \
  .orderBy('dt_cpta_cts') \
  .show(50, truncate=False)

# Comparer avec filtre Python actuel (dt_cpta_cts <= vision)
print(f"\n4. AVEC FILTRE PYTHON (dt_cpta_cts <= '{vision}')")
df_filtered_python = df.filter(col('dt_cpta_cts') <= vision)
print(f"Total lignes : {df_filtered_python.count():,}")

result_python = df_filtered_python.agg(
    _sum('mt_ht_cts').alias('sum_primes'),
    countDistinct('nu_cnt_prm').alias('nb_polices')
).collect()[0]

print(f"Somme mt_ht_cts : {result_python['sum_primes']:,.2f}")
print(f"Nombre de polices : {result_python['nb_polices']:,}")

# Comparer avec filtre année complète (toute l'année 2026)
print(f"\n5. AVEC FILTRE ANNÉE COMPLÈTE (dt_cpta_cts <= '202612')")
df_filtered_year = df.filter(col('dt_cpta_cts') <= '202612')
print(f"Total lignes : {df_filtered_year.count():,}")

result_year = df_filtered_year.agg(
    _sum('mt_ht_cts').alias('sum_primes'),
    countDistinct('nu_cnt_prm').alias('nb_polices')
).collect()[0]

print(f"Somme mt_ht_cts : {result_year['sum_primes']:,.2f}")
print(f"Nombre de polices : {result_year['nb_polices']:,}")

# Calculer le ratio
ratio = result_year['sum_primes'] / result_python['sum_primes'] if result_python['sum_primes'] else 0
print(f"\n6. RATIO ANNÉE / MOIS")
print(f"Ratio : {ratio:.2f}x")

# Vérifier par cdprod
print(f"\n7. COMPARAISON PAR CDPROD")
print("\nAvec dt_cpta_cts <= '202601' :")
df_filtered_python.groupBy('cd_prd_prm').agg(
    _sum('mt_ht_cts').alias('sum_mt_ht')
).orderBy('cd_prd_prm').show(10)

print("\nAvec dt_cpta_cts <= '202612' :")
df_filtered_year.groupBy('cd_prd_prm').agg(
    _sum('mt_ht_cts').alias('sum_mt_ht')
).orderBy('cd_prd_prm').show(10)

print("\n" + "=" * 80)
print("FIN DE L'ANALYSE")
print("=" * 80)

spark.stop()
