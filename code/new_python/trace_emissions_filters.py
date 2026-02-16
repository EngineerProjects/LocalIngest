"""
Script pour tracer le parcours complet des données Emissions
et identifier où les écarts avec SAS se produisent.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, countDistinct, lit

spark = SparkSession.builder.appName("TraceEmissions").getOrCreate()

from utils.loaders.config_loader import ConfigLoader
from src.reader import BronzeReader
from utils.transformations import lowercase_all_columns, apply_emissions_filters
import json
from pathlib import Path

config = ConfigLoader("config/config.yml")
vision = "202601"

print("=" * 80)
print("TRAÇAGE COMPLET - Pipeline Emissions")
print("=" * 80)

# Étape 1: Lecture Bronze
reader = BronzeReader(spark, config)
df = reader.read_file_group('rf_fr1_prm_dtl_midcorp_m', vision)

print(f"\n📖 LECTURE BRONZE")
print(f"   Total lignes : {df.count():,}")
print(f"   Total mt_ht_cts : {df.agg(_sum('mt_ht_cts')).collect()[0][0]:,.2f}")
print(f"   Polices uniques : {df.select(countDistinct('nu_cnt_prm')).collect()[0][0]:,}")

# Étape 2: Mise en minuscules
df = lowercase_all_columns(df)
print(f"\n🔡 APRÈS MINUSCULES")
print(f"   Total lignes : {df.count():,}")

# Traçer chaque filtre individuellement
print(f"\n🔍 FILTRES MÉTIER (UN PAR UN)")

# Charger la config emissions
emissions_config_path = Path('config/transformations/emissions_config.json')
with open(emissions_config_path, 'r') as f:
    emissions_config = json.load(f)

# Filtre 1: Marché
df_step = df.filter(col('cd_marche') == '6')
lignes_perdues = df.count() - df_step.count()
print(f"\n   1. Filtre cd_marche='6'")
print(f"      Avant : {df.count():,}")
print(f"      Après : {df_step.count():,}")
print(f"      Perdues : {lignes_perdues:,}")
print(f"      mt_ht_cts : {df_step.agg(_sum('mt_ht_cts')).collect()[0][0]:,.2f}")
df = df_step

# Filtre 2: Date
count_before = df.count()
df_step = df.filter(col('dt_cpta_cts') <= lit(vision))
print(f"\n   2. Filtre dt_cpta_cts <= '{vision}'")
print(f"      Avant : {count_before:,}")
print(f"      Après : {df_step.count():,}")
print(f"      Perdues : {count_before - df_step.count():,}")
print(f"      mt_ht_cts : {df_step.agg(_sum('mt_ht_cts')).collect()[0][0]:,.2f}")
df = df_step

# Filtre 3: Intermédiaires exclus
excluded_noint_config = emissions_config.get('excluded_intermediaries', {})
excluded_noint = excluded_noint_config.get('values', [])
if excluded_noint:
    count_before = df.count()
    df_step = df.filter(~col('cd_int_stc').isin(excluded_noint))
    print(f"\n   3. Filtre intermédiaires exclus ({len(excluded_noint)} valeurs)")
    print(f"      Avant : {count_before:,}")
    print(f"      Après : {df_step.count():,}")
    print(f"      Perdues : {count_before - df_step.count():,}")
    print(f"      mt_ht_cts : {df_step.agg(_sum('mt_ht_cts')).collect()[0][0]:,.2f}")
    df = df_step

# Filtre 4: Exclusions produit/garantie
product_guarantee_config = emissions_config.get('product_guarantee_exclusions', {})
product_guarantee_rules = product_guarantee_config.get('rules', [])
for i, exclusion in enumerate(product_guarantee_rules):
    count_before = df.count()
    if 'product_prefix' in exclusion:
        from pyspark.sql.functions import substring
        df_step = df.filter(
            ~((substring(col('cd_prd_prm'), 1, 2) == exclusion['product_prefix']) &
              (col('cd_gar_prospctiv') == exclusion['guarantee']))
        )
    elif 'intermediary' in exclusion:
        df_step = df.filter(
            ~((col('cd_int_stc') == exclusion['intermediary']) &
              (col('cd_prd_prm') == exclusion['product']))
        )
    
    print(f"\n   4.{i+1}. Exclusion produit/garantie : {exclusion}")
    print(f"        Avant : {count_before:,}")
    print(f"        Après : {df_step.count():,}")
    print(f"        Perdues : {count_before - df_step.count():,}")
    df = df_step

# Filtre 5: Garanties exclues
excluded_guarantees_config = emissions_config.get('excluded_guarantees', {})
excluded_guarantees = excluded_guarantees_config.get('values', [])
if excluded_guarantees:
    count_before = df.count()
    df_step = df.filter(~col('cd_gar_prospctiv').isin(excluded_guarantees))
    print(f"\n   5. Garanties exclues ({len(excluded_guarantees)} valeurs)")
    print(f"      Avant : {count_before:,}")
    print(f"      Après : {df_step.count():,}")
    print(f"      Perdues : {count_before - df_step.count():,}")
    print(f"      mt_ht_cts : {df_step.agg(_sum('mt_ht_cts')).collect()[0][0]:,.2f}")
    df = df_step

# Filtre 6: Produit exclu
excluded_product_config = emissions_config.get('excluded_product', {})
excluded_product = excluded_product_config.get('value')
if excluded_product:
    count_before = df.count()
    df_step = df.filter(col('cd_prd_prm') != excluded_product)
    print(f"\n   6. Produit exclu : {excluded_product}")
    print(f"      Avant : {count_before:,}")
    print(f"      Après : {df_step.count():,}")
    print(f"      Perdues : {count_before - df_step.count():,}")
    df = df_step

# Filtre 7: Catégories exclues
excluded_categories_config = emissions_config.get('excluded_categories', {})
excluded_categories = excluded_categories_config.get('values', [])
excluded_gar_cat_config = emissions_config.get('excluded_guarantee_category', {})
excluded_gar_cat = excluded_gar_cat_config.get('value')

if excluded_categories or excluded_gar_cat:
    count_before = df.count()
    filter_expr = lit(False)
    if excluded_categories:
        filter_expr = filter_expr | col('cd_cat_min').isin(excluded_categories)
    if excluded_gar_cat:
        filter_expr = filter_expr | (col('cd_gar_prospctiv') == excluded_gar_cat)
    
    df_step = df.filter(~filter_expr)
    print(f"\n   7. Catégories exclues")
    if excluded_categories:
        print(f"      cd_cat_min exclus : {excluded_categories}")
    if excluded_gar_cat:
        print(f"      cd_gar_prospctiv exclu : {excluded_gar_cat}")
    print(f"      Avant : {count_before:,}")
    print(f"      Après : {df_step.count():,}")
    print(f"      Perdues : {count_before - df_step.count():,}")
    print(f"      mt_ht_cts : {df_step.agg(_sum('mt_ht_cts')).collect()[0][0]:,.2f}")
    df = df_step

# Résumé final
print(f"\n" + "=" * 80)
print("RÉSUMÉ FINAL")
print("=" * 80)
result = df.agg(
    count('*').alias('nb_lignes'),
    _sum('mt_ht_cts').alias('sum_mt_ht'),
    countDistinct('nu_cnt_prm').alias('nb_polices')
).collect()[0]

print(f"\nTotal lignes finales : {result['nb_lignes']:,}")
print(f"Total mt_ht_cts : {result['sum_mt_ht']:,.2f}")
print(f"Polices uniques : {result['nb_polices']:,}")

print(f"\n" + "=" * 80)
print("FIN DU TRAÇAGE")
print("=" * 80)

spark.stop()
