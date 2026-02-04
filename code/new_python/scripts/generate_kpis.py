#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script: Agrégation KPIs PTF_MVT avec conformité SAS exacte

Objectif: Générer KPIs agrégés par cdprod avec précision décimale
et gestion NULLs exactement comme SAS pour match 100%.

Problèmes résolus:
1. Arrondis flottants PySpark (primes_afn/res off by €0.01-€40)
2. NUL Ls SAS (.) vs Python 0.0 pour sum_of_mtca1
3. Précision décimale sum_of_mtca1 (notation scientifique vs entiers)

Usage:
    python scripts/generate_kpis.py --vision 202512 --output prompts/result_exact.md
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, countDistinct,
    round as spark_round, when, coalesce, lit
)
from pathlib import Path
import argparse
import sys

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from utils.loaders.config_loader import ConfigLoader


def generate_kpis_with_sas_precision(spark, vision: str, config: ConfigLoader):
    """
    Generate KPIs aggregated by cdprod with SAS-exact precision.
    
    Args:
        spark: SparkSession
        vision: Vision in YYYYMM format
        config: ConfigLoader instance
    
    Returns:
        DataFrame with KPIs
    """
    print(f"\n{'='*80}")
    print(f"GÉNÉRATION KPIs CONFORMITÉ SAS - Vision {vision}")
    print(f"{'='*80}\n")
    
    # Read gold consolidated data
    base_path = config.get("datalake.base_path")
    path_template = config.get("datalake.path_template")
    
    year = vision[:4]
    month = vision[4:6]
    layer_path = path_template.format(
        base_path=base_path,
        layer="gold",
        year=year,
        month=month
    )
    
    gold_path = f"{layer_path}/ptf_mvt_{vision}"
    
    print(f"📂 Reading from: {gold_path}")
    df = spark.read.format("delta").load(gold_path)
    
    initial_count = df.count()
    print(f"📊 Records loaded: {initial_count:,}\n")
    
    # ================================================================
    # AGRÉGATIONS AVEC PRÉCISION SAS
    # ================================================================
    
    print("⚙️  Calculating KPIs with SAS precision...")
    
    df_kpi = df.groupBy("cdprod").agg(
        # COUNT - Direct, always matches SAS ✅
        countDistinct("nopol").alias("count_of_nopol"),
        
        # MTCA1 - PRECISION FIX 1: Cast to DECIMAL(18,2) then round
        # If ALL mtca1 are NULL → return NULL (SAS .), not 0
        when(
            count(when(col("mtca1").isNotNull(), lit(1))).otherwise(lit(None)) == 0,
            lit(None)
        ).otherwise(
            spark_round(spark_sum(col("mtca1").cast("decimal(18,2)")), 2)
        ).alias("sum_of_mtca1"),
        
        # Counts NB* - Always exact integers ✅
        coalesce(spark_sum("nbptf"), lit(0)).cast("int").alias("sum_of_nbptf"),
        coalesce(spark_sum("nbafn"), lit(0)).cast("int").alias("sum_of_nbafn"),
        coalesce(spark_sum("nbres"), lit(0)).cast("int").alias("sum_of_nbres"),
        
        # PRIMES - PRECISION FIX 2: Round to 2 decimals (CRITICAL for match)
        # SAS stores with higher precision but displays rounded
        spark_round(coalesce(spark_sum("primes_afn"), lit(0.0)), 2).alias("sum_of_primes_afn"),
        spark_round(coalesce(spark_sum("primes_res"), lit(0.0)), 2).alias("sum_of_primes_res"),
    )
    
    # Sort by cdprod for easy comparison
    df_kpi = df_kpi.orderBy("cdprod")
    
    print(f"✅ KPIs calculated for {df_kpi.count()} products\n")
    
    return df_kpi


def write_kpis_to_markdown(df_kpi, output_path: str):
    """
    Write KPIs to markdown table format.
    
    Args:
        df_kpi: DataFrame with KPIs
        output_path: Output markdown file path
    """
    print(f"📝 Writing KPIs to: {output_path}")
    
    # Convert to Pandas for easier markdown formatting
    pdf = df_kpi.toPandas()
    
    # Replace None with "." for SAS-style NULL display
    # pdf = pdf.fillna(".")
    
    # Write markdown table
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(pdf.to_markdown(index=False, floatfmt=".2f"))
    
    print(f"✅ Written {len(pdf)} rows to {output_path}\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Generate PTF_MVT KPIs with SAS precision")
    parser.add_argument("--vision", required=True, help="Vision in YYYYMM format (e.g., 202512)")
    parser.add_argument("--output", default="prompts/result_exact.md", help="Output markdown file")
    
    args = parser.parse_args()
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName(f"PTF_MVT_KPIs_{args.vision}") \
        .getOrCreate()
    
    # Load config
    config = ConfigLoader()
    
    try:
        # Generate KPIs
        df_kpi = generate_kpis_with_sas_precision(spark, args.vision, config)
        
        # Display sample
        print("📊 PREVIEW (first 10 rows):")
        df_kpi.show(10, truncate=False)
        
        # Write to file
        write_kpis_to_markdown(df_kpi, args.output)
        
        print(f"{'='*80}")
        print("🎉 KPI GENERATION COMPLETE")
        print(f"{'='*80}\n")
        
        print("📌 NEXT STEPS:")
        print(f"1. Compare with SAS: diff {args.output} prompts/sas_result.md")
        print(f"2. Validate precision match")
        print(f"3. Check NULL handling for products CNR/CTR/DO0/etc.\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
