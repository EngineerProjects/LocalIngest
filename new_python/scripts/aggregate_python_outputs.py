"""
Validation Script - Aggregate Python outputs to compare with SAS.

Reads gold/silver layer outputs and creates the same aggregations as SAS
for product-level comparison.

Outputs 4 CSV files matching SAS naming convention:
1. WORK_QUERY_FOR_MVT_PTF{vision}.csv
2. WORK_QUERY_FOR_AZ_AZEC_CAPITAUX_{vision}.csv
3. WORK_QUERY_FOR_PRIMES_EMISES{vision}_POL.csv
4. WORK_QUERY_FOR_PRIMES_EMISES{vision}_POL_GARP.csv

Usage:
    python scripts/validate_outputs.py --vision 202509 --output-dir ./validation_outputs
"""

import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def init_spark():
    """Initialize Spark session."""
    return SparkSession.builder \
        .appName("Validation Aggregations") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def aggregate_mvt_ptf(spark, vision: str, gold_path: str):
    """
    Aggregate MVT_PTF by cdprod + global total.
    
    Metrics: MTCA, Primes_PTF, NBPTF, NBAFN, NBRES, Primes_AFN, Primes_RES
    
    Returns:
        DataFrame with aggregations
    """
    # Read consolidated MVT_PTF (AZ + AZEC)
    # Assuming consolidation output is in gold layer
    df = spark.read.parquet(f"{gold_path}/az_azec_mvt_ptf_{vision}")
    
    # Aggregation by cdprod
    agg_by_product = df.groupBy("cdprod").agg(
        F.sum("mtca").alias("mtca"),
        F.sum("primes_ptf").alias("primes_ptf"),
        F.sum("nbptf").alias("nbptf"),
        F.sum("nbafn").alias("nbafn"),
        F.sum("nbres").alias("nbres"),
        F.sum("primes_afn").alias("primes_afn"),
        F.sum("primes_res").alias("primes_res")
    ).orderBy("cdprod")
    
    # Global total (all products)
    agg_total = df.agg(
        F.sum("mtca").alias("mtca"),
        F.sum("primes_ptf").alias("primes_ptf"),
        F.sum("nbptf").alias("nbptf"),
        F.sum("nbafn").alias("nbafn"),
        F.sum("nbres").alias("nbres"),
        F.sum("primes_afn").alias("primes_afn"),
        F.sum("primes_res").alias("primes_res")
    ).withColumn("cdprod", F.lit("TOTAL"))
    
    # Union both
    result = agg_by_product.union(agg_total)
    
    return result


def aggregate_capitaux(spark, vision: str, gold_path: str):
    """
    Aggregate CAPITAUX by cdprod.
    
    Metrics: VALUE_INSURED_100_IND, PERTE_EXP_100_IND, VALUE_INSURED_100, PERTE_EXP_100
    
    Returns:
        DataFrame with aggregations
    """
    # Read consolidated capitaux (AZ + AZEC)
    df = spark.read.parquet(f"{gold_path}/az_azec_capitaux_{vision}")
    
    # Aggregation by cdprod
    result = df.groupBy("cdprod").agg(
        F.sum("value_insured_100_ind").alias("value_insured_100_ind"),
        F.sum("perte_exp_100_ind").alias("perte_exp_100_ind"),
        F.sum("value_insured_100").alias("value_insured_100"),
        F.sum("perte_exp_100").alias("perte_exp_100")
    ).orderBy("cdprod")
    
    return result


def aggregate_primes_emises_pol(spark, vision: str, gold_path: str):
    """
    Aggregate PRIMES_EMISES_POL by cdprod.
    
    Metrics: Primes_X, Primes_N, MTCOM_X
    
    Returns:
        DataFrame with aggregations
    """
    # Read from gold layer
    df = spark.read.parquet(f"{gold_path}/primes_emises{vision}_pol")
    
    # Aggregation by cdprod
    result = df.groupBy("cdprod").agg(
        F.sum("primes_x").alias("primes_x"),
        F.sum("primes_n").alias("primes_n"),
        F.sum("mtcom_x").alias("mtcom_x")
    ).orderBy("cdprod")
    
    return result


def aggregate_primes_emises_pol_garp(spark, vision: str, gold_path: str):
    """
    Aggregate PRIMES_EMISES_POL_GARP by cdprod.
    
    Same as above but from GARP perimeter table.
    
    Metrics: Primes_X, Primes_N, MTCOM_X
    
    Returns:
        DataFrame with aggregations
    """
    # Read from gold layer (GARP detail table)
    df = spark.read.parquet(f"{gold_path}/primes_emises{vision}_pol_garp")
    
    # Aggregation by cdprod
    result = df.groupBy("cdprod").agg(
        F.sum("primes_x").alias("primes_x"),
        F.sum("primes_n").alias("primes_n"),
        F.sum("mtcom_x").alias("mtcom_x")
    ).orderBy("cdprod")
    
    return result


def main():
    parser = argparse.ArgumentParser(description="Validate Python outputs against SAS")
    parser.add_argument("--vision", required=True, help="Vision in YYYYMM format (e.g., 202509)")
    parser.add_argument("--gold-path", default="gs://your-bucket/gold", 
                        help="Path to gold layer in datalake")
    parser.add_argument("--output-dir", default="./validation_outputs",
                        help="Local directory to save CSV outputs")
    
    args = parser.parse_args()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"🚀 Starting validation aggregations for vision {args.vision}")
    print(f"📂 Reading from: {args.gold_path}")
    print(f"💾 Saving to: {output_dir}")
    
    # Initialize Spark
    spark = init_spark()
    
    try:
        # 1. MVT_PTF aggregation
        print("\n📊 Aggregating MVT_PTF...")
        mvt_ptf = aggregate_mvt_ptf(spark, args.vision, args.gold_path)
        output_file_1 = output_dir / f"WORK_QUERY_FOR_MVT_PTF{args.vision}.csv"
        mvt_ptf.toPandas().to_csv(output_file_1, index=False)
        print(f"   ✅ Saved: {output_file_1}")
        print(f"   📈 Row count: {mvt_ptf.count()}")
        
        # 2. CAPITAUX aggregation
        print("\n📊 Aggregating CAPITAUX...")
        capitaux = aggregate_capitaux(spark, args.vision, args.gold_path)
        output_file_2 = output_dir / f"WORK_QUERY_FOR_AZ_AZEC_CAPITAUX_{args.vision}.csv"
        capitaux.toPandas().to_csv(output_file_2, index=False)
        print(f"   ✅ Saved: {output_file_2}")
        print(f"   📈 Row count: {capitaux.count()}")
        
        # 3. PRIMES_EMISES_POL aggregation
        print("\n📊 Aggregating PRIMES_EMISES_POL...")
        primes_pol = aggregate_primes_emises_pol(spark, args.vision, args.gold_path)
        output_file_3 = output_dir / f"WORK_QUERY_FOR_PRIMES_EMISES{args.vision}_POL.csv"
        primes_pol.toPandas().to_csv(output_file_3, index=False)
        print(f"   ✅ Saved: {output_file_3}")
        print(f"   📈 Row count: {primes_pol.count()}")
        
        # 4. PRIMES_EMISES_POL_GARP aggregation
        print("\n📊 Aggregating PRIMES_EMISES_POL_GARP...")
        primes_garp = aggregate_primes_emises_pol_garp(spark, args.vision, args.gold_path)
        output_file_4 = output_dir / f"WORK_QUERY_FOR_PRIMES_EMISES{args.vision}_POL_GARP.csv"
        primes_garp.toPandas().to_csv(output_file_4, index=False)
        print(f"   ✅ Saved: {output_file_4}")
        print(f"   📈 Row count: {primes_garp.count()}")
        
        print("\n✅ All aggregations complete!")
        print(f"\n📁 Output files in: {output_dir}")
        print("   1. WORK_QUERY_FOR_MVT_PTF{}.csv".format(args.vision))
        print("   2. WORK_QUERY_FOR_AZ_AZEC_CAPITAUX_{}.csv".format(args.vision))
        print("   3. WORK_QUERY_FOR_PRIMES_EMISES{}_POL.csv".format(args.vision))
        print("   4. WORK_QUERY_FOR_PRIMES_EMISES{}_POL_GARP.csv".format(args.vision))
        
        print("\n🔍 Next step: Compare these files with your SAS outputs")
        
    except Exception as e:
        print(f"\n❌ Error during aggregation: {e}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
