#!/usr/bin/env python3
"""
Debug script to investigate:
1. upper_mid column issue in table_pt_gest
2. Extra 3,891 rows compared to SAS output
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
import sys

def debug_table_pt_gest(spark, vision='ref'):
    """Debug table_pt_gest schema and content."""
    print("\n" + "="*80)
    print("DEBUGGING: table_pt_gest Schema")
    print("="*80)
    
    try:
        # Read the file
        df = spark.read.format("csv") \
            .option("sep", "|") \
            .option("header", "true") \
            .option("encoding", "LATIN9") \
            .load("abfs://shared@azfrdatalab.dfs.core.windows.net/ABR/P4D/ADC/DATAMARTS/CONSTRUCTION/bronze/ref/table_pt_gest/*.csv")
        
        print(f"\n✓ File loaded successfully")
        print(f"  Total rows: {df.count():,}")
        
        # Print schema
        print("\n📋 Schema:")
        df.printSchema()
        
        # Show column names (case-sensitive)
        print("\n📋 Column Names (exact case):")
        for i, col_name in enumerate(df.columns, 1):
            print(f"  {i:2d}. '{col_name}'")
        
        # Check for upper_mid variants
        print("\n🔍 Searching for 'upper' or 'mid' in column names:")
        upper_mid_candidates = [c for c in df.columns if 'upper' in c.lower() or 'mid' in c.lower()]
        if upper_mid_candidates:
            print(f"  Found: {upper_mid_candidates}")
        else:
            print("  ❌ No columns containing 'upper' or 'mid'")
        
        # Show sample data
        print("\n📊 Sample Data (first 5 rows):")
        df.show(5, truncate=False)
        
        # Show column statistics
        print("\n📊 Column Statistics:")
        for col_name in df.columns[:10]:  # First 10 columns
            null_count = df.filter(col(col_name).isNull()).count()
            total = df.count()
            print(f"  {col_name}: {null_count:,} nulls ({100*null_count/total:.1f}%)")
        
        return df
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        return None


def debug_row_count_diff(spark, vision='202509'):
    """Compare Python vs SAS row counts by source."""
    print("\n" + "="*80)
    print("DEBUGGING: Row Count Differences")
    print("="*80)
    
    try:
        # Read AZ silver
        az_path = f"abfs://shared@azfrdatalab.dfs.core.windows.net/ABR/P4D/ADC/DATAMARTS/CONSTRUCTION/silver/2025/09/mvt_const_ptf_{vision}"
        df_az = spark.read.format("delta").load(az_path)
        az_count = df_az.count()
        
        # Read AZEC silver
        azec_path = f"abfs://shared@azfrdatalab.dfs.core.windows.net/ABR/P4D/ADC/DATAMARTS/CONSTRUCTION/silver/2025/09/azec_ptf_{vision}"
        df_azec = spark.read.format("delta").load(azec_path)
        azec_count = df_azec.count()
        
        # Read Gold
        gold_path = f"abfs://shared@azfrdatalab.dfs.core.windows.net/ABR/P4D/ADC/DATAMARTS/CONSTRUCTION/gold/2025/09/ptf_mvt_{vision}"
        df_gold = spark.read.format("delta").load(gold_path)
        gold_count = df_gold.count()
        
        print(f"\n📊 Row Counts:")
        print(f"  AZ Silver:     {az_count:,} rows")
        print(f"  AZEC Silver:   {azec_count:,} rows")
        print(f"  Gold (union):  {gold_count:,} rows")
        print(f"  Expected sum:  {az_count + azec_count:,} rows")
        print(f"  Difference:    {gold_count - (az_count + azec_count):,} rows")
        
        if gold_count != (az_count + azec_count):
            print(f"\n⚠️  WARNING: Union should have {az_count + azec_count:,} rows but has {gold_count:,}")
        
        # Check for duplicates in gold
        print(f"\n🔍 Checking for duplicates in Gold:")
        dup_count = df_gold.groupBy('nopol').count().filter(col('count') > 1).count()
        print(f"  Duplicate nopol count: {dup_count:,}")
        
        if dup_count > 0:
            print(f"\n  Top 10 duplicated nopol:")
            df_gold.groupBy('nopol').count().filter(col('count') > 1) \
                .orderBy(col('count').desc()).show(10)
        
        # Check dircom distribution
        print(f"\n📊 Distribution by DIRCOM:")
        df_gold.groupBy('dircom').count().orderBy('dircom').show()
        
        # Check cdpole distribution
        print(f"\n📊 Distribution by CDPOLE:")
        df_gold.groupBy('cdpole').count().orderBy('cdpole').show()
        
        return df_az, df_azec, df_gold
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        return None, None, None


def main():
    print("\n" + "="*80)
    print("PTF_MVT DEBUG SCRIPT")
    print("="*80)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("PTF_MVT_Debug") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        # Debug 1: table_pt_gest schema
        debug_table_pt_gest(spark)
        
        # Debug 2: Row count differences
        debug_row_count_diff(spark)
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("\n" + "="*80)
        print("Debug complete!")
        print("="*80 + "\n")


if __name__ == "__main__":
    main()
