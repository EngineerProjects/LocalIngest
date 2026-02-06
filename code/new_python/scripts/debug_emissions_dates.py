#!/usr/bin/env python3
"""
Diagnostic EMISSIONS - Pourquoi 0 records après filtre dt_cpta_cts?

Usage:
    python scripts/debug_emissions_dates.py --vision 202512
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, min as spark_min, max as spark_max, lit
from src.config import ConfigLoader
from src.reader import BronzeReader
from utils.logger import setup_logger

logger = setup_logger("debug_emissions")

def main():
    vision = "202512"
    
    logger.info("="*80)
    logger.info("  DIAGNOSTIC EMISSIONS - dt_cpta_cts Investigation")
    logger.info("="*80)
    logger.info(f"Vision: {vision}")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Debug_Emissions_Dates") \
        .getOrCreate()
    
    config = ConfigLoader()
    reader = BronzeReader(spark, config)
    
    # Read One BI data
    logger.info("\n[STEP 1] Reading One BI emissions data...")
    df = reader.read_file_group("one_bi_primes", vision=vision)
    total_records = df.count()
    logger.info(f"✓ Total records: {total_records:,}")
    
    # Check schema
    logger.info("\n[STEP 2] Checking dt_cpta_cts schema...")
    schema_info = [f for f in df.schema.fields if 'cpta' in f.name.lower()]
    for field in schema_info:
        logger.info(f"  - {field.name}: {field.dataType}")
    
    # Lowercase columns
    logger.info("\n[STEP 3] Lowercasing columns...")
    df = df.select([col(c).alias(c.lower()) for c in df.columns])
    
    # Check if column exists
    if 'dt_cpta_cts' not in df.columns:
        logger.error("❌ Column 'dt_cpta_cts' NOT FOUND after lowercase!")
        logger.info(f"Available columns: {', '.join(sorted(df.columns))}")
        return
    
    # Basic stats on dt_cpta_cts
    logger.info("\n[STEP 4] Analyzing dt_cpta_cts distribution...")
    
    stats = df.select(
        spark_min('dt_cpta_cts').alias('min_date'),
        spark_max('dt_cpta_cts').alias('max_date'),
        count('dt_cpta_cts').alias('non_null_count')
    ).collect()[0]
    
    logger.info(f"  - Min dt_cpta_cts: {stats['min_date']}")
    logger.info(f"  - Max dt_cpta_cts: {stats['max_date']}")
    logger.info(f"  - Non-null count: {stats['non_null_count']:,}")
    logger.info(f"  - Null count: {total_records - stats['non_null_count']:,}")
    
    # Apply market filter
    logger.info("\n[STEP 5] Applying market filter (cd_marche='6')...")
    df_market = df.filter(col('cd_marche') == '6')
    market_count = df_market.count()
    logger.info(f"After cd_marche='6' filter: {market_count:,} records")
    
    if market_count > 0:
        stats_market = df_market.select(
            spark_min('dt_cpta_cts').alias('min_date'),
            spark_max('dt_cpta_cts').alias('max_date')
        ).collect()[0]
        
        logger.info(f"  - Min dt_cpta_cts (market=6): {stats_market['min_date']}")
        logger.info(f"  - Max dt_cpta_cts (market=6): {stats_market['max_date']}")
    
    # Test date filter
    logger.info(f"\n[STEP 6] Testing date filter (dt_cpta_cts <= {vision})...")
    
    # Convert vision to int for comparison
    vision_int = int(vision)
    logger.info(f"Vision as integer: {vision_int}")
    
    # Try different filter approaches
    logger.info("\n[6.1] Filter with lit(vision) [as String]:")
    df_filtered_str = df_market.filter(col('dt_cpta_cts') <= lit(vision))
    count_str = df_filtered_str.count()
    logger.info(f"  → Result: {count_str:,} records")
    
    logger.info("\n[6.2] Filter with lit(int(vision)) [as Integer]:")
    df_filtered_int = df_market.filter(col('dt_cpta_cts') <= lit(vision_int))
    count_int = df_filtered_int.count()
    logger.info(f"  → Result: {count_int:,} records")
    
    logger.info("\n[6.3] Filter with dt_cpta_cts.cast('int'):")
    df_filtered_cast = df_market.filter(col('dt_cpta_cts').cast('int') <= vision_int)
    count_cast = df_filtered_cast.count()
    logger.info(f"  → Result: {count_cast:,} records")
    
    # Distribution by period
    logger.info("\n[STEP 7] Distribution by dt_cpta_cts period...")
    if market_count > 0:
        period_dist = df_market.groupBy('dt_cpta_cts') \
            .agg(count('*').alias('count')) \
            .orderBy('dt_cpta_cts', ascending=False) \
            .limit(20)
        
        logger.info("Top 20 periods (most recent):")
        for row in period_dist.collect():
            marker = "✓" if row['dt_cpta_cts'] <= vision_int else "✗"
            logger.info(f"  {marker} {row['dt_cpta_cts']}: {row['count']:,} records")
    
    # Sample data
    logger.info("\n[STEP 8] Sample data (5 rows)...")
    sample = df_market.select('dt_cpta_cts', 'cd_marche', 'cd_prd_prm', 'mt_ht_cts').limit(5)
    for row in sample.collect():
        logger.info(f"  {row}")
    
    logger.info("\n" + "="*80)
    logger.info("  DIAGNOSIS COMPLETE")
    logger.info("="*80)
    
    # Summary
    logger.info("\n📊 SUMMARY:")
    logger.info(f"  Total records: {total_records:,}")
    logger.info(f"  Market=6: {market_count:,}")
    logger.info(f"  dt_cpta_cts <= {vision} (String): {count_str:,}")
    logger.info(f"  dt_cpta_cts <= {vision} (Integer): {count_int:,}")
    
    if count_int == 0:
        logger.warning("\n⚠️  CONCLUSION: All dt_cpta_cts values > 202512")
        logger.warning(f"   → Data contains FUTURE periods (likely 2026)")
        logger.warning(f"   → Min value in data: {stats_market['min_date'] if market_count > 0 else stats['min_date']}")
        logger.warning(f"   → This is a DATA issue, not a CODE issue")
        logger.warning(f"   → Options:")
        logger.warning(f"      1. Use more recent vision (e.g., 202601)")
        logger.warning(f"      2. Check data source update date")
        logger.warning(f"      3. Confirm expected vision with business")
    else:
        logger.info(f"\n✓ Filter working correctly: {count_int:,} records pass filter")
    
    spark.stop()

if __name__ == "__main__":
    main()
