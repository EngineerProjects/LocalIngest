#!/usr/bin/env python3
"""
Product Traceability Script - Track a specific CDPROD through the entire PTF_MVT pipeline

Usage:
    python3 scripts/trace_product.py 01234  # Trace product 01234

This script will:
1. Find the product in Bronze (raw data)
2. Track it through AZ/AZEC processors
3. Check if it reaches Silver
4. Follow it through Consolidation
5. Verify it's in Gold
6. Compare values at each step
7. Alert if the product disappears at any stage
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit
from datetime import datetime

class ProductTracer:
    def __init__(self, spark, cdprod, vision='202509'):
        self.spark = spark
        self.cdprod = cdprod
        self.vision = vision
        self.trace_results = {}
        
    def print_header(self, title):
        """Print formatted section header."""
        print("\n" + "="*80)
        print(f"  {title}")
        print("="*80)
    
    def print_status(self, stage, found, count=0):
        """Print stage status."""
        if found:
            print(f"✅ {stage}: Product FOUND ({count} rows)")
        else:
            print(f"❌ {stage}: Product NOT FOUND (disappeared!)")
    
    def show_data(self, df, stage_name, max_rows=5):
        """Display data for the product at this stage."""
        if df is not None and df.count() > 0:
            print(f"\n📊 Data at {stage_name}:")
            df.show(max_rows, truncate=False, vertical=True)
            
            # Show key metrics
            print(f"\n📈 Key Columns:")
            key_cols = ['cdprod', 'nopol', 'cdpole', 'primes_ptf', 'nbptf', 'expo_ytd', 'cmarch', 'cseg']
            available_cols = [c for c in key_cols if c in df.columns]
            if available_cols:
                df.select(available_cols).show(max_rows, truncate=False)
        else:
            print(f"⚠️  No data to display at {stage_name}")
    
    def trace_bronze_az(self):
        """Trace product in Bronze AZ files (PTF16 + PTF36)."""
        self.print_header("STAGE 1: BRONZE - AZ (PTF16 + PTF36)")
        
        try:
            # Read PTF16 (Agent - Pole 1)
            ptf16_path = f"abfs://shared@azfrdatalab.dfs.core.windows.net/ABR/P4D/ADC/DATAMARTS/CONSTRUCTION/bronze/2025/09/ipf16.csv"
            df_ptf16 = self.spark.read.format("csv") \
                .option("sep", "|") \
                .option("header", "true") \
                .option("encoding", "LATIN9") \
                .load(ptf16_path)
            
            # Filter for our product
            df_product_16 = df_ptf16.filter(col('cdprod') == self.cdprod)
            count_16 = df_product_16.count()
            
            self.print_status("PTF16 (Pole 1)", count_16 > 0, count_16)
            if count_16 > 0:
                self.show_data(df_product_16, "PTF16 (Bronze AZ Pole 1)")
            
            # Read PTF36 (Courtage - Pole 3)
            ptf36_path = f"abfs://shared@azfrdatalab.dfs.core.windows.net/ABR/P4D/ADC/DATAMARTS/CONSTRUCTION/bronze/2025/09/ipf36.csv"
            df_ptf36 = self.spark.read.format("csv") \
                .option("sep", "|") \
                .option("header", "true") \
                .option("encoding", "LATIN9") \
                .load(ptf36_path)
            
            df_product_36 = df_ptf36.filter(col('cdprod') == self.cdprod)
            count_36 = df_product_36.count()
            
            self.print_status("PTF36 (Pole 3)", count_36 > 0, count_36)
            if count_36 > 0:
                self.show_data(df_product_36, "PTF36 (Bronze AZ Pole 3)")
            
            total_bronze_az = count_16 + count_36
            self.trace_results['bronze_az'] = {
                'found': total_bronze_az > 0,
                'count': total_bronze_az,
                'pole_1': count_16,
                'pole_3': count_36
            }
            
            return total_bronze_az > 0, total_bronze_az
            
        except Exception as e:
            print(f"❌ ERROR reading Bronze AZ: {e}")
            self.trace_results['bronze_az'] = {'found': False, 'error': str(e)}
            return False, 0
    
    def trace_bronze_azec(self):
        """Trace product in Bronze AZEC (POLIC_CU)."""
        self.print_header("STAGE 2: BRONZE - AZEC (POLIC_CU)")
        
        try:
            polic_cu_path = f"abfs://shared@azfrdatalab.dfs.core.windows.net/ABR/P4D/ADC/DATAMARTS/CONSTRUCTION/bronze/2025/09/polic_cu.csv"
            df = self.spark.read.format("csv") \
                .option("sep", "|") \
                .option("header", "true") \
                .option("encoding", "LATIN9") \
                .load(polic_cu_path)
            
            # AZEC uses 'produit' column, not 'cdprod'
            df_product = df.filter(col('produit') == self.cdprod)
            count_azec = df_product.count()
            
            self.print_status("POLIC_CU", count_azec > 0, count_azec)
            if count_azec > 0:
                self.show_data(df_product, "POLIC_CU (Bronze AZEC)")
            
            self.trace_results['bronze_azec'] = {
                'found': count_azec > 0,
                'count': count_azec
            }
            
            return count_azec > 0, count_azec
            
        except Exception as e:
            print(f"❌ ERROR reading Bronze AZEC: {e}")
            self.trace_results['bronze_azec'] = {'found': False, 'error': str(e)}
            return False, 0
    
    def trace_silver_az(self):
        """Trace product in Silver AZ."""
        self.print_header("STAGE 3: SILVER - AZ (After Transformation)")
        
        try:
            silver_path = f"abfs://shared@azfrdatalab.dfs.core.windows.net/ABR/P4D/ADC/DATAMARTS/CONSTRUCTION/silver/2025/09/mvt_const_ptf_{self.vision}"
            df = self.spark.read.format("delta").load(silver_path)
            
            df_product = df.filter(col('cdprod') == self.cdprod)
            count_silver_az = df_product.count()
            
            self.print_status("Silver AZ", count_silver_az > 0, count_silver_az)
            if count_silver_az > 0:
                self.show_data(df_product, "Silver AZ (After AZProcessor)")
                
                # Check if deduplication affected it
                if count_silver_az < self.trace_results.get('bronze_az', {}).get('count', 0):
                    print(f"\n⚠️  DEDUPLICATION: {self.trace_results['bronze_az']['count']} → {count_silver_az} rows")
            
            self.trace_results['silver_az'] = {
                'found': count_silver_az > 0,
                'count': count_silver_az
            }
            
            return count_silver_az > 0, count_silver_az
            
        except Exception as e:
            print(f"❌ ERROR reading Silver AZ: {e}")
            self.trace_results['silver_az'] = {'found': False, 'error': str(e)}
            return False, 0
    
    def trace_silver_azec(self):
        """Trace product in Silver AZEC."""
        self.print_header("STAGE 4: SILVER - AZEC (After Transformation)")
        
        try:
            silver_path = f"abfs://shared@azfrdatalab.dfs.core.windows.net/ABR/P4D/ADC/DATAMARTS/CONSTRUCTION/silver/2025/09/azec_ptf_{self.vision}"
            df = self.spark.read.format("delta").load(silver_path)
            
            # After transformation, AZEC also uses 'cdprod'
            df_product = df.filter(col('cdprod') == self.cdprod)
            count_silver_azec = df_product.count()
            
            self.print_status("Silver AZEC", count_silver_azec > 0, count_silver_azec)
            if count_silver_azec > 0:
                self.show_data(df_product, "Silver AZEC (After AZECProcessor)")
                
                # Check for market filter impact
                bronze_count = self.trace_results.get('bronze_azec', {}).get('count', 0)
                if count_silver_azec < bronze_count:
                    print(f"\n⚠️  FILTERED: {bronze_count} → {count_silver_azec} rows (cmarch='6' filter)")
            
            self.trace_results['silver_azec'] = {
                'found': count_silver_azec > 0,
                'count': count_silver_azec
            }
            
            return count_silver_azec > 0, count_silver_azec
            
        except Exception as e:
            print(f"❌ ERROR reading Silver AZEC: {e}")
            self.trace_results['silver_azec'] = {'found': False, 'error': str(e)}
            return False, 0
    
    def trace_gold(self):
        """Trace product in Gold (Consolidated)."""
        self.print_header("STAGE 5: GOLD - Consolidated (AZ + AZEC)")
        
        try:
            gold_path = f"abfs://shared@azfrdatalab.dfs.core.windows.net/ABR/P4D/ADC/DATAMARTS/CONSTRUCTION/gold/2025/09/ptf_mvt_{self.vision}"
            df = self.spark.read.format("delta").load(gold_path)
            
            df_product = df.filter(col('cdprod') == self.cdprod)
            count_gold = df_product.count()
            
            self.print_status("Gold", count_gold > 0, count_gold)
            if count_gold > 0:
                self.show_data(df_product, "Gold (Final Consolidated)", max_rows=10)
                
                # Show distribution by source
                print(f"\n📊 Distribution by DIRCOM:")
                df_product.groupBy('dircom').count().show()
                
                print(f"\n📊 Distribution by CDPOLE:")
                df_product.groupBy('cdpole').count().show()
            
            # Verify union
            expected_count = self.trace_results.get('silver_az', {}).get('count', 0) + \
                           self.trace_results.get('silver_azec', {}).get('count', 0)
            
            if count_gold != expected_count:
                print(f"\n⚠️  UNION ISSUE: Expected {expected_count} rows but found {count_gold}")
            
            self.trace_results['gold'] = {
                'found': count_gold > 0,
                'count': count_gold
            }
            
            return count_gold > 0, count_gold
            
        except Exception as e:
            print(f"❌ ERROR reading Gold: {e}")
            self.trace_results['gold'] = {'found': False, 'error': str(e)}
            return False, 0
    
    def print_summary(self):
        """Print traceability summary."""
        self.print_header("TRACEABILITY SUMMARY")
        
        print(f"\n🔍 Product: {self.cdprod}")
        print(f"📅 Vision: {self.vision}")
        print(f"🕐 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        print(f"\n📊 Journey:")
        stages = [
            ('Bronze AZ (PTF16+PTF36)', 'bronze_az'),
            ('Bronze AZEC (POLIC_CU)', 'bronze_azec'),
            ('Silver AZ', 'silver_az'),
            ('Silver AZEC', 'silver_azec'),
            ('Gold (Final)', 'gold')
        ]
        
        for stage_name, stage_key in stages:
            if stage_key in self.trace_results:
                result = self.trace_results[stage_key]
                if result.get('found'):
                    count = result.get('count', 0)
                    status = f"✅ FOUND ({count} rows)"
                else:
                    status = "❌ NOT FOUND"
                    if 'error' in result:
                        status += f" (Error: {result['error']})"
                
                print(f"  {stage_name:30s} : {status}")
        
        # Check for disappearance
        print(f"\n🚨 Alerts:")
        disappearances = []
        
        if self.trace_results.get('bronze_az', {}).get('found') and \
           not self.trace_results.get('silver_az', {}).get('found'):
            disappearances.append("AZ: Product disappeared between Bronze and Silver!")
        
        if self.trace_results.get('bronze_azec', {}).get('found') and \
           not self.trace_results.get('silver_azec', {}).get('found'):
            disappearances.append("AZEC: Product disappeared between Bronze and Silver!")
        
        if (self.trace_results.get('silver_az', {}).get('found') or \
            self.trace_results.get('silver_azec', {}).get('found')) and \
           not self.trace_results.get('gold', {}).get('found'):
            disappearances.append("Product disappeared during Consolidation!")
        
        if disappearances:
            for alert in disappearances:
                print(f"  ⚠️  {alert}")
        else:
            print(f"  ✅ No disappearances detected")
    
    def run_trace(self):
        """Run complete product trace."""
        print("\n" + "🔬"*40)
        print(f"PRODUCT TRACEABILITY ANALYSIS")
        print(f"Product Code: {self.cdprod}")
        print("🔬"*40)
        
        # Run all stages
        self.trace_bronze_az()
        self.trace_bronze_azec()
        self.trace_silver_az()
        self.trace_silver_azec()
        self.trace_gold()
        
        # Print summary
        self.print_summary()


def main():
    if len(sys.argv) < 2:
        print("\n❌ Usage: python3 trace_product.py <CDPROD>")
        print("\nExample: python3 trace_product.py 01234")
        print("\nTo find products in SAS output:")
        print("  - Check output/mvt_ptf202509.csv")
        print("  - Look for interesting cdprod values")
        sys.exit(1)
    
    cdprod = sys.argv[1]
    vision = sys.argv[2] if len(sys.argv) > 2 else '202509'
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName(f"ProductTrace_{cdprod}") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        tracer = ProductTracer(spark, cdprod, vision)
        tracer.run_trace()
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
