"""
Main Validation Script - Simple Configuration-Based.

This script orchestrates the full SAS vs Python validation workflow:
1. If Python KPIs don't exist: Calculate them from gold layer
2. Compare Python KPIs with SAS KPIs
3. Generate comparison report

Configuration: Edit the variables below instead of using command-line args.
"""

from pathlib import Path
import sys

# ============================================================================
# CONFIGURATION - EDIT THESE VARIABLES
# ============================================================================

# Vision to process (YYYYMM format)
VISION = "202509"

# Path to gold layer in datalake (where Python outputs are stored)
GOLD_LAYER_PATH = "gs://your-bucket/gold"
# Example local: "/home/amiche/data/gold"
# Example GCS: "gs://my-bucket/gold"

# Path to SAS KPI files folder
SAS_KPIS_FOLDER = "/home/amiche/Downloads/sas_kpis"
# This folder should contain:
#   - WORK_QUERY_FOR_MVT_PTF202509.csv
#   - WORK_QUERY_FOR_AZ_AZEC_CAPITAUX_202509.csv
#   - WORK_QUERY_FOR_PRIMES_EMISES202509_POL.csv
#   - WORK_QUERY_FOR_PRIMES_EMISES202509_POL_GARP.csv

# Path to Python KPI files folder
# If None: Will calculate KPIs from gold layer first
# If set: Will skip calculation and go directly to comparison
PYTHON_KPIS_FOLDER = None
# Example: "/home/amiche/Downloads/python_kpis"

# Tolerance for numeric comparison (absolute difference)
TOLERANCE = 0.01

# Output report path
OUTPUT_REPORT = "./validation_report.xlsx"

# ============================================================================
# END CONFIGURATION
# ============================================================================


def calculate_python_kpis(vision: str, gold_path: str, output_folder: str):
    """
    Calculate Python KPIs from gold layer.
    
    Imports and runs the aggregate_python_outputs script.
    """
    print("\n" + "="*80)
    print("📊 STEP 1: Calculating Python KPIs from Gold Layer")
    print("="*80)
    print(f"Vision: {vision}")
    print(f"Gold Layer: {gold_path}")
    print(f"Output Folder: {output_folder}")
    
    # Import the aggregation functions
    sys.path.insert(0, str(Path(__file__).parent))
    from aggregate_python_outputs import (
        init_spark,
        aggregate_mvt_ptf,
        aggregate_capitaux,
        aggregate_primes_emises_pol,
        aggregate_primes_emises_pol_garp
    )
    
    # Create output directory
    output_dir = Path(output_folder)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize Spark
    print("\n🚀 Starting Spark session...")
    spark = init_spark()
    
    try:
        # 1. MVT_PTF aggregation
        print("\n📊 Aggregating MVT_PTF...")
        mvt_ptf = aggregate_mvt_ptf(spark, vision, gold_path)
        output_file_1 = output_dir / f"WORK_QUERY_FOR_MVT_PTF{vision}.csv"
        mvt_ptf.toPandas().to_csv(output_file_1, index=False)
        print(f"   ✅ Saved: {output_file_1} ({mvt_ptf.count()} rows)")
        
        # 2. CAPITAUX aggregation
        print("\n📊 Aggregating CAPITAUX...")
        capitaux = aggregate_capitaux(spark, vision, gold_path)
        output_file_2 = output_dir / f"WORK_QUERY_FOR_AZ_AZEC_CAPITAUX_{vision}.csv"
        capitaux.toPandas().to_csv(output_file_2, index=False)
        print(f"   ✅ Saved: {output_file_2} ({capitaux.count()} rows)")
        
        # 3. PRIMES_EMISES_POL aggregation
        print("\n📊 Aggregating PRIMES_EMISES_POL...")
        primes_pol = aggregate_primes_emises_pol(spark, vision, gold_path)
        output_file_3 = output_dir / f"WORK_QUERY_FOR_PRIMES_EMISES{vision}_POL.csv"
        primes_pol.toPandas().to_csv(output_file_3, index=False)
        print(f"   ✅ Saved: {output_file_3} ({primes_pol.count()} rows)")
        
        # 4. PRIMES_EMISES_POL_GARP aggregation
        print("\n📊 Aggregating PRIMES_EMISES_POL_GARP...")
        primes_garp = aggregate_primes_emises_pol_garp(spark, vision, gold_path)
        output_file_4 = output_dir / f"WORK_QUERY_FOR_PRIMES_EMISES{vision}_POL_GARP.csv"
        primes_garp.toPandas().to_csv(output_file_4, index=False)
        print(f"   ✅ Saved: {output_file_4} ({primes_garp.count()} rows)")
        
        print("\n✅ Python KPI calculation complete!")
        
    except Exception as e:
        print(f"\n❌ Error during KPI calculation: {e}")
        raise
    
    finally:
        spark.stop()


def compare_kpis(vision: str, sas_folder: str, python_folder: str, tolerance: float, report_path: str):
    """
    Compare SAS vs Python KPIs.
    
    Imports and runs the validation script.
    """
    print("\n" + "="*80)
    print("🔍 STEP 2: Comparing SAS vs Python KPIs")
    print("="*80)
    print(f"Vision: {vision}")
    print(f"SAS Folder: {sas_folder}")
    print(f"Python Folder: {python_folder}")
    print(f"Tolerance: ±{tolerance}")
    print(f"Report: {report_path}")
    
    # Import validation functions
    sys.path.insert(0, str(Path(__file__).parent))
    from validate_outputs import (
        load_csv_pair,
        compare_dataframes,
        print_comparison_report,
        save_detailed_report,
        ComparisonResult
    )
    
    sas_dir = Path(sas_folder)
    python_dir = Path(python_folder)
    
    if not sas_dir.exists():
        raise ValueError(f"SAS directory not found: {sas_dir}")
    if not python_dir.exists():
        raise ValueError(f"Python directory not found: {python_dir}")
    
    # File pairs to compare
    file_configs = [
        {
            'name': 'MVT_PTF',
            'sas_file': f'WORK_QUERY_FOR_MVT_PTF{vision}.csv',
            'python_file': f'WORK_QUERY_FOR_MVT_PTF{vision}.csv',
            'key': 'cdprod'
        },
        {
            'name': 'CAPITAUX',
            'sas_file': f'WORK_QUERY_FOR_AZ_AZEC_CAPITAUX_{vision}.csv',
            'python_file': f'WORK_QUERY_FOR_AZ_AZEC_CAPITAUX_{vision}.csv',
            'key': 'cdprod'
        },
        {
            'name': 'PRIMES_EMISES_POL',
            'sas_file': f'WORK_QUERY_FOR_PRIMES_EMISES{vision}_POL.csv',
            'python_file': f'WORK_QUERY_FOR_PRIMES_EMISES{vision}_POL.csv',
            'key': 'cdprod'
        },
        {
            'name': 'PRIMES_EMISES_POL_GARP',
            'sas_file': f'WORK_QUERY_FOR_PRIMES_EMISES{vision}_POL_GARP.csv',
            'python_file': f'WORK_QUERY_FOR_PRIMES_EMISES{vision}_POL_GARP.csv',
            'key': 'cdprod'
        }
    ]
    
    results = []
    
    for config in file_configs:
        print(f"\n🔄 Comparing {config['name']}...")
        
        sas_path = sas_dir / config['sas_file']
        python_path = python_dir / config['python_file']
        
        try:
            sas_df, python_df = load_csv_pair(sas_path, python_path)
            
            result = compare_dataframes(
                sas_df,
                python_df,
                key_column=config['key'],
                tolerance=tolerance
            )
            result.file_name = config['name']
            results.append(result)
            
            status = "✅" if result.is_valid else "❌"
            print(f"   {status} {result.sas_rows} rows compared")
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            # Create failed result
            result = ComparisonResult(
                file_name=config['name'],
                sas_rows=0,
                python_rows=0,
                matching_rows=0,
                mismatched_columns=[],
                max_difference=0.0,
                differences=None,
                is_valid=False
            )
            results.append(result)
    
    # Print report
    print_comparison_report(results, tolerance)
    
    # Save detailed report
    save_detailed_report(results, Path(report_path))
    
    # Return status
    all_valid = all(r.is_valid for r in results)
    return all_valid


def main():
    """Main orchestration function."""
    print("\n" + "="*80)
    print("🎯 SAS vs Python Validation Workflow")
    print("="*80)
    print(f"Vision: {VISION}")
    print(f"Gold Layer: {GOLD_LAYER_PATH}")
    print(f"SAS KPIs: {SAS_KPIS_FOLDER}")
    print(f"Python KPIs: {PYTHON_KPIS_FOLDER or '(will calculate)'}")
    print("="*80)
    
    # Determine Python KPIs folder
    if PYTHON_KPIS_FOLDER is None:
        # Need to calculate Python KPIs first
        python_kpis = "./python_kpis_output"
        print(f"\n📌 Python KPIs folder not specified.")
        print(f"   Will calculate KPIs and save to: {python_kpis}")
        
        calculate_python_kpis(VISION, GOLD_LAYER_PATH, python_kpis)
    else:
        # Python KPIs already exist
        python_kpis = PYTHON_KPIS_FOLDER
        print(f"\n📌 Using existing Python KPIs from: {python_kpis}")
        print("   Skipping KPI calculation...")
    
    # Compare SAS vs Python
    all_valid = compare_kpis(
        VISION,
        SAS_KPIS_FOLDER,
        python_kpis,
        TOLERANCE,
        OUTPUT_REPORT
    )
    
    # Final summary
    print("\n" + "="*80)
    if all_valid:
        print("✅ SUCCESS - All validations passed!")
        print("   SAS and Python outputs match perfectly.")
    else:
        print("❌ VALIDATION FAILED - Differences detected")
        print(f"   See detailed report: {OUTPUT_REPORT}")
    print("="*80)
    
    # Exit with appropriate code
    sys.exit(0 if all_valid else 1)


if __name__ == "__main__":
    main()
