"""
Validation Script - Compare SAS vs Python aggregated outputs.

Reads the 4 aggregated CSV files from both SAS and Python,
compares them row-by-row with tolerance for floating point precision,
and generates a detailed comparison report.

Usage:
    python scripts/validate_outputs.py \\
        --vision 202509 \\
        --sas-dir ./sas_outputs \\
        --python-dir ./validation_outputs \\
        --tolerance 0.01
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Tuple, Dict, List
from dataclasses import dataclass


@dataclass
class ComparisonResult:
    """Results of comparing a single file."""
    file_name: str
    sas_rows: int
    python_rows: int
    matching_rows: int
    mismatched_columns: List[str]
    max_difference: float
    differences: pd.DataFrame
    is_valid: bool


def load_csv_pair(sas_path: Path, python_path: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load SAS and Python CSV files.
    
    Args:
        sas_path: Path to SAS CSV file
        python_path: Path to Python CSV file
        
    Returns:
        Tuple of (sas_df, python_df)
    """
    if not sas_path.exists():
        raise FileNotFoundError(f"SAS file not found: {sas_path}")
    if not python_path.exists():
        raise FileNotFoundError(f"Python file not found: {python_path}")
    
    sas_df = pd.read_csv(sas_path)
    python_df = pd.read_csv(python_path)
    
    # Normalize column names to lowercase
    sas_df.columns = sas_df.columns.str.lower()
    python_df.columns = python_df.columns.str.lower()
    
    return sas_df, python_df


def compare_dataframes(
    sas_df: pd.DataFrame,
    python_df: pd.DataFrame,
    key_column: str,
    tolerance: float = 0.01
) -> ComparisonResult:
    """
    Compare two DataFrames with tolerance for floating point differences.
    
    Args:
        sas_df: SAS DataFrame
        python_df: Python DataFrame
        key_column: Column to join on (e.g., 'cdprod')
        tolerance: Absolute tolerance for numeric differences
        
    Returns:
        ComparisonResult object with comparison details
    """
    # Merge on key column
    merged = sas_df.merge(
        python_df,
        on=key_column,
        how='outer',
        suffixes=('_sas', '_python'),
        indicator=True
    )
    
    # Track differences
    differences = []
    mismatched_columns = []
    max_diff = 0.0
    
    # Get common columns (excluding key)
    value_columns = [col for col in sas_df.columns if col != key_column]
    
    for col in value_columns:
        sas_col = f"{col}_sas"
        python_col = f"{col}_python"
        
        if sas_col in merged.columns and python_col in merged.columns:
            # Calculate absolute difference
            diff = np.abs(merged[sas_col] - merged[python_col])
            
            # Find rows exceeding tolerance
            mismatches = merged[diff > tolerance].copy()
            
            if len(mismatches) > 0:
                mismatched_columns.append(col)
                max_diff = max(max_diff, diff.max())
                
                # Record differences
                for _, row in mismatches.iterrows():
                    differences.append({
                        key_column: row[key_column],
                        'column': col,
                        'sas_value': row[sas_col],
                        'python_value': row[python_col],
                        'difference': row[sas_col] - row[python_col],
                        'pct_difference': ((row[sas_col] - row[python_col]) / row[sas_col] * 100) 
                                         if row[sas_col] != 0 else np.nan
                    })
    
    # Check for missing rows
    only_sas = merged[merged['_merge'] == 'left_only']
    only_python = merged[merged['_merge'] == 'right_only']
    
    if len(only_sas) > 0:
        for _, row in only_sas.iterrows():
            differences.append({
                key_column: row[key_column],
                'column': 'ROW_MISSING',
                'sas_value': 'EXISTS',
                'python_value': 'MISSING',
                'difference': np.nan,
                'pct_difference': np.nan
            })
    
    if len(only_python) > 0:
        for _, row in only_python.iterrows():
            differences.append({
                key_column: row[key_column],
                'column': 'ROW_EXTRA',
                'sas_value': 'MISSING',
                'python_value': 'EXISTS',
                'difference': np.nan,
                'pct_difference': np.nan
            })
    
    diff_df = pd.DataFrame(differences) if differences else pd.DataFrame()
    
    is_valid = (
        len(differences) == 0 and
        len(only_sas) == 0 and
        len(only_python) == 0
    )
    
    return ComparisonResult(
        file_name="",  # Will be set by caller
        sas_rows=len(sas_df),
        python_rows=len(python_df),
        matching_rows=len(merged[merged['_merge'] == 'both']),
        mismatched_columns=mismatched_columns,
        max_difference=max_diff,
        differences=diff_df,
        is_valid=is_valid
    )


def print_comparison_report(results: List[ComparisonResult], tolerance: float):
    """
    Print a detailed comparison report.
    
    Args:
        results: List of ComparisonResult objects
        tolerance: Tolerance used for comparison
    """
    print("\n" + "="*80)
    print("📊 SAS vs Python Validation Report")
    print("="*80)
    print(f"Tolerance: ±{tolerance}")
    print()
    
    all_valid = all(r.is_valid for r in results)
    
    for result in results:
        status = "✅ PASS" if result.is_valid else "❌ FAIL"
        print(f"\n{status} {result.file_name}")
        print(f"  SAS rows:    {result.sas_rows}")
        print(f"  Python rows: {result.python_rows}")
        print(f"  Matching:    {result.matching_rows}")
        
        if not result.is_valid:
            print(f"  ⚠️  Mismatched columns: {', '.join(result.mismatched_columns)}")
            print(f"  📈 Max difference: {result.max_difference:,.2f}")
            
            if len(result.differences) > 0:
                print(f"\n  Top 10 differences:")
                top_diffs = result.differences.nlargest(10, 'difference', keep='all')
                print(top_diffs.to_string(index=False))
    
    print("\n" + "="*80)
    if all_valid:
        print("✅ ALL VALIDATIONS PASSED - Perfect parity!")
    else:
        print("❌ VALIDATION FAILED - Differences detected")
    print("="*80)


def save_detailed_report(results: List[ComparisonResult], output_path: Path):
    """
    Save detailed differences to Excel file.
    
    Args:
        results: List of ComparisonResult objects
        output_path: Path to save Excel report
    """
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
# Summary sheet
        summary_data = []
        for result in results:
            summary_data.append({
                'File': result.file_name,
                'Status': '✅ PASS' if result.is_valid else '❌ FAIL',
                'SAS Rows': result.sas_rows,
                'Python Rows': result.python_rows,
                'Matching Rows': result.matching_rows,
                'Mismatched Columns': ', '.join(result.mismatched_columns),
                'Max Difference': result.max_difference
            })
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        # Individual difference sheets
        for result in results:
            if len(result.differences) > 0:
                sheet_name = result.file_name[:31]  # Excel limit
                result.differences.to_excel(writer, sheet_name=sheet_name, index=False)
    
    print(f"\n📄 Detailed report saved: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Compare SAS vs Python aggregated outputs")
    parser.add_argument("--vision", required=True, help="Vision in YYYYMM format (e.g., 202509)")
    parser.add_argument("--sas-dir", required=True, help="Directory containing SAS CSV files")
    parser.add_argument("--python-dir", required=True, help="Directory containing Python CSV files")
    parser.add_argument("--tolerance", type=float, default=0.01,
                        help="Absolute tolerance for numeric differences (default: 0.01)")
    parser.add_argument("--output-report", default="./validation_report.xlsx",
                        help="Path to save detailed Excel report")
    
    args = parser.parse_args()
    
    sas_dir = Path(args.sas_dir)
    python_dir = Path(args.python_dir)
    vision = args.vision
    
    if not sas_dir.exists():
        raise ValueError(f"SAS directory not found: {sas_dir}")
    if not python_dir.exists():
        raise ValueError(f"Python directory not found: {python_dir}")
    
    print(f"� Comparing SAS vs Python outputs for vision {vision}")
    print(f"📂 SAS directory:    {sas_dir}")
    print(f"� Python directory: {python_dir}")
    print(f"⚖️  Tolerance: ±{args.tolerance}")
    
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
                tolerance=args.tolerance
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
                differences=pd.DataFrame(),
                is_valid=False
            )
            results.append(result)
    
    # Print report
    print_comparison_report(results, args.tolerance)
    
    # Save detailed report
    save_detailed_report(results, Path(args.output_report))
    
    # Exit with appropriate code
    all_valid = all(r.is_valid for r in results)
    exit(0 if all_valid else 1)


if __name__ == "__main__":
    main()
