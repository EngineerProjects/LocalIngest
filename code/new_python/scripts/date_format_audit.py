"""
Date Format Audit Script for All FileGroups - PROPERLY REFACTORED

Scans all CSV files used in AZ, AZEC, and CONSOLIDATION pipelines
to detect actual date formats and identify mismatches with reading_config.json

OPTIMIZATION: Uses SCHEMA_REGISTRY from config.schemas to extract DateType columns

Usage:
    cd /home/amiche/Projects/LocalIngest/code/new_python
    spark-submit scripts/date_format_audit.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType
import re

# Import schemas properly
from config.schemas import SCHEMA_REGISTRY

# Load reading_config.json - robust path detection
import json
from pathlib import Path

# Find config/reading_config.json by searching upward from script location
script_path = Path(__file__).resolve()
current_dir = script_path.parent

# Search for config directory (handles both scripts/ and root execution)
config_path = None
for _ in range(3):  # Search up to 3 levels
    potential_config = current_dir / "config" / "reading_config.json"
    if potential_config.exists():
        config_path = potential_config
        break
    current_dir = current_dir.parent

if config_path is None:
    raise FileNotFoundError(
        f"Could not find config/reading_config.json. "
        f"Searched from {script_path.parent} upward"
    )

with open(config_path, "r") as f:
    reading_config = json.load(f)

# Configuration
DATALAKE_BASE = "abfss://shared@azfrdatalab.dfs.core.windows.net/ABR/P4D/ADC/DATAMARTS/CONSTRUCTION"
VISION = "202509"
YEAR = VISION[:4]
MONTH = VISION[4:6]

bronze_month_path = f"{DATALAKE_BASE}/bronze/{YEAR}/{MONTH}"
bronze_ref_path = f"{DATALAKE_BASE}/bronze/ref"

# Initialize Spark
spark = SparkSession.builder \
    .appName("DateFormatAudit") \
    .getOrCreate()



def extract_date_columns_from_schema(schema_struct):
    """
    Extract DateType column names from a StructType schema.
    
    Args:
        schema_struct: PySpark StructType schema
    
    Returns:
        list: Column names with DateType
    """
    if schema_struct is None:
        return []
    
    date_columns = []
    for field in schema_struct.fields:
        if isinstance(field.dataType, DateType):
            date_columns.append(field.name)
    
    return date_columns


def build_filegroup_date_columns_map():
    """
    Build mapping of file_group → list of date columns using SCHEMA_REGISTRY.
    
    Returns:
        dict: {file_group_name: [date_col1, date_col2, ...]}
    """
    mapping = {}
    
    for fg_name, schema_struct in SCHEMA_REGISTRY.items():
        date_cols = extract_date_columns_from_schema(schema_struct)
        if date_cols:
            mapping[fg_name] = date_cols
    
    return mapping


# File groups to audit (prioritized by date column likelihood)
FILE_GROUPS_TO_AUDIT = [
    # HIGH PRIORITY: Known to have many date columns
    "polic_cu_azec",       # AZEC - ✅ FIXED to yyyy-MM-dd
    "ipf_az",              # AZ - 14+ date columns
    "constrcu",            # AZEC - site dates
    
    # MEDIUM PRIORITY: Likely have date columns
    "capitxcu_azec",       # AZEC
    "incendcu_azec",       # AZEC
    "table_pt_gest",       # AZEC
    "ref_mig_azec_vs_ims", # AZEC
    "ipfm0024_1",          # CONSOLIDATION
    "ipfm0024_3",          # CONSOLIDATION
    "ipfm63_1",            # CONSOLIDATION
    "ipfm63_3",            # CONSOLIDATION
    "ipfm99_1",            # CONSOLIDATION
    "ipfm99_3",            # CONSOLIDATION
    "do_dest",             # CONSOLIDATION
    
    # LOW PRIORITY: Verify if needed
    "rcentcu_azec",        # AZEC
    "risteccu_azec",       # AZEC
    "mulprocu_azec",       # AZEC
    "mpacu_azec",          # AZEC
]


def detect_date_format(sample_value):
    """
    Detect date format from a sample string value.
    
    Returns:
        str: Detected format (e.g., 'yyyy-MM-dd', 'dd/MM/yyyy', 'MM/dd/yyyy')
    """
    if not sample_value or sample_value == "NULL":
        return "NULL"
    
    # Remove whitespace
    sample_value = sample_value.strip()
    
    # Pattern matching
    patterns = {
        r'^\d{4}-\d{2}-\d{2}$': 'yyyy-MM-dd',  # ISO 8601
        r'^\d{2}/\d{2}/\d{4}$': 'dd/MM/yyyy or MM/dd/yyyy',  # Ambiguous
        r'^\d{4}/\d{2}/\d{2}$': 'yyyy/MM/dd',
        r'^\d{8}$': 'yyyyMMdd',
        r'^\d{2}-\d{2}-\d{4}$': 'dd-MM-yyyy or MM-dd-yyyy',  # Ambiguous
    }
    
    for pattern, fmt in patterns.items():
        if re.match(pattern, sample_value):
            return fmt
    
    return "UNKNOWN"


def audit_file_group(file_group_name, date_columns_map):
    """
    Audit a single file group to detect date column formats.
    
    Args:
        file_group_name: Name of the file group
        date_columns_map: Pre-computed mapping of file_group → date columns
    
    Returns:
        dict: {column_name: detected_format}
    """
    try:
        # Get config from reading_config
        fg_config = reading_config["file_groups"].get(file_group_name)
        
        if not fg_config:
            print(f"\n⚠️  {file_group_name}: Not found in reading_config.json")
            return {}
        
        file_pattern = fg_config["file_patterns"][0]
        sep = fg_config["read_options"].get("sep", "|")
        configured_format = fg_config["read_options"].get("dateFormat", "NOT SPECIFIED")
        location_type = fg_config.get("location_type", "monthly")
        
        # Get date columns from schema
        date_cols = date_columns_map.get(file_group_name, [])
        
        if not date_cols:
            print(f"\n⚠️  {file_group_name}: No date columns found in schema")
            return {}
        
        # Determine path
        if location_type == "monthly":
            file_path = f"{bronze_month_path}/{file_pattern}"
        else:
            file_path = f"{bronze_ref_path}/{file_pattern}"
        
        print(f"\n{'='*80}")
        print(f"📁 File Group: {file_group_name}")
        print(f"📍 Path: {file_path}")
        print(f"⚙️  Configured format: {configured_format}")
        print(f"📋 Date columns from schema: {len(date_cols)} cols")
        print(f"{'='*80}")
        
        # Read CSV with only date columns (optimization!)
        df = spark.read.csv(file_path, sep=sep, header=True, inferSchema=False)
        
        # Check if columns exist in actual CSV
        actual_cols = df.columns
        existing_date_cols = [c for c in date_cols if c in actual_cols]
        missing_cols = [c for c in date_cols if c not in actual_cols]
        
        if missing_cols:
            print(f"⚠️  {len(missing_cols)} columns from schema NOT in CSV: {missing_cols[:5]}")
        
        if not existing_date_cols:
            print("❌ No date columns found in CSV")
            return {}
        
        # Select only existing date columns for performance
        df = df.select(*existing_date_cols)
        
        print(f"✅ Reading {len(existing_date_cols)} date columns from CSV\n")
        
        results = {}
        
        for col_name in existing_date_cols:
            # Get first non-null value, excluding SAS NULL values (., empty, spaces)
            sample = df.filter(
                col(col_name).isNotNull() & 
                ~col(col_name).isin(".", "", " ", "  ")  # Exclude SAS NULL representations
            ).select(col_name).first()
            
            if sample:
                sample_val = sample[0]
                detected_fmt = detect_date_format(sample_val)
                
                # Count non-null
                non_null_count = df.filter(col(col_name).isNotNull()).count()
                total_count = df.count()
                
                results[col_name] = detected_fmt
                
                # Display
                match = "✅" if detected_fmt == configured_format else "⚠️"
                print(f"{match} {col_name:20} | Sample: {sample_val:15} | Format: {detected_fmt:20} | Non-NULL: {non_null_count:,}/{total_count:,}")
            else:
                results[col_name] = "ALL NULL"
                print(f"❌ {col_name:20} | ALL NULL")
        
        return results
        
    except Exception as e:
        print(f"❌ ERROR auditing {file_group_name}: {e}")
        import traceback
        traceback.print_exc()
        return {}


# Main audit
print("\n" + "="*80)
print("🔬 DATE FORMAT AUDIT - USING SCHEMA_REGISTRY")
print("="*80)

# Step 1: Build date columns map from SCHEMA_REGISTRY
print("\n📚 Step 1: Extracting date columns from SCHEMA_REGISTRY...")
date_columns_map = build_filegroup_date_columns_map()

print(f"\n✅ Extracted date columns for {len(date_columns_map)} file groups")
for fg, cols in list(date_columns_map.items())[:5]:
    print(f"  - {fg}: {len(cols)} date columns")
if len(date_columns_map) > 5:
    print(f"  ... ({len(date_columns_map) - 5} more)")

# Step 2: Audit each file group
print("\n📊 Step 2: Auditing file groups...")

all_results = {}

for fg in FILE_GROUPS_TO_AUDIT:
    results = audit_file_group(fg, date_columns_map)
    all_results[fg] = results

# Summary
print("\n" + "="*80)
print("📊 SUMMARY - FORMAT MISMATCHES")
print("="*80)

mismatches = []

for fg, cols in all_results.items():
    if not cols:
        continue
    
    fg_config = reading_config["file_groups"].get(fg)
    if not fg_config:
        continue
    
    configured_fmt = fg_config["read_options"].get("dateFormat", "NOT SPECIFIED")
    
    for col_name, detected_fmt in cols.items():
        if detected_fmt != "ALL NULL" and detected_fmt != "NULL":
            if configured_fmt == "NOT SPECIFIED":
                mismatches.append(f"❌ {fg}.{col_name}: No format specified (detected: {detected_fmt})")
            elif detected_fmt not in configured_fmt and "UNKNOWN" not in detected_fmt:
                if "/" in detected_fmt and "dd/MM/yyyy" in configured_fmt:
                    # Ambiguous - need manual check
                    mismatches.append(f"⚠️  {fg}.{col_name}: AMBIGUOUS - detected '{detected_fmt}', config '{configured_fmt}'")
                else:
                    mismatches.append(f"❌ {fg}.{col_name}: MISMATCH - detected '{detected_fmt}', config '{configured_fmt}'")

if mismatches:
    print("\n🚨 ISSUES FOUND:\n")
    for m in mismatches:
        print(f"  {m}")
    
    print(f"\n📝 Total issues: {len(mismatches)}")
else:
    print("\n✅ All date formats match configuration!")

print("\n" + "="*80)
print("✅ Audit complete!")
print("="*80)

spark.stop()
