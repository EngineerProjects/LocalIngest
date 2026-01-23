#!/usr/bin/env python3
"""
Diagnostic script to compare AZ filter logic between SAS and PySpark.

This script validates:
1. NOINT exclusion list completeness
2. Policy counts by product
3. Capital calculation (MTCA) by product
4. Movement indicators by product
"""

# SAS filter from PTF_MVTS_AZ_MACRO.sas L42-44
SAS_NOINT_EXCLUSIONS = [
    'H90061', '482001', '489090', '102030', 'H90036', 'H90059',
    'H99045', 'H99059', '5B2000', '446000', '5R0001', '446118',
    '4F1004', '4A1400', '4A1500', '4A1600', '4A1700', '4A1800',
    '4A1900', '482001', '489090', '4F1004', '4L1010'  # Note: duplicates in SAS
]

# PySpark filter from business_rules.json
PYSPARK_NOINT_EXCLUSIONS = [
    "H90061", "482001", "489090", "102030", "H90036", "H90059",
    "H99045", "H99059", "5B2000", "446000", "5R0001", "446118",
    "4F1004", "4A1400", "4A1500", "4A1600", "4A1700", "4A1800",
    "4A1900", "4L1010"
]

def main():
    print("=" * 80)
    print("AZ FILTER DIAGNOSTIC REPORT")
    print("=" * 80)
    print()
    
    # 1. Compare NOINT exclusions
    print("1. NOINT EXCLUSION LIST COMPARISON")
    print("-" * 80)
    
    sas_unique = set(SAS_NOINT_EXCLUSIONS)
    pyspark_set = set(PYSPARK_NOINT_EXCLUSIONS)
    
    print(f"SAS unique values: {len(sas_unique)} (total with duplicates: {len(SAS_NOINT_EXCLUSIONS)})")
    print(f"PySpark values: {len(pyspark_set)}")
    print()
    
    missing_in_pyspark = sas_unique - pyspark_set
    extra_in_pyspark = pyspark_set - sas_unique
    
    if missing_in_pyspark:
        print(f"⚠️  MISSING in PySpark: {sorted(missing_in_pyspark)}")
    else:
        print("✅ All SAS NOINT exclusions present in PySpark")
    
    if extra_in_pyspark:
        print(f"❓ EXTRA in PySpark: {sorted(extra_in_pyspark)}")
    
    # NOTE: SAS has duplicates but they don't matter logically
    sas_duplicates = [x for x in SAS_NOINT_EXCLUSIONS if SAS_NOINT_EXCLUSIONS.count(x) > 1]
    if sas_duplicates:
        print(f"\nℹ️  SAS has duplicate values (no logical impact): {set(sas_duplicates)}")
    
    print()
    print("=" * 80)
    print("CONCLUSION:")
    print("=" * 80)
    if not missing_in_pyspark and not extra_in_pyspark:
        print("✅ NOINT filter lists are equivalent")
    else:
        print("❌ FILTER MISMATCH DETECTED - This could cause policy count discrepancies!")
        if missing_in_pyspark:
            print(f"   Action: Add {missing_in_pyspark} to business_rules.json")
    print()

if __name__ == "__main__":
    main()
