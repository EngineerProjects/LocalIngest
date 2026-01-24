#!/usr/bin/env python3
"""
AZEC Movements Diagnostic Script

Purpose: Identify why NBAFN/NBRES/NBPTF are all zero
Run this INSIDE the azec_processor.transform() right before calculate_azec_movements
"""

from pyspark.sql.functions import col, month, year, count, when, sum as spark_sum
from config.variables import AZEC_PRODUIT_LIST

def diagnose_azec_movements(df, year_int=2025, month_int=9, dtdeb_an="2025-01-01", dtfinmn="2025-09-30"):
    """
    Run comprehensive diagnostics on AZEC movements calculation
    
    Args:
        df: DataFrame before calculate_azec_movements
        year_int: Year (e.g., 2025)
        month_int: Month (e.g., 9)
        dtdeb_an: Start of year date string
        dtfinmn: End of month date string
    """
    from pyspark.sql.functions import to_date, lit
    
    dtdeb_an_col = to_date(lit(dtdeb_an))
    dtfinmn_col = to_date(lit(dtfinmn))
    
    print("\n" + "="*80)
    print("AZEC MOVEMENTS DIAGNOSTIC ANALYSIS")
    print("="*80)
    
    # TEST 1: Vérifier AZEC_PRODUIT_LIST
    print("\n[TEST 1] AZEC_PRODUIT_LIST Configuration")
    print(f"  Length: {len(AZEC_PRODUIT_LIST)}")
    print(f"  First 10: {AZEC_PRODUIT_LIST[:10]}")
    print(f"  Last 5: {AZEC_PRODUIT_LIST[-5:]}")
    
    # TEST 2: Compter base_afn
    print("\n[TEST 2] Base AFN Condition")
    base_afn_df = df.filter(
        (col("etatpol") == "R") &
        (~col("produit").isin("CNR", "DO0")) &
        (col("nbptf_non_migres_azec") == 1)
    )
    base_afn_count = base_afn_df.count()
    total_count = df.count()
    print(f"  base_afn matches: {base_afn_count:,} / {total_count:,} ({100*base_afn_count/total_count:.1f}%)")
    
    # TEST 3: Compter afn_in_list (PRODUITS SPÉCIAUX)
    print("\n[TEST 3] AFN In-List (Special Products)")
    
    # Combien de produits sont dans la liste?
    in_list_count = df.filter(col("produit").isin(AZEC_PRODUIT_LIST)).count()
    print(f"  Products IN list: {in_list_count:,} / {total_count:,}")
    
    # Combien ont datafn non-NULL?
    in_list_with_datafn = df.filter(
        col("produit").isin(AZEC_PRODUIT_LIST) &
        col("datafn").isNotNull()
    ).count()
    print(f"  Products IN list + datafn NOT NULL: {in_list_with_datafn:,}")
    
    # Combien passent la condition month/year?
    afn_in_list_full = df.filter(
        col("produit").isin(AZEC_PRODUIT_LIST) &
        col("datafn").isNotNull() &
        (month(col("datafn")) <= month_int) &
        (year(col("datafn")) == year_int)
    )
    afn_in_list_count = afn_in_list_full.count()
    print(f"  AFN in_list FULL condition: {afn_in_list_count:,}")
    
    # Sample pour debug
    if afn_in_list_count > 0:
        print("\n  Sample rows matching afn_in_list:")
        afn_in_list_full.select("produit", "datafn", "etatpol", "nbptf_non_migres_azec").show(5, truncate=False)
    
    # TEST 4: Compter afn_not_in_list (PRODUITS STANDARD)
    print("\n[TEST 4] AFN Not-In-List (Standard Products)")
    
    not_in_list_count = df.filter(~col("produit").isin(AZEC_PRODUIT_LIST)).count()
    print(f"  Products NOT in list: {not_in_list_count:,} / {total_count:,}")
    
    # Cas 1: effetpol dans année ET datafn <= fin mois
    afn_not_in_list_case1 = df.filter(
        ~col("produit").isin(AZEC_PRODUIT_LIST) &
        col("effetpol").isNotNull() &
        col("datafn").isNotNull() &
        (col("effetpol") >= dtdeb_an_col) &
        (col("effetpol") <= dtfinmn_col) &
        (col("datafn") <= dtfinmn_col)
    )
    case1_count = afn_not_in_list_case1.count()
    print(f"  AFN not_in_list CASE 1 (effetpol in year): {case1_count:,}")
    
    # Cas 2: effetpol avant année ET datafn dans année
    afn_not_in_list_case2 = df.filter(
        ~col("produit").isin(AZEC_PRODUIT_LIST) &
        col("effetpol").isNotNull() &
        col("datafn").isNotNull() &
        (col("effetpol") < dtdeb_an_col) &
        (col("datafn") >= dtdeb_an_col) &
        (col("datafn") <= dtfinmn_col)
    )
    case2_count = afn_not_in_list_case2.count()
    print(f"  AFN not_in_list CASE 2 (effetpol before year): {case2_count:,}")
    
    total_afn_not_in_list = case1_count + case2_count
    print(f"  AFN not_in_list TOTAL: {total_afn_not_in_list:,}")
    
    # TEST 5: FINAL COMBINED
    print("\n[TEST 5] Final Combined AFN Condition")
    final_afn = base_afn_df.filter(
        col("produit").isin(AZEC_PRODUIT_LIST) &
        col("datafn").isNotNull() &
        (month(col("datafn")) <= month_int) &
        (year(col("datafn")) == year_int)
    ).count() + base_afn_df.filter(
        ~col("produit").isin(AZEC_PRODUIT_LIST) &
        (
            (
                col("effetpol").isNotNull() & col("datafn").isNotNull() &
                (col("effetpol") >= dtdeb_an_col) & (col("effetpol") <= dtfinmn_col) &
                (col("datafn") <= dtfinmn_col)
            ) |
            (
                col("effetpol").isNotNull() & col("datafn").isNotNull() &
                (col("effetpol") < dtdeb_an_col) &
                (col("datafn") >= dtdeb_an_col) & (col("datafn") <= dtfinmn_col)
            )
        )
    ).count()
    
    print(f"  Expected NBAFN=1 count: {final_afn:,}")
    
    # TEST 6: Type check
    print("\n[TEST 6] Column Types")
    print(f"  datafn type: {df.schema['datafn'].dataType}")
    print(f"  effetpol type: {df.schema['effetpol'].dataType}")
    print(f"  datfin type: {df.schema['datfin'].dataType}")
    print(f"  datresil type: {df.schema['datresil'].dataType}")
    
    # TEST 7: Sample values
    print("\n[TEST 7] Sample Date Values (first 10 non-NULL)")
    df.filter(col("datafn").isNotNull()).select("police", "produit", "datafn", "effetpol", "etatpol").show(10, truncate=False)
    
    print("\n" + "="*80)
    print(f"DIAGNOSTIC SUMMARY")
    print("="*80)
    print(f"Total rows: {total_count:,}")
    print(f"Base AFN eligible: {base_afn_count:,}")
    print(f"AFN in-list matches: {afn_in_list_count:,}")
    print(f"AFN not-in-list matches: {total_afn_not_in_list:,}")
    print(f"EXPECTED NBAFN=1: {final_afn:,}")
    print("="*80 + "\n")
    
    return {
        "total": total_count,
        "base_afn": base_afn_count,
        "afn_in_list": afn_in_list_count,
        "afn_not_in_list": total_afn_not_in_list,
        "expected_nbafn": final_afn
    }


# Example usage in azec_processor.py:
# from test_azec_movements_diagnostic import diagnose_azec_movements
# stats = diagnose_azec_movements(df, year_int, month_int, dates['dtdeb_an'], dates['dtfinmn'])
