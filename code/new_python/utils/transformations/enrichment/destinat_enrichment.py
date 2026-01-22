"""
DESTINAT (Destination) calculation for construction sites.

Parses DSTCSC field to categorize construction site destinations.
Based on PTF_MVTS_CONSOLIDATION_MACRO.sas L416-437.
"""

from pyspark.sql import DataFrame # type: ignore
from pyspark.sql.functions import col, when, lit, upper, regexp_extract, coalesce # type: ignore
from typing import Optional, Any

# =========================================================================
# DESTINAT CONSOLIDATION LOGIC
# =========================================================================

def apply_destinat_consolidation_logic(df: DataFrame) -> DataFrame:
    """
    Apply DESTINAT business logic for construction sites (Chantiers).
    
    This applies consolidation-specific logic that fills missing DESTINAT
    values after the detailed calculation.
    
    Based on: PTF_MVTS_CONSOLIDATION_MACRO.sas L423-437
    
    Logic:
    1. Only applies to segment2="Chantiers" where DESTINAT is missing
    2. Checks DSTCSC and LBNATTRV for housing keywords
    3. Checks DSTCSC numeric codes for residential classification
    4. Default to "Autres" if no match
    
    Args:
        df: Consolidated DataFrame
        
    Returns:
        DataFrame with DESTINAT filled for Chantiers
        
    Example:
        >>> from utils.transformations.destinat_calculation import apply_destinat_consolidation_logic
        >>> df = apply_destinat_consolidation_logic(df)
    """
    if "segment2" not in df.columns or "destinat" not in df.columns:
        return df
    
    # Only process Chantiers with missing DESTINAT
    is_chantier_missing = (col("segment2") == "Chantiers") & col("destinat").isNull()
    
    # Prepare uppercase columns for pattern matching
    dstcsc_upper = upper(col("dstcsc")) if "dstcsc" in df.columns else lit(None)
    lbnattrv_upper = upper(col("lbnattrv")) if "lbnattrv" in df.columns else lit(None)
    
    # Pattern matching for "Habitation" (SAS L427-434)
    # Keywords: HABIT, LOG, LGT, MAIS, APPA, VILLA, INDIV
    is_habitation = (
        dstcsc_upper.contains("HABIT") | lbnattrv_upper.contains("HABIT") |
        dstcsc_upper.contains("LOG") | lbnattrv_upper.contains("LOG") |
        dstcsc_upper.contains("LGT") | lbnattrv_upper.contains("LGT") |
        dstcsc_upper.contains("MAIS") | lbnattrv_upper.contains("MAIS") |
        dstcsc_upper.contains("APPA") | lbnattrv_upper.contains("APPA") |
        dstcsc_upper.contains("VILLA") | lbnattrv_upper.contains("VILLA") |
        dstcsc_upper.contains("INDIV") | lbnattrv_upper.contains("INDIV")
    )
    
    # Numeric codes for "Habitation" (SAS L436)
    if "dstcsc" in df.columns:
        is_habitation = is_habitation | col("dstcsc").isin(["01", "02", "03", "04", "1", "2", "3", "4", "22"])
    
    # Apply logic (SAS L426-437)
    df = df.withColumn(
        "destinat",
        when(
            is_chantier_missing,
            when(is_habitation, lit("Habitation"))
            .otherwise(lit("Autres"))
        ).otherwise(col("destinat"))
    )
    
    return df
