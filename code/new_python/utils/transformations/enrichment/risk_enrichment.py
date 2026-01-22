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

"""
RISK enrichment utilities for consolidation processor.

Provides reusable functions for joining and coalescing IRD risk data
(Q45, Q46, QAN) to avoid code duplication.
"""

from typing import List, Dict, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, to_date, broadcast
from pyspark.sql.types import StringType, DateType


def enrich_with_risk_data(
    df: DataFrame,
    risk_sources: List[str],
    vision: str,
    bronze_reader,
    logger=None
) -> DataFrame:
    """
    Generic risk enrichment for multiple IRD sources (Q45/Q46/QAN).
    
    Eliminates code duplication by processing all sources with same logic.
    Follows SAS sequential join-coalesce-drop pattern.
    
    Args:
        df: Main DataFrame to enrich
        risk_sources: List of source names ['q46', 'q45', 'qan']
        vision: Vision in YYYYMM format
        bronze_reader: BronzeReader instance for reading IRD files
        logger: Optional logger instance
    
    Returns:
        DataFrame enriched with all specified RISK sources
    
    Example:
        >>> df = enrich_with_risk_data(
        ...     df, ['q46', 'q45', 'qan'], '202509', reader, logger
        ... )
    """
    # Configuration for each IRD source
    IRD_CONFIG = {
        'q46': {
            'file_group': 'ird_risk_q46',
            'date_columns': ['dtouchan', 'dtrectrx', 'dtreffin'],
            'text_columns': ['ctprvtrv', 'ctdeftra', 'lbnattrv', 'lbdstcsc']
        },
        'q45': {
            'file_group': 'ird_risk_q45',
            'date_columns': ['dtouchan', 'dtrectrx', 'dtreffin'],
            'text_columns': ['ctprvtrv', 'ctdeftra', 'lbnattrv', 'lbdstcsc']
        },
        'qan': {
            'file_group': 'ird_risk_qan',
            'date_columns': ['dtouchan', 'dtrcppr'],
            'text_columns': ['ctprvtrv', 'ctdeftra', 'lbnattrv', 'dstcsc']
        }
    }
    
    # Initialize dtreffin if needed
    if 'dtreffin' not in df.columns:
        from utils.processor_helpers import add_null_columns
        df = add_null_columns(df, {'dtreffin': DateType})
    
    # Process each source sequentially (SAS pattern)
    for source in risk_sources:
        if source not in IRD_CONFIG:
            if logger:
                logger.warning(f"Unknown RISK source: {source}, skipping")
            continue
        
        df = _join_single_risk_source(
            df, IRD_CONFIG[source], source, vision, bronze_reader, logger
        )
    
    if logger:
        logger.info(f"RISK enrichment completed for sources: {risk_sources}")
    
    return df


def _join_single_risk_source(
    df: DataFrame,
    config: Dict,
    source_name: str,
    vision: str,
    bronze_reader,
    logger=None
) -> DataFrame:
    """
    Join single IRD source, coalesce values, drop temp columns.
    
    Internal helper for enrich_with_risk_data().
    
    Args:
        df: Main DataFrame
        config: Source configuration (file_group, date_columns, text_columns)
        source_name: Source name for logging ('q45', 'q46', 'qan')
        vision: Vision string
        bronze_reader: BronzeReader instance
        logger: Optional logger
    
    Returns:
        DataFrame with IRD data coalesced and _risk columns dropped
    """
    try:
        # Read IRD file
        df_ird = bronze_reader.read_file_group(config['file_group'], vision)
        
        if df_ird is None or df_ird.count() == 0:
            if logger:
                logger.debug(f"IRD {source_name.upper()}: No data found, skipping")
            return df
        
        # Deduplicate on NOPOL if needed
        df_ird = _deduplicate_on_nopol(
            df_ird, config['date_columns'], source_name, logger
        )
        
        # Prepare IRD columns with _risk suffix
        select_cols = ['nopol']
        
        for col_name in config['date_columns']:
            if col_name in df_ird.columns:
                select_cols.append(to_date(col(col_name)).alias(f"{col_name}_risk"))
        
        for col_name in config['text_columns']:
            if col_name in df_ird.columns:
                select_cols.append(col(col_name).alias(f"{col_name}_risk"))
            else:
                select_cols.append(lit(None).cast(StringType()).alias(f"{col_name}_risk"))
        
        df_ird_select = df_ird.select(*select_cols)
        
        # Left join
        df = df.join(broadcast(df_ird_select), on='nopol', how='left')
        
        # Coalesce values
        df = _coalesce_risk_columns(
            df, config['date_columns'], config['text_columns'], source_name
        )
        
        # Drop temporary _risk columns
        risk_cols_to_drop = [
            f"{c}_risk" for c in config['date_columns'] + config['text_columns']
        ]
        risk_cols_existing = [c for c in risk_cols_to_drop if c in df.columns]
        if risk_cols_existing:
            df = df.drop(*risk_cols_existing)
        
        if logger:
            logger.debug(f"IRD {source_name.upper()}: Joined and coalesced successfully")
    
    except Exception as e:
        if logger:
            logger.warning(f"IRD {source_name.upper()} enrichment failed: {e}")
    
    return df


def _deduplicate_on_nopol(
    df_ird: DataFrame,
    date_columns: List[str],
    source_name: str,
    logger=None
) -> DataFrame:
    """
    Remove duplicate NOPOLs from IRD data, keeping most recent entry.
    
    Args:
        df_ird: IRD DataFrame to deduplicate
        date_columns: Date columns to use for ordering (most recent wins)
        source_name: Source name for logging
        logger: Optional logger
    
    Returns:
        Deduplicated DataFrame
    """
    df_ird.cache()
    
    total_count = df_ird.count()
    distinct_nopol_count = df_ird.select("nopol").distinct().count()
    
    if total_count > distinct_nopol_count:
        dup_count = total_count - distinct_nopol_count
        if logger:
            logger.warning(
                f"IRD {source_name.upper()}: {dup_count} duplicates detected "
                f"(total={total_count}, unique={distinct_nopol_count}) - keeping most recent"
            )
        
        # Keep most recent entry per NOPOL
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        
        # Build ordering by latest dates (nulls last)
        order_cols = []
        for date_col in date_columns:
            if date_col in df_ird.columns:
                order_cols.append(col(date_col).desc_nulls_last())
        
        if not order_cols:
            order_cols = [lit(1)]  # Fallback if no dates
        
        window_spec = Window.partitionBy("nopol").orderBy(*order_cols)
        df_ird = (df_ird
                  .withColumn("_row_num", row_number().over(window_spec))
                  .filter(col("_row_num") == 1)
                  .drop("_row_num"))
        
        if logger:
            logger.info(
                f"IRD {source_name.upper()}: Deduplication complete "
                f"- kept {distinct_nopol_count} unique records"
            )
    
    df_ird.unpersist()
    return df_ird


def _coalesce_risk_columns(
    df: DataFrame,
    date_columns: List[str],
    text_columns: List[str],
    source_name: str
) -> DataFrame:
    """
    Coalesce risk columns with main DataFrame columns.
    
    Args:
        df: Main DataFrame with _risk columns from join
        date_columns: Date columns to coalesce
        text_columns: Text columns to coalesce
        source_name: Source name for special logic
    
    Returns:
        DataFrame with coalesced values
    """
    for col_name in date_columns + text_columns:
        risk_col = f"{col_name}_risk"

        # SAS: Q45/Q46 -> lbdstcsc maps to dstcsc
        #      QAN     -> dstcsc maps to dstcsc
        if col_name == 'lbdstcsc':
            target_col = 'dstcsc'
        elif source_name == 'qan' and col_name == 'dstcsc':
            target_col = 'dstcsc'
        else:
            target_col = col_name

        if risk_col in df.columns:

            # SAS: Q46 overwrites DTREFFIN without condition
            if col_name == 'dtreffin' and source_name == 'q46':
                df = df.withColumn('dtreffin', col(risk_col))

            else:
                df = df.withColumn(
                    target_col,
                    when(col(target_col).isNull(), col(risk_col))
                    .otherwise(col(target_col))
                )

    return df


def log_risk_enrichment_stats(
    df_before: DataFrame,
    df_after: DataFrame,
    source_name: str,
    columns: List[str],
    logger
) -> None:
    """
    Log statistics about RISK enrichment for debugging.
    
    Args:
        df_before: DataFrame before enrichment
        df_after: DataFrame after enrichment
        source_name: RISK source name
        columns: Columns that were enriched
        logger: Logger instance
    
    Example:
        >>> log_risk_enrichment_stats(df_before, df_after, 'Q46', 
        ...                            ['dtouchan', 'ctdeftra'], logger)
    """
    logger.info(f"RISK {source_name} Enrichment Stats:")
    
    for col_name in columns:
        if col_name in df_before.columns and col_name in df_after.columns:
            nulls_before = df_before.filter(col(col_name).isNull()).count()
            nulls_after = df_after.filter(col(col_name).isNull()).count()
            filled = nulls_before - nulls_after
            
            if filled > 0:
                logger.info(f"  {col_name}: Filled {filled} missing values")
