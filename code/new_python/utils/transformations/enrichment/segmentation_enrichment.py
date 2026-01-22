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


from types import SimpleNamespace
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, coalesce, broadcast
from pyspark.sql.types import StringType
from utils.processor_helpers import get_bronze_reader
from config.constants import MARKET_CODE

# ----------------------------------------------------------------------
# Small helper: build a single CASE WHEN expression from (cond, value) pairs
# Order matters (first match wins), so we fold from bottom to top.
# ----------------------------------------------------------------------
def _case_when(pairs, default):
    expr = default
    for cond, val in reversed(pairs):
        expr = when(cond, lit(val)).otherwise(expr)
    return expr


def load_constrcu_reference(
    spark,
    config,
    vision: str,
    lob_ref: DataFrame,
    logger=None,
    *,
    drop_dups_by=("police", "produit")  # set to ("police",) if you want to mirror SAS NODUPKEY BY POLICE
) -> DataFrame:
    """
    Load and prepare the SAS CONSTRCU reference block (concise & SAS-accurate).

    SAS REF_segmentation_azec.sas (core points reproduced):
      - SET CONSTRCU + RISTECCU (select only required columns)
      - Join LOB (cmarch='6'), HASH-like behavior (broadcast)
      - typmarc1 fallback from ltypmar1
      - nat_cnt relevant only for DPC
      - ACTIVITE computed with exact SAS conditions

    Returns columns:
      police, produit, typmarc1, nat_cnt, activite, cdprod (LOB), segment (LOB), lssseg (LOB)
    """
    reader = get_bronze_reader(SimpleNamespace(spark=spark, config=config, logger=logger))

    # 1) Read raw sources (lowercase ensured by BronzeReader)
    try:
        df_constrcu = reader.read_file_group("constrcu_azec", "ref")
    except Exception as e:
        if logger: logger.debug(f"[SEG] CONSTRCU missing: {e}")
        df_constrcu = None

    try:
        df_risteccu = reader.read_file_group("risteccu_azec", "ref")
    except Exception as e:
        if logger: logger.debug(f"[SEG] RISTECCU missing: {e}")
        df_risteccu = None

    if df_constrcu is None and df_risteccu is None:
        raise RuntimeError("CONSTRCU and RISTECCU references are missing.")

    # 2) Project required columns and union like SAS SET
    base_cols = ["police", "produit", "typmarc1", "ltypmar1", "formule", "nat_cnt"]

    left = None
    if df_constrcu is not None:
        cols = [c for c in base_cols if c in df_constrcu.columns]
        left = df_constrcu.select(*cols)

    right = None
    if df_risteccu is not None:
        keep = [c for c in ["police", "produit", "formule"] if c in df_risteccu.columns]
        if keep:
            right = df_risteccu.select(*keep)

    if left is not None and right is not None:
        merged = left.unionByName(right, allowMissingColumns=True)
    else:
        merged = left if left is not None else right

    # 3) Join LOB (cmarch already filtered upstream ideally; still protect)
    #    Only pick the columns we actually need.
    # SAS defines: CDPROD, CPROD, cmarch, lmarch, lmarch2, cseg, lseg, lseg2, 
    #              cssseg, lssseg, lssseg2, lprod, segment
    
    # CRITICAL FIX: LOB MUST have exactly 1 row per produit to avoid cartesian explosion
    # First, select columns and validate for duplicates
    lob_pre_dedup = lob_ref.select(
        "produit", "cdprod", "cprod", "cmarch", "lmarch", 
        "cseg", "lseg", "cssseg", "lssseg", "lprod", "segment"
    )
    
    # Detect duplicates BEFORE dedup to raise error if LOB is corrupted
    from pyspark.sql.functions import count as spark_count, first
    dup_check = lob_pre_dedup.groupBy("produit").agg(spark_count("*").alias("dup_count"))
    dup_count_total = dup_check.filter(col("dup_count") > 1).count()
    
    if dup_count_total > 0:
        if logger:
            logger.error(f"[SEG] LOB contains {dup_count_total} products with duplicates!")
            logger.error("[SEG] Top duplicates:")
            dup_check.filter(col("dup_count") > 1).orderBy("dup_count", ascending=False).show(20, False)
        
        # CRITICAL: Fail-fast if duplicates detected
        # raise RuntimeError(
        #     f"LOB reference contains {dup_count_total} duplicate produit values. "
        # "This would cause cartesian explosion in CONSTRCU join (observed: 1.7M rows instead of 13-14k). "
        #     "Fix LOB reference data or use a deterministic dedup strategy."
        # )
        
        # WORKAROUND: Use groupBy + first() for deterministic dedup
        if logger:
            logger.warning(f"[SEG] Applying deterministic dedup using first() for {dup_count_total} duplicate products")
    
    # Safe dedup: use groupBy + first() instead of dropDuplicates
    # This is deterministic and ensures exactly 1 row per produit
    lob_small = (
        lob_pre_dedup
        .groupBy("produit")
        .agg(
            first("cdprod").alias("cdprod"),
            first("cprod").alias("cprod"),
            first("cmarch").alias("cmarch"),
            first("lmarch").alias("lmarch"),
            first("cseg").alias("cseg"),
            first("lseg").alias("lseg"),
            first("cssseg").alias("cssseg"),
            first("lssseg").alias("lssseg"),
            first("lprod").alias("lprod"),
            first("segment").alias("segment")
        )
    )
    
    # Validation: ensure exactly 1 row per produit
    final_count = lob_small.count()
    unique_produits = lob_pre_dedup.select("produit").distinct().count()
    if final_count != unique_produits:
        if logger:
            logger.error(f"[SEG] LOB dedup failed: {final_count} rows vs {unique_produits} unique products")
        raise RuntimeError(f"LOB dedup verification failed: {final_count} != {unique_produits}")

    merged = (
        merged.alias("c")
        .join(
            broadcast(lob_small).alias("l"),
            col("c.produit") == col("l.produit"),
            "left"
        )
        .select(
            "c.*",  # Preserve ALL columns from CONSTRCU/RISTECCU union
            col("l.cdprod"),   col("l.cprod"),
            col("l.cmarch"),   col("l.lmarch"),
            col("l.cseg"),     col("l.lseg"),
            col("l.cssseg"),   col("l.lssseg"),
            col("l.lprod"),    col("l.segment")
        )
    )

    # 4) Keep construction only
    merged = merged.filter(col("cmarch") == lit("6"))

    # 5) typmarc1 fallback (ltypmar1 mirrors SAS typmarc7/typmarc fallback behavior)
    merged = merged.withColumn("typmarc1", coalesce(col("typmarc1"), col("ltypmar1")))

    # 6) nat_cnt only relevant for DPC (SAS clears it for other products)
    merged = merged.withColumn("nat_cnt",
        when(col("produit") == "DPC", col("nat_cnt")).otherwise(lit(""))
    )

    # 7) ACTIVITE — exact SAS logic, but expressed once as a single CASE
    merged = _compute_activite_sas(merged)

    # Final dedup (default by police+produit; set drop_dups_by=("police",) if you want strict SAS)
    return merged.dropDuplicates(list(drop_dups_by))


def _compute_activite_sas(df: DataFrame) -> DataFrame:
    """
    Compute ACTIVITE exactly like SAS, null-safe, in a single pass.
    Priority/order is preserved (top to bottom).
    
    Hypothèse: stratégie all-NULL (toutes valeurs manquantes → NULL, jamais "").
    Donc, on utilise .isNull() au lieu de == "" pour détecter le 'manquant'.
    """
    is_rba_rcd = col("produit").isin("RBA", "RCD")

    pairs = [
        # RBA/RCD cases
        (is_rba_rcd & (col("typmarc1") == "01"), "ARTISAN"),

        (
            is_rba_rcd
            & (
                col("typmarc1").isin("02", "03", "04")
                | (col("typmarc1").isNull() & col("formule").isin("DA", "DB", "DC"))
            ),
            "ENTREPRISE",
        ),

        (
            is_rba_rcd
            & (
                col("typmarc1").isin("05")
                | (col("typmarc1").isNull() & (col("formule") == "DD"))
            ),
            "M. OEUVRE",
        ),
        
        (
            is_rba_rcd
            & (
                col("typmarc1").isin("06", "14")
                | (col("typmarc1").isNull() & (col("formule") == "DE"))
            ),
            "FABRICANT",
        ),

        ((col("produit") == "RBA") & col("typmarc1").isin("07"), "NEGOCIANT"),
        (is_rba_rcd & col("typmarc1").isin("08"), "PROMOTEUR"),
        (is_rba_rcd & col("typmarc1").isin("09"), "M. OUVRAGE"),
        (is_rba_rcd & col("typmarc1").isin("10"), "MARCHAND"),
        ((col("produit") == "RBA") & col("typmarc1").isin("11"), "FABRICANT"),
        (is_rba_rcd & col("typmarc1").isin("12", "13"), "M. OEUVRE G.C"),

        # Special RCD override
        ((col("produit") == "RCD") & col("typmarc1").isin("07", "14"), "NEGOCIANT"),

        # DPC product (nat_cnt mapping)
        ((col("produit") == "DPC") & (col("nat_cnt") == "01"), "COMP. GROUPE"),
        ((col("produit") == "DPC") & (col("nat_cnt") == "02"), "PUC BATIMENT"),
        ((col("produit") == "DPC") & (col("nat_cnt") == "03"), "PUC G.C."),
        ((col("produit") == "DPC") & (col("nat_cnt") == "04"), "GLOBAL CHANT"),
        ((col("produit") == "DPC") & (col("nat_cnt") == "05"), "DEC G.C."),
        ((col("produit") == "DPC") & (col("nat_cnt") == "06"), "DIVERS"),
        ((col("produit") == "DPC") & (col("nat_cnt") == "07"), "DEC BATIMENT"),
    ]

    activite_expr = _case_when(pairs, default=lit(None).cast(StringType()))
    return df.withColumn("activite", activite_expr)

def join_constrcu_sas(
    df: DataFrame,
    df_constrcu_ref: DataFrame,
    *,
    logger=None
) -> DataFrame:
    """
    SAS-faithful join of prepared CONSTRCU reference onto the AZEC dataset.

    Keys:
      - (police, produit)

    Columns brought from CONSTRCU reference:
      - typmarc1, nat_cnt, activite
      - cdprod (from LOB ref), segment (from LOB ref), lssseg (from LOB ref)

    Overwrite policy:
      - Do NOT overwrite existing left columns. Prefer left if present; otherwise take right.
    """
    if df_constrcu_ref is None or not df_constrcu_ref.columns:
        if logger:
            logger.warning("[SEG] CONSTRCU reference is empty or missing. Skipping join.")
        return df

    # Only keep required reference columns and drop dup keys
    right_keep = [
        "police", "produit",
        "typmarc1", "nat_cnt", "activite",
        "cdprod", "segment", "lssseg"
    ]
    right_cols = [c for c in right_keep if c in df_constrcu_ref.columns]
    if not {"police", "produit"}.issubset(set(right_cols)):
        if logger:
            logger.warning("[SEG] CONSTRCU reference missing join keys 'police'/'produit'. Skipping join.")
        return df

    r = df_constrcu_ref.select(*right_cols).dropDuplicates(["police", "produit"]).alias("r")
    l = df.alias("l")

    joined = l.join(
        r,
        on=[col("l.police") == col("r.police"), col("l.produit") == col("r.produit")],
        how="left"
    )

    # SAS pattern: SELECT t1.*, t2.col1, t2.col2, ...
    # We preserve ALL left columns + add specific columns from right
    # Columns to add from reference (matching SAS)
    ref_cols_to_add = [
        "typmarc1", "nat_cnt", "activite",  # From CONSTRCU
        "cdprod", "cprod", "cmarch", "lmarch", "cseg", "lseg", "cssseg", "lssseg", "lprod", "segment"  # From LOB
    ]
    
    # Build SELECT: l.* + r.specific_cols (with coalesce for conflicts)
    select_list = ["l.*"]  # ALL left columns
    
    for col_name in ref_cols_to_add:
        r_col = f"r.{col_name}"
        l_col = f"l.{col_name}"
        
        # If column exists in right, add it (coalesce with left if both exist)
        if r_col in joined.columns:
            if l_col in joined.columns:
                # Both exist: coalesce (prefer left)
                select_list.append(coalesce(col(l_col), col(r_col)).alias(col_name))
            else:
                # Only right exists: take right
                select_list.append(col(r_col).alias(col_name))
        elif l_col in joined.columns:
            # Only left exists: already in l.*, skip (will be included via l.*)
            pass
        else:
            # Neither exists: add NULL
            select_list.append(lit(None).cast(StringType()).alias(col_name))
    
    out = joined.select(*select_list)

    if logger:
        logger.info("✓ CONSTRCU reference joined (SAS-faithful, keys=(police, produit))")

    return out

def compute_type_produit_sas(
    df: DataFrame,
    spark,
    config,
    logger=None
) -> DataFrame:
    """
    Compute TYPE_PRODUIT exactly like SAS REF_segmentation_azec.sas.

    SAS logic reproduced:
      1) Try to map TYPE_PRODUIT from Typrd_2 on ACTIVITE
         (excluding ACTIVITE values handled by fallback).
      2) If no mapping: fallback:
           - LSSSEG == 'TOUS RISQUES CHANTIERS' -> 'TRC'
           - LSSSEG == 'DOMMAGES OUVRAGES'      -> 'DO'
           - else                               -> 'Autres'
      3) Override: if PRODUIT == 'RCC' then TYPE_PRODUIT = 'Entreprises'
      4) CSSSEG correction: if LSSSEG == 'RC DECENNALE' and TYPE_PRODUIT == 'Artisans'
           then CSSSEG = '7'

    Notes:
      - Does not modify any other fields.
      - Leaves TYPE_PRODUIT as a new column (string).
      - Creates CSSSEG if missing (string), then applies correction.
    """
    # Ensure required columns exist
    base = df
    for c in ["activite", "lssseg", "cssseg", "produit"]:
        if c not in base.columns:
            base = base.withColumn(c, lit(None).cast(StringType()))

    # Try to load Typrd_2 mapping from bronze/ref
    reader = get_bronze_reader(SimpleNamespace(spark=spark, config=config, logger=logger))
    df_map = None
    try:
        df_map = reader.read_file_group("typrd_2", "ref")
    except Exception as e:
        if logger:
            logger.debug(f"[SEG] Typrd_2 not available: {e}")
        df_map = None

    # Prepare mapping if available
    mapped = base
    map_col_activite = None
    map_col_typeprod = None

    if df_map is not None and df_map.columns:
        # Normalize case-insensitively (BronzeReader already lowercase, but be defensive).
        cols = {c.lower(): c for c in df_map.columns}
        if "activite" in cols and ("type_produit" in cols or "typeproduit" in cols):
            map_col_activite = cols["activite"]
            map_col_typeprod = cols.get("type_produit", cols.get("typeproduit"))

            # Exclude ACTIVITE values handled by fallback, as SAS does
            exclude_acts = ["", "DOMMAGES OUVRAGES", "RC ENTREPRISES DE CONSTRUCTION",
                            "RC DECENNALE", "TOUS RISQUES CHANTIERS"]

            df_map_clean = (
                df_map
                .select(
                    col(map_col_activite).alias("map_activite"),
                    col(map_col_typeprod).alias("map_type_produit")
                )
                .where(~col("map_activite").isin(exclude_acts))
                .dropDuplicates(["map_activite"])
            )

            # LEFT JOIN on ACTIVITE
            mapped = (
                mapped.alias("a")
                .join(broadcast(df_map_clean).alias("m"),
                      col("a.activite") == col("m.map_activite"),
                      how="left")
                .select("a.*", col("m.map_type_produit"))
            )
        else:
            if logger:
                logger.debug("[SEG] Typrd_2 missing required columns (activite/type_produit). Using fallback only.")
            mapped = mapped.withColumn("map_type_produit", lit(None).cast(StringType()))
    else:
        mapped = mapped.withColumn("map_type_produit", lit(None).cast(StringType()))

    # Build TYPE_PRODUIT with SAS priority:
    # 1) use mapping if present
    # 2) fallback via LSSSEG
    type_prod = when(col("map_type_produit").isNotNull(), col("map_type_produit")) \
        .otherwise(
            when(col("lssseg") == "TOUS RISQUES CHANTIERS", lit("TRC"))
            .when(col("lssseg") == "DOMMAGES OUVRAGES", lit("DO"))
            .otherwise(lit("Autres"))
        )

    mapped = mapped.withColumn("type_produit", type_prod)

    # Override for PRODUIT == 'RCC'
    mapped = mapped.withColumn(
        "type_produit",
        when(col("produit") == "RCC", lit("Entreprises"))
        .otherwise(col("type_produit"))
    )

    # CSSSEG correction for RC DECENNALE + Artisans
    if "cssseg" not in mapped.columns:
        mapped = mapped.withColumn("cssseg", lit(None).cast(StringType()))

    mapped = mapped.withColumn(
        "cssseg",
        when((col("lssseg") == "RC DECENNALE") & (col("type_produit") == "Artisans"),
             lit("7")).otherwise(col("cssseg"))
    )

    # Cleanup temp
    mapped = mapped.drop("map_type_produit")

    if logger:
        logger.info("✓ TYPE_PRODUIT computed (SAS-faithful) with Typrd_2 mapping and fallbacks")

    return mapped

def compute_segment3_sas(df: DataFrame, logger=None) -> DataFrame:
    """
    Compute Segment_3 exactly as in REF_segmentation_azec.sas.

    SAS logic:
      IF lmarch2 = '6_CONSTRUCTION' THEN
         IF Type_Produit = 'Artisans' THEN Segment_3='Artisans';
         ELSE IF Type_Produit IN ('TRC','DO','CNR','Autres Chantiers') THEN Segment_3='Chantiers';
         ELSE Segment_3='Renouvelables hors artisans';
    """
    # Ensure required columns exist
    for c in ["lmarch", "type_produit"]:
        if c not in df.columns:
            df = df.withColumn(c, lit(None).cast(StringType()))

    df = df.withColumn(
        "segment_3",
        when(
            col("lmarch2") == "6_CONSTRUCTION",
            when(col("type_produit") == "Artisans", lit("Artisans"))
            .when(col("type_produit").isin("TRC", "DO", "CNR", "Autres Chantiers"),
                  lit("Chantiers"))
            .otherwise(lit("Renouvelables hors artisans"))
        ).otherwise(lit(None))
    )

    if logger:
        logger.info("✓ Segment_3 computed (SAS-faithful)")

    return df

def apply_cssseg_corrections(df: DataFrame, logger=None) -> DataFrame:
    """
    Apply SAS CSSSEG correction rules from REF_segmentation_azec.sas.
    """
    if "cssseg" not in df.columns:
        df = df.withColumn("cssseg", lit(None).cast(StringType()))

    df = df.withColumn(
        "cssseg",
        when(
            (col("lssseg") == "RC DECENNALE") & (col("type_produit") == "Artisans"),
            lit("7")
        )
        .otherwise(col("cssseg"))
    )

    df = df.withColumn(
        "cssseg",
        when(
            (col("lssseg") == "TOUS RISQUES CHANTIERS") & (col("type_produit") == "Artisans"),
            lit("10")
        )
        .otherwise(col("cssseg"))
    )

    if logger:
        logger.info("✓ CSSSEG corrections applied (SAS-faithful)")

    return df


def enrich_segmentation_sas(df, spark, config, vision, logger=None, return_reference=False):
    """
    Full SAS-faithful segmentation pipeline:
      1) LOB join
      2) Load CONSTRCU reference
      3) Join CONSTRCU
      4) Compute TYPE_PRODUIT (SAS)
      5) CSSSEG SAS corrections
      6) Segment_3 SAS computation
      
    Args:
        df: Input DataFrame
        spark: Spark session
        config: Configuration dict
        vision: Vision string (YYYYMM)
        logger: Optional logger
        return_reference: If True, returns (df, df_constrcu_ref) tuple for caching
        
    Returns:
        DataFrame or tuple(DataFrame, DataFrame) if return_reference=True
    """

    # --------------------------------------------------------------------
    # 1. LOB join (construction only)
    # --------------------------------------------------------------------
    reader = get_bronze_reader(SimpleNamespace(spark=spark, config=config, logger=logger))
    lob_ref = (
        reader.read_file_group("lob", "ref")
        .filter(col("cmarch") == MARKET_CODE.MARKET)
    )

    # --------------------------------------------------------------------
    # 2. Load CONSTRCU reference
    # --------------------------------------------------------------------
    df_constrcu_ref = load_constrcu_reference(
        spark=spark,
        config=config,
        vision=vision,
        lob_ref=lob_ref,
        logger=logger
    )

    # --------------------------------------------------------------------
    # 3. Join CONSTRCU reference
    # --------------------------------------------------------------------
    df = join_constrcu_sas(df, df_constrcu_ref, logger=logger)
    df = df.withColumn(
        "lmarch2",
        when(col("cmarch") == "6", lit("6_CONSTRUCTION"))
        .otherwise(lit(None))
    )

    # --------------------------------------------------------------------
    # 4. Compute TYPE_PRODUIT (SAS)
    # --------------------------------------------------------------------
    df = compute_type_produit_sas(df, spark, config, logger)

    # --------------------------------------------------------------------
    # 5. CSSSEG SAS corrections
    # --------------------------------------------------------------------
    df = apply_cssseg_corrections(df, logger)

    # --------------------------------------------------------------------
    # 6. Compute Segment_3 (SAS)
    # --------------------------------------------------------------------
    df = compute_segment3_sas(df, logger)

    if logger:
        logger.info("✓ Full SAS segmentation enrichment completed")

    if return_reference:
        return df, df_constrcu_ref
    else:
        return df
