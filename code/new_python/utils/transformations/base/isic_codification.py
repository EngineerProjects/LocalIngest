# -*- coding: utf-8 -*-
"""
ISIC Codification for Construction Data Pipeline.

Implements the complete ISIC code assignment logic from CODIFICATION_ISIC_CONSTRUCTION.sas.
Assigns ISIC codes and HAZARD_GRADES based on multiple fallback strategies.

Order of precedence (SAS-faithful):
  1) Contracts with activity (CDNATP='R') via mapping ACT
  2) Construction-site destination (CDNATP='C') via DESTI parsing + mapping CHT
  3) Fallbacks via NAF:
        NAF08_PTF > NAF03_PTF > NAF03_CLI > NAF08_CLI
  4) SUI (tracking) if still empty
  5) Hazard grades mapping from ISIC reference
"""

from typing import Dict, Optional, Any
from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql.functions import (  # type: ignore
    col, when, lit, upper, trim,
    least, greatest, coalesce
)

from types import SimpleNamespace

# Import your existing helper
from utils.processor_helpers import get_bronze_reader

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
ISIC_CORRECTIONS = {
    "22000": "022000", " 22000": "022000",
    "24021": "024000", " 24021": "024000",
    "242025": "242005",
    "329020": "329000",
    "731024": "731000",
    "81020": "081000", " 81020": "081000",
    "81023": "081000", " 81023": "081000",
    "981020": "981000",
}


def assign_isic_codes(
    df: DataFrame,
    spark,
    config,
    vision: str,
    logger: Optional[Any] = None
) -> DataFrame:
    """
    Full SAS ISIC codification pipeline orchestrator.
    Reproduces CODIFICATION_ISIC_CONSTRUCTION.sas end-to-end.

    Steps:
      1. Load reference tables (SUI, ACT, CHT, NAF03, NAF08, HAZARD)
      2. Apply SAS join order:
            §3.1 join_isic_sui
            §3.3 map_naf_to_isic                  (cascade NAF -> ISIC)
            §3.4 join_isic_const_act              (ACT contracts, CDNATP='R')
            §3.5 compute_destination_isic         (DSTCSC parsing)
            §3.6 join_isic_const_cht              (CHT contracts, CDNATP='C')
      3. Finalize ISIC codes (overwrite rules)
      4. Join hazard grades
      5. Drop temporary columns
    """

    if logger:
        logger.info(f"[ISIC] Starting full ISIC codification pipeline for vision {vision}")

    # ============================================================
    # LOAD REFERENCE TABLES (SAS §1)
    # ============================================================
    if logger: logger.debug("[ISIC] Loading reference tables...")

    df_sui    = load_sui_table(spark, config, vision, logger)
    df_act    = load_mapping_const_act(spark, config, vision, logger)
    df_cht    = load_mapping_const_cht(spark, config, vision, logger)
    df_naf03  = load_mapping_naf2003(spark, config, vision, logger)
    df_naf08  = load_mapping_naf2008(spark, config, vision, logger)
    df_hazard = load_hazard_grades(spark, config, vision, logger)

    # ============================================================
    # APPLY SAS JOIN ORDER (3.1 → 3.6)
    # ============================================================

    # --- 3.1: SUI join
    if logger: logger.debug("[ISIC] Step 3.1: Joining SUI reference")
    df = join_isic_sui(df, df_sui)

    # --- 3.3: NAF cascade (PTF NAF08 → PTF NAF03 → CLI NAF03 → CLI NAF08)
    if logger: logger.debug("[ISIC] Step 3.3: Mapping NAF -> ISIC cascade")
    df = map_naf_to_isic(df, df_naf03, df_naf08)

    # --- 3.4: ACT mapping (CDNATP='R')
    if logger: logger.debug("[ISIC] Step 3.4: Joining CONST_ACT (CDNATP='R')")
    df = join_isic_const_act(df, df_act)

    # --- 3.5: DESTI_ISIC parsing using DSTCSC
    if logger: logger.debug("[ISIC] Step 3.5: Computing DESTI_ISIC from DSTCSC patterns")
    df = compute_destination_isic(df)

    # --- 3.6: CHT mapping (CDNATP='C')
    if logger: logger.debug("[ISIC] Step 3.6: Joining CONST_CHT (CDNATP='C')")
    df = join_isic_const_cht(df, df_cht)

    # ============================================================
    # FINALIZATION (SAS §4)
    # ============================================================
    if logger: logger.debug("[ISIC] Step 4: Finalizing ISIC codes")
    df = finalize_isic_codes(df)

    # ============================================================
    # HAZARD GRADES JOIN (SAS §5)
    # ============================================================
    if logger: logger.debug("[ISIC] Step 5: Joining hazard grades")
    df = join_isic_hazard_grades(df, df_hazard)

    # ============================================================
    # CLEANUP (drop temp columns)
    # ============================================================
    if logger: logger.debug("[ISIC] Dropping temporary ISIC columns")
    df = drop_isic_temp_columns(df)

    if logger:
        logger.info("[ISIC] ISIC codification pipeline completed successfully")

    return df

def join_isic_sui(df: DataFrame, df_sui: DataFrame) -> DataFrame:
    """
    Join ISIC/NAF information coming from SUI (Suivi des Engagements).
    Strict SAS-compliant logic for Section 3.1 of CODIFICATION_ISIC_CONSTRUCTION.sas.

    SAS Rules:
    - LEFT JOIN on (POLICE = NOPOL AND PRODUIT = CDPROD)
    - Add:
        CDNAF2008_TEMP     ← cdnaf08 from SUI
        ISIC_CODE_SUI_TEMP ← cdisic from SUI
    - Apply only for construction business (CMARCH = '6')
    - Do NOT overwrite the final ISIC fields here. Only store TEMP values.
    """

    # If SUI is unavailable, simply add NULL TEMP columns (SAS equivalent of empty join)
    if df_sui is None:
        return (
            df
            .withColumn("cdnaf2008_temp", lit(None).cast("string"))
            .withColumn("isic_code_sui_temp", lit(None).cast("string"))
        )

    # Aliases
    left = df.alias("l")
    right = df_sui.alias("r")

    # Detect correct key mapping
    if "nopol" in df.columns:
        key_left_pol = col("l.nopol")
    elif "police" in df.columns:
        key_left_pol = col("l.police")
    else:
        raise Exception("join_isic_sui: aucune colonne nopol/police trouvée")

    if "cdprod" in df.columns:
        key_left_prod = col("l.cdprod")
    elif "produit" in df.columns:
        key_left_prod = col("l.produit")
    else:
        raise Exception("join_isic_sui: aucune colonne cdprod/produit trouvée")

    # Base join condition
    join_cond = (
        key_left_pol == col("r.nopol")
    ) & (
        key_left_prod == col("r.cdprod")
    )

    # Join and immediately SELECT to avoid column duplication (SAS: SELECT t1.*, t2.col)
    out = (
        left
        .join(right, join_cond, how="left")
        .select(
            "l.*",  # Preserve ALL left columns (SAS t1.*)
            when(
                (col("l.cmarch") == "6") & col("r.cdnaf08").isNotNull(),
                col("r.cdnaf08")
            ).otherwise(lit(None).cast("string")).alias("cdnaf2008_temp"),
            when(
                (col("l.cmarch") == "6") & col("r.cdisic").isNotNull(),
                col("r.cdisic")
            ).otherwise(lit(None).cast("string")).alias("isic_code_sui_temp")
        )
    )

    return out

def map_naf_to_isic(
    df: DataFrame,
    df_naf03: DataFrame,
    df_naf08: DataFrame
) -> DataFrame:
    """
    SAS Section 3.3 — NAF→ISIC cascade with strict priority and origin tags.

    Joins (LEFT) with cmarch='6' in ON clause:
      - PTF NAF03:   ref_naf03.isic_code  on df.cdnaf
      - PTF NAF08:   ref_naf08.isic_code  on df.cdnaf2008_temp
      - CLI NAF03:   ref_naf03.isic_code  on df.cdnaf03_cli
      - CLI NAF08:   ref_naf08.isic_code  on df.cdnaf08_w6

    Produces TEMP columns only:
      - isic_code_temp
      - origine_isic_temp

    Notes:
      - Keeps all left rows (left join)
      - Conditions apply only for construction (cmarch='6'), as in SAS
      - Null-safe; if refs are missing, falls back to NULL/empty strings appropriately
    """

    # Ensure key columns exist on left; if not, add them as NULL (SAS keeps blanks; we use NULL)
    needed_left = ["cdnaf", "cdnaf2008_temp", "cdnaf03_cli", "cdnaf08_w6", "cmarch"]
    cur = df
    for c in needed_left:
        if c not in cur.columns:
            cur = cur.withColumn(c, lit(None).cast("string"))

    # Prepare alias
    left = cur.alias("l")

    # 1) PTF NAF03 (t2)
    if df_naf03 is not None and set(["cdnaf_2003", "isic_code"]).issubset(set(df_naf03.columns)):
        t2 = df_naf03.select(
            col("cdnaf_2003").alias("_ptf_naf03_key"),
            col("isic_code").alias("_ptf_naf03_isic")
        ).dropDuplicates(["_ptf_naf03_key"])
        left = (
            left
            .join(
                t2,
                (col("l.cmarch") == "6") & (col("l.cdnaf") == col("_ptf_naf03_key")),
                how="left"
            )
        )
    else:
        # Columns not available → add NULL placeholders to keep expression simple
        left = left.withColumn("_ptf_naf03_isic", lit(None).cast("string"))

    # 2) PTF NAF08 (t3)
    if df_naf08 is not None and set(["cdnaf_2008", "isic_code"]).issubset(set(df_naf08.columns)):
        t3 = df_naf08.select(
            col("cdnaf_2008").alias("_ptf_naf08_key"),
            col("isic_code").alias("_ptf_naf08_isic")
        ).dropDuplicates(["_ptf_naf08_key"])
        left = (
            left
            .join(
                t3,
                (col("l.cmarch") == "6") & (col("l.cdnaf2008_temp") == col("_ptf_naf08_key")),
                how="left"
            )
        )
    else:
        left = left.withColumn("_ptf_naf08_isic", lit(None).cast("string"))

    # 3) CLI NAF03 (t4)
    if df_naf03 is not None and set(["cdnaf_2003", "isic_code"]).issubset(set(df_naf03.columns)):
        t4 = df_naf03.select(
            col("cdnaf_2003").alias("_cli_naf03_key"),
            col("isic_code").alias("_cli_naf03_isic")
        ).dropDuplicates(["_cli_naf03_key"])
        left = (
            left
            .join(
                t4,
                (col("l.cmarch") == "6") & (col("l.cdnaf03_cli") == col("_cli_naf03_key")),
                how="left"
            )
        )
    else:
        left = left.withColumn("_cli_naf03_isic", lit(None).cast("string"))

    # 4) CLI NAF08 (t5)
    if df_naf08 is not None and set(["cdnaf_2008", "isic_code"]).issubset(set(df_naf08.columns)):
        t5 = df_naf08.select(
            col("cdnaf_2008").alias("_cli_naf08_key"),
            col("isic_code").alias("_cli_naf08_isic")
        ).dropDuplicates(["_cli_naf08_key"])
        left = (
            left
            .join(
                t5,
                (col("l.cmarch") == "6") & (col("l.cdnaf08_w6") == col("_cli_naf08_key")),
                how="left"
            )
        )
    else:
        left = left.withColumn("_cli_naf08_isic", lit(None).cast("string"))

    # Build ISIC_CODE_TEMP with SAS priority:
    #   PTF NAF08 (t3) → PTF NAF03 (t2) → CLI NAF03 (t4) → CLI NAF08 (t5) → "" (SAS)
    isic_temp = when(col("_ptf_naf08_isic").isNotNull(), col("_ptf_naf08_isic")) \
        .otherwise(
            when(col("_ptf_naf03_isic").isNotNull(), col("_ptf_naf03_isic"))
            .otherwise(
                when(col("_cli_naf03_isic").isNotNull(), col("_cli_naf03_isic"))
                .otherwise(
                    when(col("_cli_naf08_isic").isNotNull(), col("_cli_naf08_isic"))
                    .otherwise(lit(None).cast("string"))  # SAS uses "", we prefer NULL to align with Spark NULL semantics
                )
            )
        )

    # Build ORIGINE_ISIC_TEMP consistently with the same cascade
    origine_temp = when(col("_ptf_naf08_isic").isNotNull(), lit("NATIF")) \
        .otherwise(
            when(col("_ptf_naf03_isic").isNotNull(), lit("NAF03"))
            .otherwise(
                when(col("_cli_naf03_isic").isNotNull(), lit("CLI03"))
                .otherwise(
                    when(col("_cli_naf08_isic").isNotNull(), lit("CLI08"))
                    .otherwise(lit(""))
                )
            )
        )

    # SAS pattern: SELECT t1.*, CASE...END AS ISIC_CODE_TEMP, CASE...END AS ORIGINE_ISIC_TEMP
    # Use "l.*" to preserve ALL original columns from left DataFrame
    out = (
        left
        .select(
            "l.*",  # ALL columns from original DataFrame
            isic_temp.alias("isic_code_temp"),
            origine_temp.alias("origine_isic_temp")
        )
    )

    return out

def join_isic_const_act(df: DataFrame, df_act: DataFrame) -> DataFrame:
    """
    SAS Section 3.4 — Activity contracts (CDNATP='R') mapping.
    Adds *_CONST_R columns using a LEFT JOIN on ACTPRIN with cmarch='6' and cdnatp='R' in the ON clause.

    Output columns created (matching SAS):
      - cdnaf08_const_r
      - cdnaf03_const_r
      - cdtre_const_r
      - cdisic_const_r

    Behavior:
      - If mapping is missing/unavailable, creates the columns as NULL (SAS-compatible).
      - Only applies to construction (cmarch='6') and activity contracts (cdnatp='R').
      - Does NOT overwrite any final ISIC fields here (finalization happens later).
    """
    # Ensure required left columns exist (Spark NULL behaves like SAS blanks for our purpose)
    needed_left = ["actprin", "cmarch", "cdnatp"]
    cur = df
    for c in needed_left:
        if c not in cur.columns:
            cur = cur.withColumn(c, lit(None).cast("string"))

    # If reference mapping is unavailable or incomplete, add NULL columns and return
    required_right = {"actprin", "cdnaf08", "cdnaf03", "cdtre", "cdisic"}
    if df_act is None or not required_right.issubset(set(df_act.columns)):
        out = cur
        if "cdnaf08_const_r" not in out.columns:
            out = out.withColumn("cdnaf08_const_r", lit(None).cast("string"))
        if "cdnaf03_const_r" not in out.columns:
            out = out.withColumn("cdnaf03_const_r", lit(None).cast("string"))
        if "cdtre_const_r" not in out.columns:
            out = out.withColumn("cdtre_const_r", lit(None).cast("string"))
        if "cdisic_const_r" not in out.columns:
            out = out.withColumn("cdisic_const_r", lit(None).cast("string"))
        return out

    # Build join with SAS ON conditions (cmarch='6' and cdnatp='R')
    left = cur.alias("l")
    right = (
        df_act
        .select("actprin", "cdnaf08", "cdnaf03", "cdtre", "cdisic")
        .dropDuplicates(["actprin"])
        .alias("r")
    )

    join_cond = (
        (col("l.actprin") == col("r.actprin")) &
        (col("l.cmarch") == lit("6")) &
        (col("l.cdnatp") == lit("R"))
    )

    joined = left.join(right, join_cond, how="left")

    # Use explicit SELECT to avoid column duplication (SAS: SELECT t1.*, t2.col AS col_r)
    out = (
        joined
        .select(
            "l.*",  # Preserve ALL left columns
            when(col("r.cdnaf08").isNotNull(), col("r.cdnaf08")).otherwise(lit(None).cast("string")).alias("cdnaf08_const_r"),
            when(col("r.cdnaf03").isNotNull(), col("r.cdnaf03")).otherwise(lit(None).cast("string")).alias("cdnaf03_const_r"),
            when(col("r.cdtre").isNotNull(), col("r.cdtre")).otherwise(lit(None).cast("string")).alias("cdtre_const_r"),
            when(col("r.cdisic").isNotNull(), col("r.cdisic")).otherwise(lit(None).cast("string")).alias("cdisic_const_r")
        )
    )

    return out

def compute_destination_isic(df: DataFrame) -> DataFrame:
    """
    SAS Section 3.5 — DESTI_ISIC assignment for construction site (chantier) contracts.

    Rules (strict SAS):
      - Apply only when cmarch='6' and cdnatp='C'
      - cdprod='01059' -> DESTI_ISIC='VENTE'
      - For cdprod in ('00548','01071') with an already available ISIC (from prior cascade),
        keep existing ISIC: do not force a DESTI_ISIC value (leave null)
      - Otherwise, parse DSTCSC to assign one of:
        RESIDENTIEL, MAISON, INDUSTRIE, BUREAU, INDUSTRIE_LIGHT, COMMERCE,
        HOPITAL, VOIRIE, AUTRES_GC, AUTRES_BAT, PHOTOV, NON_RESIDENTIEL, etc.
      - If ISIC_CODE_TEMP is empty AND DESTI_ISIC is empty -> default to 'AUTRES_BAT'

    Inputs expected on `df`:
      - cmarch, cdnatp, cdprod, dstcsc
      - isic_code_temp (from prior NAF cascade), isic_code_sui_temp (optional)

    Output:
      - Adds/overwrites 'desti_isic' (string)
    """
    # Ensure expected columns exist (Spark null-safe)
    cur = df
    required = ["cmarch", "cdnatp", "cdprod", "dstcsc", "isic_code_temp", "isic_code_sui_temp"]
    for c in required:
        if c not in cur.columns:
            cur = cur.withColumn(c, lit(None).cast("string"))

    is_construction_chantier = (col("cmarch") == "6") & (col("cdnatp") == "C")
    has_existing_isic = col("isic_code_temp").isNotNull() | col("isic_code_sui_temp").isNotNull()
    dst = upper(col("dstcsc"))

    # Pattern cascade per SAS (PRXMATCH equivalents)
    pattern_based = (
        when(dst.rlike("(COLL)"),         lit("RESIDENTIEL"))
        .when(dst.rlike("(MAISON)"),      lit("MAISON"))
        .when(dst.rlike("(LOGE)"),        lit("RESIDENTIEL"))
        .when(dst.rlike("(APPA)"),        lit("RESIDENTIEL"))
        .when(dst.rlike("(HABIT)"),       lit("RESIDENTIEL"))
        .when(dst.rlike("(INDUS)"),       lit("INDUSTRIE"))
        .when(dst.rlike("(BUREAU)"),      lit("BUREAU"))
        .when(dst.rlike("(STOCK)"),       lit("INDUSTRIE_LIGHT"))
        .when(dst.rlike("(SUPPORT)"),     lit("INDUSTRIE_LIGHT"))
        .when(dst.rlike("(COMMER)"),      lit("COMMERCE"))
        .when(dst.rlike("(GARAGE)"),      lit("INDUSTRIE"))
        .when(dst.rlike("(TELECOM)"),     lit("INDUSTRIE"))
        .when(dst.rlike("(R\\+)"),        lit("RESIDENTIEL"))
        .when(dst.rlike("(HOTEL)"),       lit("BUREAU"))
        .when(dst.rlike("(TOURIS)"),      lit("BUREAU"))
        .when(dst.rlike("(VAC)"),         lit("BUREAU"))
        .when(dst.rlike("(LOIS)"),        lit("BUREAU"))
        .when(dst.rlike("(AGRIC)"),       lit("INDUSTRIE_LIGHT"))
        .when(dst.rlike("(CLINI )"),      lit("HOPITAL"))
        .when(dst.rlike("(HOP)"),         lit("HOPITAL"))
        .when(dst.rlike("(HOSP)"),        lit("HOPITAL"))
        .when(dst.rlike("(RESID)"),       lit("RESIDENTIEL"))
        .when(dst.rlike("(CIAL)"),        lit("COMMERCE"))
        .when(dst.rlike("(SPOR)"),        lit("COMMERCE"))
        .when(dst.rlike("(ECOL)"),        lit("BUREAU"))
        .when(dst.rlike("(ENSEI)"),       lit("BUREAU"))
        .when(dst.rlike("(CHIR)"),        lit("HOPITAL"))
        .when(dst.rlike("(BAT)"),         lit("RESIDENTIEL"))
        .when(dst.rlike("(INDIV)"),       lit("MAISON"))
        .when(dst.rlike("(VRD)"),         lit("VOIRIE"))
        .when(dst.rlike("(NON SOUMIS)"),  lit("AUTRES_GC"))
        .when(dst.rlike("(SOUMIS)"),      lit("AUTRES_BAT"))
        .when(dst.rlike("(PHOTOV)"),      lit("PHOTOV"))
        .when(dst.rlike("(PARK)"),        lit("VOIRIE"))
        .when(dst.rlike("(STATIONNEMENT)"), lit("VOIRIE"))
        .when(dst.rlike("(MANEGE)"),      lit("NON_RESIDENTIEL"))
        .when(dst.rlike("(MED)"),         lit("BUREAU"))
        .when(dst.rlike("(BANC)"),        lit("BUREAU"))
        .when(dst.rlike("(BANQ)"),        lit("BUREAU"))
        .when(dst.rlike("(AGENCE)"),      lit("BUREAU"))
        .when(dst.rlike("(CRECHE)"),      lit("BUREAU"))
        .when(dst.rlike("(EHPAD)"),       lit("RESIDENTIEL"))
        .when(dst.rlike("(ENTREPOT)"),    lit("INDUSTRIE_LIGHT"))
        .when(dst.rlike("(HANGAR)"),      lit("INDUSTRIE_LIGHT"))
        .when(dst.rlike("(AQUAT)"),       lit("COMMERCE"))
        .when(dst.rlike("(LGTS)"),        lit("RESIDENTIEL"))
        .when(dst.rlike("(LOGTS)"),       lit("RESIDENTIEL"))
        .when(dst.rlike("(LOGS)"),        lit("RESIDENTIEL"))
    )

    # Numeric code mapping per SAS (exact groupings)
    num_code_based = (
        when(col("dstcsc").isin("01", "02", "03", "03+22", "04", "06", "08", "1", "2", "3", "4", "6", "8"),
             lit("RESIDENTIEL"))
        .when(col("dstcsc") == "22", lit("MAISON"))
        .when(col("dstcsc").isin("27", "99"), lit("AUTRES_GC"))
        .when(col("dstcsc").isin("05", "07", "09", "10", "13", "14", "16", "5", "7", "9"),
             lit("BUREAU"))
        .when(col("dstcsc").isin("17", "23"), lit("COMMERCE"))
        .when(col("dstcsc") == "15", lit("HOPITAL"))
        .when(col("dstcsc") == "25", lit("STADE"))
        .when(col("dstcsc").isin("12", "18", "19"), lit("INDUSTRIE"))
        .when(col("dstcsc").isin("11", "20", "21"), lit("INDUSTRIE_LIGHT"))
        .when(col("dstcsc").isin("24", "26", "28"), lit("VOIRIE"))
    )

    # Start with a NULL desti_isic
    cur = cur.withColumn("desti_isic", lit(None).cast("string"))

    # Special product 01059: set VENTE (only for construction chantier)
    cur = cur.withColumn(
        "desti_isic",
        when(is_construction_chantier & (col("cdprod") == "01059"), lit("VENTE"))
        .otherwise(col("desti_isic"))
    )

    # For other chantier contracts (excluding 01059, and excluding 00548/01071 with existing ISIC):
    eligible_for_parsing = (
        is_construction_chantier &
        (col("cdprod") != "01059") &
        ~( (col("cdprod").isin("00548", "01071")) & has_existing_isic )
    )

    # Apply pattern parsing first
    cur = cur.withColumn(
        "desti_isic",
        when(
            eligible_for_parsing & col("desti_isic").isNull() & col("dstcsc").isNotNull(),
            pattern_based
        ).otherwise(col("desti_isic"))
    )

    # Then apply numeric code fallback (still only if not set)
    cur = cur.withColumn(
        "desti_isic",
        when(
            eligible_for_parsing & col("desti_isic").isNull() & col("dstcsc").isNotNull(),
            num_code_based
        ).otherwise(col("desti_isic"))
    )

    # Final default: if no ISIC found and desti_isic still empty -> 'AUTRES_BAT'
    # (SAS: if ISIC_CODE_TEMP="" and DESTI_ISIC="" then "AUTRES_BAT")
    cur = cur.withColumn(
        "desti_isic",
        when(
            is_construction_chantier &
            col("desti_isic").isNull() &
            col("isic_code_temp").isNull(),
            lit("AUTRES_BAT")
        ).otherwise(col("desti_isic"))
    )

    return cur

def join_isic_const_cht(df: DataFrame, df_cht: DataFrame) -> DataFrame:
    """
    SAS Section 3.6 — Construction site (CHT) ISIC mapping.
    
    Applies mapping for chantier contracts using DESTI_ISIC categories.

    SAS logic replicated exactly:
      LEFT JOIN mapping_isic_const_cht
        ON (l.desti_isic = r.desti_isic AND l.cmarch='6' AND l.cdnatp='C')

    Produces the following TEMP-like columns:
      - cdnaf08_const_c
      - cdnaf03_const_c
      - cdtre_const_c
      - cdisic_const_c

    These columns are NOT final. Final overwrite happens later (Section 4).
    """

    # Ensure required columns exist on the left side
    needed_left = ["desti_isic", "cmarch", "cdnatp"]
    cur = df
    for c in needed_left:
        if c not in cur.columns:
            cur = cur.withColumn(c, lit(None).cast("string"))

    # If mapping table is missing, create NULL CONST_C columns and return
    required_right = {"desti_isic", "cdnaf08", "cdnaf03", "cdtre", "cdisic"}
    if df_cht is None or not required_right.issubset(set(df_cht.columns)):
        out = cur
        for cname in ["cdnaf08_const_c", "cdnaf03_const_c", "cdtre_const_c", "cdisic_const_c"]:
            if cname not in out.columns:
                out = out.withColumn(cname, lit(None).cast("string"))
        return out

    # Prepare reference table
    right = (
        df_cht
        .select(
            col("desti_isic"),
            col("cdnaf08").alias("r_cdnaf08"),
            col("cdnaf03").alias("r_cdnaf03"),
            col("cdtre").alias("r_cdtre"),
            col("cdisic").alias("r_cdisic")
        )
        .dropDuplicates(["desti_isic"])
        .alias("r")
    )

    left = cur.alias("l")

    # The exact SAS ON condition:
    join_cond = (
        (col("l.desti_isic") == col("r.desti_isic")) &
        (col("l.cmarch") == lit("6")) &
        (col("l.cdnatp") == lit("C"))
    )

    joined = left.join(right, join_cond, how="left")

    # Use explicit SELECT to avoid column duplication (SAS: SELECT t1.*, t2.col AS col_c)
    out = (
        joined
        .select(
            "l.*",  # Preserve ALL left columns
            when(col("r_cdnaf08").isNotNull(), col("r_cdnaf08")).otherwise(lit(None).cast("string")).alias("cdnaf08_const_c"),
            when(col("r_cdnaf03").isNotNull(), col("r_cdnaf03")).otherwise(lit(None).cast("string")).alias("cdnaf03_const_c"),
            when(col("r_cdtre").isNotNull(), col("r_cdtre")).otherwise(lit(None).cast("string")).alias("cdtre_const_c"),
            when(col("r_cdisic").isNotNull(), col("r_cdisic")).otherwise(lit(None).cast("string")).alias("cdisic_const_c")
        )
    )

    return out


def finalize_isic_codes(df: DataFrame) -> DataFrame:
    """
    SAS Section 4 — Finalization of ISIC codes.
    
    Applies final overwrite rules based on contract type (ACT/CHT)
    using *_CONST_R and *_CONST_C columns.
    
    Produces FINAL columns:
        cdnaf2008
        cdnaf
        cdtre
        destinat_isic
        isic_code
        origine_isic
        isic_code_sui

    Drops TEMP columns afterwards.
    """

    cur = df

    # Ensure required columns exist
    temp_cols = [
        "cdnaf2008_temp", "cdnaf03_temp", "cdtre_temp",
        "isic_code_temp", "origine_isic_temp", "desti_isic_temp",
        "isic_code_sui_temp"
    ]
    for c in temp_cols:
        if c not in cur.columns:
            cur = cur.withColumn(c, lit(None).cast("string"))

    # Helper: does the CONST_R/C exist?
    for c in [
        "cdnaf08_const_r", "cdnaf03_const_r", "cdtre_const_r", "cdisic_const_r",
        "cdnaf08_const_c", "cdnaf03_const_c", "cdtre_const_c", "cdisic_const_c"
    ]:
        if c not in cur.columns:
            cur = cur.withColumn(c, lit(None).cast("string"))
    
    # CRITICAL FIX: Initialize _temp columns from existing columns (SAS modifies in-place, we use temp versions)
    # SAS directly modifies CDNAF, CDTRE, CDNAF2008, etc. We create _temp versions first
    if "cdnaf" in cur.columns:
        cur = cur.withColumn("cdnaf_temp", col("cdnaf"))
    else:
        cur = cur.withColumn("cdnaf_temp", lit(None).cast("string"))
    
    if "cdtre" in cur.columns:
        cur = cur.withColumn("cdtre_temp", col("cdtre"))
    else:
        cur = cur.withColumn("cdtre_temp", lit(None).cast("string"))

    # ==============================
    # Activity contracts (CDNATP='R')
    # ==============================
    cond_act = (
        (col("cmarch") == "6") &
        (col("cdnatp") == "R") &
        col("cdisic_const_r").isNotNull()
    )

    cur = (
        cur
        .withColumn(
            "cdnaf2008_temp",
            when(cond_act, col("cdnaf08_const_r")).otherwise(col("cdnaf2008_temp"))
        )
        .withColumn(
            "cdnaf_temp",   # Equivalent to SAS’s CDNAF assignment
            when(cond_act, col("cdnaf03_const_r")).otherwise(col("cdnaf_temp"))
        )
        .withColumn(
            "cdtre_temp",
            when(cond_act, col("cdtre_const_r")).otherwise(col("cdtre_temp"))
        )
        .withColumn(
            "desti_isic_temp",
            when(cond_act, lit(None).cast("string")).otherwise(col("desti_isic_temp"))
        )
        .withColumn(
            "isic_code_temp",
            when(cond_act, col("cdisic_const_r")).otherwise(col("isic_code_temp"))
        )
        .withColumn(
            "origine_isic_temp",
            when(cond_act, lit("NCLAT")).otherwise(col("origine_isic_temp"))
        )
    )

    # ==============================
    # Construction-site contracts (CDNATP='C')
    # ==============================
    cond_cht = (
        (col("cmarch") == "6") &
        (col("cdnatp") == "C") &
        col("cdisic_const_c").isNotNull()
    )

    cur = (
        cur
        .withColumn(
            "cdnaf2008_temp",
            when(cond_cht, col("cdnaf08_const_c")).otherwise(col("cdnaf2008_temp"))
        )
        .withColumn(
            "cdnaf_temp",
            when(cond_cht, col("cdnaf03_const_c")).otherwise(col("cdnaf_temp"))
        )
        .withColumn(
            "cdtre_temp",
            when(cond_cht, col("cdtre_const_c")).otherwise(col("cdtre_temp"))
        )
        .withColumn(
            "desti_isic_temp",
            when(cond_cht, col("desti_isic")).otherwise(col("desti_isic_temp"))
        )
        .withColumn(
            "isic_code_temp",
            when(cond_cht, col("cdisic_const_c")).otherwise(col("isic_code_temp"))
        )
        .withColumn(
            "origine_isic_temp",
            when(cond_cht, lit("DESTI")).otherwise(col("origine_isic_temp"))
        )
    )

    # Build final output fields exactly as SAS does
    cur = (
        cur
        .withColumn("cdnaf2008", col("cdnaf2008_temp"))
        .withColumn("cdnaf", col("cdnaf_temp"))
        .withColumn("cdtre", col("cdtre_temp"))
        .withColumn("isic_code", col("isic_code_temp"))
        .withColumn("origine_isic", col("origine_isic_temp"))
        .withColumn("destinat_isic", col("desti_isic_temp"))
        .withColumn("isic_code_sui", col("isic_code_sui_temp"))
    )

    # Drop TEMP columns (SAS removes these)
    drop_list = [
        "cdnaf2008_temp", "cdnaf_temp", "cdtre_temp",
        "isic_code_temp", "origine_isic_temp", "desti_isic_temp",
        "isic_code_sui_temp",

        "cdnaf08_const_r", "cdnaf03_const_r", "cdtre_const_r", "cdisic_const_r",
        "cdnaf08_const_c", "cdnaf03_const_c", "cdtre_const_c", "cdisic_const_c"
    ]

    for c in drop_list:
        if c in cur.columns:
            cur = cur.drop(c)

    return cur

def join_isic_hazard_grades(df: DataFrame, df_hazard: DataFrame) -> DataFrame:
    """
    SAS Section 5 — Join hazard grades based on final ISIC code.
    
    Joins hazard grades using:
        LEFT JOIN df_hazard ON df_hazard.isic_code = df.isic_code
    
    Produces TEMP columns as SAS does:
        hazard_grades_fire_temp
        hazard_grades_bi_temp
        hazard_grades_rca_temp
        hazard_grades_rce_temp
        hazard_grades_trc_temp
        hazard_grades_rcd_temp
        hazard_grades_do_temp

    Notes:
      - If hazard table missing, create all hazard TEMP columns as NULL.
      - Join uses FINAL isic_code (SAS uses ISIC_CODE_TEMP before finalization).
        Our pipeline finalizes ISIC before hazard join, but semantically equivalent
        because final ISIC_CODE == ISIC_CODE_TEMP after finalization.
    """

    # If hazard reference missing, create empty hazard TEMP columns
    if df_hazard is None or "isic_code" not in df_hazard.columns:
        out = df
        for cname in [
            "hazard_grades_fire_temp",
            "hazard_grades_bi_temp",
            "hazard_grades_rca_temp",
            "hazard_grades_rce_temp",
            "hazard_grades_trc_temp",
            "hazard_grades_rcd_temp",
            "hazard_grades_do_temp",
        ]:
            if cname not in out.columns:
                out = out.withColumn(cname, lit(None).cast("string"))
        return out

    # Prepare hazard reference table
    hazard_cols = [
        "isic_code",
        "hazard_grades_fire",
        "hazard_grades_bi",
        "hazard_grades_rca",
        "hazard_grades_rce",
        "hazard_grades_trc",
        "hazard_grades_rcd",
        "hazard_grades_do",
    ]

    available_cols = [c for c in hazard_cols if c in df_hazard.columns]

    right = df_hazard.select(*available_cols).alias("h")
    left = df.alias("l")

    # SAS join condition: t2.ISIC_CODE = final ISIC_CODE
    join_cond = col("l.isic_code") == col("h.isic_code")

    joined = left.join(right, join_cond, how="left")

    # Use explicit SELECT to avoid column duplication (SAS: SELECT t1.*, t2.col AS col_temp)
    out = (
        joined
        .select(
            "l.*",  # Preserve ALL left columns
            col("h.hazard_grades_fire").alias("hazard_grades_fire_temp"),
            col("h.hazard_grades_bi").alias("hazard_grades_bi_temp"),
            col("h.hazard_grades_rca").alias("hazard_grades_rca_temp"),
            col("h.hazard_grades_rce").alias("hazard_grades_rce_temp"),
            col("h.hazard_grades_trc").alias("hazard_grades_trc_temp"),
            col("h.hazard_grades_rcd").alias("hazard_grades_rcd_temp"),
            col("h.hazard_grades_do").alias("hazard_grades_do_temp")
        )
    )

    return out

def drop_isic_temp_columns(df: DataFrame) -> DataFrame:
    """
    Final SAS cleanup step — remove all temporary and intermediate ISIC columns.

    SAS drops:
      - *_CONST_R
      - *_CONST_C
      - *_TEMP
      - DESTI_ISIC (SAS keeps DESTINAT_ISIC instead)
      - any helper join columns

    We replicate the exact list from CODIFICATION_ISIC_CONSTRUCTION.sas.
    """

    to_drop = [
        # CONST R
        "cdnaf08_const_r", "cdnaf03_const_r", "cdtre_const_r", "cdisic_const_r",
        # CONST C
        "cdnaf08_const_c", "cdnaf03_const_c", "cdtre_const_c", "cdisic_const_c",

        # TEMP main fields
        "cdnaf2008_temp", "cdnaf_temp", "cdtre_temp",
        "isic_code_temp", "origine_isic_temp", "desti_isic_temp",
        "isic_code_sui_temp",

        # Hazard TEMP fields
        "hazard_grades_fire_temp",
        "hazard_grades_bi_temp",
        "hazard_grades_rca_temp",
        "hazard_grades_rce_temp",
        "hazard_grades_trc_temp",
        "hazard_grades_rcd_temp",
        "hazard_grades_do_temp",

        # DESTI_ISIC (raw chantier category — final output uses DESTINAT_ISIC)
        "desti_isic",
    ]

    cur = df
    for c in to_drop:
        if c in cur.columns:
            cur = cur.drop(c)

    return cur

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def load_sui_table(spark, config, vision: str, logger=None):
    """
    Charge la table IRD_SUIVI_ENGAGEMENTS pour la vision donnée.
    Reproduit le comportement SAS : si la vision n'existe pas, on peut fallback à 'ref'.

    Retourne un DataFrame avec :
      - nopol
      - cdprod
      - cdnaf08
      - cdisic
    """
    from utils.processor_helpers import get_bronze_reader
    from pyspark.sql.functions import col

    reader = get_bronze_reader(SimpleNamespace(spark=spark, config=config, logger=logger))

    try:
        vis_int = int(vision)
    except Exception:
        vis_int = 0

    if vis_int < 202103:
        if logger:
            logger.info(f"[ISIC] SUI disabled for vision {vision} (< 202103) — SAS parity")
        return None

    try:
        df_sui = reader.read_file_group("ird_suivi_engagements", vision)
    except Exception as e:
        if logger:
            logger.warning(f"SUI {vision} introuvable, tentative 'ref': {e}")
        try:
            df_sui = reader.read_file_group("ird_suivi_engagements", vision="ref")
        except Exception as e2:
            if logger:
                logger.error(f"SUI indisponible en 'ref' également : {e2}")
            return None

    if df_sui is None or not df_sui.columns:
        if logger:
            logger.warning("Table SUI vide ou sans colonnes")
        return None

    # Colonnes SAS nécessaires (après lowercase du BronzeReader)
    needed = []
    if "nopol" in df_sui.columns:       needed.append(col("nopol"))
    if "cdprod" in df_sui.columns:      needed.append(col("cdprod"))
    if "cdnaf08" in df_sui.columns:     needed.append(col("cdnaf08"))
    if "cdisic" in df_sui.columns:      needed.append(col("cdisic"))

    if not needed:
        if logger:
            logger.warning("SUI ne contient aucune colonne utile pour ISIC")
        return None

    return df_sui.select(*needed).dropDuplicates(["nopol", "cdprod"])


def load_mapping_const_act(spark, config, vision: str, logger=None):
    """
    Charge le mapping des contrats à activité (MAPPING_ISIC_CONST_ACT).
    Colonnes nécessaires selon SAS :
      - actprin
      - cdnaf08
      - cdnaf03
      - cdtre
      - cdisic
    """
    from utils.processor_helpers import get_bronze_reader
    from pyspark.sql.functions import col

    reader = get_bronze_reader(SimpleNamespace(spark=spark, config=config, logger=logger))

    # Essayer vision → fallback ref
    try:
        df_act = reader.read_file_group("mapping_isic_const_act", vision)
    except:
        if logger:
            logger.info("mapping_isic_const_act: fallback vision='ref'")
        try:
            df_act = reader.read_file_group("mapping_isic_const_act", vision="ref")
        except:
            return None

    if df_act is None or not df_act.columns:
        return None

    cols = {}
    for c in ["actprin", "cdnaf08", "cdnaf03", "cdtre", "cdisic"]:
        if c in df_act.columns:
            cols[c] = col(c)
        elif c.upper() in df_act.columns:
            cols[c] = col(c.upper()).alias(c)

    if "actprin" not in cols:
        return None  # impossible de joindre comme SAS

    return df_act.select(*cols.values()).dropDuplicates(["actprin"])


def load_mapping_const_cht(spark, config, vision: str, logger=None):
    """
    Charge le mapping ISIC des chantiers selon DESTI_ISIC.
    Colonnes utiles :
      - desti_isic
      - cdnaf08
      - cdnaf03
      - cdtre
      - cdisic
    """
    from utils.processor_helpers import get_bronze_reader
    from pyspark.sql.functions import col

    reader = get_bronze_reader(SimpleNamespace(spark=spark, config=config, logger=logger))

    try:
        df_cht = reader.read_file_group("mapping_isic_const_cht", vision)
    except:
        if logger:
            logger.info("mapping_isic_const_cht: fallback 'ref'")
        try:
            df_cht = reader.read_file_group("mapping_isic_const_cht", vision="ref")
        except:
            return None

    if df_cht is None or not df_cht.columns:
        return None

    cols = {}
    for c in ["desti_isic", "cdnaf08", "cdnaf03", "cdtre", "cdisic"]:
        if c in df_cht.columns:
            cols[c] = col(c)
        elif c.upper() in df_cht.columns:
            cols[c] = col(c.upper()).alias(c)

    if "desti_isic" not in cols:
        return None

    return df_cht.select(*cols.values()).dropDuplicates(["desti_isic"])


def load_mapping_naf2003(spark, config, vision: str, logger=None):
    from utils.processor_helpers import get_bronze_reader
    from pyspark.sql.functions import col

    reader = get_bronze_reader(SimpleNamespace(spark=spark, config=config, logger=logger))

    try:
        df_naf03 = reader.read_file_group("mapping_cdnaf2003_isic", vision)
    except:
        try:
            df_naf03 = reader.read_file_group("mapping_cdnaf2003_isic", vision="ref")
        except:
            return None

    if df_naf03 is None or not df_naf03.columns:
        return None

    if "cdnaf_2003" not in df_naf03.columns or "isic_code" not in df_naf03.columns:
        return None

    return df_naf03.select("cdnaf_2003", "isic_code").dropDuplicates(["cdnaf_2003"])


def load_mapping_naf2008(spark, config, vision: str, logger=None):
    from utils.processor_helpers import get_bronze_reader
    from pyspark.sql.functions import col

    reader = get_bronze_reader(SimpleNamespace(spark=spark, config=config, logger=logger))

    try:
        df_naf08 = reader.read_file_group("mapping_cdnaf2008_isic", vision)
    except:
        try:
            df_naf08 = reader.read_file_group("mapping_cdnaf2008_isic", vision="ref")
        except:
            return None

    if df_naf08 is None or not df_naf08.columns:
        return None

    if "cdnaf_2008" not in df_naf08.columns or "isic_code" not in df_naf08.columns:
        return None

    return df_naf08.select("cdnaf_2008", "isic_code").dropDuplicates(["cdnaf_2008"])


def load_hazard_grades(spark, config, vision: str, logger=None):
    """
    Charge la table des hazard grades ISIC.
    """
    from utils.processor_helpers import get_bronze_reader
    from pyspark.sql.functions import col

    reader = get_bronze_reader(SimpleNamespace(spark=spark, config=config, logger=logger))

    try:
        df = reader.read_file_group("table_isic_tre_naf", vision)
    except:
        try:
            df = reader.read_file_group("table_isic_tre_naf", "ref")
        except:
            return None

    if df is None or not df.columns:
        return None

    cols = []
    for c in df.columns:
        if c.lower() in [
            "isic_code",
            "hazard_grades_fire",
            "hazard_grades_bi",
            "hazard_grades_rca",
            "hazard_grades_rce",
            "hazard_grades_trc",
            "hazard_grades_rcd",
            "hazard_grades_do"
        ]:
            cols.append(col(c).alias(c.lower()))

    if "isic_code" not in [c._jc.toString().lower() for c in cols]:
        return None

    return df.select(*cols).dropDuplicates(["isic_code"])



def add_partenariat_berlitz_flags(df: DataFrame) -> DataFrame:
    """
    Flags Berlioz/Partenariat d'après NOINT (consolidation SAS).
    """
    if "noint" not in df.columns:
        return df.withColumn("top_berlioz", lit(0)).withColumn("top_partenariat", lit(0))

    df = df.withColumn("top_berlioz", when(col("noint") == "4A5766", lit(1)).otherwise(lit(0)))
    df = df.withColumn("top_partenariat", when(col("noint").isin(["4A6160", "4A6947", "4A6956"]), lit(1)).otherwise(lit(0)))
    return df