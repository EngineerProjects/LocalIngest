"""
ISIC Codification for Construction Data Pipeline.

Implements the complete ISIC code assignment logic from CODIFICATION_ISIC_CONSTRUCTION.sas.
Assigns ISIC codes and HAZARD_GRADES based on multiple fallback strategies.

Based on: CODIFICATION_ISIC_CONSTRUCTION.sas (300+ lines)
"""

from pyspark.sql import DataFrame # type: ignore
from pyspark.sql.functions import ( # type: ignore
    col, when, lit, upper, regexp_extract, coalesce,
    trim, length, substring
)
from typing import Dict, Optional, Any
import re


def assign_isic_codes(
    df: DataFrame,
    vision: str,
    logger: Optional[Any] = None
) -> DataFrame:
    """
    Assign ISIC codes and HAZARD_GRADES to construction portfolio data.

    Implements full ISIC codification logic with multiple fallback strategies:
    1. NAF08 from IRD_SUIVI_ENGAGEMENTS (if available)
    2. NAF03 from portfolio data
    3. NAF03 from client data
    4. NAF08 from client data
    5. Activity-based ISIC (CDNATP='R')
    6. Construction site destination ISIC (CDNATP='C')

    Args:
        df: Input DataFrame (must have cmarch, cseg, cssseg, cdprod, etc.)
        vision: Vision in YYYYMM format
        logger: Optional logger instance

    Returns:
        DataFrame with ISIC columns added:
        - cdisic: ISIC code
        - origine_isic: Origin of ISIC code
        - hazard_grades_fire: Fire risk grade
        - hazard_grades_bi: Business interruption grade
        - hazard_grades_rca: Post-delivery liability grade
        - hazard_grades_rce: Contractor liability grade
        - hazard_grades_trc: All-risk construction grade
        - hazard_grades_rcd: Ten-year liability grade
        - hazard_grades_do: Structural damage grade

    Example:
        >>> df = assign_isic_codes(df, '202509', logger)
    """
    if logger:
        logger.info("Starting ISIC codification process")

    # Initialize columns that aren't added during joins
    if "origine_isic" not in df.columns:
        df = df.withColumn("origine_isic", lit(None).cast("string"))
    if "isic_code_sui" not in df.columns:
        df = df.withColumn("isic_code_sui", lit(None).cast("string"))

    # Only process construction market (cmarch = '6')
    is_construction = col("cmarch") == "6"

    # Step 1: Assign CDNAF2008 from IRD_SUIVI_ENGAGEMENTS (if joined earlier)
    # Populate cdnaf2008 column for later MAPPING_CDNAF2008_ISIC join (SAS line 75)
    if "cdnaf08_sui" in df.columns and "cdisic_sui" in df.columns:
        df = df.withColumn(
            "cdnaf2008",
            when(is_construction & col("cdnaf08_sui").isNotNull(), col("cdnaf08_sui"))
            .otherwise(col("cdnaf2008"))
        )
        df = df.withColumn(
            "isic_code_sui",
            when(is_construction & col("cdisic_sui").isNotNull(), col("cdisic_sui"))
            .otherwise(col("isic_code_sui"))
        )

    # Step 2: Assign ISIC for activity-based contracts (CDNATP='R')
    # Uses MAPPING_ISIC_CONST_ACT (joined earlier)
    if "cdisic_const_r" in df.columns:
        df = df.withColumn(
            "cdisic",
            when(
                is_construction & (col("cdnatp") == "R") & col("cdisic_const_r").isNotNull(),
                col("cdisic_const_r")
            ).otherwise(col("cdisic"))
        )
        df = df.withColumn(
            "origine_isic",
            when(
                is_construction & (col("cdnatp") == "R") & col("cdisic_const_r").isNotNull(),
                lit("ACTPRIN")
            ).otherwise(col("origine_isic"))
        )

    # Step 3: Assign ISIC for construction site contracts (CDNATP='C')
    # Parse DSTCSC field to determine destination type
    df = _assign_destination_isic(df, is_construction, logger)

    # Step 4: Assign ISIC from construction site destination mapping
    # Uses MAPPING_ISIC_CONST_CHT (if joined earlier)
    if "cdisic_const_c" in df.columns:
        df = df.withColumn(
            "cdisic",
            when(
                is_construction &
                (col("cdnatp") == "C") &
                col("desti_isic").isNotNull() &
                col("cdisic_const_c").isNotNull(),
                col("cdisic_const_c")
            ).otherwise(col("cdisic"))
        )
        df = df.withColumn(
            "origine_isic",
            when(
                is_construction &
                (col("cdnatp") == "C") &
                col("desti_isic").isNotNull() &
                col("cdisic_const_c").isNotNull(),
                lit("DESTI_CHT")
            ).otherwise(col("origine_isic"))
        )

    # Step 5: Assign ISIC from NAF codes (fallback strategy)
    # Priority: NAF08_PTF > NAF03_PTF > NAF03_CLI > NAF08_CLI
    if "isic_from_naf08" in df.columns:
        df = df.withColumn(
            "cdisic",
            when(
                is_construction & col("cdisic").isNull() & col("isic_from_naf08").isNotNull(),
                col("isic_from_naf08")
            ).otherwise(col("cdisic"))
        )
        df = df.withColumn(
            "origine_isic",
            when(
                is_construction & col("origine_isic").isNull() & col("isic_from_naf08").isNotNull(),
                lit("NAF08")
            ).otherwise(col("origine_isic"))
        )

    if "isic_from_naf03" in df.columns:
        df = df.withColumn(
            "cdisic",
            when(
                is_construction & col("cdisic").isNull() & col("isic_from_naf03").isNotNull(),
                col("isic_from_naf03")
            ).otherwise(col("cdisic"))
        )
        df = df.withColumn(
            "origine_isic",
            when(
                is_construction & col("origine_isic").isNull() & col("isic_from_naf03").isNotNull(),
                lit("NAF03")
            ).otherwise(col("origine_isic"))
        )

    # Step 6: Use ISIC from SUI if still null
    df = df.withColumn(
        "cdisic",
        when(
            is_construction & col("cdisic").isNull() & col("isic_code_sui").isNotNull(),
            col("isic_code_sui")
        ).otherwise(col("cdisic"))
    )
    df = df.withColumn(
        "origine_isic",
        when(
            is_construction & col("origine_isic").isNull() & col("isic_code_sui").isNotNull(),
            lit("SUI")
        ).otherwise(col("origine_isic"))
    )

    # Step 7: Assign HAZARD_GRADES (if table_isic_tre_naf joined)
    df = _assign_hazard_grades(df, is_construction, logger)

    if logger:
        logger.success("ISIC codification completed")

    return df


def _assign_destination_isic(
    df: DataFrame,
    is_construction,
    logger: Optional[Any] = None
) -> DataFrame:
    """
    Assign DESTI_ISIC based on DSTCSC field pattern matching.

    Implements the complex pattern matching from CODIFICATION_ISIC_CONSTRUCTION.sas L95-170.

    Args:
        df: Input DataFrame
        is_construction: Construction market filter condition
        logger: Optional logger

    Returns:
        DataFrame with desti_isic column populated
    """
    if "dstcsc" not in df.columns:
        return df

    if logger:
        logger.debug("Assigning DESTI_ISIC from DSTCSC patterns")

    # Build cascading when conditions for pattern matching
    desti_expr = (
        when(upper(col("dstcsc")).rlike("(COLL)"), lit("RESIDENTIEL"))
        .when(upper(col("dstcsc")).rlike("(MAISON)"), lit("MAISON"))
        .when(upper(col("dstcsc")).rlike("(LOGE)"), lit("RESIDENTIEL"))
        .when(upper(col("dstcsc")).rlike("(APPA)"), lit("RESIDENTIEL"))
        .when(upper(col("dstcsc")).rlike("(HABIT)"), lit("RESIDENTIEL"))
        .when(upper(col("dstcsc")).rlike("(INDUS)"), lit("INDUSTRIE"))
        .when(upper(col("dstcsc")).rlike("(BUREAU)"), lit("BUREAU"))
        .when(upper(col("dstcsc")).rlike("(STOCK)"), lit("INDUSTRIE_LIGHT"))
        .when(upper(col("dstcsc")).rlike("(SUPPORT)"), lit("INDUSTRIE_LIGHT"))
        .when(upper(col("dstcsc")).rlike("(COMMER)"), lit("COMMERCE"))
        .when(upper(col("dstcsc")).rlike("(GARAGE)"), lit("INDUSTRIE"))
        .when(upper(col("dstcsc")).rlike("(TELECOM)"), lit("INDUSTRIE"))
        .when(upper(col("dstcsc")).rlike("(R\\+)"), lit("RESIDENTIEL"))
        .when(upper(col("dstcsc")).rlike("(HOTEL)"), lit("BUREAU"))
        .when(upper(col("dstcsc")).rlike("(TOURIS)"), lit("BUREAU"))
        .when(upper(col("dstcsc")).rlike("(VAC)"), lit("BUREAU"))
        .when(upper(col("dstcsc")).rlike("(LOIS)"), lit("BUREAU"))
        .when(upper(col("dstcsc")).rlike("(AGRIC)"), lit("INDUSTRIE_LIGHT"))
        .when(upper(col("dstcsc")).rlike("(CLINI )"), lit("HOPITAL"))
        .when(upper(col("dstcsc")).rlike("(HOP)"), lit("HOPITAL"))
        .when(upper(col("dstcsc")).rlike("(HOSP)"), lit("HOPITAL"))
        .when(upper(col("dstcsc")).rlike("(RESID)"), lit("RESIDENTIEL"))
        .when(upper(col("dstcsc")).rlike("(CIAL)"), lit("COMMERCE"))
        .when(upper(col("dstcsc")).rlike("(SPOR)"), lit("COMMERCE"))
        .when(upper(col("dstcsc")).rlike("(ECOL)"), lit("BUREAU"))
        .when(upper(col("dstcsc")).rlike("(ENSEI)"), lit("BUREAU"))
        .when(upper(col("dstcsc")).rlike("(CHIR)"), lit("HOPITAL"))
        .when(upper(col("dstcsc")).rlike("(BAT)"), lit("MAISON"))
        .when(upper(col("dstcsc")).rlike("(INDIV)"), lit("MAISON"))
        .when(upper(col("dstcsc")).rlike("(VRD)"), lit("VOIRIE"))
        .when(upper(col("dstcsc")).rlike("(MOB.*SOURIS)"), lit("AUTRES_GC"))
        .when(upper(col("dstcsc")).rlike("(SOUMIS)"), lit("AUTRES_BAT"))
        .when(upper(col("dstcsc")).rlike("(NON SOUMIS)"), lit("AUTRES_GC"))
        .when(upper(col("dstcsc")).rlike("(PHOTOV)"), lit("PHOTOV"))
        .when(upper(col("dstcsc")).rlike("(PARK)"), lit("VOIRIE"))
        .when(upper(col("dstcsc")).rlike("(STATIONNEMENT)"), lit("VOIRIE"))
        .when(upper(col("dstcsc")).rlike("(MANEGE)"), lit("NON_RESIDENTIEL"))
        .when(upper(col("dstcsc")).rlike("(MED)"), lit("BUREAU"))
        .when(upper(col("dstcsc")).rlike("(BANC)"), lit("BUREAU"))
        .when(upper(col("dstcsc")).rlike("(BANQ)"), lit("BUREAU"))
        .when(upper(col("dstcsc")).rlike("(AGENCE)"), lit("BUREAU"))
        .when(upper(col("dstcsc")).rlike("(CRECHE)"), lit("BUREAU"))
        .when(upper(col("dstcsc")).rlike("(EHPAD)"), lit("RESIDENTIEL"))
        .when(upper(col("dstcsc")).rlike("(ENTREPOT)"), lit("INDUSTRIE_LIGHT"))
        .when(upper(col("dstcsc")).rlike("(HANGAR)"), lit("INDUSTRIE_LIGHT"))
        .when(upper(col("dstcsc")).rlike("(AQUAT)"), lit("COMMERCE"))
        .when(upper(col("dstcsc")).rlike("(LGTS)"), lit("RESIDENTIEL"))
        .when(upper(col("dstcsc")).rlike("(LOGIS)"), lit("RESIDENTIEL"))
        .when(upper(col("dstcsc")).rlike("(LOGS)"), lit("RESIDENTIEL"))
        .when(upper(col("dstcsc")).rlike("(RESTAURA)"), lit("BUREAU"))
        .when(upper(col("dstcsc")).rlike("(HAB)"), lit("RESIDENTIEL"))
        # Numeric codes
        .when(col("dstcsc").isin(["01", "02", "03", "03+22", "04", "06", "08", "1", "2", "3", "4", "6", "8"]),
              lit("RESIDENTIEL"))
        .when(col("dstcsc").isin(["22"]), lit("MAISON"))
        .when(col("dstcsc").isin(["27", "99"]), lit("AUTRES_GC"))
        .otherwise(lit(None))
    )

    # Apply only for construction sites
    df = df.withColumn(
        "desti_isic",
        when(
            is_construction & (col("cdnatp") == "C") & (col("cdprod") != "01059"),
            desti_expr
        ).otherwise(col("desti_isic"))
    )

    return df


def _assign_hazard_grades(
    df: DataFrame,
    is_construction,
    logger: Optional[Any] = None
) -> DataFrame:
    """
    Assign HAZARD_GRADES from table_isic_tre_naf (if joined).

    Implements HAZARD_GRADES assignment from CODIFICATION_ISIC_CONSTRUCTION.sas L204-225.

    Args:
        df: Input DataFrame
        is_construction: Construction market filter condition
        logger: Optional logger

    Returns:
        DataFrame with hazard_grades columns populated
    """
    # Initialize all hazard_grades expressions in dictionary
    hazard_mapping = {
        "hazard_grades_fire_isic": "hazard_grades_fire",
        "hazard_grades_bi_isic": "hazard_grades_bi",
        "hazard_grades_rca_isic": "hazard_grades_rca",
        "hazard_grades_rce_isic": "hazard_grades_rce",
        "hazard_grades_trc_isic": "hazard_grades_trc",
        "hazard_grades_rcd_isic": "hazard_grades_rcd",
        "hazard_grades_do_isic": "hazard_grades_do"
    }

    # Build all column expressions at once
    new_columns = {}
    for source_col, target_col in hazard_mapping.items():
        if source_col in df.columns:
            # If source exists, use it when construction and not null
            new_columns[target_col] = when(
                is_construction & col(source_col).isNotNull(),
                col(source_col)
            ).otherwise(col(target_col) if target_col in df.columns else lit(None).cast("string"))
        elif target_col not in df.columns:
            # Initialize if doesn't exist
            new_columns[target_col] = lit(None).cast("string")

    # Apply all in single select
    if new_columns:
        df = df.select("*", *[expr.alias(name) for name, expr in new_columns.items()])

    if logger:
        logger.debug(f"HAZARD_GRADES assigned from ISIC table ({len(new_columns)} columns)")

    return df


def join_isic_reference_tables(
    df: DataFrame,
    spark,
    config,
    vision: str,
    logger: Optional[Any] = None
) -> DataFrame:
    """
    Join all ISIC reference tables to portfolio data.

    Joins:
    1. IRD_SUIVI_ENGAGEMENTS_{vision} - NAF08 and CDISIC from tracking
    2. MAPPING_CDNAF2003_ISIC_{vision} - NAF03 � ISIC mapping
    3. MAPPING_CDNAF2008_ISIC_{vision} - NAF08 � ISIC mapping
    4. MAPPING_ISIC_CONST_ACT_{vision} - Activity � ISIC for CDNATP='R'
    5. MAPPING_ISIC_CONST_CHT_{vision} - Destination � ISIC for CDNATP='C'
    6. table_isic_tre_naf_{vision} - ISIC � HAZARD_GRADES
    7. 1SIC_LG_202306 - ISIC local � ISIC global

    Args:
        df: Input DataFrame
        spark: SparkSession
        config: ConfigLoader instance
        vision: Vision in YYYYMM format
        logger: Optional logger

    Returns:
        DataFrame with all ISIC reference data joined
    """
    from src.reader import BronzeReader

    reader = BronzeReader(spark, config)
    year, month = vision[:4], vision[4:6]

    if logger:
        logger.info("Joining ISIC reference tables")

    # Columns will be added right before the joins that need them

    # Removed count() calls - they trigger expensive full table scans
    # Spark's lazy evaluation means join will only execute when needed

    # 1. Join IRD_SUIVI_ENGAGEMENTS (if available)
    try:
        df_sui = reader.read_file_group("ird_suivi_engagements", vision)
        if df_sui is not None:  # OPTIMIZED: Removed count() check
            df = df.join(
                df_sui.select("nopol", "cdprod",
                             col("cdnaf08").alias("cdnaf08_sui"),  # Production uses CDNAF08
                             col("cdisic").alias("cdisic_sui")),
                on=["nopol", "cdprod"],
                how="left"
            )
            if logger:
                logger.debug("Joined IRD_SUIVI_ENGAGEMENTS")
    except Exception as e:
        if logger:
            logger.warning(f"IRD_SUIVI_ENGAGEMENTS not available: {e}")

    # CRITICAL: Use SELECT to FORCE column creation (matching SAS lines 88-90)
    # withColumn doesn't work with Spark lazy evaluation - must use select()
    df = df.select(
        "*",
        lit(None).cast("string").alias("cdnaf2008"),
        lit(None).cast("string").alias("desti_isic"),
        lit(None).cast("string").alias("cdisic")
    )

    # 2. Join MAPPING_CDNAF2003_ISIC
    try:
        df_naf03 = reader.read_file_group("mapping_cdnaf2003_isic", vision)
        if df_naf03 is not None:  # OPTIMIZED: Removed count() check
            df = df.join(
                df_naf03.select(
                    col("cdnaf_2003").alias("cdnaf"),
                    col("isic_code").alias("isic_from_naf03")
                ),
                on=["cdnaf"],
                how="left"
            )
            if logger:
                logger.debug("Joined MAPPING_CDNAF2003_ISIC")
    except Exception as e:
        if logger:
            logger.warning(f"MAPPING_CDNAF2003_ISIC not available: {e}")

    # 3. Join MAPPING_CDNAF2008_ISIC
    # Column cdnaf2008 now exists (created unconditionally after IRD join)
    try:
        df_naf08 = reader.read_file_group("mapping_cdnaf2008_isic", vision)
        if df_naf08 is not None:  # OPTIMIZED: Removed count() check
            df = df.join(
                df_naf08.select(
                    col("cdnaf_2008").alias("cdnaf2008"),
                    col("isic_code").alias("isic_from_naf08")
                ),
                on=["cdnaf2008"],
                how="left"
            )
            if logger:
                logger.debug("Joined MAPPING_CDNAF2008_ISIC")
    except Exception as e:
        if logger:
            logger.warning(f"MAPPING_CDNAF2008_ISIC not available: {e}")

    # 4. Join MAPPING_ISIC_CONST_ACT (for CDNATP='R')
    try:
        df_act = reader.read_file_group("mapping_isic_const_act", vision)
        if df_act is not None:  # OPTIMIZED: Removed count() check
            df = df.join(
                df_act.select(
                    "actprin",
                    col("cdnaf08").alias("cdnaf08_const_r"),  # Production uses CDNAF08
                    col("cdtre").alias("cdtre_const_r"),
                    col("cdnaf03").alias("cdnaf03_const_r"),  # Production uses CDNAF03
                    col("cdisic").alias("cdisic_const_r")
                ),
                on=["actprin"],
                how="left"
            )
            if logger:
                logger.debug("Joined MAPPING_ISIC_CONST_ACT")
    except Exception as e:
        if logger:
            logger.warning(f"MAPPING_ISIC_CONST_ACT not available: {e}")

    # 5. Join MAPPING_ISIC_CONST_CHT (for CDNATP='C')
    # First ensure desti_isic column exists
    if "desti_isic" not in df.columns:
        df = df.withColumn("desti_isic", lit(None).cast("string"))
    
    try:
        df_cht = reader.read_file_group("mapping_isic_const_cht", vision)
        if df_cht is not None:  # OPTIMIZED: Removed count() check
            # Will join after desti_isic is calculated
            df = df.join(
                df_cht.select(
                    col("desti_isic"),
                    col("cdnaf08").alias("cdnaf08_const_c"),  # Production uses CDNAF08
                    col("cdtre").alias("cdtre_const_c"),
                    col("cdnaf03").alias("cdnaf03_const_c"),  # Production uses CDNAF03
                    col("cdisic").alias("cdisic_const_c")
                ),
                on=["desti_isic"],
                how="left"
            )
            if logger:
                logger.debug("Joined MAPPING_ISIC_CONST_CHT")
    except Exception as e:
        if logger:
            logger.warning(f"MAPPING_ISIC_CONST_CHT not available: {e}")

    # 6. Join table_isic_tre_naf (HAZARD_GRADES)
    # First ensure cdisic column exists
    if "cdisic" not in df.columns:
        df = df.withColumn("cdisic", lit(None).cast("string"))
    
    try:
        df_hazard = reader.read_file_group("table_isic_tre_naf", vision)
        if df_hazard is not None:  # OPTIMIZED: Removed count() check
            df = df.join(
                df_hazard.select(
                    col("isic_code"),
                    col("hazard_grades_fire").alias("hazard_grades_fire_isic"),
                    col("hazard_grades_bi").alias("hazard_grades_bi_isic"),
                    col("hazard_grades_rca").alias("hazard_grades_rca_isic"),
                    col("hazard_grades_rce").alias("hazard_grades_rce_isic"),
                    col("hazard_grades_trc").alias("hazard_grades_trc_isic"),
                    col("hazard_grades_rcd").alias("hazard_grades_rcd_isic"),
                    col("hazard_grades_do").alias("hazard_grades_do_isic")
                ),
                on=[df["cdisic"] == df_hazard["isic_code"]],
                how="left"
            )
            if logger:
                logger.debug("Joined table_isic_tre_naf for HAZARD_GRADES")
    except Exception as e:
        if logger:
            logger.warning(f"table_isic_tre_naf not available: {e}")

    # 7. Join 1SIC_LG_202306 (ISIC local → global)
    try:
        df_isic_global = reader.read_file_group("1sic_lg", "202306")  # Fixed version
        if df_isic_global is not None:  # OPTIMIZED: Removed count() check
            df = df.join(
                df_isic_global.select(
                    col("isic_local"),
                    col("isic_global")
                ),
                on=[df["cdisic"] == df_isic_global["isic_local"]],  # Use cdisic not isic_code
                how="left"
            )
            if logger:
                logger.debug("Joined 1SIC_LG for ISIC global mapping")
    except Exception as e:
        if logger:
            logger.warning(f"1SIC_LG not available: {e}")

    if logger:
        logger.success("All available ISIC reference tables joined")

    return df


# =========================================================================
# ISIC CODE CORRECTIONS
# =========================================================================

def apply_isic_corrections(df: DataFrame) -> DataFrame:
    """
    Apply manual corrections to known bad ISIC codes.
    
    These corrections fix errors in the reference mapping tables where
    ISIC codes are incorrectly formatted or mapped.
    
    Based on: PTF_MVTS_CONSOLIDATION_MACRO.sas L576-590
    
    Args:
        df: DataFrame with cdisic column
        
    Returns:
        DataFrame with corrected cdisic values
        
    Example:
        >>> from utils.transformations.isic_codification import apply_isic_corrections
        >>> df = apply_isic_corrections(df)
    """
    if "cdisic" not in df.columns:
        return df
    
    # Apply corrections in cascade (SAS L578-589)
    # Note: Using trim() to handle both " 22000" and "22000" cases
    df = df.withColumn(
        "cdisic",
        when(trim(col("cdisic")) == "22000", "022000")
        .when(trim(col("cdisic")) == "24021", "024000")
        .when(col("cdisic") == "242025", "242005")
        .when(col("cdisic") == "329020", "329000")
        .when(col("cdisic") == "731024", "731000")
        .when(trim(col("cdisic")) == "81020", "081000")
        .when(trim(col("cdisic")) == "81023", "081000")
        .when(col("cdisic") == "981020", "981000")
        .otherwise(col("cdisic"))
    )
    
    return df


# Mapping of corrections for documentation/reference
ISIC_CORRECTIONS = {
    "22000": "022000",    # Missing leading zero
    " 22000": "022000",   # Leading space + missing zero
    "24021": "024000",    # Wrong code + missing zero
    " 24021": "024000",   # Leading space + wrong code
    "242025": "242005",   # Typo in last digit
    "329020": "329000",   # Wrong precision
    "731024": "731000",   # Wrong precision
    "81020": "081000",    # Missing leading zero
    " 81020": "081000",   # Leading space + missing zero
    "81023": "081000",    # Wrong code + missing zero
    " 81023": "081000",   # Leading space + wrong code
    "981020": "981000",   # Wrong precision
}


# =========================================================================
# PARTNERSHIP AND BERLIOZ FLAGS
# =========================================================================

def add_partenariat_berlitz_flags(df: DataFrame) -> DataFrame:
    """
    Add partnership and Berlioz flags based on intermediary codes.
    
    Flags specific intermediaries with special business relationships:
    - Berlioz: NOINT = '4A5766'
    - Partnership: NOINT in ('4A6160', '4A6947', '4A6956')
    
    Based on: PTF_MVTS_CONSOLIDATION_MACRO.sas L596-600
    
    Args:
        df: Input DataFrame with noint column (intermediary code)
    
    Returns:
        DataFrame with top_berlioz and top_partenariat flag columns added
        
    Example:
        >>> from utils.transformations.isic_codification import add_partenariat_berlitz_flags
        >>> df = add_partenariat_berlitz_flags(df)
    """
    if "noint" not in df.columns:
        # If no noint column, add NULL flags
        df = df.withColumn("top_berlioz", lit(0))
        df = df.withColumn("top_partenariat", lit(0))
        return df
    
    # Add Berlioz flag (SAS L598)
    df = df.withColumn(
        "top_berlioz",
        when(col("noint") == "4A5766", lit(1)).otherwise(lit(0))
    )
    
    # Add Partnership flag (SAS L599)
    df = df.withColumn(
        "top_partenariat",
        when(col("noint").isin(["4A6160", "4A6947", "4A6956"]), lit(1)).otherwise(lit(0))
    )
    
    return df
