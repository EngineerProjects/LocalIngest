"""
DESTINAT (Destination) calculation for construction sites.

Parses DSTCSC field to categorize construction site destinations.
Based on PTF_MVTS_CONSOLIDATION_MACRO.sas L416-437.
"""

from pyspark.sql import DataFrame # type: ignore
from pyspark.sql.functions import col, when, lit, upper, regexp_extract, coalesce # type: ignore
from typing import Optional, Any


def calculate_destinat(
    df: DataFrame,
    logger: Optional[Any] = None
) -> DataFrame:
    """
    Calculate DESTINAT from DSTCSC field for construction sites.

    Based on PTF_MVTS_CONSOLIDATION_MACRO.sas L424-437.

    Uses pattern matching to categorize construction destinations:
    - UNITE, DUPLEX, TRIPLEX, PAVILLON, VILLA
    - COLLECTIF (batiments collectifs)
    - MAISON (individual housing)
    - etc.

    Args:
        df: Input DataFrame
        logger: Optional logger

    Returns:
        DataFrame with destinat column added

    Note:
        Requires DO_DEST reference table to be joined first.
        If not available, uses pattern matching on DSTCSC as fallback.
    """
    if logger:
        logger.info("Calculating DESTINAT from DSTCSC patterns")

    # Initialize destinat column
    df = df.withColumn("destinat", lit(None).cast("string"))

    # If DO_DEST was joined, use it (highest priority)
    if "destinat_do_dest" in df.columns:
        df = df.withColumn(
            "destinat",
            coalesce(col("destinat_do_dest"), col("destinat"))
        )
        if logger:
            logger.debug("DESTINAT assigned from DO_DEST table")

    # Fallback: Pattern matching on DSTCSC (only for segment2="Chantiers")
    if "dstcsc" not in df.columns:
        if logger:
            logger.warning("DSTCSC column not found, DESTINAT will be NULL")
        return df

    # Only apply to construction sites (segment2 = "Chantiers")
    is_chantier = col("segment2") == "Chantiers" if "segment2" in df.columns else lit(True)

    df = df.withColumn("_dstcsc_upper", upper(col("dstcsc")))

    # Build pattern matching cascade using pre-computed uppercase column
    # SAS: PRXMATCH("/(UNIT)/", UPCASE(DSTCSC))
    destinat_expr = (
        when(col("_dstcsc_upper").rlike("(UNIT)"), lit("UNITE"))
        .when(col("_dstcsc_upper").rlike("(DUPLEX)"), lit("DUPLEX"))
        .when(col("_dstcsc_upper").rlike("(TRIPLEX)"), lit("TRIPLEX"))
        .when(col("_dstcsc_upper").rlike("(PAVILLON)"), lit("PAVILLON"))
        .when(col("_dstcsc_upper").rlike("(VILLA)"), lit("VILLA"))
        .when(col("_dstcsc_upper").rlike("(COLL)"), lit("COLLECTIF"))
        .when(col("_dstcsc_upper").rlike("(MAISON)"), lit("MAISON"))
        .when(col("_dstcsc_upper").rlike("(LOGE)"), lit("LOGEMENT"))
        .when(col("_dstcsc_upper").rlike("(APPA)"), lit("APPARTEMENT"))
        .when(col("_dstcsc_upper").rlike("(HABIT)"), lit("HABITATION"))
        .when(col("_dstcsc_upper").rlike("(INDUS)"), lit("INDUSTRIE"))
        .when(col("_dstcsc_upper").rlike("(BUREAU)"), lit("BUREAU"))
        .when(col("_dstcsc_upper").rlike("(COMMER)"), lit("COMMERCE"))
        .when(col("_dstcsc_upper").rlike("(HOTEL)"), lit("HOTEL"))
        .when(col("_dstcsc_upper").rlike("(TOURIS)"), lit("TOURISME"))
        .when(col("_dstcsc_upper").rlike("(ECOL)"), lit("ENSEIGNEMENT"))
        .when(col("_dstcsc_upper").rlike("(ENSEI)"), lit("ENSEIGNEMENT"))
        .when(col("_dstcsc_upper").rlike("(HOP)"), lit("HOPITAL"))
        .when(col("_dstcsc_upper").rlike("(HOSP)"), lit("HOPITAL"))
        .when(col("_dstcsc_upper").rlike("(CLINI)"), lit("CLINIQUE"))
        .when(col("_dstcsc_upper").rlike("(CHIR)"), lit("CHIRURGIE"))
        .when(col("_dstcsc_upper").rlike("(GARAGE)"), lit("GARAGE"))
        .when(col("_dstcsc_upper").rlike("(PARK)"), lit("PARKING"))
        .when(col("_dstcsc_upper").rlike("(STATIONNEMENT)"), lit("PARKING"))
        .when(col("_dstcsc_upper").rlike("(STOCK)"), lit("STOCKAGE"))
        .when(col("_dstcsc_upper").rlike("(ENTREPOT)"), lit("ENTREPOT"))
        .when(col("_dstcsc_upper").rlike("(HANGAR)"), lit("HANGAR"))
        .when(col("_dstcsc_upper").rlike("(AGRIC)"), lit("AGRICOLE"))
        .when(col("_dstcsc_upper").rlike("(SPOR)"), lit("SPORTIF"))
        .when(col("_dstcsc_upper").rlike("(AQUAT)"), lit("AQUATIQUE"))
        .when(col("_dstcsc_upper").rlike("(CRECHE)"), lit("CRECHE"))
        .when(col("_dstcsc_upper").rlike("(EHPAD)"), lit("EHPAD"))
        .when(col("_dstcsc_upper").rlike("(RESID)"), lit("RESIDENTIEL"))
        .when(col("_dstcsc_upper").rlike("(RESTAURA)"), lit("RESTAURATION"))
        .when(col("_dstcsc_upper").rlike("(VRD)"), lit("VRD"))
        .when(col("_dstcsc_upper").rlike("(PHOTOV)"), lit("PHOTOVOLTAIQUE"))
        .when(col("_dstcsc_upper").rlike("(BAT)"), lit("BATIMENT"))
        .when(col("_dstcsc_upper").rlike("(INDIV)"), lit("INDIVIDUEL"))
        # Numeric codes for residential
        .when(col("dstcsc").isin(["01", "02", "03", "04", "06", "08", "1", "2", "3", "4", "6", "8"]),
              lit("RESIDENTIEL"))
        .when(col("dstcsc") == "22", lit("MAISON"))
        .when(col("dstcsc").isin(["27", "99"]), lit("AUTRES"))
        .otherwise(lit("NON_CLASSIFIE"))
    )

    # Apply pattern matching (only if destinat still null and is chantier)
    df = df.withColumn(
        "destinat",
        when(
            is_chantier & col("destinat").isNull() & col("dstcsc").isNotNull(),
            destinat_expr
        ).otherwise(col("destinat"))
    )

    # Drop temporary uppercase column
    df = df.drop("_dstcsc_upper")

    if logger:
        logger.success("DESTINAT calculation completed")

    return df


def join_do_dest_table(
    df: DataFrame,
    spark,
    config,
    logger: Optional[Any] = None
) -> DataFrame:
    """
    Join DO_DEST reference table for DESTINAT.

    Based on PTF_MVTS_CONSOLIDATION_MACRO.sas L416-421.

    Args:
        df: Input DataFrame
        spark: SparkSession
        config: ConfigLoader instance
        logger: Optional logger

    Returns:
        DataFrame with DO_DEST data joined
    """
    from src.reader import DataLakeReader

    reader = DataLakeReader(spark, config, logger)

    if logger:
        logger.info("Joining DO_DEST reference table")

    try:
        # Note: SAS uses DO_DEST202110 (fixed version)
        # You may need to implement version selection logic
        df_do_dest = reader.read_file_group("do_dest", "202110")

        if df_do_dest is not None and df_do_dest.count() > 0:
            # Join on NOPOL
            df = df.join(
                df_do_dest.select(
                    "nopol",
                    col("destinat").alias("destinat_do_dest")
                ),
                on=["nopol"],
                how="left"
            )

            if logger:
                logger.debug("Joined DO_DEST table")
        else:
            if logger:
                logger.warning("DO_DEST table is empty")

    except Exception as e:
        if logger:
            logger.warning(f"DO_DEST table not available: {e}")

    return df


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
