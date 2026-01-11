"""
Client data enrichment for Construction Data Pipeline.

Handles SIREN, credit scores, and client reference data.
Based on PTF_MVTS_CONSOLIDATION_MACRO.sas L385-394.
"""

from pyspark.sql import DataFrame # type: ignore
from pyspark.sql.functions import col, when, lit, coalesce # type: ignore
from typing import Optional, Any


def join_client_data(
    df: DataFrame,
    spark,
    config,
    vision: str,
    logger: Optional[Any] = None
) -> DataFrame:
    """
    Join client reference data (CLACENT1, CLACENT3).

    Based on PTF_MVTS_CONSOLIDATION_MACRO.sas L385-394.
    
    Note: HISTO_NOTE_RISQUE (note_euler) is handled separately by 
    consolidation_processor._enrich_euler_risk_note() to avoid duplication.

    Joins:
    1. CLIENT1 (CLIACT14) - Client data generation 14
    2. CLIENT3 (CLIACT3) - Client data generation 3

    Args:
        df: Input DataFrame
        spark: SparkSession
        config: ConfigLoader instance
        vision: Vision in YYYYMM format
        logger: Optional logger

    Returns:
        DataFrame with client data enriched
    """
    from src.reader import BronzeReader

    reader = BronzeReader(spark, config)
    year, month = vision[:4], vision[4:6]
    dtfin = f"{year}-{month}-01"  # Simplified - should use last day of month

    if logger:
        logger.info("Joining client reference data")

    # 1. Join CLIENT1 (CLIACT14) for CDSIRECT, CDSIREP
    try:
        df_client1 = reader.read_file_group("cliact14", vision)
        if df_client1 is not None:  # OPTIMIZED: Removed count() check
            df = df.join(
                df_client1.select(
                    col("noclt"),
                    col("cdsiret").alias("cdsiret_c1"),  # Fixed: was cdsirect
                    col("cdsiren").alias("cdsiren_c1")   # Fixed: was cdsirep
                ),
                on=["noclt"],
                how="left"
            )
            if logger:
                logger.debug("Joined CLIACT14")
    except Exception as e:
        if logger:
            logger.warning(f"CLIACT14 not available: {e}")

    # 2. Join CLIENT3 (CLIACT3) for CDSIRECT, CDSIREP (fallback)
    try:
        df_client3 = reader.read_file_group("cliact3", vision)
        if df_client3 is not None:  # OPTIMIZED: Removed count() check
            df = df.join(
                df_client3.select(
                    col("noclt"),
                    col("cdsiret").alias("cdsiret_c3"),  # Fixed: was cdsirect
                    col("cdsiren").alias("cdsiren_c3")   # Fixed: was cdsirep
                ),
                on=["noclt"],
                how="left"
            )
            if logger:
                logger.debug("Joined CLIACT3")
    except Exception as e:
        if logger:
            logger.warning(f"CLIACT3 not available: {e}")

    # 3. Coalesce CDSIRET and CDSIREN (CLIENT1 priority over CLIENT3)
    if "cdsiret_c1" in df.columns or "cdsiret_c3" in df.columns:
        df = df.withColumn(
            "cdsiret",  # Fixed: was cdsirect
            coalesce(
                col("cdsiret_c1") if "cdsiret_c1" in df.columns else lit(None),
                col("cdsiret_c3") if "cdsiret_c3" in df.columns else lit(None)
            )
        )
        df = df.withColumn(
            "cdsiren",  # Fixed: was cdsirep
            coalesce(
                col("cdsiren_c1") if "cdsiren_c1" in df.columns else lit(None),
                col("cdsiren_c3") if "cdsiren_c3" in df.columns else lit(None)
            )
        )

        # Drop temporary columns
        if "cdsiret_c1" in df.columns:
            df = df.drop("cdsiret_c1", "cdsiren_c1")  # Fixed column names
        if "cdsiret_c3" in df.columns:
            df = df.drop("cdsiret_c3", "cdsiren_c3")  # Fixed column names


    if logger:
        logger.success("✓ SUCCESS: Client data enrichment completed")

    return df
