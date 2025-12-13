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
    Join client reference data (CLACENT1, CLACENT3, HISTO_NOTE_RISQUE).

    Based on PTF_MVTS_CONSOLIDATION_MACRO.sas L385-410.

    Joins:
    1. CLIENT1 (CLIACT14) - Client data generation 14
    2. CLIENT3 (CLIACT3) - Client data generation 3
    3. BINSEE.HISTO_NOTE_RISQUE - Risk scoring history

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
        if df_client1 is not None and df_client1.count() > 0:
            df = df.join(
                df_client1.select(
                    col("noclt"),
                    col("cdsirect").alias("cdsirect_c1"),
                    col("cdsirep").alias("cdsirep_c1")
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
        if df_client3 is not None and df_client3.count() > 0:
            df = df.join(
                df_client3.select(
                    col("noclt"),
                    col("cdsirect").alias("cdsirect_c3"),
                    col("cdsirep").alias("cdsirep_c3")
                ),
                on=["noclt"],
                how="left"
            )
            if logger:
                logger.debug("Joined CLIACT3")
    except Exception as e:
        if logger:
            logger.warning(f"CLIACT3 not available: {e}")

    # 3. Coalesce CDSIRECT and CDSIREP (CLIENT1 priority over CLIENT3)
    if "cdsirect_c1" in df.columns or "cdsirect_c3" in df.columns:
        df = df.withColumn(
            "cdsirect",
            coalesce(
                col("cdsirect_c1") if "cdsirect_c1" in df.columns else lit(None),
                col("cdsirect_c3") if "cdsirect_c3" in df.columns else lit(None)
            )
        )
        df = df.withColumn(
            "cdsirep",
            coalesce(
                col("cdsirep_c1") if "cdsirep_c1" in df.columns else lit(None),
                col("cdsirep_c3") if "cdsirep_c3" in df.columns else lit(None)
            )
        )

        # Drop temporary columns
        if "cdsirect_c1" in df.columns:
            df = df.drop("cdsirect_c1", "cdsirep_c1")
        if "cdsirect_c3" in df.columns:
            df = df.drop("cdsirect_c3", "cdsirep_c3")

    # 4. Join HISTO_NOTE_RISQUE for risk scoring (if CDSIREN available)
    if "cdsiren" in df.columns:
        try:
            df_histo_note = reader.read_file_group("histo_note_risque", vision)
            if df_histo_note is not None and df_histo_note.count() > 0:
                # Filter by validity dates (SAS: dtdeb_valid <= dtfin AND dtfin_valid > dtfin)
                df_histo_note_valid = df_histo_note.filter(
                    (col("dtdeb_valid") <= lit(dtfin)) &
                    (col("dtfin_valid") > lit(dtfin))
                )

                df = df.join(
                    df_histo_note_valid.select(
                        "cdsiren",
                        col("note_risque").alias("note_risque_histo"),
                        col("score_credit").alias("score_credit_histo")
                    ),
                    on=["cdsiren"],
                    how="left"
                )

                if logger:
                    logger.debug("Joined HISTO_NOTE_RISQUE")
        except Exception as e:
            if logger:
                logger.warning(f"HISTO_NOTE_RISQUE not available: {e}")

    if logger:
        logger.success("Client data enrichment completed")

    return df
