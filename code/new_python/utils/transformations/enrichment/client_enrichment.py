"""
Enrichissement des données client pour le Pipeline de Données Construction.

Gère SIREN, SIRET et les données de référence client.
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
    Jointure des données de référence client (CLACENT1, CLACENT3).

    Note : HISTO_NOTE_RISQUE (note_euler) est géré séparément par
    consolidation_processor._enrich_euler_risk_note() pour éviter la duplication.

    Jointures :
    1. CLIENT1 (CLIACT14) - Données client génération 14
    2. CLIENT3 (CLIACT3) - Données client génération 3

    Args:
        df: DataFrame en entrée
        spark: SparkSession
        config: Instance ConfigLoader
        vision: Vision au format YYYYMM
        logger: Logger optionnel

    Returns:
        DataFrame avec données client enrichies
    """
    from src.reader import BronzeReader

    reader = BronzeReader(spark, config)
    year, month = vision[:4], vision[4:6]
    dtfin = f"{year}-{month}-01"  # Simplifié - devrait utiliser le dernier jour du mois

    if logger:
        logger.info("Jointure des données de référence client")

    # 1. Joindre CLIENT1 (CLIACT14) pour CDSIRNET, CDSIREP
    try:
        df_client1 = reader.read_file_group("cliact14", vision)
        if df_client1 is not None:
            df = df.join(
                df_client1.select(
                    col("noclt"),
                    col("cdsiret").alias("cdsiret_c1"),
                    col("cdsiren").alias("cdsiren_c1")
                ),
                on=["noclt"],
                how="left"
            )
            if logger:
                logger.debug("Jointure CLIACT14 effectuée")
    except Exception as e:
        if logger:
            logger.warning(f"CLIACT14 non disponible : {e}")

    # 2. Joindre CLIENT3 (CLIACT3) pour CDSIRNET, CDSIREP (repli)
    try:
        df_client3 = reader.read_file_group("cliact3", vision)
        if df_client3 is not None:
            df = df.join(
                df_client3.select(
                    col("noclt"),
                    col("cdsiret").alias("cdsiret_c3"),
                    col("cdsiren").alias("cdsiren_c3")
                ),
                on=["noclt"],
                how="left"
            )
            if logger:
                logger.debug("Jointure CLIACT3 effectuée")
    except Exception as e:
        if logger:
            logger.warning(f"CLIACT3 non disponible : {e}")

    # 3. Fusionner (Coalesce) CDSIRET et CDSIREN (Priorité CLIENT1 sur CLIENT3)
    if "cdsiret_c1" in df.columns or "cdsiret_c3" in df.columns:
        df = df.withColumn(
            "cdsiret",
            coalesce(
                col("cdsiret_c1") if "cdsiret_c1" in df.columns else lit(None),
                col("cdsiret_c3") if "cdsiret_c3" in df.columns else lit(None)
            )
        )
        df = df.withColumn(
            "cdsiren",
            coalesce(
                col("cdsiren_c1") if "cdsiren_c1" in df.columns else lit(None),
                col("cdsiren_c3") if "cdsiren_c3" in df.columns else lit(None)
            )
        )

        # Supprimer les colonnes temporaires
        if "cdsiret_c1" in df.columns:
            df = df.drop("cdsiret_c1", "cdsiren_c1")
        if "cdsiret_c3" in df.columns:
            df = df.drop("cdsiret_c3", "cdsiren_c3")


    if logger:
        logger.success("✓ SUCCESS : Enrichissement des données client terminé")

    return df
