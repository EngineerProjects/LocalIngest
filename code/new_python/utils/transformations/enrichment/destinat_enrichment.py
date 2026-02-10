"""
Calcul de DESTINAT (Destination) pour les chantiers.

Parse le champ DSTCSC pour catégoriser les destinations des chantiers.
Basé sur la logique de consolidation.
"""

from pyspark.sql import DataFrame # type: ignore
from pyspark.sql.functions import col, when, lit, upper, regexp_extract, coalesce # type: ignore
from typing import Optional, Any

# =========================================================================
# LOGIQUE DE CONSOLIDATION DESTINAT
# =========================================================================

def apply_destinat_consolidation_logic(df: DataFrame) -> DataFrame:
    """
    Applique la logique métier DESTINAT pour les chantiers.

    Cette fonction applique une logique spécifique à la consolidation pour remplir
    les valeurs DESTINAT manquantes après le calcul détaillé.

    Logique :
    1. S'applique uniquement au segment2="Chantiers" où DESTINAT est manquant
    2. Vérifie DSTCSC et LBNATTRV pour des mots-clés de logement
    3. Vérifie les codes numériques DSTCSC pour la classification résidentielle
    4. Défaut à "Autres" si aucune correspondance

    Args:
        df: DataFrame consolidé

    Returns:
        DataFrame avec DESTINAT rempli pour les Chantiers

    Exemple:
        >>> from utils.transformations.destinat_calculation import apply_destinat_consolidation_logic
        >>> df = apply_destinat_consolidation_logic(df)
    """
    if "segment2" not in df.columns or "destinat" not in df.columns:
        return df

    # Traiter uniquement les Chantiers avec DESTINAT manquant
    is_chantier_missing = (col("segment2") == "Chantiers") & col("destinat").isNull()

    # Préparer les colonnes en majuscules pour la correspondance de motifs
    dstcsc_upper = upper(col("dstcsc")) if "dstcsc" in df.columns else lit(None)
    lbnattrv_upper = upper(col("lbnattrv")) if "lbnattrv" in df.columns else lit(None)

    # Correspondance de motifs pour "Habitation"
    # Mots-clés : HABIT, LOG, LGT, MAIS, APPA, VILLA, INDIV
    is_habitation = (
        dstcsc_upper.contains("HABIT") | lbnattrv_upper.contains("HABIT") |
        dstcsc_upper.contains("LOG") | lbnattrv_upper.contains("LOG") |
        dstcsc_upper.contains("LGT") | lbnattrv_upper.contains("LGT") |
        dstcsc_upper.contains("MAIS") | lbnattrv_upper.contains("MAIS") |
        dstcsc_upper.contains("APPA") | lbnattrv_upper.contains("APPA") |
        dstcsc_upper.contains("VILLA") | lbnattrv_upper.contains("VILLA") |
        dstcsc_upper.contains("INDIV") | lbnattrv_upper.contains("INDIV")
    )

    # Codes numériques pour "Habitation"
    if "dstcsc" in df.columns:
        is_habitation = is_habitation | col("dstcsc").isin(["01", "02", "03", "04", "1", "2", "3", "4", "22"])

    # Appliquer la logique
    df = df.withColumn(
        "destinat",
        when(
            is_chantier_missing,
            when(is_habitation, lit("Habitation"))
            .otherwise(lit("Autres"))
        ).otherwise(col("destinat"))
    )

    return df
