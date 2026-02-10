# -*- coding: utf-8 -*-
"""
Codification ISIC pour le Pipeline de Données Construction.

Implémente la logique complète d'attribution des codes ISIC.
Attribue les codes ISIC et les GRADES DE RISQUE (Hazard Grades) basés sur plusieurs stratégies de repli.

Ordre de priorité :
  1) Contrats avec activité (CDNATP='R') via mapping ACT
  2) Destination chantier (CDNATP='C') via parsing DESTI + mapping CHT
  3) Replis via NAF :
        NAF08_PTF > NAF03_PTF > NAF03_CLI > NAF08_CLI
  4) SUI (suivi engagements) si encore vide
  5) Grades de risque via table de référence ISIC
"""

from typing import Dict, Optional, Any
from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql.functions import (  # type: ignore
    col, when, lit, upper, trim,
    least, greatest, coalesce
)

from types import SimpleNamespace

# Import du helper existant
from utils.processor_helpers import get_bronze_reader

# ---------------------------------------------------------------------------
# API Publique
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
    Orchestrateur complet du pipeline de codification ISIC.
    Reproduit la logique métier de bout en bout.

    Étapes :
      1. Chargement des tables de référence (SUI, ACT, CHT, NAF03, NAF08, HAZARD)
      2. Application de l'ordre de jointure :
            - Jointure SUI
            - Mapping NAF -> ISIC (cascade)
            - Jointure ACT (Contrats activité, CDNATP='R')
            - Calcul DESTI_ISIC (Parsing DSTCSC)
            - Jointure CHT (Contrats chantier, CDNATP='C')
      3. Finalisation des codes ISIC (règles d'écrasement)
      4. Jointure des grades de risque (Hazard Grades)
      5. Suppression des colonnes temporaires
    """

    if logger:
        logger.info(f"[ISIC] Démarrage du pipeline de codification ISIC pour la vision {vision}")

    # ============================================================
    # CHARGEMENT DES TABLES DE RÉFÉRENCE
    # ============================================================
    if logger: logger.debug("[ISIC] Chargement des tables de référence...")

    df_sui    = load_sui_table(spark, config, vision, logger)
    df_act    = load_mapping_const_act(spark, config, vision, logger)
    df_cht    = load_mapping_const_cht(spark, config, vision, logger)
    df_naf03  = load_mapping_naf2003(spark, config, vision, logger)
    df_naf08  = load_mapping_naf2008(spark, config, vision, logger)
    df_hazard = load_hazard_grades(spark, config, vision, logger)

    # ============================================================
    # APPLICATION DE LA LOGIQUE DE JOINTURE
    # ============================================================

    # --- 3.1: Jointure SUI
    if logger: logger.debug("[ISIC] Étape 3.1 : Jointure référence SUI")
    df = join_isic_sui(df, df_sui)

    # --- 3.3: Cascade NAF (PTF NAF08 -> PTF NAF03 -> CLI NAF03 -> CLI NAF08)
    if logger: logger.debug("[ISIC] Étape 3.3 : Mapping NAF -> ISIC en cascade")
    df = map_naf_to_isic(df, df_naf03, df_naf08)

    # --- 3.4: Mapping ACT (CDNATP='R')
    if logger: logger.debug("[ISIC] Étape 3.4 : Jointure CONST_ACT (CDNATP='R')")
    df = join_isic_const_act(df, df_act)

    # --- 3.5: Parsing DESTI_ISIC depuis DSTCSC
    if logger: logger.debug("[ISIC] Étape 3.5 : Calcul DESTI_ISIC depuis patterns DSTCSC")
    df = compute_destination_isic(df)

    # --- 3.6: Mapping CHT (CDNATP='C')
    if logger: logger.debug("[ISIC] Étape 3.6 : Jointure CONST_CHT (CDNATP='C')")
    df = join_isic_const_cht(df, df_cht)

    # ============================================================
    # FINALISATION
    # ============================================================
    if logger: logger.debug("[ISIC] Étape 4 : Finalisation des codes ISIC")
    df = finalize_isic_codes(df)

    # ============================================================
    # JOINTURE GRADES DE RISQUE
    # ============================================================
    if logger: logger.debug("[ISIC] Étape 5 : Jointure des grades de risque")
    df = join_isic_hazard_grades(df, df_hazard)

    # ============================================================
    # NETTOYAGE (suppression colonnes temporaires)
    # ============================================================
    if logger: logger.debug("[ISIC] Suppression des colonnes ISIC temporaires")
    df = drop_isic_temp_columns(df)

    if logger:
        logger.info("[ISIC] Pipeline de codification ISIC terminé avec succès")

    return df

def join_isic_sui(df: DataFrame, df_sui: DataFrame) -> DataFrame:
    """
    Jointure des informations ISIC/NAF provenant de SUI (Suivi des Engagements).
    Logique stricte pour la section 3.1.

    Règles :
    - LEFT JOIN sur (POLICE = NOPOL ET PRODUIT = CDPROD)
    - Ajout :
        CDNAF2008_TEMP     <- cdnaf08 de SUI
        ISIC_CODE_SUI_TEMP <- cdisic de SUI
    - Appliquer uniquement pour le marché construction (CMARCH = '6')
    - N'écrase PAS les champs ISIC finaux ici. Stocke uniquement des valeurs TEMP.
    """

    # Si SUI indisponible, ajouter simplement des colonnes TEMP NULL
    if df_sui is None:
        return (
            df
            .withColumn("cdnaf2008_temp", lit(None).cast("string"))
            .withColumn("isic_code_sui_temp", lit(None).cast("string"))
        )

    # Alias
    left = df.alias("l")
    right = df_sui.alias("r")

    # Détection mapping clé
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

    # Condition de jointure de base
    join_cond = (
        key_left_pol == col("r.nopol")
    ) & (
        key_left_prod == col("r.cdprod")
    )

    # Jointure et SELECT immédiat pour éviter duplication de colonnes
    out = (
        left
        .join(right, join_cond, how="left")
        .select(
            "l.*",  # Préserve TOUTES les colonnes de gauche
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
    Section 3.3 — Cascade NAF->ISIC avec priorité stricte et tags d'origine.

    Jointures (LEFT) avec cmarch='6' dans la clause ON :
      - PTF NAF03:   ref_naf03.isic_code  sur df.cdnaf
      - PTF NAF08:   ref_naf08.isic_code  sur df.cdnaf2008_temp
      - CLI NAF03:   ref_naf03.isic_code  sur df.cdnaf03_cli
      - CLI NAF08:   ref_naf08.isic_code  sur df.cdnaf08_w6

    Produit uniquement des colonnes TEMP :
      - isic_code_temp
      - origine_isic_temp

    Notes :
      - Conserve toutes les lignes de gauche (left join)
      - Les conditions s'appliquent uniquement à la construction (cmarch='6')
      - Null-safe ; si références manquantes, fallback vers NULL/chaîne vide approprié
    """

    # S'assurer que les colonnes clés existent à gauche ; sinon, ajouts comme NULL
    needed_left = ["cdnaf", "cdnaf2008_temp", "cdnaf03_cli", "cdnaf08_w6", "cmarch"]
    cur = df
    for c in needed_left:
        if c not in cur.columns:
            cur = cur.withColumn(c, lit(None).cast("string"))

    # Préparation alias
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
        # Colonnes non disponibles -> ajout placeholders NULL
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

    # Construction ISIC_CODE_TEMP avec priorité :
    #   PTF NAF08 (t3) -> PTF NAF03 (t2) -> CLI NAF03 (t4) -> CLI NAF08 (t5) -> ""
    isic_temp = when(col("_ptf_naf08_isic").isNotNull(), col("_ptf_naf08_isic")) \
        .otherwise(
            when(col("_ptf_naf03_isic").isNotNull(), col("_ptf_naf03_isic"))
            .otherwise(
                when(col("_cli_naf03_isic").isNotNull(), col("_cli_naf03_isic"))
                .otherwise(
                    when(col("_cli_naf08_isic").isNotNull(), col("_cli_naf08_isic"))
                    .otherwise(lit(None).cast("string"))
                )
            )
        )

    # Construction ORIGINE_ISIC_TEMP cohérente avec la même cascade
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

    out = (
        left
        .select(
            "l.*",  # TOUTES les colonnes du DataFrame original
            isic_temp.alias("isic_code_temp"),
            origine_temp.alias("origine_isic_temp")
        )
    )

    return out

def join_isic_const_act(df: DataFrame, df_act: DataFrame) -> DataFrame:
    """
    Section 3.4 — Mapping contrats d'activité (CDNATP='R').
    Ajoute les colonnes *_CONST_R via LEFT JOIN sur ACTPRIN avec cmarch='6' et cdnatp='R' dans la clause ON.

    Colonnes de sortie créées :
      - cdnaf08_const_r
      - cdnaf03_const_r
      - cdtre_const_r
      - cdisic_const_r

    Comportement :
      - Si mapping manquant/indisponible, crée les colonnes NULL.
      - S'applique uniquement à la construction (cmarch='6') et contrats activité (cdnatp='R').
      - N'écrase PAS les champs ISIC finaux ici (finalisation plus tard).
    """
    # S'assurer que les colonnes gauche requises existent
    needed_left = ["actprin", "cmarch", "cdnatp"]
    cur = df
    for c in needed_left:
        if c not in cur.columns:
            cur = cur.withColumn(c, lit(None).cast("string"))

    # Si table de mapping indisponible ou incomplète, ajouter colonnes NULL et retourner
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

    # Construire jointure avec conditions ON (cmarch='6' et cdnatp='R')
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

    # Utilisation SELECT explicite pour éviter duplication de colonnes
    out = (
        joined
        .select(
            "l.*",  # Préserve TOUTES les colonnes de gauche
            when(col("r.cdnaf08").isNotNull(), col("r.cdnaf08")).otherwise(lit(None).cast("string")).alias("cdnaf08_const_r"),
            when(col("r.cdnaf03").isNotNull(), col("r.cdnaf03")).otherwise(lit(None).cast("string")).alias("cdnaf03_const_r"),
            when(col("r.cdtre").isNotNull(), col("r.cdtre")).otherwise(lit(None).cast("string")).alias("cdtre_const_r"),
            when(col("r.cdisic").isNotNull(), col("r.cdisic")).otherwise(lit(None).cast("string")).alias("cdisic_const_r")
        )
    )

    return out

def compute_destination_isic(df: DataFrame) -> DataFrame:
    """
    Section 3.5 — Attribution DESTI_ISIC pour contrats chantier.

    Règles :
      - Appliquer uniquement si cmarch='6' et cdnatp='C'
      - cdprod='01059' -> DESTI_ISIC='VENTE'
      - Pour cdprod dans ('00548','01071') avec un ISIC déjà disponible,
        garder ISIC existant : ne pas forcer DESTI_ISIC (laisser null)
      - Sinon, parser DSTCSC pour attribuer l'une des catégories :
        RESIDENTIEL, MAISON, INDUSTRIE, BUREAU, INDUSTRIE_LIGHT, COMMERCE,
        HOPITAL, VOIRIE, AUTRES_GC, AUTRES_BAT, PHOTOV, NON_RESIDENTIEL, etc.
      - Si ISIC_CODE_TEMP vide ET DESTI_ISIC vide -> défaut 'AUTRES_BAT'

    Sortie :
      - Ajoute/écrase 'desti_isic' (string)
    """
    # S'assurer que les colonnes attendues existent
    cur = df
    required = ["cmarch", "cdnatp", "cdprod", "dstcsc", "isic_code_temp", "isic_code_sui_temp"]
    for c in required:
        if c not in cur.columns:
            cur = cur.withColumn(c, lit(None).cast("string"))

    is_construction_chantier = (col("cmarch") == "6") & (col("cdnatp") == "C")
    has_existing_isic = col("isic_code_temp").isNotNull() | col("isic_code_sui_temp").isNotNull()
    dst = upper(col("dstcsc"))

    # Cascade de patterns (équivalents PRXMATCH)
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

    # Mapping code numérique
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

    # Départ avec desti_isic NULL
    cur = cur.withColumn("desti_isic", lit(None).cast("string"))

    # Produit spécial 01059 : set VENTE (uniquement pour chantier construction)
    cur = cur.withColumn(
        "desti_isic",
        when(is_construction_chantier & (col("cdprod") == "01059"), lit("VENTE"))
        .otherwise(col("desti_isic"))
    )

    # Pour les autres contrats chantier (hors 01059, et hors 00548/01071 avec ISIC existant) :
    eligible_for_parsing = (
        is_construction_chantier &
        (col("cdprod") != "01059") &
        ~( (col("cdprod").isin("00548", "01071")) & has_existing_isic )
    )

    # Appliquer parsing pattern d'abord
    cur = cur.withColumn(
        "desti_isic",
        when(
            eligible_for_parsing & col("desti_isic").isNull() & col("dstcsc").isNotNull(),
            pattern_based
        ).otherwise(col("desti_isic"))
    )

    # Puis fallback code numérique (seulement si non défini)
    cur = cur.withColumn(
        "desti_isic",
        when(
            eligible_for_parsing & col("desti_isic").isNull() & col("dstcsc").isNotNull(),
            num_code_based
        ).otherwise(col("desti_isic"))
    )

    # Défaut final : si aucun ISIC trouvé et desti_isic toujours vide -> 'AUTRES_BAT'
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
    Section 3.6 — Mapping ISIC chantier (CHT).

    Applique le mapping pour les contrats chantier utilisant les catégories DESTI_ISIC.

    Logique :
      LEFT JOIN mapping_isic_const_cht
        ON (l.desti_isic = r.desti_isic AND l.cmarch='6' AND l.cdnatp='C')

    Produit les colonnes TEMP :
      - cdnaf08_const_c
      - cdnaf03_const_c
      - cdtre_const_c
      - cdisic_const_c

    Ces colonnes ne sont PAS finales. L'écrasement final se produit plus tard.
    """

    # S'assurer que les colonnes gauche requises existent
    needed_left = ["desti_isic", "cmarch", "cdnatp"]
    cur = df
    for c in needed_left:
        if c not in cur.columns:
            cur = cur.withColumn(c, lit(None).cast("string"))

    # Si table mapping manquante, créer colonnes NULL et retourner
    required_right = {"desti_isic", "cdnaf08", "cdnaf03", "cdtre", "cdisic"}
    if df_cht is None or not required_right.issubset(set(df_cht.columns)):
        out = cur
        for cname in ["cdnaf08_const_c", "cdnaf03_const_c", "cdtre_const_c", "cdisic_const_c"]:
            if cname not in out.columns:
                out = out.withColumn(cname, lit(None).cast("string"))
        return out

    # Préparation table référence
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

    # Condition jointure exacte
    join_cond = (
        (col("l.desti_isic") == col("r.desti_isic")) &
        (col("l.cmarch") == lit("6")) &
        (col("l.cdnatp") == lit("C"))
    )

    joined = left.join(right, join_cond, how="left")

    # Utilisation SELECT explicite
    out = (
        joined
        .select(
            "l.*",  # Préserve TOUTES les colonnes de gauche
            when(col("r_cdnaf08").isNotNull(), col("r_cdnaf08")).otherwise(lit(None).cast("string")).alias("cdnaf08_const_c"),
            when(col("r_cdnaf03").isNotNull(), col("r_cdnaf03")).otherwise(lit(None).cast("string")).alias("cdnaf03_const_c"),
            when(col("r_cdtre").isNotNull(), col("r_cdtre")).otherwise(lit(None).cast("string")).alias("cdtre_const_c"),
            when(col("r_cdisic").isNotNull(), col("r_cdisic")).otherwise(lit(None).cast("string")).alias("cdisic_const_c")
        )
    )

    return out


def finalize_isic_codes(df: DataFrame) -> DataFrame:
    """
    Section 4 — Finalisation des codes ISIC.

    Applique les règles d'écrasement finales basées sur le type de contrat (ACT/CHT)
    en utilisant les colonnes *_CONST_R et *_CONST_C.

    Produit les colonnes FINAL :
        cdnaf2008
        cdnaf
        cdtre
        destinat_isic
        isic_code
        origine_isic
        isic_code_sui

    Supprime les colonnes TEMP ensuite.
    """

    cur = df

    # S'assurer que les colonnes requises existent
    temp_cols = [
        "cdnaf2008_temp", "cdnaf03_temp", "cdtre_temp",
        "isic_code_temp", "origine_isic_temp", "desti_isic_temp",
        "isic_code_sui_temp"
    ]
    for c in temp_cols:
        if c not in cur.columns:
            cur = cur.withColumn(c, lit(None).cast("string"))

    # Helper: CONST_R/C existent ?
    for c in [
        "cdnaf08_const_r", "cdnaf03_const_r", "cdtre_const_r", "cdisic_const_r",
        "cdnaf08_const_c", "cdnaf03_const_c", "cdtre_const_c", "cdisic_const_c"
    ]:
        if c not in cur.columns:
            cur = cur.withColumn(c, lit(None).cast("string"))

    # Initialiser _temp colonnes depuis colonnes existantes
    if "cdnaf" in cur.columns:
        cur = cur.withColumn("cdnaf_temp", col("cdnaf"))
    else:
        cur = cur.withColumn("cdnaf_temp", lit(None).cast("string"))

    if "cdtre" in cur.columns:
        cur = cur.withColumn("cdtre_temp", col("cdtre"))
    else:
        cur = cur.withColumn("cdtre_temp", lit(None).cast("string"))

    # ==============================
    # Contrats Activité (CDNATP='R')
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
            "cdnaf_temp",   # Équivalent à l'assignation CDNAF initiale
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
    # Contrats Chantier (CDNATP='C')
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

    # Construire champs de sortie finaux
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

    # Supprimer colonnes TEMP
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
    Section 5 — Jointure des grades de risque basés sur le code ISIC final.

    Jointure hazard grades utilisant :
        LEFT JOIN df_hazard ON df_hazard.isic_code = df.isic_code

    Produit les colonnes TEMP :
        hazard_grades_fire_temp
        hazard_grades_bi_temp
        hazard_grades_rca_temp
        hazard_grades_rce_temp
        hazard_grades_trc_temp
        hazard_grades_rcd_temp
        hazard_grades_do_temp

    Notes :
      - Si table hazard manquante, créer colonnes TEMP à NULL.
      - La jointure utilise l'ISIC code FINAL.
    """

    # Si référence hazard manquante, créer colonnes vides
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

    # Préparation table référence hazard
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

    # Condition jointure : isic_code final
    join_cond = col("l.isic_code") == col("h.isic_code")

    joined = left.join(right, join_cond, how="left")

    # Utilisation SELECT explicite
    out = (
        joined
        .select(
            "l.*",  # Préserve TOUTES les colonnes de gauche
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
    Nettoyage final — suppression de toutes les colonnes ISIC temporaires et intermédiaires.

    Supprimés :
      - *_CONST_R
      - *_CONST_C
      - *_TEMP
      - DESTI_ISIC (Garde DESTINAT_ISIC à la place)
      - colonnes aides de jointure

    Liste répliquée exactement.
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

        # DESTI_ISIC (brut chantier — sortie finale utilise DESTINAT_ISIC)
        "desti_isic",
    ]

    cur = df
    for c in to_drop:
        if c in cur.columns:
            cur = cur.drop(c)

    return cur

# ---------------------------------------------------------------------------
# Helpers Internes
# ---------------------------------------------------------------------------

def load_sui_table(spark, config, vision: str, logger=None):
    """
    Charge la table IRD_SUIVI_ENGAGEMENTS pour la vision donnée.
    Reproduit le comportement : si la vision n'existe pas, on peut fallback à 'ref'.

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
            logger.info(f"[ISIC] SUI désactivé pour vision {vision} (< 202103)")
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

    # Colonnes nécessaires
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
    Colonnes nécessaires :
      - actprin
      - cdnaf08
      - cdnaf03
      - cdtre
      - cdisic
    """
    from utils.processor_helpers import get_bronze_reader
    from pyspark.sql.functions import col

    reader = get_bronze_reader(SimpleNamespace(spark=spark, config=config, logger=logger))

    # Essayer vision -> fallback ref
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
        return None

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
    Flags Berlioz/Partenariat d'après NOINT.
    """
    if "noint" not in df.columns:
        return df.withColumn("top_berlioz", lit(0)).withColumn("top_partenariat", lit(0))

    df = df.withColumn("top_berlioz", when(col("noint") == "4A5766", lit(1)).otherwise(lit(0)))
    df = df.withColumn("top_partenariat", when(col("noint").isin(["4A6160", "4A6947", "4A6956"]), lit(1)).otherwise(lit(0)))
    return df