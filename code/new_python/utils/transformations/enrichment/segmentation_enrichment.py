from types import SimpleNamespace
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, coalesce, broadcast, first, count as spark_count
from pyspark.sql.types import StringType
from utils.processor_helpers import get_bronze_reader
from config.constants import MARKET_CODE

# ----------------------------------------------------------------------
# Petit utilitaire : construire une expression CASE WHEN à partir de paires (cond, valeur)
# L'ordre compte (le premier qui correspond gagne), donc on plie de bas en haut.
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
    drop_dups_by=("police", "produit")
) -> DataFrame:
    """
    Charge et prépare le bloc de référence CONSTRCU.

    Points clés de la logique métier :
      - SET CONSTRCU + RISTECCU (sélectionner uniquement les colonnes requises)
      - Joindre LOB (cmarch='6'), comportement type HASH (broadcast)
      - Repli typmarc1 depuis ltypmar1
      - nat_cnt pertinent uniquement pour DPC
      - ACTIVITE calculé avec conditions exactes

    Retourne les colonnes :
      police, produit, typmarc1, nat_cnt, activite, cdprod (LOB), segment (LOB), lssseg (LOB)
    """
    reader = get_bronze_reader(SimpleNamespace(spark=spark, config=config, logger=logger))

    # 1) Lire les sources brutes (minuscules assurées par BronzeReader)
    try:
        df_constrcu = reader.read_file_group("constrcu", "ref")
    except Exception as e:
        if logger: logger.debug(f"[SEG] CONSTRCU manquante : {e}")
        df_constrcu = None

    try:
        df_risteccu = reader.read_file_group("risteccu_azec", "ref")
    except Exception as e:
        if logger: logger.debug(f"[SEG] RISTECCU manquante : {e}")
        df_risteccu = None

    if df_constrcu is None and df_risteccu is None:
        raise RuntimeError("Les références CONSTRCU et RISTECCU sont manquantes.")

    # 2) Projeter les colonnes requises et unir comme un SET
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

    # 3) Joindre LOB (cmarch déjà filtré en amont idéalement ; protéger quand même)
    #    Ne prendre que les colonnes dont nous avons réellement besoin.
    #    Définit : CDPROD, CPROD, cmarch, lmarch, lmarch2, cseg, lseg, lseg2,
    #              cssseg, lssseg, lssseg2, lprod, segment
    
    # LOB doit avoir exactement 1 ligne par produit pour éviter l'explosion cartésienne
    # D'abord, sélectionner les colonnes et valider les doublons
    lob_pre_dedup = lob_ref.select(
        "produit", "cdprod", "cprod", "cmarch", "lmarch", 
        "cseg", "lseg", "cssseg", "lssseg", "lprod", "segment"
    )
    
    # Détecter les doublons AVANT de dédupliquer pour lever une erreur si LOB est corrompu
    dup_check = lob_pre_dedup.groupBy("produit").agg(spark_count("*").alias("dup_count"))
    dup_count_total = dup_check.filter(col("dup_count") > 1).count()
    
    if dup_count_total > 0:
        if logger:
            logger.error(f"[SEG] LOB contient {dup_count_total} produits avec doublons !")
            logger.error("[SEG] Top doublons :")
            dup_check.filter(col("dup_count") > 1).orderBy("dup_count", ascending=False).show(20, False)
        
        # Contournement : Utiliser groupBy + first() pour une déduplication déterministe
        if logger:
            logger.warning(f"[SEG] Application de la déduplication déterministe avec first() pour {dup_count_total} produits dupliqués")
    
    # Déduplication sûre : utiliser groupBy + first() au lieu de dropDuplicates
    # C'est déterministe et assure exactement 1 ligne par produit
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
    
    # Validation : assurer exactement 1 ligne par produit
    final_count = lob_small.count()
    unique_produits = lob_pre_dedup.select("produit").distinct().count()
    if final_count != unique_produits:
        if logger:
            logger.error(f"[SEG] Échec de la déduplication LOB : {final_count} lignes vs {unique_produits} produits uniques")
        raise RuntimeError(f"Échec de la vérification déduplication LOB : {final_count} != {unique_produits}")

    merged = (
        merged.alias("c")
        .join(
            broadcast(lob_small).alias("l"),
            col("c.produit") == col("l.produit"),
            "left"
        )
        .select(
            "c.*",  # Préserver TOUTES les colonnes de l'union CONSTRCU/RISTECCU
            col("l.cdprod"),   col("l.cprod"),
            col("l.cmarch"),   col("l.lmarch"),
            col("l.cseg"),     col("l.lseg"),
            col("l.cssseg"),   col("l.lssseg"),
            col("l.lprod"),    col("l.segment")
        )
    )

    # 4) Garder construction uniquement
    merged = merged.filter(col("cmarch") == lit("6"))

    # 5) Repli typmarc1 (ltypmar1 reflète le comportement de repli typmarc7/typmarc)
    merged = merged.withColumn("typmarc1", coalesce(col("typmarc1"), col("ltypmar1")))

    # 6) nat_cnt pertinent uniquement pour DPC (la logique nettoie pour les autres produits)
    merged = merged.withColumn("nat_cnt",
        when(col("produit") == "DPC", col("nat_cnt")).otherwise(lit(""))
    )

    # 7) ACTIVITE — logique exacte, mais exprimée une fois en un seul CASE
    merged = _compute_activite(merged)
    
    # 8) TYPE_PRODUIT — requis pour CONSTRCU_AZEC
    merged_with_lmarch2 = merged.withColumn(
        "lmarch2",
        when(col("cmarch") == "6", lit("6_CONSTRUCTION")).otherwise(lit(None))
    )
    merged = compute_type_produit(merged_with_lmarch2, spark, config, logger)

    # Déduplication finale (par défaut police+produit)
    return merged.dropDuplicates(list(drop_dups_by))


def _compute_activite(df: DataFrame) -> DataFrame:
    """
    Calcule ACTIVITE exactement comme la logique métier, null-safe, en une seule passe.
    La priorité/ordre est préservée (haut vers bas).

    Hypothèse : stratégie all-NULL (toutes valeurs manquantes → NULL, jamais "").
    Donc, on utilise .isNull() au lieu de == "" pour détecter le 'manquant'.
    """
    is_rba_rcd = col("produit").isin("RBA", "RCD")

    pairs = [
        # Cas RBA/RCD
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

        # Remplacement spécial RCD
        ((col("produit") == "RCD") & col("typmarc1").isin("07", "14"), "NEGOCIANT"),

        # Produit DPC (mappage nat_cnt)
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


def join_constrcu(
    df: DataFrame,
    df_constrcu_ref: DataFrame,
    *,
    logger=None
) -> DataFrame:
    """
    Jointure fidèle de la référence CONSTRCU préparée sur le jeu de données AZEC.

    Clés :
      - (police, produit)

    Colonnes apportées de la référence CONSTRCU :
      - typmarc1, nat_cnt, activite
      - cdprod (de réf LOB), segment (de réf LOB), lssseg (de réf LOB)

    Politique d'écrasement :
      - NE PAS écraser les colonnes de gauche existantes. Préférer gauche si présent ; sinon prendre droite.
    """
    if df_constrcu_ref is None or not df_constrcu_ref.columns:
        if logger:
            logger.warning("[SEG] La référence CONSTRCU est vide ou manquante. Jointure ignorée.")
        return df

    # Garder uniquement les colonnes de référence requises et supprimer les doublons de clés
    right_keep = [
        "police", "produit",
        "typmarc1", "nat_cnt", "activite",
        "cdprod", "segment", "lssseg"
    ]
    right_cols = [c for c in right_keep if c in df_constrcu_ref.columns]
    if not {"police", "produit"}.issubset(set(right_cols)):
        if logger:
            logger.warning("[SEG] La référence CONSTRCU manque les clés de jointure 'police'/'produit'. Jointure ignorée.")
        return df

    r = df_constrcu_ref.select(*right_cols).dropDuplicates(["police", "produit"]).alias("r")
    l = df.alias("l")

    joined = l.join(
        r,
        on=[col("l.police") == col("r.police"), col("l.produit") == col("r.produit")],
        how="left"
    )

    # Modèle : SELECT t1.*, t2.col1, t2.col2, ...
    # Nous préservons TOUTES les colonnes de gauche + ajoutons des colonnes spécifiques de droite
    # Colonnes à ajouter de la référence
    ref_cols_to_add = [
        "typmarc1", "nat_cnt", "activite",  # De CONSTRCU
        "cdprod", "cprod", "cmarch", "lmarch", "cseg", "lseg", "cssseg", "lssseg", "lprod", "segment"  # De LOB
    ]
    
    # Construire SELECT : l.* + r.specific_cols (avec coalesce pour conflits)
    select_list = ["l.*"]  # TOUTES colonnes gauche
    
    for col_name in ref_cols_to_add:
        r_col = f"r.{col_name}"
        l_col = f"l.{col_name}"
        
        # Si la colonne existe à droite, l'ajouter (coalesce avec gauche si les deux existent)
        if r_col in joined.columns:
            if l_col in joined.columns:
                # Les deux existent : coalesce (préférer gauche)
                select_list.append(coalesce(col(l_col), col(r_col)).alias(col_name))
            else:
                # Seulement droite existe : prendre droite
                select_list.append(col(r_col).alias(col_name))
        elif l_col in joined.columns:
            # Seulement gauche existe : déjà dans l.*, ignorer (sera inclus via l.*)
            pass
        else:
            # Aucun n'existe : ajouter NULL
            select_list.append(lit(None).cast(StringType()).alias(col_name))
    
    out = joined.select(*select_list)

    if logger:
        logger.info("✓ Référence CONSTRCU jointe (fidèle à la logique métier, clés=(police, produit))")

    return out


def compute_type_produit(
    df: DataFrame,
    spark,
    config,
    logger=None
) -> DataFrame:
    """
    Calcule TYPE_PRODUIT exactement comme la logique métier.

    Logique reproduite :
      1) Essayer de mapper TYPE_PRODUIT depuis Typrd_2 sur ACTIVITE
         (excluant les valeurs ACTIVITE gérées par repli).
      2) Si pas de mappage : repli via LSSSEG :
           - LSSSEG == 'TOUS RISQUES CHANTIERS' -> 'TRC'
           - LSSSEG == 'DOMMAGES OUVRAGES'      -> 'DO'
           - sinon                              -> 'Autres'
      3) Remplacement : si PRODUIT == 'RCC' alors TYPE_PRODUIT = 'Entreprises'
      4) Correction CSSSEG : si LSSSEG == 'RC DECENNALE' et TYPE_PRODUIT == 'Artisans'
           alors CSSSEG = '7'

    Notes :
      - Ne modifie aucun autre champ.
      - Laisse TYPE_PRODUIT comme une nouvelle colonne (string).
      - Crée CSSSEG si manquant (string), puis applique la correction.
    """
    # Assurer que les colonnes requises existent
    base = df
    for c in ["activite", "lssseg", "cssseg", "produit"]:
        if c not in base.columns:
            base = base.withColumn(c, lit(None).cast(StringType()))

    # Essayer de charger le mappage Typrd_2 depuis bronze/ref
    reader = get_bronze_reader(SimpleNamespace(spark=spark, config=config, logger=logger))
    df_map = None
    try:
        df_map = reader.read_file_group("typrd_2", "ref")
    except Exception as e:
        if logger:
            logger.debug(f"[SEG] Typrd_2 non disponible : {e}")
        df_map = None

    # Préparer le mappage si disponible
    mapped = base
    map_col_activite = None
    map_col_typeprod = None

    if df_map is not None and df_map.columns:
        # Normaliser insensible à la casse (BronzeReader déjà minuscule, mais être défensif).
        cols = {c.lower(): c for c in df_map.columns}
        if "activite" in cols and ("type_produit" in cols or "typeproduit" in cols):
            map_col_activite = cols["activite"]
            map_col_typeprod = cols.get("type_produit", cols.get("typeproduit"))

            # Exclure les valeurs ACTIVITE gérées par repli
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

            # LEFT JOIN sur ACTIVITE
            mapped = (
                mapped.alias("a")
                .join(broadcast(df_map_clean).alias("m"),
                      col("a.activite") == col("m.map_activite"),
                      how="left")
                .select("a.*", col("m.map_type_produit"))
            )
        else:
            if logger:
                logger.debug("[SEG] Typrd_2 manque colonnes requises (activite/type_produit). Utilisation repli uniquement.")
            mapped = mapped.withColumn("map_type_produit", lit(None).cast(StringType()))
    else:
        mapped = mapped.withColumn("map_type_produit", lit(None).cast(StringType()))

    # Construire TYPE_PRODUIT avec priorité :
    # 1) utiliser mappage si présent
    # 2) repli via LSSSEG
    type_prod = when(col("map_type_produit").isNotNull(), col("map_type_produit")) \
        .otherwise(
            when(col("lssseg") == "TOUS RISQUES CHANTIERS", lit("TRC"))
            .when(col("lssseg") == "DOMMAGES OUVRAGES", lit("DO"))
            .otherwise(lit("Autres"))
        )

    mapped = mapped.withColumn("type_produit", type_prod)

    # Remplacement pour PRODUIT == 'RCC'
    mapped = mapped.withColumn(
        "type_produit",
        when(col("produit") == "RCC", lit("Entreprises"))
        .otherwise(col("type_produit"))
    )

    # Correction CSSSEG pour RC DECENNALE + Artisans
    if "cssseg" not in mapped.columns:
        mapped = mapped.withColumn("cssseg", lit(None).cast(StringType()))

    mapped = mapped.withColumn(
        "cssseg",
        when((col("lssseg") == "RC DECENNALE") & (col("type_produit") == "Artisans"),
             lit("7")).otherwise(col("cssseg"))
    )

    # Nettoyage temp
    mapped = mapped.drop("map_type_produit")

    if logger:
        logger.info("✓ TYPE_PRODUIT calculé avec mappage Typrd_2 et replis")

    return mapped


def compute_segment3(df: DataFrame, logger=None) -> DataFrame:
    """
    Calcule Segment_3 exactement comme dans la logique métier.

    Logique :
      SI lmarch2 = '6_CONSTRUCTION' ALORS
         SI Type_Produit = 'Artisans' ALORS Segment_3='Artisans'
         SINON SI Type_Produit DANS ('TRC','DO','CNR','Autres Chantiers') ALORS Segment_3='Chantiers'
         SINON Segment_3='Renouvelables hors artisans'
    """
    # Assurer que les colonnes requises existent
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
        logger.info("✓ Segment_3 calculé")

    return df


def apply_cssseg_corrections(df: DataFrame, logger=None) -> DataFrame:
    """
    Applique les règles de correction CSSSEG.
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
        logger.info("✓ Corrections CSSSEG appliquées")

    return df


def enrich_segmentation(df, spark, config, vision, logger=None, return_reference=False):
    """
    Pipeline complet d'enrichissement de segmentation :
      1) Jointure LOB
      2) Chargement référence CONSTRCU
      3) Jointure CONSTRCU
      4) Calcul TYPE_PRODUIT
      5) Corrections CSSSEG
      6) Calcul Segment_3
      
    Args:
        df: DataFrame en entrée
        spark: Session Spark
        config: Dictionnaire de configuration
        vision: Chaîne vision (YYYYMM)
        logger: Logger optionnel
        return_reference: Si True, retourne le tuple (df, df_constrcu_ref) pour mise en cache
    """

    # --------------------------------------------------------------------
    # 1. Jointure LOB (construction uniquement)
    # --------------------------------------------------------------------
    reader = get_bronze_reader(SimpleNamespace(spark=spark, config=config, logger=logger))
    lob_ref = (
        reader.read_file_group("lob", "ref")
        .filter(col("cmarch") == MARKET_CODE.MARKET)
    )

    # --------------------------------------------------------------------
    # 2. Chargement référence CONSTRCU
    # --------------------------------------------------------------------
    df_constrcu_ref = load_constrcu_reference(
        spark=spark,
        config=config,
        vision=vision,
        lob_ref=lob_ref,
        logger=logger
    )

    # --------------------------------------------------------------------
    # 3. Jointure référence CONSTRCU
    # --------------------------------------------------------------------
    df = join_constrcu(df, df_constrcu_ref, logger=logger)
    df = df.withColumn(
        "lmarch2",
        when(col("cmarch") == "6", lit("6_CONSTRUCTION"))
        .otherwise(lit(None))
    )

    # --------------------------------------------------------------------
    # 4. Calcul TYPE_PRODUIT
    # --------------------------------------------------------------------
    df = compute_type_produit(df, spark, config, logger)

    # --------------------------------------------------------------------
    # 5. Corrections CSSSEG
    # --------------------------------------------------------------------
    df = apply_cssseg_corrections(df, logger)

    # --------------------------------------------------------------------
    # 6. Calcul Segment_3
    # --------------------------------------------------------------------
    df = compute_segment3(df, logger)

    if logger:
        logger.info("✓ Enrichissement complet de la segmentation terminé")

    if return_reference:
        return df, df_constrcu_ref
    else:
        return df
