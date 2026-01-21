
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
    lob_small = (
        lob_ref
        .select("produit", "cdprod", "cmarch", "segment", "lssseg")
        .dropDuplicates(["produit"])
    )

    merged = (
        merged.alias("c")
        .join(
            broadcast(lob_small).alias("l"),
            col("c.produit") == col("l.produit"),
            "left"
        )
        .select(
            col("c.police"), col("c.produit"),
            col("c.typmarc1"), col("c.ltypmar1"),
            col("c.formule"),  col("c.nat_cnt"),
            col("l.cdprod"),   col("l.segment"),
            col("l.lssseg"),   col("l.cmarch")
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
    Compute ACTIVITE exactly like SAS, but concisely and in a single pass.
    Priority/order is preserved (top to bottom).
    """
    is_rba_rcd = col("produit").isin("RBA", "RCD")

    pairs = [
        # RBA/RCD cases
        (is_rba_rcd & (col("typmarc1") == "01"), "ARTISAN"),
        (is_rba_rcd & (col("typmarc1").isin("02", "03", "04") |
                       ((col("typmarc1") == "") & col("formule").isin("DA", "DB", "DC"))), "ENTREPRISE"),
        (is_rba_rcd & (col("typmarc1").isin("05") |
                       ((col("typmarc1") == "") & (col("formule") == "DD"))), "M. OEUVRE"),
        (is_rba_rcd & (col("typmarc1").isin("06", "14") |
                       ((col("typmarc1") == "") & (col("formule") == "DE"))), "FABRICANT"),
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

    # Coalesce each target (prefer left if exists; otherwise right; otherwise NULL)
    targets = ["typmarc1", "nat_cnt", "activite", "cdprod", "segment", "lssseg"]
    out = joined

    # De-qualify left columns first (Spark often qualifies with 'l.'/'r.')
    # Safely map 'l.col' → 'col' if duplication does not happen
    for c in out.columns:
        if c.startswith("l."):
            base = c.split(".", 1)[1]
            if base not in out.columns:
                out = out.withColumnRenamed(c, base)

    # Now coalesce with right-side values
    for tgt in targets:
        r_col = f"r.{tgt}"
        left_has = tgt in out.columns
        right_has = r_col in out.columns
        if left_has and right_has:
            out = out.withColumn(tgt, coalesce(col(tgt), col(r_col)))
        elif (not left_has) and right_has:
            out = out.withColumn(tgt, col(r_col))
        elif not left_has and not right_has:
            out = out.withColumn(tgt, lit(None))

    # Drop remaining 'r.*' columns and any leftover 'l.*'
    drop_cols = [c for c in out.columns if c.startswith("r.") or c.startswith("l.")]
    if drop_cols:
        out = out.drop(*drop_cols)

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


def enrich_segmentation_sas(df, spark, config, vision, logger=None):
    """
    Full SAS-faithful segmentation pipeline:
      1) LOB join
      2) Load CONSTRCU reference
      3) Join CONSTRCU
      4) Compute TYPE_PRODUIT (SAS)
      5) CSSSEG SAS corrections
      6) Segment_3 SAS computation
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

    return df
