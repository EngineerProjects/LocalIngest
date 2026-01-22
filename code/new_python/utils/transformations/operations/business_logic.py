# -*- coding: utf-8 -*-
"""
Business logic transformations for Construction Data Pipeline.

This module provides:
- Capital extraction from labeled capital fields (AZ side, LBCAPI*/MTCAPI*)
- Movement indicators:
    * AZ:    calculate_az_movements (AFN, RES, RPT, RPC, NBPTF)
    * AZEC:  calculate_azec_movements (NBAFN, NBRES, NBPTF)
- Exposure calculations (YTD, GLI) with a column mapping (AZ / AZEC)
- Suspension calculation (AZEC only)

Design notes
------------
- AZ and AZEC movement logics are different and MUST be separate functions.
- Date literals are always converted to DateType (SAS uses numeric dates).
- No reliance on eval() outside controlled, explicit transformations.
- All outputs use lowercase column names to ensure consistency.
"""

from typing import Dict, Any, List, Optional
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import (
    col, when, lit, coalesce, upper, greatest, least, to_date,
    year as year_func, month as month_func, datediff, date_add, date_sub
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_date_lit(d: str):
    """Return a typed date literal (yyyy-MM-dd) as a PySpark Column."""
    return to_date(lit(d), 'yyyy-MM-dd')

def _get_date_columns_from_context(dates: Dict[str, str]) -> Dict[str, Any]:
    """
    Normalize date keys coming from helpers.compute_date_ranges() (lowercase).
    Returns typed Spark date columns matching SAS naming.
    """
    return {
        "DTFIN":    _to_date_lit(dates["dtfin"]),
        "DTDEB_AN": _to_date_lit(dates["dtdeb_an"]),
        "DTFINMN":  _to_date_lit(dates["dtfinmn"]),
        "DTFINMN1": _to_date_lit(dates["dtfinmm1"]),
    }

# ---------------------------------------------------------------------------
# Capitals (AZ – LBCAPI/MTCAPI scanning via config)
# ---------------------------------------------------------------------------

def extract_capitals(df: DataFrame, config: Dict[str, Dict]) -> DataFrame:
    """
    Extract capital amounts scanning labeled fields (LBCAPIk / MTCAPIk) as in SAS.

    The config format (per target column):
    {
      "perte_exp": {
        "keywords": ["PERTE EXPLOITATION", "PERTES D'EXPLOITATION", ...],
        "exclude_keywords": [],
        "label_prefix": "lbcapi",
        "amount_prefix": "mtcapi",
        "num_indices": 14
      },
      ...
    }

    SAS behavior ("last write wins"):
    - Iterate k = 1..14. If a label matches the keywords (case-insensitive)
      and does not match any excluded keyword, assign MTCAPIk to the target.
    - The last matching k overwrites previous results.
    """
    for target_col, extraction_config in config.items():
        keywords = extraction_config['keywords']
        exclude_keywords = extraction_config.get('exclude_keywords', [])
        label_prefix = extraction_config['label_prefix'].lower()
        amount_prefix = extraction_config['amount_prefix'].lower()
        num_indices = int(extraction_config['num_indices'])

        # Default (SAS initializes to 0)
        result_expr = lit(0.0).cast(DoubleType())

        for i in range(1, num_indices + 1):
            label_col = f"{label_prefix}{i}"
            amount_col = f"{amount_prefix}{i}"
            if label_col not in df.columns or amount_col not in df.columns:
                continue

            kw_exprs = [upper(col(label_col)).contains(kw.upper()) for kw in keywords]
            if not kw_exprs:
                continue
            match_expr = reduce(lambda a, b: a | b, kw_exprs)

            if exclude_keywords:
                ex_exprs = [upper(col(label_col)).contains(ek.upper()) for ek in exclude_keywords]
                match_expr = match_expr & (~reduce(lambda a, b: a | b, ex_exprs))

            # "last write wins"
            result_expr = when(match_expr, coalesce(col(amount_col), lit(0.0))).otherwise(result_expr)

        df = df.withColumn(target_col.lower(), result_expr)

    return df


# ---------------------------------------------------------------------------
# Movements — AZ
# ---------------------------------------------------------------------------

def calculate_az_movements(
    df: DataFrame,
    dates: Dict[str, str],
    annee: int,
    column_mapping: Optional[Dict[str, str]] = None
) -> DataFrame:
    """
    Calculate AZ movement indicators (AFN, RES, RPT, RPC, NBPTF) exactly as SAS az_mvt_ptf.

    SAS references (Step 5):
      - NBPTF: CSSSEG != '5' AND CDNATP in ('R','O')
               AND ( (cdsitp='1' AND dtcrepol <= DTFIN) OR (cdsitp='3' AND dtresilp > DTFIN) )
      - AFN:
          (DTDEB_AN <= dteffan <= DTFIN AND DTDEB_AN <= dttraan <= DTFIN)
        OR (DTDEB_AN <= dtcrepol <= DTFIN)
        OR (year(dteffan) < year(DTFIN) AND DTDEB_AN <= dttraan <= DTFIN)
      - RES (exclude chantiers):
          CDNATP != 'C' AND cdsitp='3' AND
          ( (DTDEB_AN <= dtresilp <= DTFIN)
            OR (DTDEB_AN <= dttraar <= DTFIN AND dtresilp <= DTFIN) )
      - RPC (replaced):
          NBRES=1 + RP on any cdtypli* in year(DTFIN)
      - RPT (replacing):
          NBAFN=1 + RE on any cdtypli* in year(DTFIN)

    Preconditions:
      - 'primeto' exists. If not, will compute when 'mtprprto' and 'tx' present.
    """
    # Column defaults (AZ schema)
    creation_date   = "dtcrepol"
    effective_date  = "dteffan"
    termination_date= "dtresilp"
    transfer_start  = "dttraan"
    transfer_end    = "dttraar"
    type_col1, type_col2, type_col3 = "cdtypli1", "cdtypli2", "cdtypli3"
    type_date1, type_date2, type_date3 = "dttypli1", "dttypli2", "dttypli3"

    # Map overrides if provided
    if column_mapping:
        creation_date   = column_mapping.get('creation_date',   creation_date)
        effective_date  = column_mapping.get('effective_date',  effective_date)
        termination_date= column_mapping.get('termination_date',termination_date)
        transfer_start  = column_mapping.get('transfer_start',  transfer_start)
        transfer_end    = column_mapping.get('transfer_end',    transfer_end)
        type_col1       = column_mapping.get('type_col1',       type_col1)
        type_col2       = column_mapping.get('type_col2',       type_col2)
        type_col3       = column_mapping.get('type_col3',       type_col3)
        type_date1      = column_mapping.get('type_date1',      type_date1)
        type_date2      = column_mapping.get('type_date2',      type_date2)
        type_date3      = column_mapping.get('type_date3',      type_date3)

    # Ensure lowercase columns exist
    for c in [creation_date, effective_date, termination_date, transfer_start, transfer_end,
              type_col1, type_col2, type_col3, type_date1, type_date2, type_date3]:
        # Create missing columns as NULL to avoid crashes (defensive)
        if c and c not in df.columns:
            df = df.withColumn(c, lit(None))

    # Dates
    d = _get_date_columns_from_context(dates)
    dtfin, dtdeb_an = d["DTFIN"], d["DTDEB_AN"]

    # Ensure primeto
    if "primeto" not in df.columns:
        if "mtprprto" in df.columns and "tx" in df.columns:
            df = df.withColumn("primeto", col("mtprprto") * (lit(1.0) - (col("tx") / 100.0)))
        else:
            df = df.withColumn("primeto", lit(0.0))

    # Initialize outputs if missing
    for c in ['nbafn','nbres','nbrpt','nbrpc','nbptf','primes_afn','primes_res','primes_rpt','primes_rpc','primes_ptf']:
        if c not in df.columns:
            df = df.withColumn(c, lit(0))

    # --- NBPTF ---
    nbptf_cond = (
        ~col('cssseg').isin(['5']) &  # Explicit NULL-safe (excludes NULLs)
        (col('cdnatp').isin('R', 'O')) &
        (
            ((col('cdsitp') == '1') & (col(creation_date) <= dtfin)) |
            ((col('cdsitp') == '3') & (col(termination_date) >  dtfin))
        )
    )
    df = df.withColumn("nbptf", when(nbptf_cond, lit(1)).otherwise(lit(0))) \
           .withColumn("primes_ptf", when(col("nbptf") == 1, col("primeto")).otherwise(lit(0)))

    # --- AFN ---
    afn_cond = (
        (
            (col(effective_date).isNotNull()) &
            (col(transfer_start).isNotNull()) &
            (col(effective_date) >= dtdeb_an) & (col(effective_date) <= dtfin) &
            (col(transfer_start) >= dtdeb_an) & (col(transfer_start) <= dtfin)
        ) |
        (
            (col(creation_date).isNotNull()) &
            (col(creation_date) >= dtdeb_an) & (col(creation_date) <= dtfin)
        ) |
        (
            (col(effective_date).isNotNull()) &
            (year_func(col(effective_date)) < year_func(dtfin)) &
            (col(transfer_start).isNotNull()) &
            (col(transfer_start) >= dtdeb_an) & (col(transfer_start) <= dtfin)
        )
    )
    df = df.withColumn("nbafn", when(afn_cond, lit(1)).otherwise(lit(0))) \
           .withColumn("primes_afn", when(col("nbafn") == 1, col("primeto")).otherwise(lit(0)))

    # --- RES (exclude chantiers) ---
    res_cond = (
        ~col("cdnatp").isin(['C']) &  # Explicit NULL-safe (excludes NULLs)
        (col("cdsitp") == "3") &
        (
            ((col(termination_date).isNotNull()) &
             (col(termination_date) >= dtdeb_an) & (col(termination_date) <= dtfin)) |
            ((col(transfer_end).isNotNull()) &
             (col(transfer_end) >= dtdeb_an) & (col(transfer_end) <= dtfin) &
             (col(termination_date).isNotNull()) & (col(termination_date) <= dtfin))
        )
    )
    df = df.withColumn("nbres", when(res_cond, lit(1)).otherwise(lit(0))) \
           .withColumn("primes_res", when(col("nbres") == 1, col("primeto")).otherwise(lit(0)))

    # --- RPC (replaced) ---
    rpc_cond = (col("nbres") == 1) & (
        ((col(type_col1) == "RP") & (year_func(col(type_date1)) == year_func(dtfin))) |
        ((col(type_col2) == "RP") & (year_func(col(type_date2)) == year_func(dtfin))) |
        ((col(type_col3) == "RP") & (year_func(col(type_date3)) == year_func(dtfin)))
    )
    df = df.withColumn("nbrpc", when(rpc_cond, lit(1)).otherwise(lit(0))) \
           .withColumn("primes_rpc", when(col("nbrpc") == 1, col("primeto")).otherwise(lit(0))) \
           .withColumn("nbres", when(col("nbrpc") == 1, lit(0)).otherwise(col("nbres"))) \
           .withColumn("primes_res", when(col("nbrpc") == 1, lit(0)).otherwise(col("primes_res")))

    # --- RPT (replacing) ---
    rpt_cond = (col("nbafn") == 1) & (
        ((col(type_col1) == "RE") & (year_func(col(type_date1)) == year_func(dtfin))) |
        ((col(type_col2) == "RE") & (year_func(col(type_date2)) == year_func(dtfin))) |
        ((col(type_col3) == "RE") & (year_func(col(type_date3)) == year_func(dtfin)))
    )
    df = df.withColumn("nbrpt", when(rpt_cond, lit(1)).otherwise(lit(0))) \
           .withColumn("primes_rpt", when(col("nbrpt") == 1, col("primeto")).otherwise(lit(0))) \
           .withColumn("nbafn", when(col("nbrpt") == 1, lit(0)).otherwise(col("nbafn"))) \
           .withColumn("primes_afn", when(col("nbrpt") == 1, lit(0)).otherwise(col("primes_afn")))

    return df


# Backward-compatibility alias (AZ processors may import calculate_movements)
calculate_movements = calculate_az_movements


# ---------------------------------------------------------------------------
# Movements — AZEC
# ---------------------------------------------------------------------------

def calculate_azec_movements(
    df: DataFrame,
    dates: Dict[str, str],
    annee: int,
    mois: int
) -> DataFrame:
    """
    Calculate AZEC-specific movement indicators (NBAFN, NBRES, NBPTF) per SAS azec_mvt_ptf.

    Key differences vs AZ:
      - Product-specific behavior (product list)
      - Date fields: effetpol/datafn/datfin/datresil
      - Uses NBPTF_NON_MIGRES_AZEC
    """
    from config.variables import AZEC_PRODUIT_LIST

    d = _get_date_columns_from_context(dates)
    dtdeb_an, dtfinmn = d["DTDEB_AN"], d["DTFINMN"]

    # Initialize
    for c in ["nbafn", "nbres", "nbptf"]:
        if c not in df.columns:
            df = df.withColumn(c, lit(0))

    # Base conditions
    base_afn = (
        (col("etatpol") == "R") &
        (~col("produit").isin("CNR", "DO0")) &
        (col("nbptf_non_migres_azec") == 1)
    )
    base_res = (
        (col("etatpol") == "R") &
        (~col("produit").isin("CNR", "DO0")) &
        (col("nbptf_non_migres_azec") == 1)
    )

    # AFN — produits spéciaux vs autres
    afn_in_list = (
        col("produit").isin(AZEC_PRODUIT_LIST) &
        (month_func(col("datafn")) <= lit(mois)) &
        (year_func(col("datafn")) == lit(annee))
    )
    afn_not_in_list = (
        ~col("produit").isin(AZEC_PRODUIT_LIST) &
        (
            (
                col("effetpol").isNotNull() & col("datafn").isNotNull() &
                (col("effetpol") >= dtdeb_an) & (col("effetpol") <= dtfinmn) &
                (col("datafn")   <= dtfinmn)
            ) |
            (
                col("effetpol").isNotNull() & col("datafn").isNotNull() &
                (col("effetpol") <  dtdeb_an) &
                (col("datafn")   >= dtdeb_an) & (col("datafn") <= dtfinmn)
            )
        )
    )
    df = df.withColumn("nbafn", when(base_afn & (afn_in_list | afn_not_in_list), lit(1)).otherwise(lit(0)))

    # RES — produits spéciaux vs autres
    res_in_list = (
        col("produit").isin(AZEC_PRODUIT_LIST) &
        (month_func(col("datresil")) <= lit(mois)) &
        (year_func(col("datresil")) == lit(annee))
    )
    res_not_in_list = (
        ~col("produit").isin(AZEC_PRODUIT_LIST) &
        (
            (
                col("datfin").isNotNull() & col("datresil").isNotNull() &
                (col("datfin")    >= dtdeb_an) & (col("datfin")    <= dtfinmn) &
                (col("datresil")  <= dtfinmn)
            ) |
            (
                col("datfin").isNotNull() & col("datresil").isNotNull() &
                (col("datfin")    <= dtfinmn) &
                (col("datresil")  >= dtdeb_an) & (col("datresil") <= dtfinmn)
            )
        )
    )
    df = df.withColumn("nbres", when(base_res & (res_in_list | res_not_in_list), lit(1)).otherwise(lit(0)))

    # NBPTF — actif à fin de mois, hors DO0/TRC/CTR/CNR (SAS)
    nbptf_cond = (
        (col("nbptf_non_migres_azec") == 1) &
        col("effetpol").isNotNull() & (col("effetpol") <= dtfinmn) &
        col("datafn").isNotNull()   & (col("datafn")   <= dtfinmn) &
        (
            col("datfin").isNull() |
            (col("datfin")   > dtfinmn) |
            (col("datresil") > dtfinmn)
        ) &
        (
            (col("etatpol") == "E") |
            ((col("etatpol") == "R") & (col("datfin") >= dtfinmn))
        ) &
        (~col("produit").isin("DO0", "TRC", "CTR", "CNR"))
    )
    df = df.withColumn("nbptf", when(nbptf_cond, lit(1)).otherwise(lit(0)))

    return df

# ---------------------------------------------------------------------------
# Exposures (AZ / AZEC via mapping)
# ---------------------------------------------------------------------------

def calculate_exposures(
    df: DataFrame,
    dates: Dict[str, str],
    annee: int,
    column_mapping: Dict[str, str]
) -> DataFrame:
    """
    Compute expo_ytd and expo_gli using SAS-like inclusive windows.

    Mapping provides:
      - creation_date: dtcrepol (AZ) or effetpol (AZEC)
      - termination_date: dtresilp (AZ) or datfin (AZEC)

    YTD:
      expo_ytd = max(0, (min(term, DTFIN) - max(crea, DTDEB_AN) + 1)) / (Dec31(annee) - DTDEB_AN + 1)
    GLI (current month):
      expo_gli = max(0, (min(term, DTFIN) - max(crea, DTFINMN1+1) + 1)) / (DTFIN - DTFINMN1)

    The selection perimeter matches SAS WHERE conditions used for exposure scopes.
    """
    creation_date = column_mapping['creation_date']
    termination_date = column_mapping['termination_date']

    # Dates
    d = _get_date_columns_from_context(dates)
    dtfin, dtdeb_an, dtfinmn, dtfinmn1 = d["DTFIN"], d["DTDEB_AN"], d["DTFINMN"], d["DTFINMN1"]
    dtdeb_mois = date_add(dtfinmn1, 1)

    # Exposure perimeter (SAS-like). This clause is AZ-oriented but safe for AZEC if mapping matches.
    expo_where_cond = lit(True)
    if 'cdnatp' in df.columns and 'cdsitp' in df.columns:
        branch1 = (
            (col('cdnatp').isin('R','O')) &
            (
                ((col('cdsitp') == '1') & (col(creation_date) <= dtfinmn)) |
                ((col('cdsitp') == '3') & (col(termination_date) >  dtfinmn))
            )
        )
        branch2 = (
            (col('cdsitp').isin('1','3')) &
            (coalesce(col('cdnatp'), lit('X')) != 'F') &
            (
                ((col(creation_date) <= dtfinmn) &
                 (col(termination_date).isNull() |
                  ((col(termination_date) >= dtfinmn1) & (col(termination_date) < dtfinmn)))) |
                ((col(creation_date) <= dtfinmn) & (col(termination_date) > dtfinmn)) |
                ((col(creation_date) >  dtfinmn1) & (col(creation_date) <= dtfinmn) &
                 (col(termination_date) >= dtfinmn1))
            )
        )
        expo_where_cond = branch1 | branch2

    # Denominators
    year_end = _to_date_lit(f"{annee}-12-31")
    denom_ytd = (datediff(year_end, dtdeb_an) + 1)  # inclusive
    denom_gli = datediff(dtfin, dtfinmn1)           # days in current month

    # Numerators
    num_ytd = (datediff(least(col(termination_date), dtfin), greatest(col(creation_date), dtdeb_an)) + 1)
    num_gli = (datediff(least(col(termination_date), dtfin), greatest(col(creation_date), dtdeb_mois)) + 1)

    df = df.withColumn(
        "expo_ytd",
        when(
            expo_where_cond &
            (col(creation_date) <= dtfin) &
            (col(termination_date) >= dtdeb_an),
            (when(num_ytd > 0, num_ytd).otherwise(lit(0))) / denom_ytd
        ).otherwise(lit(0.0))
    ).withColumn(
        "dt_deb_expo",
        greatest(col(creation_date), dtdeb_an)
    ).withColumn(
        "dt_fin_expo",
        when(col(termination_date).isNotNull() & (col(termination_date) < dtfin), col(termination_date))
        .otherwise(dtfin)
    ).withColumn(
        "expo_gli",
        when(
            expo_where_cond &
            (col(creation_date) <= dtfin) &
            (col(termination_date) >= dtdeb_mois) &
            (denom_gli > 0),
            (when(num_gli > 0, num_gli).otherwise(lit(0))) / denom_gli
        ).otherwise(lit(0.0))
    )

    return df

def calculate_exposures_azec(
    df: DataFrame,
    dates: Dict[str, str]
) -> DataFrame:
    """
    Compute AZEC exposures (YTD, GLI) exactly like SAS Step 5 (no perimeter).
      - expo_ytd = max(0, (min(datfin, DTFIN) - max(effetpol, DTDEB_AN) + 1)) / (Dec31(annee) - DTDEB_AN + 1)
      - expo_gli = max(0, (min(datfin, DTFIN) - max(effetpol, DTFINMN1 + 1) + 1)) / (DTFIN - DTFINMN1)
      - dt_deb_expo = max(effetpol, DTDEB_AN)
      - dt_fin_expo = min(datfin, DTFIN)
    """
    d = _get_date_columns_from_context(dates)
    dtfin, dtdeb_an, dtfinmn, dtfinmn1 = d["DTFIN"], d["DTDEB_AN"], d["DTFINMN"], d["DTFINMN1"]
    dtdeb_mois = date_add(dtfinmn1, 1)

    # Dénominateurs (SAS : inclusif YTD)
    year_end = _to_date_lit(str(int(dates['dtdeb_an'][:4]) + 0) + "-12-31")  # annee = dtdeb_an.year
    denom_ytd = (datediff(year_end, dtdeb_an) + 1)
    denom_gli = datediff(dtfin, dtfinmn1)  # nb jours dans le mois courant

    # Numérateurs (inclusif +1)
    num_ytd = (datediff(least(col("datfin"), dtfin), greatest(col("effetpol"), dtdeb_an)) + 1)
    num_gli = (datediff(least(col("datfin"), dtfin), greatest(col("effetpol"), dtdeb_mois)) + 1)

    df = df.withColumn(
        "expo_ytd",
        when(
            (col("effetpol").isNotNull()) & (col("datfin").isNotNull()),
            (when(num_ytd > 0, num_ytd).otherwise(lit(0))) / denom_ytd
        ).otherwise(lit(0.0))
    ).withColumn(
        "dt_deb_expo",
        greatest(col("effetpol"), dtdeb_an)
    ).withColumn(
        "dt_fin_expo",
        least(col("datfin"), dtfin)
    ).withColumn(
        "expo_gli",
        when(
            (col("effetpol").isNotNull()) & (col("datfin").isNotNull()) & (denom_gli > 0),
            (when(num_gli > 0, num_gli).otherwise(lit(0))) / denom_gli
        ).otherwise(lit(0.0))
    )

    return df

# ---------------------------------------------------------------------------
# Suspension — AZEC
# ---------------------------------------------------------------------------

def calculate_azec_suspension(df: DataFrame, dates: Dict[str, str]) -> DataFrame:
    """
    Calculate nbj_susp_ytd (suspension days) for AZEC (SAS Step 5).
    CASE
      WHEN (DTDEBN <= datresil <= DTFINMN) OR (DTDEBN <= datfin <= DTFINMN)
        THEN min(datfin, DTFINMN, datexpir) - max(DTDEBN-1, datresil-1)
      WHEN (datresil <= DTDEBN) AND (datfin >= DTFINMN)
        THEN (DTFINMN - DTDEBN + 1)
      ELSE 0
    END
    """
    dtdebn = _to_date_lit(dates['dtdebn'])
    dtfinmn = _to_date_lit(dates['dtfinmn'])

    cond1 = (
        ((col("datresil") >= dtdebn) & (col("datresil") <= dtfinmn)) |
        ((col("datfin")   >= dtdebn) & (col("datfin")   <= dtfinmn))
    )
    susp_days_1 = datediff(
        least(col("datfin"), dtfinmn, col("datexpir")),
        greatest(date_sub(dtdebn, 1), date_sub(col("datresil"), 1))
    )

    cond2 = (
        col("datresil").isNotNull() &
        (col("datresil") <= dtdebn) &
        (col("datfin")   >= dtfinmn)
    )
    susp_days_2 = datediff(dtfinmn, dtdebn) + 1

    return df.withColumn(
        "nbj_susp_ytd",
        when(cond1, susp_days_1).when(cond2, susp_days_2).otherwise(lit(0))
    )