"""
Column-level transformation utilities.

This module handles:
- Column passthrough selection (SAS-style projection)
- Optional renaming
- Simple computed columns (safe operations only)
- Column initialization with types
- Metadata injection (vision, year, month)

Important:
This file intentionally does NOT support complex expressions,
date arithmetic, nested logic, or any operation that depends
on SAS UPDATE ordering. Those MUST remain inside PySpark code,
not configuration files.

All output column names remain lowercase.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, coalesce, to_date
from pyspark.sql.types import DataType


# =========================================================
# Public API
# =========================================================

def lowercase_all_columns(df: DataFrame) -> DataFrame:
    """
    Lowercase all DataFrame column names.

    Notes:
        SAS is case-insensitive. Spark is not.
        To ensure consistency across the pipeline,
        all columns are normalized to lowercase.
    """
    return df.select([col(c).alias(c.lower()) for c in df.columns])


def apply_column_config(
    df: DataFrame,
    config: dict,
    vision: str = None,
    annee: int = None,
    mois: int = None
) -> DataFrame:
    """
    Apply a structured column selection and initialization configuration.

    Supported actions:
    - Passthrough: keep columns present in the DataFrame
    - Renaming: simple old_name -> new_name mapping
    - Computed columns: only very simple operations (coalesce, default)
    - Init columns: create missing columns with default typed values
    - Metadata: add VISION, EXEVUE, MOISVUE as SAS does

    This function explicitly avoids:
        - any eval()
        - any complex condition parsing
        - any unsafe expression evaluation
        - any multi-column arithmetic logic

    Parameters
    ----------
    df : DataFrame
        Input DataFrame with lowercase column names.
    config : dict
        Section from transformations JSON.
    vision : str
        YYYYMM string.
    annee : int
        Year extracted from vision.
    mois : int
        Month extracted from vision.

    Returns
    -------
    DataFrame
        Transformed DataFrame with selected / initialized columns.
    """

    select_exprs = []

    # ---------------------------
    # 2. Renamed columns
    # ---------------------------
    rename_map = {old.lower(): new.lower() for old, new in config.get("rename", {}).items()}

    for orig_col in df.columns:
        lc = orig_col.lower()
        if lc in rename_map:
            select_exprs.append(col(orig_col).alias(rename_map[lc]))
        else:
            select_exprs.append(col(orig_col))

    # ---------------------------
    # 3. Computed columns (safe)
    # ---------------------------
    for col_name, comp_cfg in config.get("computed", {}).items():
        expr = _safe_computed_expression(df, comp_cfg)
        select_exprs.append(expr.alias(col_name.lower()))

    # ---------------------------
    # 4. Init columns
    # ---------------------------
    for col_name, (default_val, dtype) in config.get("init", {}).items():
        cname = col_name.lower()
        if isinstance(dtype, DataType):
            select_exprs.append(lit(default_val).cast(dtype).alias(cname))
        else:
            select_exprs.append(lit(default_val).alias(cname))

    # ---------------------------
    # 5. Metadata columns
    # ---------------------------
    if vision is not None:
        select_exprs.append(lit(str(vision)).alias("vision"))
    if annee is not None:
        select_exprs.append(lit(int(annee)).alias("exevue"))
    if mois is not None:
        select_exprs.append(lit(int(mois)).alias("moisvue"))

    # ---------------------------
    # 6. Drop columns
    # ---------------------------
    drop_set = set(d.lower() for d in config.get("drop", []))
    for d in drop_set:
        if d in df.columns:
            df = df.drop(d)

    return df.select(*select_exprs)


# =========================================================
# Internal helpers
# =========================================================

def _safe_computed_expression(df: DataFrame, cfg: dict):
    """
    Build a safe 'computed' column expression.

    Supported types:
    - coalesce_default: coalesce(column, default)
    - constant: literal constant
    - flag_equality: column == value ? 1 : 0

    Unsupported (on purpose):
    - ANY expression requiring eval()
    - ANY arithmetic between columns
    - ANY logical multi-column expression
    - ANY date computation

    SAS performs most of its computed logic inside code,
    not configuration. This function intentionally remains minimal.
    """

    ctype = cfg.get("type", "").lower()

    # -----------------------------------
    # coalesce_default
    # -----------------------------------
    if ctype == "coalesce_default":
        src = cfg["source_col"].lower()
        default = cfg.get("default")
        if src not in df.columns:
            return lit(default)
        return coalesce(col(src), lit(default))

    # -----------------------------------
    # constant
    # -----------------------------------
    if ctype == "constant":
        return lit(cfg.get("value"))

    # -----------------------------------
    # flag equality
    # -----------------------------------
    if ctype == "flag_equality":
        src = cfg["source_col"].lower()
        val = cfg["value"]
        if src not in df.columns:
            return lit(0)
        return (col(src) == val).cast("int")

    # -----------------------------------
    # Unsupported → raise error
    # -----------------------------------
    raise ValueError(
        f"Unsupported computed column type '{ctype}'. "
        f"Complex computed expressions must be implemented in PySpark code."
    )
