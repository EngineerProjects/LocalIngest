"""
Column transformation operations.

Handles column standardization, renaming, initialization, and computed expressions.
All operations work with lowercase column names.
"""

from pyspark.sql import DataFrame # type: ignore
from pyspark.sql.functions import col, when, lit, coalesce # type: ignore
from typing import Dict, Any


def lowercase_all_columns(df: DataFrame) -> DataFrame:
    """
    Convert all DataFrame columns to lowercase.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with all columns in lowercase

    Example:
        >>> df = spark.read.csv("data.csv", header=True)
        >>> df = lowercase_all_columns(df)
    """
    return df.select([col(c).alias(c.lower()) for c in df.columns])


def apply_column_config(
    df: DataFrame,
    config: Dict[str, Any],
    vision: str = None,
    annee: int = None,
    mois: int = None
) -> DataFrame:
    """
    Apply column configuration from config/variables.py.

    Handles:
      - Passthrough columns (no transformation)
      - Renamed columns (old_name → new_name)
      - Computed columns (expressions from config)
      - Initialized columns (default values with types)
      - Vision metadata columns

    Args:
        df: Input DataFrame (columns already lowercase)
        config: Column configuration dict from variables.py
        vision: Vision string (YYYYMM)
        annee: Year as integer
        mois: Month as integer

    Returns:
        DataFrame with configured columns (all lowercase)

    Example:
        >>> from config.variables import AZ_COLUMN_CONFIG
        >>> df = apply_column_config(df, AZ_COLUMN_CONFIG, '202509', 2025, 9)
    """
    select_exprs = []

    # Passthrough columns
    for c in config.get('passthrough', []):
        if c.lower() in df.columns:
            select_exprs.append(col(c.lower()))

    # Renamed columns
    for old_name, new_name in config.get('rename', {}).items():
        if old_name.lower() in df.columns:
            select_exprs.append(col(old_name.lower()).alias(new_name.lower()))

    # Computed columns
    for col_name, comp_config in config.get('computed', {}).items():
        expr = build_computed_expression(df, comp_config)
        select_exprs.append(expr.alias(col_name.lower()))

    # Initialized columns
    for col_name, (default_val, dtype) in config.get('init', {}).items():
        select_exprs.append(lit(default_val).cast(dtype).alias(col_name.lower()))

    # Vision metadata
    if vision:
        select_exprs.append(lit(vision).alias('vision'))
    if annee is not None:
        select_exprs.append(lit(annee).alias('exevue'))
    if mois is not None:
        select_exprs.append(lit(mois).alias('moisvue'))

    return df.select(*select_exprs)


def build_computed_expression(df: DataFrame, comp_config: Dict[str, Any]):
    """
    Build PySpark expression from config dictionary.

    Supported types:
      - 'coalesce_default': coalesce(col, default)
      - 'flag_equality': when(col == value, 1).otherwise(0)
      - 'flag': when(condition_expr, 1).otherwise(0)
      - 'arithmetic': evaluate arithmetic expression with col() replacements
      - 'constant': lit(value)

    Args:
        df: DataFrame (for column existence checks)
        comp_config: Computation configuration dict

    Returns:
        PySpark Column expression

    Example:
        >>> config = {'type': 'coalesce_default', 'source_col': 'txcede', 'default': 0}
        >>> expr = build_computed_expression(df, config)
    """
    comp_type = comp_config.get('type')

    if comp_type == 'coalesce_default':
        source_col = comp_config['source_col'].lower()
        default = comp_config['default']
        return coalesce(col(source_col), lit(default))

    elif comp_type == 'flag_equality':
        source_col = comp_config['source_col'].lower()
        value = comp_config['value']
        return when(col(source_col) == value, lit(1)).otherwise(lit(0))

    elif comp_type == 'flag':
        # Parse condition expression
        condition_str = comp_config['condition']
        # Replace column names with col() calls
        import re
        condition_work = condition_str
        sorted_cols = sorted(df.columns, key=len, reverse=True)
        for c in sorted_cols:
            pattern = rf'\b{re.escape(c)}\b'
            condition_work = re.sub(pattern, f'col("{c}")', condition_work)
        
        # Replace logical operators
        condition_work = condition_work.replace(' not in ', ' .notin ')  # Temporary
        condition_work = condition_work.replace(' in ', ' .isin ')
        condition_work = condition_work.replace(' .notin ', ' not in ')  # Restore
        condition_work = condition_work.replace(' and ', ' & ')
        condition_work = condition_work.replace(' or ', ' | ')
        
        # Evaluate condition
        condition_expr = eval(condition_work)
        return when(condition_expr, lit(1)).otherwise(lit(0))

    elif comp_type == 'arithmetic':
        # Parse arithmetic expression
        expression_str = comp_config['expression']
        # Replace column names with col() calls
        import re
        expr_work = expression_str
        sorted_cols = sorted(df.columns, key=len, reverse=True)
        for c in sorted_cols:
            pattern = rf'\b{re.escape(c)}\b'
            expr_work = re.sub(pattern, f'col("{c}")', expr_work)
        
        # Evaluate expression
        return eval(expr_work)

    elif comp_type == 'constant':
        return lit(comp_config['value'])

    else:
        raise ValueError(f"Unknown computed type: {comp_type}")


def rename_columns(df: DataFrame, rename_mapping: Dict[str, str]) -> DataFrame:
    """
    Rename multiple columns at once using a mapping dictionary.

    More efficient than sequential .withColumnRenamed() calls.
    All column names are converted to lowercase before and after renaming.

    Args:
        df: Input DataFrame
        rename_mapping: Dictionary of {old_name: new_name} mappings
                       Case-insensitive (automatically lowercased)

    Returns:
        DataFrame with renamed columns (all lowercase)

    Example:
        >>> rename_map = {
        ...     'DTCREPOL': 'creation_date',
        ...     'DTRESILP': 'termination_date',
        ...     'NOPOL': 'policy_number'
        ... }
        >>> df = rename_columns(df, rename_map)

    Note:
        This function consolidates the pattern of sequential renames:
        df.withColumnRenamed('old1', 'new1').withColumnRenamed('old2', 'new2')...

        Into a single operation:
        df = rename_columns(df, {'old1': 'new1', 'old2': 'new2'})
    """
    # Normalize all column names to lowercase
    rename_mapping_lower = {
        old_name.lower(): new_name.lower()
        for old_name, new_name in rename_mapping.items()
    }

    # Build select expression with renamed columns
    select_exprs = []
    for col_name in df.columns:
        col_name_lower = col_name.lower()
        if col_name_lower in rename_mapping_lower:
            # Rename this column
            new_name = rename_mapping_lower[col_name_lower]
            select_exprs.append(col(col_name_lower).alias(new_name))
        else:
            # Keep column as-is (lowercase)
            select_exprs.append(col(col_name_lower))

    return df.select(*select_exprs)
