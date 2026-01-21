"""
Generic transformation utilities.

Provides reusable, configuration-driven transformation functions that work
across all domains and processors.
"""

from pyspark.sql import DataFrame # type: ignore
from pyspark.sql.functions import col, when, lit, coalesce, expr # type: ignore
from typing import Dict, Any, List, Optional
import re
from utils.loaders import get_default_loader


def apply_conditional_transform(
    df: DataFrame,
    target_col: str,
    config: Dict[str, Any]
) -> DataFrame:
    """
    Apply conditional transformation using when/otherwise logic.

    Builds when().when().otherwise() chains from configuration dictionaries.

    Args:
        df: Input DataFrame
        target_col: Column to create
        config: Condition configuration with 'conditions' and 'default' keys

    Returns:
        DataFrame with new column

    Example:
        >>> config = {
        ...     'conditions': [
        ...         {'col': 'cdpolgp1', 'op': '==', 'value': '1', 'result': 1}
        ...     ],
        ...     'default': 0
        ... }
        >>> df = apply_conditional_transform(df, 'top_coass', config)
    """
    conditions = config.get('conditions', [])
    default = config.get('default')

    expr = None
    for cond in conditions:
        cond_expr = build_condition(df, cond)

        # Determine result value
        if 'result' in cond:
            result_expr = lit(cond['result'])
        elif 'result_value' in cond:
            result_expr = lit(cond['result_value'])
        elif 'result_col' in cond:
            result_col = cond['result_col'].lower()
            result_expr = col(result_col) if result_col in df.columns else lit(None)
        elif 'result_expr' in cond:
            result_expr_str = cond['result_expr'].lower()
            for c in df.columns:
                if c in result_expr_str:
                    result_expr_str = result_expr_str.replace(c, f'col("{c}")')
            result_expr = eval(result_expr_str)
        else:
            result_expr = lit(None)

        if expr is None:
            expr = when(cond_expr, result_expr)
        else:
            expr = expr.when(cond_expr, result_expr)

    if default is not None:
        expr = expr.otherwise(lit(default))
    else:
        expr = expr.otherwise(lit(None))

    return df.withColumn(target_col.lower(), expr)


def build_condition(df: DataFrame, cond: Dict[str, Any]):
    """
    Build PySpark condition from configuration dictionary.

    Supports:
      - Comparison operators: ==, !=, >, <, >=, <=
      - Membership: in, not_in
      - Null checks: is_null, is_not_null
      - Multi-column AND conditions

    Args:
        df: DataFrame (for column existence checks)
        cond: Condition configuration

    Returns:
        PySpark Column condition

    Example:
        >>> cond = {'col': 'price', 'op': '>', 'value': 100}
        >>> condition = build_condition(df, cond)
    """
    col_name = cond['col'].lower()
    op = cond.get('op', '==')
    value = cond.get('value')

    # Base condition
    base_cond = None
    if op == '==':
        base_cond = col(col_name) == value
    elif op == '!=':
        base_cond = col(col_name) != value
    elif op == 'is_not_null':
        base_cond = col(col_name).isNotNull()
    elif op == 'is_null':
        base_cond = col(col_name).isNull()
    elif op == 'in':
        base_cond = col(col_name).isin(value)
    elif op == 'not_in':
        base_cond = ~col(col_name).isin(value)
    elif op == '>':
        base_cond = col(col_name) > value
    elif op == '<':
        base_cond = col(col_name) < value
    elif op == '>=':
        base_cond = col(col_name) >= value
    elif op == '<=':
        base_cond = col(col_name) <= value
    else:
        base_cond = lit(True)

    # AND condition (multi-column)
    if 'and_col' in cond:
        and_col = cond['and_col'].lower()
        if 'and_in' in cond:
            and_cond = col(and_col).isin(cond['and_in'])
            base_cond = base_cond & and_cond
        elif 'and_not_in' in cond:
            and_cond = ~col(and_col).isin(cond['and_not_in'])
            base_cond = base_cond & and_cond
        elif 'and_value' in cond:
            and_cond = col(and_col) == cond['and_value']
            base_cond = base_cond & and_cond

    return base_cond


def _build_expression_from_string(
    expr_str: str,
    columns: list,
    context: dict
) -> Any:
    """
    Build PySpark expression from string with column and context substitution.

    Handles:
    - Column names → col("column_name")
    - Context variables → their values
    - Operators: ==, !=, >, <, >=, <=, and, or, in, not
    - Functions: is null, is not null, contains

    Args:
        expr_str: Expression string
        columns: List of DataFrame column names
        context: Context dictionary with variables

    Returns:
        PySpark Column expression

    Example:
        >>> expr = _build_expression_from_string(
        ...     "price > 100 and category in VALID_CATS",
        ...     ['price', 'category'],
        ...     {'VALID_CATS': ['A', 'B']}
        ... )
    """
    expr_work = expr_str.lower()

    # Replace context variables first
    for key, value in context.items():
        pattern = re.compile(re.escape(key), re.IGNORECASE)
        expr_work = pattern.sub(repr(value), expr_work)

    # Replace column names with col() calls
    sorted_cols = sorted(columns, key=len, reverse=True)
    for c in sorted_cols:
        pattern = rf'\b{re.escape(c)}\b'
        expr_work = re.sub(pattern, f'col("{c}")', expr_work)

    # CRITICAL: Replace null checks BEFORE logical operators
    # Otherwise 'is not null' becomes 'is ~ null' due to ' not ' → ' ~ ' replacement
    expr_work = re.sub(r'(col\(["\'][^"\']+["\']\))\s+is\s+not\s+null', r'\1.isNotNull()', expr_work, flags=re.IGNORECASE)
    expr_work = re.sub(r'(col\(["\'][^"\']+["\']\))\s+is\s+null', r'\1.isNull()', expr_work, flags=re.IGNORECASE)

    # Replace logical operators AFTER null checks
    expr_work = expr_work.replace(' and ', ' & ')
    expr_work = expr_work.replace(' or ', ' | ')
    expr_work = expr_work.replace(' not ', ' ~ ')

    # Replace contains
    expr_work = re.sub(r'\.contains\s*\(\s*["\']([^"\']+)["\']\s*\)', r'.contains("\1")', expr_work)

    # Evaluate and return
    try:
        return eval(expr_work)
    except Exception as e:
        raise ValueError(f"Failed to build expression from '{expr_str}': {e}\nProcessed: {expr_work}")

def apply_transformations(
    df: DataFrame,
    transformations: List[Dict[str, Any]],
    context: Dict[str, Any] = None
) -> DataFrame:
    """
    Apply a series of column transformations from configuration.

    Supports multiple transformation types:
    - constant: Simple constant value
    - coalesce_columns: Coalesce multiple columns
    - coalesce_default: Coalesce with literal default
    - arithmetic: Arithmetic expression
    - conditional: Conditional logic (when/otherwise)
    - flag: Simple 1/0 flag based on condition
    - mapping: Map values from one column to another
    - cleanup: Replace NULL/specific values with another column

    Args:
        df: Input DataFrame
        transformations: List of transformation configurations
        context: Optional context dictionary with dates, constants, etc.

    Returns:
        DataFrame with all transformations applied
    """
    if context is None:
        context = {}

    for transform in transformations:
        col_name = transform['column'].lower()
        transform_type = transform['type']

        if transform_type == 'constant':
            value = transform['value']
            df = df.withColumn(col_name, lit(value))

        elif transform_type == 'coalesce_columns':
            sources = [col(c.lower()) for c in transform['sources']]
            df = df.withColumn(col_name, coalesce(*sources))

        elif transform_type == 'coalesce_default':
            source = col(transform['source'].lower())
            default = lit(transform['default'])
            df = df.withColumn(col_name, coalesce(source, default))

        elif transform_type == 'arithmetic':
            expr_str = transform['expression']
            expr = _build_expression_from_string(expr_str, df.columns, context)
            df = df.withColumn(col_name, expr)

        elif transform_type == 'conditional':
            conditions = transform['conditions']
            default = transform.get('default')
            default_expr = transform.get('default_expr')

            expr = None
            for cond in conditions:
                check_str = cond['check']
                result = cond.get('result')

                cond_expr = _build_expression_from_string(check_str, df.columns, context)

                if result is not None:
                    result_expr = lit(result)
                elif 'result_col' in cond:
                    result_expr = col(cond['result_col'].lower())
                elif 'result_expr' in cond:
                    result_expr = _build_expression_from_string(cond['result_expr'], df.columns, context)
                else:
                    result_expr = col(col_name)

                if expr is None:
                    expr = when(cond_expr, result_expr)
                else:
                    expr = expr.when(cond_expr, result_expr)

            if default is not None:
                expr = expr.otherwise(lit(default))
            elif default_expr is not None:
                default_col_expr = _build_expression_from_string(default_expr, df.columns, context)
                expr = expr.otherwise(default_col_expr)
            else:
                expr = expr.otherwise(col(col_name))

            df = df.withColumn(col_name, expr)

        elif transform_type == 'flag':
            condition = transform['condition']
            cond_expr = _build_expression_from_string(condition, df.columns, context)
            df = df.withColumn(col_name, when(cond_expr, lit(1)).otherwise(lit(0)))

        elif transform_type == 'mapping':
            source_col = transform.get('source', col_name).lower()
            mapping = transform.get('mapping', {})
            default = transform.get('default', '')

            expr = None
            for key, value in mapping.items():
                if expr is None:
                    expr = when(col(source_col) == key, lit(value))
                else:
                    expr = expr.when(col(source_col) == key, lit(value))

            if expr is not None:
                expr = expr.otherwise(lit(default))
                df = df.withColumn(col_name, expr)
            else:
                df = df.withColumn(col_name, lit(default))

        elif transform_type == 'cleanup':
            source = col(transform['source'].lower())
            null_values = transform.get('null_values', [None, ' '])
            replacement_col = transform.get('replacement')

            cond = source.isNull()
            for val in null_values:
                if val is not None:
                    cond = cond | (source == val)

            if replacement_col:
                replacement = col(replacement_col.lower())
            else:
                replacement = lit(None)

            df = df.withColumn(col_name, when(cond, replacement).otherwise(source))

    return df

def apply_business_filters(df, filter_config, logger=None, business_rules=None):
    """
    Apply SAS-style business filters coming from JSON configuration.

    This function preserves SAS WHERE semantics:
    - NOT IN keeps NULL rows
    - != keeps NULL rows
    - IN excludes NULL rows
    - AND/OR/NOT precedence is respected via the safe parser
    - Complex expressions are evaluated via safe JSON expression parsing
    - Comparisons involving NULL behave like SAS (NULL included where expected)

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    filter_config : dict
        Configuration JSON node under "business_filters".
    logger : Logger, optional
        Debug logger (if provided).
    business_rules : dict, optional
        For values_ref expansion.

    Returns
    -------
    DataFrame
        Filtered DataFrame.
    """

    filters = filter_config.get("filters", [])
    if not filters:
        return df

    for spec in filters:
        ftype = spec["type"]

        if ftype == "equals":
            df = df.filter(col(spec["column"]) == spec["value"])

        elif ftype == "not_equals":
            val = spec["value"]
            df = df.filter(col(spec["column"]).isNull() | (col(spec["column"]) != val))

        elif ftype == "not_in":
            vals = spec.get("values", spec.get("value", []))
            if not isinstance(vals, (list, tuple, set)):
                vals = [vals]
            df = df.filter(col(spec["column"]).isNull() | (~col(spec["column"]).isin(vals)))

        elif ftype == "in":
            vals = spec["values"]
            df = df.filter(col(spec["column"]).isin(vals))

        elif ftype == "not_equals_column":
            a = spec["column"]
            b = spec["compare_column"]
            df = df.filter(col(a).isNull() | col(b).isNull() | (col(a) != col(b)))

        else:
            raise ValueError(f"Unsupported filter type: {ftype}")

    return df