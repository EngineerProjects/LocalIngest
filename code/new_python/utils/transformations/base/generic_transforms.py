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

    Example:
        >>> transforms = [
        ...     {'column': 'primeto', 'type': 'arithmetic',
        ...      'expression': 'mtprprto * (1 - tx / 100.0)'},
        ...     {'column': 'top_lta', 'type': 'flag',
        ...      'condition': 'ctduree > 1 and tydrisi in LTA_TYPES'}
        ... ]
        >>> df = apply_transformations(df, transforms, {'LTA_TYPES': ["QAM"]})
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


def apply_business_filters(
    df: DataFrame,
    filter_config: Dict[str, Any],
    logger: Optional[Any] = None,
    business_rules: Optional[Dict[str, Any]] = None
) -> DataFrame:
    """
    Apply business filters from configuration dictionary.
    Supports:
      - equals, not_equals
      - in, not_in
      - not_equals_column
      - complex expressions
      - values_ref (AZEC-specific)
    """

    filters = filter_config.get('filters', [])

    # Load business rules root if needed (for values_ref)
    if business_rules is None:
        business_rules = get_default_loader().get_business_rules()

    for filter_spec in filters:
        filter_type = filter_spec.get('type')
        column_name = filter_spec.get('column', '').lower()
        description = filter_spec.get('description', '')

        if logger:
            logger.debug(f"Applying filter: {description or filter_type}")

        # ---------------------------------------------------------
        # 1. Resolve value / values / values_ref
        # ---------------------------------------------------------
        value = None

        # Direct value or values
        if 'value' in filter_spec:
            value = filter_spec['value']
        elif 'values' in filter_spec:
            value = filter_spec['values']

        # values_ref → lookup in business_rules
        elif 'values_ref' in filter_spec:
            ref_name = filter_spec['values_ref']
            value = business_rules['business_filters']['azec'].get(ref_name)

            if value is None:
                raise ValueError(f"Unknown values_ref '{ref_name}' in filter: {filter_spec}")

        # Legacy: values_from_constant
        elif 'values_from_constant' in filter_spec:
            from config import constants
            value = getattr(constants, filter_spec['values_from_constant'])

        # Resolve @CONSTANT syntax
        value = _resolve_constant_reference(value)

        # ---------------------------------------------------------
        # 2. Apply filter
        # ---------------------------------------------------------

        if filter_type in ['equals', '==']:
            df = df.filter(col(column_name) == value)

        elif filter_type in ['not_equals', '!=']:
            df = df.filter(col(column_name) != value)

        elif filter_type == 'in':
            if not value:
                raise ValueError(f"Filter 'in' requires values: {filter_spec}")
            df = df.filter(col(column_name).isin(value))

        elif filter_type == 'not_in':
            if not value:
                raise ValueError(f"Filter 'not_in' requires values: {filter_spec}")
            df = df.filter(~col(column_name).isin(value))

        elif filter_type == 'not_equals_column':
            compare_column = filter_spec.get('compare_column', '').lower()
            if not compare_column:
                raise ValueError(f"Filter 'not_equals_column' requires compare_column")
            df = df.filter(col(column_name) != col(compare_column))

        elif filter_type == 'complex':
            expression = filter_spec.get('expression')
            if not expression:
                raise ValueError(f"Filter 'complex' requires expression")
            filter_condition = _parse_filter_expression(expression, df.columns)
            df = df.filter(filter_condition)

        else:
            raise ValueError(f"Unsupported filter type: {filter_type}")

    if logger:
        logger.info(f"Business filters applied successfully ({len(filters)} filters)")

    return df



def _resolve_constant_reference(value):
    """
    Resolve constant references in filter values.
    
    Supports @CONSTANT_NAME syntax to reference constants from config.constants module.
    
    Args:
        value: Filter value (can be string, list, or any other type)
    
    Returns:
        Resolved value (constant value if @CONSTANT found, otherwise original value)
    
    Examples:
        >>> _resolve_constant_reference("@EXCLUDED_NOINT")
        ["H90061", "482001", ...]  # Returns list from constants.EXCLUDED_NOINT
        
        >>> _resolve_constant_reference("@POLICY_STATUS.EXCLUDED")
        ["4", "5"]  # Returns list from constants.POLICY_STATUS.EXCLUDED
        
        >>> _resolve_constant_reference(["4", "5"])
        ["4", "5"]  # Returns as-is (no @ prefix)
    """
    if isinstance(value, str) and value.startswith('@'):
        # Extract constant path (e.g., "@EXCLUDED_NOINT" → "EXCLUDED_NOINT")
        constant_path = value[1:]
        
        # Import constants module
        from config import constants
        
        # Navigate nested attributes if needed (e.g., "POLICY_STATUS.EXCLUDED")
        parts = constant_path.split('.')
        obj = constants
        for part in parts:
            obj = getattr(obj, part)
        
        return obj
    
    return value



def _parse_filter_expression(expression: str, columns: List[str]) -> Any:
    """
    Parse filter expression string to PySpark condition.

    Supports:
    - Column references: 'duree', 'produit'
    - Comparisons: ==, !=, <, >, <=, >=
    - Logical operators: &, |, ~
    - Functions: .isin([...])

    Args:
        expression: Filter expression string (e.g., '(duree == "00") & ~produit.isin(["TRC"])')
        columns: List of DataFrame column names for validation

    Returns:
        PySpark Column expression

    Example:
        >>> expr = _parse_filter_expression('(col_a > 10) & (col_b != "X")', df.columns)
        >>> df_filtered = df.filter(expr)
    """
    # Replace column names with col() references
    expr_work = expression

    # Sort columns by length (longest first) to avoid partial matches
    sorted_cols = sorted(columns, key=len, reverse=True)
    for c in sorted_cols:
        # Match whole words only
        pattern = rf'\b{re.escape(c)}\b'
        expr_work = re.sub(pattern, f'col("{c}")', expr_work)

    # Evaluate as PySpark expression
    try:
        return eval(expr_work)
    except Exception as e:
        raise ValueError(f"Failed to parse filter expression: {expression}\nError: {e}")
