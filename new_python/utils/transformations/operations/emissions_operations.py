"""
Emissions-specific transformation operations.

Simple helpers for:
- Distribution channel (CDPOLE) assignment from CD_NIV_2_STC
- Current/prior year split (EXERCICE) calculation
- Guarantee code extraction (CGARP)
- Business filter applications

Based on: EMISSIONS_RUN.sas (308 lines)
"""

from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql.functions import (  # type: ignore
    col, when, lit, substring, regexp_replace
)
from typing import Dict, Any, Optional


def assign_distribution_channel(df: DataFrame) -> DataFrame:
    """
    Assign distribution channel (CDPOLE) based on CD_NIV_2_STC.
    
    Based on EMISSIONS_RUN.sas L174-177:
    - DCAG, DCPS, DIGITAL → '1' (Agent)
    - BROKDIV → '3' (Courtage)
    
    Args:
        df: Input DataFrame with cd_niv_2_stc column
    
    Returns:
        DataFrame with cdpole column added
    
    Example:
        >>> df = assign_distribution_channel(df)
    """
    df = df.withColumn('cdpole',
        when(col('cd_niv_2_stc').isin(['DCAG', 'DCPS', 'DIGITAL']), lit('1'))
        .when(col('cd_niv_2_stc') == 'BROKDIV', lit('3'))
        .otherwise(lit(None))
    )
    return df


def calculate_exercice_split(
    df: DataFrame,
    vision: str,
    year_col: str = 'nu_ex_ratt_cts'
) -> DataFrame:
    """
    Split premiums into current year (cou) vs prior year (ant).
    
    Based on EMISSIONS_RUN.sas L173:
    - If attachment year >= vision year → 'cou' (current)
    - Else → 'ant' (anterior/prior)
    
    Args:
        df: Input DataFrame
        vision: Vision in YYYYMM format
        year_col: Column containing attachment year
    
    Returns:
        DataFrame with exercice column added
    
    Example:
        >>> df = calculate_exercice_split(df, '202509')
    """
    from utils.helpers import extract_year_month_int
    year_int, _ = extract_year_month_int(vision)
    
    df = df.withColumn('exercice',
        when(col(year_col) >= lit(str(year_int)), lit('cou'))
        .otherwise(lit('ant'))
    )
    return df


def extract_guarantee_code(
    df: DataFrame,
    source_col: str = 'cd_gar_prospctiv',
    target_col: str = 'cgarp'
) -> DataFrame:
    """
    Extract guarantee code from prospective guarantee field.
    
    Based on EMISSIONS_RUN.sas L172, L275:
    - Extract characters 3-5 from cd_gar_prospctiv
    - Remove spaces (compress)
    
    Args:
        df: Input DataFrame
        source_col: Source column name
        target_col: Target column name for extracted code
    
    Returns:
        DataFrame with extracted guarantee code
    
    Example:
        >>> df = extract_guarantee_code(df)
    """
    df = df.withColumn(target_col,
        regexp_replace(
            substring(col(source_col), 3, 5),
            r'\s+', ''
        )
    )
    return df


def apply_emissions_filters(
    df: DataFrame,
    config: Dict[str, Any],
    vision: str,
    logger: Optional[Any] = None
) -> DataFrame:
    """
    Apply all Emissions business filters.
    
    Based on EMISSIONS_RUN.sas L159, L185-189, L198:
    1. Construction market filter (cd_marche = '6')
    2. Accounting date filter (dt_cpta_cts <= vision)
    3. Excluded intermediaries
    4. Product/guarantee exclusions
    5. Category exclusions
    
    Args:
        df: Input DataFrame
        config: Emissions configuration dict
        vision: Vision in YYYYMM format
        logger: Optional logger instance
    
    Returns:
        Filtered DataFrame
    
    Example:
        >>> df = apply_emissions_filters(df, emissions_config, '202509')
    """
    initial_count = df.count() if logger else 0
    
    # Filter 1: Construction market (cd_marche = '6')
    df = df.filter(col('cd_marche') == '6')
    if logger:
        logger.info(f"After market filter (cd_marche='6'): {df.count():,} records")
    
    # Filter 2: Accounting date <= vision
    df = df.filter(col('dt_cpta_cts') <= lit(vision))
    if logger:
        logger.info(f"After date filter (dt_cpta_cts <= {vision}): {df.count():,} records")
    
    # Filter 3: Excluded intermediaries
    excluded_noint = config.get('excluded_intermediaries', [])
    if excluded_noint:
        df = df.filter(~col('cd_int_stc').isin(excluded_noint))
        if logger:
            logger.info(f"After intermediary filter ({len(excluded_noint)} excluded): {df.count():,} records")
    
    # Filter 4: Product/guarantee exclusions
    for exclusion in config.get('product_guarantee_exclusions', []):
        if 'product_prefix' in exclusion:
            # Product prefix with guarantee
            df = df.filter(
                ~((substring(col('cd_prd_prm'), 1, 2) == exclusion['product_prefix']) &
                  (col('cd_gar_prospctiv') == exclusion['guarantee']))
            )
        elif 'intermediary' in exclusion:
            # Intermediary with product
            df = df.filter(
                ~((col('cd_int_stc') == exclusion['intermediary']) &
                  (col('cd_prd_prm') == exclusion['product']))
            )
    
    # Filter 5: Excluded guarantees
    excluded_guarantees = config.get('excluded_guarantees', [])
    if excluded_guarantees:
        df = df.filter(~col('cd_gar_prospctiv').isin(excluded_guarantees))
    
    # Filter 6: Excluded product
    excluded_product = config.get('excluded_product')
    if excluded_product:
        df = df.filter(col('cd_prd_prm') != excluded_product)
    
    # Filter 7: Excluded categories and guarantee category
    excluded_categories = config.get('excluded_categories', [])
    excluded_gar_cat = config.get('excluded_guarantee_category')
    
    if excluded_categories or excluded_gar_cat:
        filter_expr = lit(False)
        if excluded_categories:
            filter_expr = filter_expr | col('cd_cat_min').isin(excluded_categories)
        if excluded_gar_cat:
            filter_expr = filter_expr | (col('cd_gar_prospctiv') == excluded_gar_cat)
        
        df = df.filter(~filter_expr)
    
    if logger:
        final_count = df.count()
        filtered_out = initial_count - final_count
        logger.success(f"Filters applied: {filtered_out:,} records filtered out, {final_count:,} remaining")
    
    return df


def aggregate_by_policy_guarantee(
    df: DataFrame,
    group_cols: list
) -> DataFrame:
    """
    Aggregate premiums and commissions by specified grouping columns.
    
    Based on EMISSIONS_RUN.sas L284-294:
    - Group by vision, dircom, cdpole, nopol, cdprod, noint, cgarp, cmarch, cseg, cssseg, cd_cat_min
    - Sum: primes_x, primes_n, mtcom_x
    
    Args:
        df: Input DataFrame with premium data
        group_cols: List of columns to group by
    
    Returns:
        Aggregated DataFrame
    
    Example:
        >>> group_cols = ['vision', 'dircom', 'cdpole', 'nopol', 'cdprod', 
        ...               'noint', 'cgarp', 'cmarch', 'cseg', 'cssseg', 'cd_cat_min']
        >>> df_agg = aggregate_by_policy_guarantee(df, group_cols)
    """
    from pyspark.sql.functions import sum as _sum
    
    df_agg = df.groupBy(*group_cols).agg(
        _sum('mt_ht_cts').alias('primes_x'),
        _sum('primes_n').alias('primes_n'),
        _sum('mtcom').alias('mtcom_x')
    )
    
    return df_agg
