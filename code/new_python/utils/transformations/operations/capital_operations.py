"""
Capital-specific transformation operations for Capitaux component.

Simple helpers for:
- Extended capital extraction (7 types vs 4 in business_logic.py)
- Normalization to 100% basis
- Capitaux business rules (SMP completion, RC limits)
- AZEC capital processing

Based on:
- CAPITAUX_AZ_MACRO.sas (313 lines)
- CAPITAUX_AZEC_MACRO.sas (149 lines)
"""

from pyspark.sql import DataFrame # type: ignore
from pyspark.sql.functions import col, when, lit, coalesce, greatest, sum as _sum # type: ignore
from typing import Dict, List


def extract_capitals_extended(
    df: DataFrame,
    config: Dict[str, Dict],
    num_capitals: int = 14,
    indexed: bool = False
) -> DataFrame:
    """
    Extract capitals by matching keywords in label columns.
    
    Extended version supporting ALL capital types for Capitaux component:
    - SMP_100, LCI_100, PERTE_EXP_100, RISQUE_DIRECT_100 (from PTF_MVT)
    - LIMITE_RC_100_PAR_SIN, LIMITE_RC_100_PAR_AN (new for Capitaux)
    - SMP_PE_100, SMP_RD_100 (new for Capitaux)
    
    Args:
        df: Input DataFrame
        config: Capital extraction configuration from JSON
        num_capitals: Number of capital columns to check (default 14)
        indexed: If True, use indexed columns (mtcapi1i), else non-indexed (mtcapi1)
    
    Returns:
        DataFrame with capital columns added
    
    Example:
        >>> config = {
        ...     'smp_100': {'keywords': ['SMP GLOBAL', 'SMP RETENU'], 'exclude': []},
        ...     'lci_100': {'keywords': ['LCI GLOBAL'], 'exclude': []},
        ...     'limite_rc_100_par_sin': {'keywords': ['DOMMAGES CORPORELS'], 'exclude': []}
        ... }
        >>> df = extract_capitals_extended(df, config, 14, indexed=False)
    """
    suffix = 'i' if indexed else ''
    mtcapi_prefix = f'mtcapi'
    lbcapi_prefix = 'lbcapi'
    
    # For each capital type in config
    for capital_name, capital_config in config.items():
        # Skip comment and metadata keys
        if capital_name.startswith('_'):
            continue
            
        keywords = capital_config.get('keywords', [])
        exclude_keywords = capital_config.get('exclude_keywords', [])
        
        # Initialize with 0
        capital_expr = lit(0.0)
        
        # Loop through mtcapi1-14 / lbcapi1-14
        for i in range(1, num_capitals + 1):
            mtcapi_col = f'{mtcapi_prefix}{i}{suffix}'
            lbcapi_col = f'{lbcapi_prefix}{i}'
            
            if mtcapi_col not in df.columns or lbcapi_col not in df.columns:
                continue
            
            # Build match condition
            match_condition = lit(False)
            for keyword in keywords:
                match_condition = match_condition | col(lbcapi_col).contains(keyword)
            
            # Build exclude condition
            if exclude_keywords:
                for exclude_kw in exclude_keywords:
                    match_condition = match_condition & ~col(lbcapi_col).contains(exclude_kw)
            
            # Take MAX: if label matches, use this capital amount
            capital_expr = greatest(
                capital_expr,
                when(match_condition, coalesce(col(mtcapi_col), lit(0.0))).otherwise(lit(0.0))
            )
        
        # Append _ind suffix to column name if indexed
        target_col_name = f"{capital_name}_ind" if indexed else capital_name
        df = df.withColumn(target_col_name, capital_expr)
    
    return df


def normalize_capitals_to_100(
    df: DataFrame,
    capital_columns: List[str],
    coinsurance_col: str = 'prcdcie'
) -> DataFrame:
    """
    Normalize all capitals to 100% technical basis.
    
    Based on CAPITAUX_AZ_MACRO.sas L262-286:
    - Set PRCDCIE = 100 if missing or 0
    - Capital_100 = (Capital * 100) / PRCDCIE
    
    Args:
        df: Input DataFrame
        capital_columns: List of capital column names to normalize
        coinsurance_col: Coinsurance percentage column (default 'prcdcie')
    
    Returns:
        DataFrame with normalized capitals
    
    Example:
        >>> cols = ['smp_100_ind', 'lci_100_ind', 'perte_exp_100_ind']
        >>> df = normalize_capitals_to_100(df, cols, 'prcdcie')
    """
    # SAS L264-265: Set PRCDCIE = 100 WHERE PRCDCIE = . or PRCDCIE = 0
    df = df.withColumn(
        coinsurance_col,
        when(
            (col(coinsurance_col).isNull()) | (col(coinsurance_col) == 0),
            lit(100)
        ).otherwise(col(coinsurance_col))
    )
    
    # SAS L268-286: Normalize each capital to 100%
    for capital_col in capital_columns:
        if capital_col not in df.columns:
            continue
        
        df = df.withColumn(
            capital_col,
            (col(capital_col) * 100) / col(coinsurance_col)
        )
    
    return df


def apply_capitaux_business_rules(
    df: DataFrame,
    indexed: bool = True
) -> DataFrame:
    """
    Apply Capitaux-specific business rules.
    
    Based on CAPITAUX_AZ_MACRO.sas L288-293:
    1. SMP completion: SMP_100 = MAX(SMP_100, SMP_PE_100 + SMP_RD_100)
    2. RC limit: LIMITE_RC_100 = MAX(LIMITE_RC_100_PAR_SIN, LIMITE_RC_100_PAR_AN)
    
    Args:
        df: Input DataFrame
        indexed: If True, apply to indexed columns (_IND suffix), else non-indexed
    
    Returns:
        DataFrame with business rules applied
    
    Example:
        >>> df = apply_capitaux_business_rules(df, indexed=True)
    """
    suffix = '_ind' if indexed else ''
    
    # OPTIMIZATION: Reduce withColumn operations by combining expressions
    # Original: 3 separate withColumn calls
    # Optimized: 2 withColumn calls (one for each business rule)
    
    # Rule 1: SMP completion (Nov 16, 2020)
    # SMP_100 = MAX(SMP_100, SMP_PE_100 + SMP_RD_100)
    smp_col = f'smp_100{suffix}'
    smp_pe_col = f'smp_pe_100{suffix}'
    smp_rd_col = f'smp_rd_100{suffix}'
    
    if all(c in df.columns for c in [smp_col, smp_pe_col, smp_rd_col]):
        df = df.withColumn(
            smp_col,
            greatest(
                col(smp_col),
                coalesce(col(smp_pe_col), lit(0.0)) + coalesce(col(smp_rd_col), lit(0.0))
            )
        )
    
    # Rule 2: RC limit - Combine all RC variants in one expression
    # SAS L146-150: RC_PAR_SIN includes both direct matches and TOUS_DOM variant
    # SAS L293: LIMITE_RC_100 = MAX(LIMITE_RC_100_PAR_SIN, LIMITE_RC_100_PAR_AN)
    rc_col = f'limite_rc_100{suffix}'
    rc_sin_col = f'limite_rc_100_par_sin{suffix}'
    rc_sin_tous_dom_col = f'limite_rc_100_par_sin_tous_dom{suffix}'
    rc_an_col = f'limite_rc_100_par_an{suffix}'

    # OPTIMIZATION: Single expression instead of 2 separate withColumn calls
    if all(c in df.columns for c in [rc_sin_col, rc_an_col]):
        rc_sin_expr = col(rc_sin_col)
        
        # Include TOUS_DOM variant if it exists
        if rc_sin_tous_dom_col in df.columns:
            rc_sin_expr = greatest(
                coalesce(col(rc_sin_col), lit(0.0)),
                coalesce(col(rc_sin_tous_dom_col), lit(0.0))
            )
        
        df = df.withColumn(
            rc_col,
            greatest(
                coalesce(rc_sin_expr, lit(0.0)),
                coalesce(col(rc_an_col), lit(0.0))
            )
        )

    return df


def process_azec_capitals(df: DataFrame) -> DataFrame:
    """
    Process AZEC capital data from CAPITXCU.

    Based on CAPITAUX_AZEC_MACRO.sas L30-96:
    - LCI/SMP by branch (IP0=PE, ID0=Direct Damage)
    - Process both CAPX_100 (100% basis) and CAPX_CUA (company share)
    - Aggregate by POLICE + PRODUIT

    Args:
        df: CAPITXCU DataFrame with columns:
            - smp_sre: Type ('LCI' or 'SMP')
            - brch_rea: Branch ('IP0' or 'ID0')
            - capx_100: Capital amount at 100%
            - capx_cua: Capital amount company share
            - police, produit: Policy and product identifiers

    Returns:
        DataFrame aggregated by police/produit with:
            - smp_100_ind, lci_100_ind (100% basis)
            - smp_cie, lci_cie (company share)

    Example:
        >>> df_capitxcu = process_azec_capitals(df_capitxcu)
    """
    # =========================================================================
    # CAPX_100 Processing (100% Basis)
    # =========================================================================

    # SAS L33-37: LCI Perte d'Exploitation (100%)
    df = df.withColumn(
        'lci_pe_100',
        when((col('smp_sre') == 'LCI') & (col('brch_rea') == 'IP0'),
             coalesce(col('capx_100'), lit(0.0))
        ).otherwise(lit(0.0))
    )

    # SAS L40-44: LCI Dommages Directs (100%)
    df = df.withColumn(
        'lci_dd_100',
        when((col('smp_sre') == 'LCI') & (col('brch_rea') == 'ID0'),
             coalesce(col('capx_100'), lit(0.0))
        ).otherwise(lit(0.0))
    )

    # SAS L57-61: SMP Perte d'Exploitation (100%)
    df = df.withColumn(
        'smp_pe_100',
        when((col('smp_sre') == 'SMP') & (col('brch_rea') == 'IP0'),
             coalesce(col('capx_100'), lit(0.0))
        ).otherwise(lit(0.0))
    )

    # SAS L63-67: SMP Dommages Directs (100%)
    df = df.withColumn(
        'smp_dd_100',
        when((col('smp_sre') == 'SMP') & (col('brch_rea') == 'ID0'),
             coalesce(col('capx_100'), lit(0.0))
        ).otherwise(lit(0.0))
    )

    # =========================================================================
    # CAPX_CUA Processing (Company Share)
    # =========================================================================

    # SAS L35-38: LCI Perte d'Exploitation (Company Share)
    df = df.withColumn(
        'lci_pe_cie',
        when((col('smp_sre') == 'LCI') & (col('brch_rea') == 'IP0'),
             coalesce(col('capx_cua'), lit(0.0))
        ).otherwise(lit(0.0))
    )

    # SAS L42-45: LCI Dommages Directs (Company Share)
    df = df.withColumn(
        'lci_dd_cie',
        when((col('smp_sre') == 'LCI') & (col('brch_rea') == 'ID0'),
             coalesce(col('capx_cua'), lit(0.0))
        ).otherwise(lit(0.0))
    )

    # SAS L59-62: SMP Perte d'Exploitation (Company Share)
    df = df.withColumn(
        'smp_pe_cie',
        when((col('smp_sre') == 'SMP') & (col('brch_rea') == 'IP0'),
             coalesce(col('capx_cua'), lit(0.0))
        ).otherwise(lit(0.0))
    )

    # SAS L65-68: SMP Dommages Directs (Company Share)
    df = df.withColumn(
        'smp_dd_cie',
        when((col('smp_sre') == 'SMP') & (col('brch_rea') == 'ID0'),
             coalesce(col('capx_cua'), lit(0.0))
        ).otherwise(lit(0.0))
    )

    # =========================================================================
    # SAS L86-96: Aggregate by POLICE + PRODUIT
    # =========================================================================
    df_agg = df.groupBy('police', 'produit').agg(
        # 100% basis aggregates
        _sum('smp_pe_100').alias('smp_pe_100'),
        _sum('smp_dd_100').alias('smp_dd_100'),
        _sum('lci_pe_100').alias('lci_pe_100'),
        _sum('lci_dd_100').alias('lci_dd_100'),
        # Company share aggregates
        _sum('smp_pe_cie').alias('smp_pe_cie'),
        _sum('smp_dd_cie').alias('smp_dd_cie'),
        _sum('lci_pe_cie').alias('lci_pe_cie'),
        _sum('lci_dd_cie').alias('lci_dd_cie')
    )

    # SAS L76-77: Calculate totals (100% basis)
    df_agg = df_agg.withColumn(
        'smp_100_ind',
        col('smp_pe_100') + col('smp_dd_100')
    )

    df_agg = df_agg.withColumn(
        'lci_100_ind',
        col('lci_pe_100') + col('lci_dd_100')
    )

    # SAS L76-77: Calculate totals (Company share)
    df_agg = df_agg.withColumn(
        'smp_cie',
        col('smp_pe_cie') + col('smp_dd_cie')
    )

    df_agg = df_agg.withColumn(
        'lci_cie',
        col('lci_pe_cie') + col('lci_dd_cie')
    )

    # Rename to match expected schema
    df_agg = df_agg.withColumnRenamed('police', 'nopol')
    df_agg = df_agg.withColumnRenamed('produit', 'cdprod')

    return df_agg


def aggregate_azec_pe_rd(df: DataFrame) -> DataFrame:
    """
    Aggregate AZEC perte d'exploitation and risque direct data.
    
    Based on CAPITAUX_AZEC_MACRO.sas L103-113:
    - Sum MT_BASPE and MT_BASDI by POLICE + PRODUIT
    
    Args:
        df: INCENDCU DataFrame with columns:
            - police, produit: Identifiers
            - mt_baspe: Business interruption amount
            - mt_basdi: Direct damage amount
    
    Returns:
        DataFrame aggregated with:
            - perte_exp_100_ind: Sum of PE
            - risque_direct_100_ind: Sum of RD
            - value_insured_100_ind: PE + RD
    
    Example:
        >>> df_pe_rd = aggregate_azec_pe_rd(df_incendcu)
    """
    df_agg = df.groupBy('police', 'produit').agg(
        _sum(coalesce(col('mt_baspe'), lit(0.0))).alias('perte_exp_100_ind'),
        _sum(coalesce(col('mt_basdi'), lit(0.0))).alias('risque_direct_100_ind')
    )
    
    df_agg = df_agg.withColumn(
        'value_insured_100_ind',
        col('perte_exp_100_ind') + col('risque_direct_100_ind')
    )
    
    # Rename to match expected schema
    df_agg = df_agg.withColumnRenamed('police', 'nopol')
    df_agg = df_agg.withColumnRenamed('produit', 'cdprod')
    
    return df_agg
