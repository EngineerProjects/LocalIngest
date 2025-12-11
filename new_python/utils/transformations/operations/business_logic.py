"""
Business logic transformations for Construction Data Pipeline.

Handles domain-specific calculations:
- Capital extraction (SMP, LCI, PERTE_EXP, RISQUE_DIRECT)
- Movement indicators (AFN, RES, RPT, RPC, NBPTF)
- Exposure calculations (expo_ytd, expo_gli)
- Business filters (construction market, policy exclusions)
"""

from pyspark.sql import DataFrame # type: ignore
from pyspark.sql.functions import ( # type: ignore
    col, when, lit, coalesce, upper, greatest, least,
    year, month, datediff, date_sub
)
from pyspark.sql.types import DoubleType, IntegerType # type: ignore
from typing import Dict, Any, List
from functools import reduce


def extract_capitals(df: DataFrame, config: Dict[str, Dict]) -> DataFrame:
    """
    Extract capital amounts by matching keywords in label columns.

    This function implements the SAS capital extraction logic that loops through
    14 MTCAPI/LBCAPI fields and extracts amounts based on label pattern matching.

    SAS Reference: PTF_MVTS_AZ_MACRO.sas lines 195-231
    Logic: %Do k=1 %To 14; UPDATE WHERE index(lbcapi&k., "keyword") > 0

    Searches label columns (e.g., lbcapi1-14) for specific keywords and extracts
    corresponding amounts (e.g., mtcapi1-14). Uses FIRST MATCH strategy - once a
    field matches, its amount is taken (mimics SAS UPDATE behavior).

    Args:
        df: Input DataFrame
        config: Capital extraction configuration with structure:
            {
                'lci_100': {
                    'keywords': ['LCI GLOBAL DU CONTRAT', 'CAPITAL REFERENCE OU LCI'],
                    'exclude_keywords': [],  # Optional
                    'label_prefix': 'lbcapi',
                    'amount_prefix': 'mtcapi',
                    'num_indices': 14
                },
                'smp_100': {
                    'keywords': ['SMP GLOBAL DU CONTRAT', 'SMP RETENU'],
                    'exclude_keywords': ['RISQUE DIRECT'],  # Exclude "SINIS MAX POSSIBLE RISQUE DIRECT"
                    'label_prefix': 'lbcapi',
                    'amount_prefix': 'mtcapi',
                    'num_indices': 14
                },
                ...
            }

    Returns:
        DataFrame with extracted capital columns (lowercase)

    Example:
        >>> from config.variables import CAPITAL_EXTRACTION_CONFIG
        >>> df = extract_capitals(df, CAPITAL_EXTRACTION_CONFIG)
    """
    for target_col, extraction_config in config.items():
        keywords = extraction_config['keywords']
        exclude_keywords = extraction_config.get('exclude_keywords', [])
        label_prefix = extraction_config['label_prefix'].lower()
        amount_prefix = extraction_config['amount_prefix'].lower()
        num_indices = extraction_config['num_indices']

        # Build cascading when() expression for FIRST MATCH strategy
        # This mimics SAS UPDATE behavior where first match wins
        result_expr = lit(0).cast(DoubleType())

        # Loop backwards so earlier fields take precedence (SAS processes 1→14 with UPDATE)
        for i in range(num_indices, 0, -1):
            label_col = f"{label_prefix}{i}"
            amount_col = f"{amount_prefix}{i}"

            if label_col not in df.columns or amount_col not in df.columns:
                continue

            # Check if label contains any keyword (case-insensitive)
            keyword_matches = reduce(
                lambda a, b: a | b,
                [upper(col(label_col)).contains(kw.upper()) for kw in keywords]
            )

            # Exclude if contains any exclude keyword
            if exclude_keywords:
                exclude_matches = reduce(
                    lambda a, b: a | b,
                    [upper(col(label_col)).contains(kw.upper()) for kw in exclude_keywords]
                )
                keyword_matches = keyword_matches & ~exclude_matches

            # Build cascading when: if this field matches, use it, otherwise use accumulated result
            result_expr = when(
                keyword_matches,
                coalesce(col(amount_col), lit(0))
            ).otherwise(result_expr)

        df = df.withColumn(target_col.lower(), result_expr)

    return df


def calculate_movements(
    df: DataFrame,
    dates: Dict[str, str],
    annee: int,
    column_mapping: Dict[str, str] = None
) -> DataFrame:
    """
    Calculate movement indicators: AFN, RES, RPT, RPC, NBPTF.

    Supports both AZ and AZEC data through column mapping configuration.

    Movement types:
    - AFN (Affaires Nouvelles): New policies
    - RES (Résiliations): Terminated policies
    - RPT (Remplaçants): Replacement policies (new) - AZ only
    - RPC (Remplacements): Replaced policies (old) - AZ only
    - NBPTF: Portfolio count

    Args:
        df: Input DataFrame
        dates: Date ranges from helpers.compute_date_ranges()
        annee: Year as integer
        column_mapping: Column mapping from variables.MOVEMENT_COLUMN_MAPPING

    Returns:
        DataFrame with movement columns added

    Example:
        >>> from config.variables import MOVEMENT_COLUMN_MAPPING
        >>> df = calculate_movements(df, dates, 2025, MOVEMENT_COLUMN_MAPPING['az'])
    """
    from pyspark.sql.functions import year as year_func # type: ignore

    if column_mapping is None:
        from config.variables import MOVEMENT_COLUMN_MAPPING
        column_mapping = MOVEMENT_COLUMN_MAPPING['az']

    creation_date = column_mapping.get('creation_date')
    effective_date = column_mapping.get('effective_date')
    termination_date = column_mapping.get('termination_date')
    transfer_start = column_mapping.get('transfer_start')
    transfer_end = column_mapping.get('transfer_end')
    type_col1 = column_mapping.get('type_col1')
    type_col2 = column_mapping.get('type_col2')
    type_col3 = column_mapping.get('type_col3')
    type_date1 = column_mapping.get('type_date1')
    type_date2 = column_mapping.get('type_date2')
    type_date3 = column_mapping.get('type_date3')

    dtfin = dates['DTFIN']
    dtdeb_an = dates['DTDEB_AN']

    # Initialize movement columns (optimized: collect missing columns then add in one select)
    missing_cols = {
        col_name: lit(0)
        for col_name in ['nbafn', 'nbres', 'nbrpt', 'nbrpc', 'nbptf',
                         'primes_afn', 'primes_res', 'primes_rpt', 'primes_rpc', 'primes_ptf']
        if col_name not in df.columns
    }
    if missing_cols:
        df = df.select("*", *[expr.alias(name) for name, expr in missing_cols.items()])

    # AFN: New business
    if creation_date and effective_date:
        afn_cond = lit(False)

        if transfer_start:
            afn_cond = afn_cond | (
                (col(effective_date) >= lit(dtdeb_an)) & (col(effective_date) <= lit(dtfin)) &
                (col(transfer_start) >= lit(dtdeb_an)) & (col(transfer_start) <= lit(dtfin))
            )

        afn_cond = afn_cond | (
            (col(creation_date) >= lit(dtdeb_an)) & (col(creation_date) <= lit(dtfin))
        )

        if transfer_start:
            afn_cond = afn_cond | (
                (year_func(col(effective_date)) < year_func(lit(dtfin))) &
                (col(transfer_start) >= lit(dtdeb_an)) & (col(transfer_start) <= lit(dtfin))
            )

        if type_col1:
            afn_cond = afn_cond & ~(
                (col(type_col1) == "RE") | (col(type_col2) == "RE") | (col(type_col3) == "RE")
            )

        # Optimized: calculate nbafn and primes_afn together
        nbafn_expr = when(afn_cond, lit(1)).otherwise(lit(0))
        # Drop before redefining to avoid ambiguous reference
        df = df.drop("nbafn", "primes_afn")
        df = df.select(
            "*",
            nbafn_expr.alias("nbafn"),
            when(nbafn_expr == 1, col("primeto")).otherwise(lit(0)).alias("primes_afn")
        )

    # RES: Terminations
    if termination_date:
        res_cond = (
            (col(termination_date) >= lit(dtdeb_an)) & (col(termination_date) <= lit(dtfin))
        )

        if transfer_end:
            res_cond = res_cond | (
                (col(transfer_end) >= lit(dtdeb_an)) & (col(transfer_end) <= lit(dtfin)) &
                (col(termination_date) <= lit(dtfin))
            )

        if type_col1:
            res_cond = res_cond & ~(
                (col(type_col1) == "RP") | (col(type_col2) == "RP") | (col(type_col3) == "RP")
            )

        if 'cdnatp' in df.columns:
            res_cond = res_cond & (col("cdnatp") != "C")

        # Optimized: calculate nbres and primes_res together
        nbres_expr = when(res_cond, lit(1)).otherwise(lit(0))
        # Drop before redefining to avoid ambiguous reference
        df = df.drop("nbres", "primes_res")
        df = df.select(
            "*",
            nbres_expr.alias("nbres"),
            when(nbres_expr == 1, col("primeto")).otherwise(lit(0)).alias("primes_res")
        )

    # RPT/RPC: Replacements (AZ only) - Must reference nbafn/nbres BEFORE dropping them
    if type_col1 and type_date1:
        # Build RPC condition (references nbres which exists from previous step)
        rpc_cond = (col("nbres") == 1) & (
            ((col(type_col1) == "RP") & (year_func(col(type_date1)) == year_func(lit(dtfin)))) |
            ((col(type_col2) == "RP") & (year_func(col(type_date2)) == year_func(lit(dtfin)))) |
            ((col(type_col3) == "RP") & (year_func(col(type_date3)) == year_func(lit(dtfin))))
        )

        # Build RPT condition (references nbafn which exists from previous step)
        rpt_cond = (col("nbafn") == 1) & (
            ((col(type_col1) == "RE") & (year_func(col(type_date1)) == year_func(lit(dtfin)))) |
            ((col(type_col2) == "RE") & (year_func(col(type_date2)) == year_func(lit(dtfin)))) |
            ((col(type_col3) == "RE") & (year_func(col(type_date3)) == year_func(lit(dtfin))))
        )

        # Build all expressions BEFORE dropping columns
        nbrpc_expr = when(rpc_cond, lit(1)).otherwise(lit(0))
        nbrpt_expr = when(rpt_cond, lit(1)).otherwise(lit(0))
        
        # Create temp columns to hold current nbafn/nbres/primes values
        # CRITICAL: Materialize these BEFORE dropping the source columns
        nbafn_current = when(nbrpt_expr == 1, lit(0)).otherwise(col("nbafn"))
        nbres_current = when(nbrpc_expr == 1, lit(0)).otherwise(col("nbres"))
        primes_afn_current = when(nbrpt_expr == 1, lit(0)).otherwise(col("primes_afn"))
        primes_res_current = when(nbrpc_expr == 1, lit(0)).otherwise(col("primes_res"))

        # Materialize temp columns with _temp suffix BEFORE dropping originals
        df = df.withColumn("nbres_temp", nbres_current) \
               .withColumn("nbafn_temp", nbafn_current) \
               .withColumn("primes_res_temp", primes_res_current) \
               .withColumn("primes_afn_temp", primes_afn_current)
        
        # Now drop old columns (safe because we have _temp versions)
        df = df.drop("nbres", "nbafn", "primes_res", "primes_afn", "nbrpc", "nbrpt", "primes_rpc", "primes_rpt")
        
        # Create new movement columns (SAS L273-286: UPDATE sets NBRPC/NBRPT, then resets NBRES/NBAFN to 0)
        df = df.select(
            "*",
            nbrpc_expr.alias("nbrpc"),
            nbrpt_expr.alias("nbrpt"),
            when(nbrpc_expr == 1, col("primeto")).otherwise(lit(0)).alias("primes_rpc"),
            when(nbrpt_expr == 1, col("primeto")).otherwise(lit(0)).alias("primes_rpt")
        ).withColumnRenamed("nbres_temp", "nbres") \
         .withColumnRenamed("nbafn_temp", "nbafn") \
         .withColumnRenamed("primes_res_temp", "primes_res") \
         .withColumnRenamed("primes_afn_temp", "primes_afn")
    else:
        # If no type columns, ensure nbrpt/nbrpc are 0 (drop and recreate to match if branch behavior)
        df = df.drop("nbrpt", "nbrpc", "primes_rpt", "primes_rpc") \
               .select(
                    "*",
                    lit(0).alias("nbrpt"),
                    lit(0).alias("nbrpc"),
                    lit(0).alias("primes_rpt"),
                    lit(0).alias("primes_rpc")
                )

    # NBPTF: Portfolio count
    nbptf_cond = lit(False)

    if 'cssseg' in df.columns and 'cdnatp' in df.columns and 'cdsitp' in df.columns:
        nbptf_cond = (
            (col('cssseg') != '5') &
            (col('cdnatp').isin(['R', 'O'])) &
            (
                ((col('cdsitp') == '1') & (col(creation_date) <= lit(dtfin))) |
                ((col('cdsitp') == '3') & (col(termination_date) > lit(dtfin)))
            )
        )
    else:
        nbptf_cond = (
            (col("nbafn") == 0) & (col("nbres") == 0) &
            (col("nbrpt") == 0) & (col("nbrpc") == 0)
        )

    # Optimized: calculate nbptf and primes_ptf together
    nbptf_expr = when(nbptf_cond, lit(1)).otherwise(lit(0))
    # Drop before redefining to avoid ambiguous reference
    df = df.drop("nbptf", "primes_ptf")
    df = df.select(
        "*",
        nbptf_expr.alias("nbptf"),
        when(nbptf_expr == 1, col("primeto")).otherwise(lit(0)).alias("primes_ptf")
    )

    return df


def calculate_exposures(
    df: DataFrame,
    dates: Dict[str, str],
    annee: int,
    column_mapping: Dict[str, str] = None
) -> DataFrame:
    """
    Calculate exposure metrics: expo_ytd and expo_gli.

    expo_ytd: Year-to-date exposure (days in year / 365)
    expo_gli: Monthly exposure (days in month / 30)

    Args:
        df: Input DataFrame
        dates: Date ranges dictionary
        annee: Year as integer
        column_mapping: Column mapping from variables.EXPOSURE_COLUMN_MAPPING

    Returns:
        DataFrame with exposure columns added

    Example:
        >>> from config.variables import EXPOSURE_COLUMN_MAPPING
        >>> df = calculate_exposures(df, dates, 2025, EXPOSURE_COLUMN_MAPPING['az'])
    """
    if column_mapping is None:
        from config.variables import EXPOSURE_COLUMN_MAPPING
        column_mapping = EXPOSURE_COLUMN_MAPPING['az']

    creation_date = column_mapping.get('creation_date')
    termination_date = column_mapping.get('termination_date')

    dtfin = dates['DTFIN']
    dtdeb_an = dates['DTDEB_AN']
    dtdeb_mm = dates.get('dtdebn', dtdeb_an)
    
    # Calculate month length for expo_gli (SAS: "&dtfinmn"d - "&dtfinmm1"d)
    # dtfinmn = last day of current month, dtfinmm1 = last day of previous month
    dtfinmn = dates['dtfinmn']
    dtfinmm1 = dates['dtfinmm1']
    
    # Import to_date to convert string dates to date type
    from pyspark.sql.functions import to_date # type: ignore
    
    # Convert string dates to date type to preserve date columns
    dtfin_date = to_date(lit(dtfin), 'yyyy-MM-dd')
    dtdeb_an_date = to_date(lit(dtdeb_an), 'yyyy-MM-dd')
    dtdeb_mm_date = to_date(lit(dtdeb_mm), 'yyyy-MM-dd')
    dtfinmn_date = to_date(lit(dtfinmn), 'yyyy-MM-dd')
    dtfinmm1_date = to_date(lit(dtfinmm1), 'yyyy-MM-dd')

    # Calculate exposure start date
    df = df.withColumn(
        "dt_deb_expo",
        when(col(creation_date) < dtdeb_an_date, dtdeb_an_date).otherwise(col(creation_date))
    )

    # Calculate exposure end date
    df = df.withColumn(
        "dt_fin_expo",
        when(
            col(termination_date).isNotNull() & (col(termination_date) < dtfin_date),
            col(termination_date)
        ).otherwise(dtfin_date)
    )

    # Apply business filter for exposure calculation
    # SAS Reference: PTF_MVTS_AZ_MACRO.sas L304-311
    expo_where_cond = lit(True)

    if 'cdnatp' in df.columns and 'cdsitp' in df.columns:
        # First OR branch: cdnatp in (R,O) with cdsitp conditions
        branch1 = (
            (col('cdnatp').isin(['R', 'O'])) &
            (
                ((col('cdsitp') == '1') & (col(creation_date) <= dtfinmn_date)) |
                ((col('cdsitp') == '3') & (col(termination_date) > dtfinmn_date))
            )
        )
        
        # Second OR branch: cdsitp in (1,3) with complex date logic (SAS L307-311)
        # Excludes cdnatp = 'F'
        branch2 = (
            (col('cdsitp').isin(['1', '3'])) &
            (col('cdnatp') != 'F') &
            (
                # Case 1: dtcrepol <= dtfinmn and (dtresilp is null OR dtfinmn1 <= dtresilp < dtfinmn)
                ((col(creation_date) <= dtfinmn_date) & 
                 (col(termination_date).isNull() | 
                  ((col(termination_date) >= dtfinmm1_date) & (col(termination_date) < dtfinmn_date)))) |
                # Case 2: dtcrepol <= dtfinmn and dtresilp > dtfinmn
                ((col(creation_date) <= dtfinmn_date) & (col(termination_date) > dtfinmn_date)) |
                # Case 3: dtfinmn1 < dtcrepol <= dtfinmn and dtfinmn1 <= dtresilp
                ((col(creation_date) > dtfinmm1_date) & (col(creation_date) <= dtfinmn_date) &
                 (col(termination_date) >= dtfinmm1_date))
            )
        )
        
        expo_where_cond = branch1 | branch2

    # Calculate expo_ytd
    df = df.withColumn(
        "expo_ytd",
        when(
            expo_where_cond &
            (col("dt_deb_expo") <= dtfin_date) & (col("dt_fin_expo") >= dtdeb_an_date),
            (datediff(least(col("dt_fin_expo"), dtfin_date), greatest(col("dt_deb_expo"), dtdeb_an_date)) + 1) / 365.0
        ).otherwise(lit(0))
    )

    # Calculate expo_gli with actual month days (SAS L131: "&dtfinmn"d - "&dtfinmm1"d)
    month_days = datediff(dtfinmn_date, dtfinmm1_date)
    
    df = df.withColumn(
        "expo_gli",
        when(
            expo_where_cond &
            (col("dt_deb_expo") <= dtfin_date) & (col("dt_fin_expo") >= dtdeb_mm_date),
            (datediff(least(col("dt_fin_expo"), dtfin_date), greatest(col("dt_deb_expo"), dtdeb_mm_date)) + 1) / month_days
        ).otherwise(lit(0))
    )

    return df


def calculate_azec_movements(
    df: DataFrame,
    dates: Dict[str, str],
    annee: int,
    mois: int
) -> DataFrame:
    """
    Calculate AZEC-specific movement indicators: NBAFN, NBRES, NBPTF.

    AZEC has different logic than AZ:
    - Product-specific date logic (AZEC_PRODUIT_LIST vs others)
    - Different date fields (datafn, datresil vs dtcrepol, dtresilp)
    - Migration handling (NBPTF_NON_MIGRES_AZEC)

    Based on SAS PTF_MVTS_AZEC_MACRO.sas L143-173

    Args:
        df: Input DataFrame with AZEC data
        dates: Date ranges from helpers.compute_date_ranges()
        annee: Year as integer
        mois: Month as integer

    Returns:
        DataFrame with NBAFN, NBRES, NBPTF columns added

    Example:
        >>> df = calculate_azec_movements(df, dates, 2025, 9)
    """
    from config.variables import AZEC_PRODUIT_LIST

    dtdeb_an = dates['DTDEB_AN']
    dtfinmn = dates['dtfinmn']  # Last day of month

    # Initialize movement columns
    df = df.withColumn("nbafn", lit(0))
    df = df.withColumn("nbres", lit(0))
    df = df.withColumn("nbptf", lit(0))

    # NBAFN (Affaires Nouvelles) - SAS L82-89
    # Base condition: ETATPOL = "R" AND PRODUIT NOT IN ("CNR", "DO0") AND NBPTF_NON_MIGRES_AZEC = 1
    base_afn_cond = (
        (col("etatpol") == "R") &
        (~col("produit").isin(["CNR", "DO0"])) &  # FIXED: Changed D00 to DO0
        (col("nbptf_non_migres_azec") == 1)
    )

    # Product-specific logic for NBAFN
    # If PRODUIT in AZEC_PRODUIT_LIST: month(datafn) <= mois AND year(datafn) = annee
    # Else: Complex date logic with effetpol and datafn
    afn_produit_in_list = (
        col("produit").isin(AZEC_PRODUIT_LIST) &
        (month(col("datafn")) <= mois) &
        (year(col("datafn")) == annee)
    )

    afn_produit_not_in_list = (
        ~col("produit").isin(AZEC_PRODUIT_LIST) &
        (
            (
                (col("effetpol") >= lit(dtdeb_an)) &
                (col("effetpol") <= lit(dtfinmn)) &
                (col("datafn") <= lit(dtfinmn))
            ) |
            (
                (col("effetpol") < lit(dtdeb_an)) &
                (col("datafn") >= lit(dtdeb_an)) &
                (col("datafn") <= lit(dtfinmn))
            )
        )
    )

    df = df.withColumn(
        "nbafn",
        when(
            base_afn_cond & (afn_produit_in_list | afn_produit_not_in_list),
            lit(1)
        ).otherwise(lit(0))
    )

    # NBRES (Résiliations) - SAS L92-99
    # Base condition: Same as NBAFN
    base_res_cond = (
        (col("etatpol") == "R") &
        (~col("produit").isin(["CNR", "DO0"])) &  # FIXED: Changed D00 to DO0
        (col("nbptf_non_migres_azec") == 1)
    )

    # Product-specific logic for NBRES
    # If PRODUIT in AZEC_PRODUIT_LIST: month(datresil) <= mois AND year(datresil) = annee
    # Else: Complex date logic with datfin and datresil
    res_produit_in_list = (
        col("produit").isin(AZEC_PRODUIT_LIST) &
        (month(col("datresil")) <= mois) &
        (year(col("datresil")) == annee)
    )

    res_produit_not_in_list = (
        ~col("produit").isin(AZEC_PRODUIT_LIST) &
        (
            (
                (col("datfin") >= lit(dtdeb_an)) &
                (col("datfin") <= lit(dtfinmn)) &
                (col("datresil") <= lit(dtfinmn))
            ) |
            (
                (col("datfin") <= lit(dtfinmn)) &
                (col("datresil") >= lit(dtdeb_an)) &
                (col("datresil") <= lit(dtfinmn))
            )
        )
    )

    df = df.withColumn(
        "nbres",
        when(
            base_res_cond & (res_produit_in_list | res_produit_not_in_list),
            lit(1)
        ).otherwise(lit(0))
    )

    # NBPTF (Portefeuille) - SAS L102-107
    # Complex condition combining multiple criteria
    nbptf_cond = (
        (col("nbptf_non_migres_azec") == 1) &
        (col("effetpol") <= lit(dtfinmn)) &
        (col("datafn") <= lit(dtfinmn)) &
        (
            col("datfin").isNull() |
            (col("datfin") > lit(dtfinmn)) |
            (col("datresil") > lit(dtfinmn))
        ) &
        (
            (col("etatpol") == "E") |
            ((col("etatpol") == "R") & (col("datfin") >= lit(dtfinmn)))
        ) &
        (~col("produit").isin(["DO0", "TRC", "CTR", "CNR"]))  # FIXED: Changed D00 to DO0
    )

    df = df.withColumn(
        "nbptf",
        when(nbptf_cond, lit(1)).otherwise(lit(0))
    )

    return df


def calculate_azec_suspension(df: DataFrame, dates: Dict[str, str]) -> DataFrame:
    """
    Calculate nbj_susp_ytd (suspension period in days) for AZEC contracts.

    Handles contracts with suspension periods during the year-to-date period.

    Based on SAS PTF_MVTS_AZEC_MACRO.sas L188-203

    Args:
        df: Input DataFrame
        dates: Date ranges dictionary

    Returns:
        DataFrame with nbj_susp_ytd column added

    Example:
        >>> df = calculate_azec_suspension(df, dates)
    """
    dtdebn = dates['dtdebn']
    dtfinmn = dates['dtfinmn']

    # SAS Logic:
    # CASE
    #   WHEN (dtdebn <= datresil <= dtfinmn) OR (dtdebn <= datfin <= dtfinmn)
    #     THEN min(datfin, dtfinmn, datexpir) - max(dtdebn-1, datresil-1)
    #   WHEN (0 <= datresil <= dtdebn) AND (datfin >= dtfinmn)
    #     THEN (dtfinmn - dtdebn + 1)
    #   ELSE 0
    # END

    # Condition 1: Suspension within the period
    cond1 = (
        (
            (col("datresil") >= lit(dtdebn)) &
            (col("datresil") <= lit(dtfinmn))
        ) |
        (
            (col("datfin") >= lit(dtdebn)) &
            (col("datfin") <= lit(dtfinmn))
        )
    )

    # Calculate days: min(datfin, dtfinmn, datexpir) - max(dtdebn-1, datresil-1)
    susp_days_1 = datediff(
        least(col("datfin"), lit(dtfinmn), col("datexpir")),
        greatest(date_sub(lit(dtdebn), 1), date_sub(col("datresil"), 1))
    )

    # Condition 2: Suspension started before period and extends through it
    cond2 = (
        col("datresil").isNotNull() &
        (col("datresil") <= lit(dtdebn)) &
        (col("datfin") >= lit(dtfinmn))
    )

    # Calculate days: (dtfinmn - dtdebn + 1)
    susp_days_2 = datediff(lit(dtfinmn), lit(dtdebn)) + 1

    df = df.withColumn(
        "nbj_susp_ytd",
        when(cond1, susp_days_1)
        .when(cond2, susp_days_2)
        .otherwise(lit(0))
    )

    return df
