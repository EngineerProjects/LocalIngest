"""
Capital Indexation for Construction Data Pipeline.

Implements the indexation logic from indexation_v2.sas macro.
Indexes capital amounts based on construction cost indices.

Based on: indexation_v2.sas (109 lines)
"""

from pyspark.sql import DataFrame # type: ignore
from pyspark.sql.functions import ( # type: ignore
    col, when, lit, year, month, dayofmonth, current_date,
    date_format, concat, lpad, to_date, coalesce, substring
)
from typing import Optional, Any, Dict


def index_capitals(
    df: DataFrame,
    num_capitals: int = 14,
    date_col: str = "dtechamm",
    contract_start_col: str = "dtefsitt",
    capital_prefix: str = "mtcapi",
    nature_prefix: str = "cdprvb",
    index_prefix: str = "prprvc",
    reference_date: Optional[str] = None,
    index_table_df: Optional[DataFrame] = None,
    logger: Optional[Any] = None
) -> DataFrame:
    """
    Index capital amounts using construction cost indices.

    Implements indexation_v2.sas macro logic (L32-108):
    - Calculates anniversary date from dtechamm
    - Retrieves origin index from DTEFSITT + nature code
    - Retrieves target index from anniversary date + nature code
    - Applies indexation formula: mtcapi_indexed = mtcapi * (indx_target / indx_origin)
    - If target date < origin date: no indexation (ratio = 1)

    Args:
        df: Input DataFrame
        num_capitals: Number of capital columns to index (default 14)
        date_col: Anniversary date column (default 'dtechamm') - SAS: DTECHANN
        contract_start_col: Contract start date (default 'dtefsitt') - SAS: DTEFSITT
        capital_prefix: Capital amount column prefix (default 'mtcapi')
        nature_prefix: Provision nature code prefix (default 'cdprvb')
        index_prefix: Index column prefix from existing data (default 'prprvc')
        reference_date: Reference date for indexation (YYYY-MM-DD format)
                       If None, uses current date
        index_table_df: Optional index reference table (structure: nature_code, index_date, index_value)
        logger: Optional logger instance

    Returns:
        DataFrame with indexed capital columns added (mtcapi1i, mtcapi2i, ..., mtcapi14i)
        and index tracking columns (indxorig1i, indxintg1i, ..., indxorig14i, indxintg14i)

    Example:
        >>> df = index_capitals(df, 14, 'dtechamm', 'dtefsitt', 'mtcapi', 'cdprvb',
        ...                     reference_date='2025-09-30', index_table_df=index_df)

    SAS Logic (indexation_v2.sas):
        L42-78: Determine origin date and index
        L80-86: Determine target date index
        L88-96: Apply indexation logic (no indexation if target < origin)
        L98-105: Calculate indexed capital amount

    Formula (L103):
        mtcapi&i.i = ifn(INDXINTG/INDXORIG=., mtcapi&i., mtcapi&i.*(INDXINTG/INDXORIG))
    """
    if logger:
        logger.info(f"Starting capital indexation for {num_capitals} columns")

    # SAS L42-46: Determine reference date (use current date if not specified)
    if reference_date is None:
        # SAS: ANNEE1 = YEAR(DATE())
        reference_date_col = current_date()
    else:
        reference_date_col = lit(reference_date)

    # Index each capital column
    for i in range(1, num_capitals + 1):
        capital_col = f"{capital_prefix}{i}"
        nature_col = f"{nature_prefix}{i}"
        index_col = f"{index_prefix}{i}"  # Existing evolution coefficient from source data
        indexed_col = f"{capital_col}i"
        index_origin_col = f"indxorig{i}i"
        index_target_col = f"indxintg{i}i"

        # Check if capital column exists
        if capital_col not in df.columns:
            if logger:
                logger.debug(f"Skipping {capital_col} - column not found")
            continue

        if logger:
            logger.debug(f"Indexing {capital_col}")

        # =====================================================================
        # DETERMINE ANNIVERSARY DATE (Target Date)
        # SAS L44-52: Extract month/day from DTECHANN, build anniversary date
        # =====================================================================

        # SAS L44-45: JOUR1 = SUBSTR(PUT(DTECHANN,4.),3,2); MOIS1 = SUBSTR(PUT(DTECHANN,4.),1,2)
        df = df.withColumn(
            f"_temp_anniv_month_{i}",
            month(col(date_col))
        )
        df = df.withColumn(
            f"_temp_anniv_day_{i}",
            dayofmonth(col(date_col))
        )

        # SAS L46-47: ANNEE1 = YEAR(DATE()); DATE = MDY(MOIS1,JOUR1,ANNEE1)
        df = df.withColumn(
            f"_temp_anniv_date_{i}",
            to_date(
                concat(
                    year(reference_date_col),
                    lit("-"),
                    lpad(col(f"_temp_anniv_month_{i}"), 2, "0"),
                    lit("-"),
                    lpad(col(f"_temp_anniv_day_{i}"), 2, "0")
                )
            )
        )

        # SAS L49-52: IF DATE > DATE() THEN DO; ANNEE1=ANNEE1-1; DATE=MDY(MOIS1,JOUR1,ANNEE1); END;
        # Adjust year if anniversary date > reference date
        df = df.withColumn(
            f"_temp_target_date_{i}",
            when(
                col(f"_temp_anniv_date_{i}") > reference_date_col,
                to_date(
                    concat(
                        year(reference_date_col) - 1,
                        lit("-"),
                        lpad(col(f"_temp_anniv_month_{i}"), 2, "0"),
                        lit("-"),
                        lpad(col(f"_temp_anniv_day_{i}"), 2, "0")
                    )
                )
            ).otherwise(col(f"_temp_anniv_date_{i}"))
        )

        # =====================================================================
        # RETRIEVE INDICES FROM INDEX TABLE OR EXISTING DATA
        # SAS L54, L75-77, L84-86: Lookup indices from $INDICE format table
        # =====================================================================

        if index_table_df is not None and nature_col in df.columns:
            # Join with index table to get origin and target indices
            # SAS L75: VAL1 = &NOMNAT&IND. !! PUT(DTEFSITT, Z5.)
            # SAS L76: IF SUBSTR(VAL1,1,1)='0' THEN INDXORIG = PUT(VAL1,$INDICE.)

            # Build lookup key for origin index: nature_code + DTEFSITT (MMDDYY format)
            df = df.withColumn(
                f"_temp_origin_key_{i}",
                concat(
                    col(nature_col),
                    date_format(col(contract_start_col), "MMddyy")
                )
            )

            # Build lookup key for target index: nature_code + target_date (MMDDYY format)
            # SAS L84: VAL2 = &NOMNAT&IND. !! PUT(DATE, Z5.)
            df = df.withColumn(
                f"_temp_target_key_{i}",
                concat(
                    col(nature_col),
                    date_format(col(f"_temp_target_date_{i}"), "MMddyy")
                )
            )

            # Join for origin index
            index_origin_alias = f"idx_orig_{i}"
            df = df.join(
                index_table_df.select(
                    col("index_key").alias(f"_origin_key_{i}"),
                    col("index_value").alias(index_origin_alias)
                ),
                df[f"_temp_origin_key_{i}"] == col(f"_origin_key_{i}"),
                "left"
            ).drop(f"_origin_key_{i}")

            # Join for target index
            index_target_alias = f"idx_target_{i}"
            df = df.join(
                index_table_df.select(
                    col("index_key").alias(f"_target_key_{i}"),
                    col("index_value").alias(index_target_alias)
                ),
                df[f"_temp_target_key_{i}"] == col(f"_target_key_{i}"),
                "left"
            ).drop(f"_target_key_{i}")

            # SAS L76-77, L85-86: Only use index if nature code starts with '0'
            df = df.withColumn(
                index_origin_col,
                when(
                    substring(col(nature_col), 1, 1) == "0",
                    coalesce(col(index_origin_alias), lit(1.0))
                ).otherwise(lit(1.0))
            ).drop(index_origin_alias)

            df = df.withColumn(
                index_target_col,
                when(
                    substring(col(nature_col), 1, 1) == "0",
                    coalesce(col(index_target_alias), lit(1.0))
                ).otherwise(lit(1.0))
            ).drop(index_target_alias)

            # Clean up temporary key columns
            df = df.drop(f"_temp_origin_key_{i}", f"_temp_target_key_{i}")

        elif index_col in df.columns:
            # SAS L54: INDXORIG = &NOMIND&IND. (use existing evolution coefficient)
            # Fallback: Use existing index column as origin (from PRPRVC)
            df = df.withColumn(
                index_origin_col,
                coalesce(col(index_col), lit(1.0))
            )
            df = df.withColumn(
                index_target_col,
                lit(1.0)  # Default target index (no indexation)
            )

            if logger:
                logger.debug(f"{capital_col}: Using existing {index_col} as origin index (index table not provided)")
        else:
            # No index data available - use ratio of 1.0 (no indexation)
            df = df.withColumn(index_origin_col, lit(1.0))
            df = df.withColumn(index_target_col, lit(1.0))

            if logger:
                logger.debug(f"{capital_col}: No index data - using ratio 1.0 (no indexation)")

        # =====================================================================
        # APPLY INDEXATION LOGIC
        # SAS L88-96: No indexation if target date < origin date
        # =====================================================================

        # SAS L93-96: IF DATE < DTEFSITT THEN DO; INDXINTG=1; INDXORIG=1; END;
        df = df.withColumn(
            index_origin_col,
            when(
                col(f"_temp_target_date_{i}") < col(contract_start_col),
                lit(1.0)
            ).otherwise(col(index_origin_col))
        )

        df = df.withColumn(
            index_target_col,
            when(
                col(f"_temp_target_date_{i}") < col(contract_start_col),
                lit(1.0)
            ).otherwise(col(index_target_col))
        )

        # =====================================================================
        # CALCULATE INDEXED CAPITAL AMOUNT
        # SAS L103: mtcapi&i.i = ifn(INDXINTG/INDXORIG=., mtcapi&i., mtcapi&i.*(INDXINTG/INDXORIG))
        # =====================================================================

        df = df.withColumn(
            indexed_col,
            when(
                (col(index_target_col).isNull()) |
                (col(index_origin_col).isNull()) |
                (col(index_origin_col) == 0),
                col(capital_col)  # No indexation if indices invalid
            ).otherwise(
                col(capital_col) * (col(index_target_col) / col(index_origin_col))
            )
        )

        # Clean up temporary columns
        df = df.drop(
            f"_temp_anniv_month_{i}",
            f"_temp_anniv_day_{i}",
            f"_temp_anniv_date_{i}",
            f"_temp_target_date_{i}"
        )

    if logger:
        logger.success(f"Capital indexation completed for {num_capitals} columns")

    return df


def load_index_table(
    spark,
    config,
    logger: Optional[Any] = None
) -> Optional[DataFrame]:
    """
    Load construction cost index reference table.

    The index table should have structure:
    - index_key: Concatenation of nature_code + date (MMDDYY format)
                 Example: "0110925" = nature code "01" + date "10/09/25"
    - index_value: Construction cost index coefficient

    This corresponds to SAS format table $INDICE used in indexation_v2.sas.

    Args:
        spark: SparkSession
        config: ConfigLoader instance
        logger: Optional logger

    Returns:
        DataFrame with index reference data, or None if not available

    Note:
        The SAS code uses OPTIONS FMTSEARCH=(INDICES) to access format $INDICE.
        This function loads the equivalent index table from the data lake.
    """
    if logger:
        logger.info("Loading construction cost index table")

    try:
        from src.reader import DataLakeReader

        reader = DataLakeReader(spark, config, logger)

        # Try to load index table from bronze/ref/
        # The exact file name should be configured in reading_config.json
        index_df = reader.read_file_group("construction_indices", vision=None)

        if index_df is not None and index_df.count() > 0:
            if logger:
                logger.success(f"Index table loaded: {index_df.count()} records")
            return index_df
        else:
            if logger:
                logger.warning("Index table is empty")
            return None

    except Exception as e:
        if logger:
            logger.warning(f"Could not load index table: {e}")
            logger.info("Indexation will use default ratio of 1.0 (no indexation)")
        return None


def create_indexed_capital_sums(
    df: DataFrame,
    num_capitals: int = 14,
    capital_prefix: str = "mtcapi",
    label_prefix: str = "lblcap",
    target_columns: Optional[Dict[str, list]] = None,
    logger: Optional[Any] = None
) -> DataFrame:
    """
    Create sum of indexed capitals for specific categories.

    Similar to extract_capitals but using indexed values (mtcapi1i, mtcapi2i, ...).

    Args:
        df: Input DataFrame with indexed capitals
        num_capitals: Number of capital columns
        capital_prefix: Indexed capital column prefix (default 'mtcapi')
        label_prefix: Label column prefix for matching (default 'lblcap')
        target_columns: Dict of target column names to label keywords
                       Example: {'smp_100_indexed': ['SMP GLOBAL', 'SMP RETENU']}
        logger: Optional logger

    Returns:
        DataFrame with summed indexed capital columns

    Example:
        >>> target_cols = {
        ...     'smp_100_indexed': ['SMP GLOBAL', 'SMP RETENU'],
        ...     'lci_100_indexed': ['LCI GLOBAL', 'LIMITE CONTRACTUELLE']
        ... }
        >>> df = create_indexed_capital_sums(df, 14, 'mtcapi', 'lblcap', target_cols)
    """
    if target_columns is None:
        target_columns = {}

    if logger:
        logger.info("Creating indexed capital sums")

    # For each target column, sum indexed capitals matching keywords
    for target_col, keywords in target_columns.items():
        sum_expr = lit(0.0)

        for i in range(1, num_capitals + 1):
            indexed_col = f"{capital_prefix}{i}i"
            label_col = f"{label_prefix}{i}"

            if indexed_col not in df.columns:
                continue

            # Build condition: label matches any keyword
            if label_col in df.columns:
                match_condition = lit(False)
                for keyword in keywords:
                    match_condition = match_condition | col(label_col).contains(keyword)

                # Add to sum if match
                sum_expr = sum_expr + when(match_condition, coalesce(col(indexed_col), lit(0.0))).otherwise(lit(0.0))

        df = df.withColumn(target_col, sum_expr)

    if logger:
        logger.success(f"Created {len(target_columns)} indexed capital sum columns")

    return df
