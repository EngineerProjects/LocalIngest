"""
Processor Helpers - Consolidated Reference Data & Deduplication Utilities.

Provides reusable patterns for:
1. Safe reference data joins with automatic NULL fallback
2. Bulk NULL column addition
3. Segmentation enrichment

Eliminates 250+ lines of duplicate code across processors.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, broadcast, col
from pyspark.sql.types import StringType, DoubleType, IntegerType, DateType
from typing import Union, List, Dict, Optional


# ============================================================================
# BronzeReader Factory
# ============================================================================

def get_bronze_reader(processor):
    """
    Factory function to create BronzeReader instance from processor context.
    
    Eliminates repeated pattern: reader = BronzeReader(self.spark, self.config)
    Found in 17 locations across 5 processor files.
    
    Args:
        processor: Processor instance (BaseProcessor subclass)
    
    Returns:
        BronzeReader instance initialized with processor's spark and config
    
    Example:
        reader = get_bronze_reader(self)
        df = reader.read_file_group('ipf_az', vision)
    """
    from src.reader import BronzeReader
    return BronzeReader(processor.spark, processor.config)


# ============================================================================
# Reference Data Join Helpers
# ============================================================================

def safe_reference_join(
    df: DataFrame,
    reader,
    file_group: str,
    vision: Union[str, None],
    join_keys: Union[str, List[str]],
    select_columns: List[str],
    null_columns: Optional[Dict[str, type]] = None,
    filter_condition: Optional[str] = None,
    use_broadcast: bool = True,
    how: str = "left",
    logger = None,
    required: bool = False
) -> DataFrame:
    """
    Safely join reference data with configurable error handling.
    
    Args:
        df: Input DataFrame
        reader: BronzeReader instance
        file_group: Name of file group to read
        vision: Vision string or 'ref' for reference data
        join_keys: Column name(s) to join on
        select_columns: Columns to select from reference table
        null_columns: Dict of {column: type} for NULL fallback (if None, infers from select_columns)
        filter_condition: Optional filter to apply on reference table (e.g., "cmarch == '6'")
        use_broadcast: Whether to broadcast the reference table
        how: Join type (default: 'left')
        logger: Logger instance for info/warning messages
        required: If True, raise error when reference data missing (fail-fast).
                  If False, add NULL columns as fallback (default: False)
    
    Returns:
        DataFrame with reference columns joined or NULL columns if unavailable (when required=False)
    
    Raises:
        RuntimeError: When required=True and reference data is unavailable
    """
    if logger:
        logger.debug(f"Attempting to join reference data: {file_group} (required={required})")
    
    try:
        # Read reference data
        df_ref = reader.read_file_group(file_group, vision)
        
        if df_ref is None:
            if required:
                error_msg = f"CRITICAL: Required reference data '{file_group}' is unavailable (returned None)"
                if logger:
                    logger.error(error_msg)
                raise RuntimeError(error_msg)
            else:
                if logger:
                    logger.warning(f"Optional reference data '{file_group}' returned None - adding NULL columns")
                return _add_null_columns(df, null_columns or _infer_null_columns(select_columns))
        
        # Apply filter if provided
        if filter_condition:
            df_ref = df_ref.filter(filter_condition)
        
        # Normalize join keys to list
        if isinstance(join_keys, str):
            join_keys = [join_keys]
        
        # Select only needed columns (+ join keys)
        needed_cols = list(set(join_keys + select_columns))
        df_ref = df_ref.select(*[col(c) for c in needed_cols if c in df_ref.columns])
        
        # Apply broadcast if requested
        if use_broadcast:
            df_ref = broadcast(df_ref)
        
        # Perform join
        df_result = df.join(df_ref, on=join_keys, how=how)
        
        if logger:
            logger.info(f"Successfully joined reference data: {file_group}")
        
        return df_result
        
    except FileNotFoundError as e:
        if required:
            error_msg = f"CRITICAL: Required reference file '{file_group}' not found"
            if logger:
                logger.error(f"{error_msg}: {e}")
            raise RuntimeError(error_msg) from e
        else:
            if logger:
                logger.warning(f"Optional reference '{file_group}' not found: {e} - adding NULL columns")
            return _add_null_columns(df, null_columns or _infer_null_columns(select_columns))
    
    except Exception as e:
        # Always raise on unexpected errors (not just missing files)
        error_msg = f"Unexpected error joining reference data '{file_group}': {e}"
        if logger:
            logger.error(error_msg)
        raise RuntimeError(error_msg) from e


def safe_multi_reference_join(
    df: DataFrame,
    reader,
    joins: List[Dict],
    logger=None
) -> DataFrame:
    """
    Perform multiple safe reference joins in sequence.
    
    Args:
        df: Input DataFrame
        reader: BronzeReader instance
        joins: List of join specifications, each a dict with:
            - file_group: str
            - vision: str
            - join_keys: str or List[str]
            - select_columns: List[str]
            - null_columns: Optional[Dict[str, type]]
            - filter_condition: Optional[str]
            - use_broadcast: bool (default True)
        logger: Logger instance
    
    Returns:
        DataFrame with all reference joins applied
        
    Example:
        df = safe_multi_reference_join(df, reader, [
            {
                'file_group': 'cproduit',
                'vision': 'ref',
                'join_keys': 'cdprod',
                'select_columns': ['type_produit_2', 'segment2'],
                'null_columns': {'type_produit_2': StringType, 'segment2': StringType}
            },
            {
                'file_group': 'table_pt_gest',
                'vision': 'ref',
                'join_keys': 'ptgst',
                'select_columns': ['upper_mid'],
                'null_columns': {'upper_mid': StringType}
            }
        ], logger=self.logger)
    """
    for join_spec in joins:
        df = safe_reference_join(
            df, reader,
            file_group=join_spec['file_group'],
            vision=join_spec.get('vision', 'ref'),
            join_keys=join_spec['join_keys'],
            select_columns=join_spec['select_columns'],
            null_columns=join_spec.get('null_columns'),
            filter_condition=join_spec.get('filter_condition'),
            use_broadcast=join_spec.get('use_broadcast', True),
            how=join_spec.get('how', 'left'),
            logger=logger
        )
    
    return df


# ============================================================================
# NULL Column Helpers
# ============================================================================

def add_null_columns(df: DataFrame, column_specs: Dict[str, type]) -> DataFrame:
    """
    Add multiple NULL columns at once (bulk operation).
    
    Consolidates patterns like:
        df = df.withColumn('col1', lit(None).cast(StringType()))
        df = df.withColumn('col2', lit(None).cast(StringType()))
        df = df.withColumn('col3', lit(None).cast(DoubleType()))
    
    Into:
        df = add_null_columns(df, {
            'col1': StringType,
            'col2': StringType,
            'col3': DoubleType
        })
    
    Args:
        df: Input DataFrame
        column_specs: Dict mapping column names to PySpark types
    
    Returns:
        DataFrame with NULL columns added
        
    Example:
        df = add_null_columns(df, {
            'mtcaenp': DoubleType,
            'mtcasst': DoubleType,
            'mtcavnt': DoubleType
        })
    """
    for col_name, col_type in column_specs.items():
        df = df.withColumn(col_name, lit(None).cast(col_type()))
    return df


# ============================================================================
# Segmentation Helper
# ============================================================================

def enrich_segmentation(
    df: DataFrame,
    reader,
    vision: str,
    market_filter: str = None,  # Will default to MARKET_CODE.MARKET if None
    join_key: str = "cdprod",
    include_cdpole: bool = False,
    logger=None
) -> DataFrame:
    """
    Enrich DataFrame with segmentation data from SEGMENTPRDT.
    
    Consolidates the pattern used in:
    - az_capitaux_processor.py L209-223
    - azec_capitaux_processor.py L122-143
    - emissions_processor.py L179-200
    
    Args:
        df: Input DataFrame
        reader: BronzeReader instance
        vision: Vision string
        market_filter: Market code filter (default: '6' for construction)
        join_key: Column to join on (default: 'cdprod')
        include_cdpole: If True, join on BOTH cdprod AND cdpole (SAS emissions logic)
        logger: Logger instance
    
    Returns:
        DataFrame with segmentation columns (cmarch, cseg, cssseg) or NULL fallback
        
    Example:
        df = enrich_segmentation(df, reader, vision, logger=self.logger)
        # For emissions (requires cdpole join):
        df = enrich_segmentation(df, reader, vision, include_cdpole=True, logger=self.logger)
    """
    if logger:
        logger.debug("Enriching with segmentation data (SEGMENTPRDT)")
    
    # Default to construction market if not specified
    if market_filter is None:
        from config.constants import MARKET_CODE
        market_filter = MARKET_CODE.MARKET
    
    try:
        df_seg = reader.read_file_group('segmentprdt', vision)
        
        if df_seg is None:
            if logger:
                logger.warning("SEGMENTPRDT not available - using NULL values")
            return _add_seg_null_columns(df)
        
        # Filter for construction market
        df_seg = df_seg.filter(col("cmarch") == market_filter)
        
        # Rename cprod to cdprod if needed (segmentation file uses 'cprod')
        if 'cprod' in df_seg.columns and join_key == 'cdprod':
            df_seg = df_seg.withColumnRenamed('cprod', 'cdprod')
        
        # Prepare join columns based on SAS logic
        if include_cdpole:
            # EMISSIONS logic (SAS L264-265): Join on BOTH cdprod AND cdpole
            # This ensures correct segmentation for each product+channel combination
            if 'cdpole' not in df.columns:
                if logger:
                    logger.error("cdpole column required for emissions segmentation join!")
                return _add_seg_null_columns(df)
            
            # Select needed columns including cdpole
            df_seg = df_seg.select(join_key, 'cdpole', 'cmarch', 'cseg', 'cssseg') \
                           .dropDuplicates([join_key, 'cdpole'])
            
            # Join on BOTH cdprod AND cdpole
            df_result = df.join(
                broadcast(df_seg),
                on=[join_key, 'cdpole'],
                how="left"
            )
            
            if logger:
                logger.info("Segmentation enrichment successful (joined on cdprod + cdpole)")
        else:
            # CAPITAUX logic: Join on cdprod only
            df_seg = df_seg.select(join_key, 'cmarch', 'cseg', 'cssseg') \
                           .dropDuplicates([join_key])
            
            # Join with broadcast
            df_result = df.join(
                broadcast(df_seg),
                on=join_key,
                how="left"
            )
            
            if logger:
                logger.info("Segmentation enrichment successful (joined on cdprod only)")
        
        return df_result
        
    except Exception as e:
        if logger:
            logger.warning(f"Segmentation enrichment failed: {e}. Using NULL values.")
        return _add_seg_null_columns(df)



# ============================================================================
# Internal Helpers
# ============================================================================

def _add_null_columns(df: DataFrame, column_specs: Dict[str, type]) -> DataFrame:
    """Internal helper for adding NULL columns (same as public version)."""
    return add_null_columns(df, column_specs)


def _infer_null_columns(select_columns: List[str]) -> Dict[str, type]:
    """
    Infer NULL column types (defaults to StringType).
    
    This is a fallback when null_columns not provided.
    For better type safety, always provide explicit null_columns.
    """
    return {col_name: StringType for col_name in select_columns}


def _add_seg_null_columns(df: DataFrame) -> DataFrame:
    """Add NULL segmentation columns as fallback."""
    return (df
            .withColumn('cmarch', lit(None).cast(StringType()))
            .withColumn('cseg', lit(None).cast(StringType()))
            .withColumn('cssseg', lit(None).cast(StringType())))
