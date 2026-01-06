"""
Helper utilities for Construction Data Pipeline.
Vision parsing, path building, date range computations.
"""

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Tuple, Dict, Optional
from pathlib import Path


def validate_vision(vision: str) -> bool:
    """
    Validate vision format (YYYYMM).

    Args:
        vision: Vision string to validate

    Returns:
        True if valid, False otherwise

    Example:
        >>> validate_vision("202509")
        True
        >>> validate_vision("20251")
        False
        >>> validate_vision("202513")
        False
    """
    if not isinstance(vision, str) or len(vision) != 6:
        return False

    try:
        year = int(vision[:4])
        month = int(vision[4:6])

        # Basic validation
        if year < 2000 or year > 2100:
            return False
        if month < 1 or month > 12:
            return False

        return True
    except ValueError:
        return False


def extract_year_month(vision: str) -> Tuple[str, str]:
    """
    Extract year and month from vision.

    Args:
        vision: Vision in YYYYMM format

    Returns:
        Tuple of (year, month) as strings ('YYYY', 'MM')

    Raises:
        ValueError: If vision format is invalid

    Example:
        >>> year, month = extract_year_month("202509")
        >>> print(year, month)
        '2025' '09'
    """
    if not validate_vision(vision):
        raise ValueError(
            f"Invalid vision format: {vision}. Expected YYYYMM (e.g., 202509)"
        )

    year = vision[:4]
    month = vision[4:6]

    return year, month


def extract_year_month_int(vision: str) -> Tuple[int, int]:
    """
    Extract year and month from vision as integers.

    Args:
        vision: Vision in YYYYMM format

    Returns:
        Tuple of (year, month) as integers

    Raises:
        ValueError: If vision format is invalid

    Example:
        >>> year, month = extract_year_month_int("202509")
        >>> print(year, month)
        2025 9
    """
    year_str, month_str = extract_year_month(vision)
    return int(year_str), int(month_str)


def build_layer_path(
    base_path: str,
    layer: str,
    vision: str,
    path_template: str = None
) -> str:
    """
    Build path for a specific layer following datalake structure.

    Args:
        base_path: Base datalake path
        layer: Layer name (bronze, silver, gold)
        vision: Vision in YYYYMM format
        path_template: Optional custom template (default: "{base_path}/{layer}/{year}/{month}")

    Returns:
        Full path to the layer

    Example:
        >>> path = build_layer_path(
        ...     "abfss://container@account.dfs.core.windows.net/construction",
        ...     "bronze",
        ...     "202509"
        ... )
        >>> print(path)
        'abfss://container@account.dfs.core.windows.net/construction/bronze/2025/09'
    """
    year, month = extract_year_month(vision)

    if path_template is None:
        path_template = "{base_path}/{layer}/{year}/{month}"

    path = path_template.format(
        base_path=base_path,
        layer=layer,
        year=year,
        month=month
    )

    return path


def build_log_filename(vision: str) -> str:
    """
    Build log file name for a vision.

    Args:
        vision: Vision in YYYYMM format

    Returns:
        Log filename (e.g., 'pipeline_202509.log')

    Example:
        >>> filename = build_log_filename("202509")
        >>> print(filename)
        'pipeline_202509.log'
    """
    if not validate_vision(vision):
        raise ValueError(f"Invalid vision format: {vision}. Expected YYYYMM.")

    return f"pipeline_{vision}.log"


def compute_date_ranges(vision: str) -> Dict[str, str]:
    """
    Compute all date ranges required for PTF_MVT processing.
    
    Generates dates used in movement calculations, exposures, etc.

    Args:
        vision: Vision in YYYYMM format

    Returns:
        Dictionary with computed dates:
        - 'DTFIN': Last day of vision month (YYYY-MM-DD)
        - 'DTDEB_AN': First day of vision year (YYYY-01-01)
        - 'dtfinmm': Last day of vision month (same as DTFIN)
        - 'dtfinmm1': Last day of previous month
        - 'dtdebn': First day of vision month
        - 'dtfinmn': Last day of vision month
        - 'finmois': Last day of vision month

    Raises:
        ValueError: If vision format is invalid

    Example:
        >>> dates = compute_date_ranges("202509")
        >>> print(dates['DTFIN'])
        '2025-09-30'
        >>> print(dates['DTDEB_AN'])
        '2025-01-01'
        >>> print(dates['dtfinmm1'])
        '2025-08-31'
    """
    if not validate_vision(vision):
        raise ValueError(f"Invalid vision format: {vision}")

    year, month = extract_year_month_int(vision)

    # Last day of vision month
    if month == 12:
        last_day_of_month = datetime(year, month, 31)
    else:
        # Go to first day of next month, then subtract one day
        next_month = datetime(year, month + 1, 1) if month < 12 else datetime(year + 1, 1, 1)
        last_day_of_month = next_month - timedelta(days=1)

    # First day of vision year
    first_day_of_year = datetime(year, 1, 1)

    # First day of vision month
    first_day_of_month = datetime(year, month, 1)

    # Last day of previous month
    first_day_this_month = datetime(year, month, 1)
    last_day_prev_month = first_day_this_month - timedelta(days=1)

    # Format all dates as strings (YYYY-MM-DD)
    dates = {
        'DTFIN': last_day_of_month.strftime('%Y-%m-%d'),
        'DTDEB_AN': first_day_of_year.strftime('%Y-%m-%d'),
        'dtfinmm': last_day_of_month.strftime('%Y-%m-%d'),
        'dtfinmm1': last_day_prev_month.strftime('%Y-%m-%d'),
        'dtdebn': first_day_of_month.strftime('%Y-%m-%d'),
        'dtfinmn': last_day_of_month.strftime('%Y-%m-%d'),
        'finmois': last_day_of_month.strftime('%Y-%m-%d'),
    }

    return dates


def write_to_layer(
    df,  # DataFrame type - avoid pyspark import in helpers
    config,  # ConfigLoader type
    layer: str,
    filename: str,
    vision: str,
    logger=None  # PipelineLogger type
) -> None:
    """
    Generic function to write a Spark DataFrame to a data lake layer (silver or gold).

    This function centralizes all logic related to output path construction,
    format selection (Parquet, Delta, CSV), compression, write mode, and optional
    partition coalescing for optimized file sizes.

    The output path is automatically built using the configured base path,
    layer name, and the vision period (YYYYMM), ensuring consistent folder
    structure across all pipelines.

    Features:
        - Supports multiple output formats via config: parquet, delta, csv
        - Automatically handles Delta-specific behavior (no file extension)
        - Optional coalesce optimization for gold layer to reduce small files
        - Delta Lake: OPTIMIZE + Z-Ordering for performance
        - Delta Lake: Schema enforcement to prevent data corruption
        - Centralized logging for traceability
        - Ensures consistent naming: <filename>_<vision>.<format>

    Args:
        df: Spark DataFrame to write. Columns are expected to be lowercase.
        config: Configuration loader providing datalake paths and output settings.
        layer: Target layer in the data lake ("silver" or "gold").
        filename: Base name of the output dataset (without extension).
        vision: Vision period in YYYYMM format, used for folder structure and filename.
        logger: Optional logger for structured pipeline logging.
        optimize: If True and format is Delta, run OPTIMIZE after write (default: True)
        zorder_columns: Optional columns for Z-Ordering (Delta only, improves data skipping)

    Examples:
        Write to Silver layer in Parquet:
            >>> write_to_layer(df, config, "silver", "mvt_const_ptf", "202509", logger)
            # → .../silver/2025/09/mvt_const_ptf_202509.parquet

        Write to Gold layer in Delta with Z-Ordering:
            >>> write_to_layer(
            ...     df, config, "gold", "construction_portfolio", "202509", logger,
            ...     optimize=True, zorder_columns=["police", "dtfin"]
            ... )
            # → .../gold/2025/09/construction_portfolio_202509 (Delta directory)

    Notes:
        - Delta format writes a directory, not a single file, so no extension is added.
        - Coalescing is applied only for gold layer to reduce the number of small files.
        - All write behavior (format, mode, compression) is controlled by config.
        - Delta tables are automatically optimized after write if optimize=True.
    """
    from typing import List, Optional
    
    base_path = config.get('datalake.base_path')
    path_template = config.get('datalake.path_template')
    year, month = extract_year_month_int(vision)
    
    # Build layer path using template
    layer_path = path_template.format(
        base_path=base_path,
        layer=layer,
        year=year,
        month=f"{month:02d}"
    )
    
    # Get output configuration
    output_format = config.get('output.format', 'parquet').lower()
    compression = config.get('output.compression', 'snappy')
    mode = config.get('output.mode', 'overwrite')
    
    # Delta tables do NOT use file extensions (they are directories)
    if output_format == "delta":
        output_path = f"{layer_path}/{filename}"
    else:
        output_path = f"{layer_path}/{filename}.{output_format}"

    if logger:
        logger.info(f"Writing {layer} data to: {output_path}")
        logger.info(f"Format: {output_format}, Compression: {compression}, Mode: {mode}")

    # OPTIMIZATION: Coalesce output for small datasets to avoid small file problem
    # Reduces number of output files while maintaining performance
    try:
        # Only count for gold layer or if explicitly requested
        if layer == 'gold':
            row_count = df.count()

            if row_count < 1_000_000:  # < 1M rows: single file
                df = df.coalesce(1)
                if logger:
                    logger.debug(f"Coalescing to 1 partition ({row_count:,} rows)")
            elif row_count < 10_000_000:  # < 10M rows: 4 files
                df = df.coalesce(4)
                if logger:
                    logger.debug(f"Coalescing to 4 partitions ({row_count:,} rows)")
            # else: Keep default partitioning for large datasets
    except Exception as e:
        # If count fails, skip coalescing
        if logger:
            logger.debug(f"Skipping coalesce optimization: {e}")

    # --- Write logic depending on format ---
    writer = df.write.mode(mode)

    if output_format == "parquet":
        writer.option("compression", compression).parquet(output_path)

    elif output_format == "delta":
        # Delta Lake with schema enforcement
        writer = (
            writer
            .format("delta")
            .option("mergeSchema", "false")  # Refuse schema changes (data quality)
            .option("overwriteSchema", str(mode == "overwrite").lower())  # Allow schema overwrite only in overwrite mode
        )
        writer.save(output_path)

    elif output_format == "csv":
        writer.option("header", True).option("delimiter", ";").csv(output_path)

    else:
        raise ValueError(f"Unsupported output format: {output_format}")

    if logger:
        logger.success(f"{layer.capitalize()} data written successfully")


def upload_log_to_datalake(
    spark,
    local_log_path: str,
    datalake_base_path: str
) -> bool:
    """
    Upload local log file to Azure Data Lake bronze/logs container.
    
    Uses Spark to upload to datalake and deletes local file on success.
    
    Args:
        spark: SparkSession instance
        local_log_path: Path to local log file (e.g., "logs/pipeline_202509.log")
        datalake_base_path: Base datalake path
    
    Returns:
        True if successful, False otherwise
    
    Example:
        >>> success = upload_log_to_datalake(
        ...     spark,
        ...     "logs/pipeline_202509.log",
        ...     "abfss://container@account.dfs.core.windows.net/construction"
        ... )
        >>> # Uploads to: {base_path}/bronze/logs/pipeline_202509.log
    """
    try:
        local_path = Path(local_log_path)
        
        if not local_path.exists():
            return False
        
        print(f"Local path to log file : {local_path}")
        datalake_log_path = f"{datalake_base_path}/bronze/logs/{local_path.name}"
        print(f"Path to log file on the datalake : {datalake_log_path}")
        
        # Read the local file
        with open(local_path, 'r', encoding="utf-8") as f:
            file_content = f.read()
        
        # Get the FileSystem for ADLS
        hadoop_conf = spark._jsc.hadoopConfiguration()
        
        # Create destination path
        dest_path = spark._jvm.org.apache.hadoop.fs.Path(datalake_log_path)
        
        # Get FileSystem from the path itself
        fs = dest_path.getFileSystem(hadoop_conf)
        
        # Open a writing stream
        fs_output_stream = fs.create(dest_path, True)  # True = Overwrite
        output_stream_writer = spark._jvm.java.io.OutputStreamWriter(fs_output_stream, "UTF-8")
        buffered_writer = spark._jvm.java.io.BufferedWriter(output_stream_writer)
        
        # Write the content
        buffered_writer.write(file_content)
        
        buffered_writer.flush()
        buffered_writer.close()
        output_stream_writer.close()
        fs_output_stream.close()
        
        print("√ File uploaded successfully")
        
        # Delete local file after successful upload
        local_path.unlink()
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False
