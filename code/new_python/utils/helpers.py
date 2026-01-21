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
    """
    if not validate_vision(vision):
        raise ValueError(f"Invalid vision format: {vision}")

    year, month = extract_year_month_int(vision)

    # Last day of vision month
    if month == 12:
        last_day_of_month = datetime(year, month, 31)
    else:
        next_month = datetime(year, month + 1, 1) if month < 12 else datetime(year + 1, 1, 1)
        last_day_of_month = next_month - timedelta(days=1)

    # First day of vision year
    first_day_of_year = datetime(year, 1, 1)

    # First day of vision month
    first_day_of_month = datetime(year, month, 1)

    # Last day of previous month
    first_day_this_month = datetime(year, month, 1)
    last_day_prev_month = first_day_this_month - timedelta(days=1)

    # Format all dates as strings (YYYY-MM-DD) — LOWERCASE KEYS ONLY
    dates = {
        'dtfin':     last_day_of_month.strftime('%Y-%m-%d'),
        'dtdeb_an':  first_day_of_year.strftime('%Y-%m-%d'),
        'dtfinmm':   last_day_of_month.strftime('%Y-%m-%d'),
        'dtfinmm1':  last_day_prev_month.strftime('%Y-%m-%d'),
        'dtdebn':    first_day_of_month.strftime('%Y-%m-%d'),
        'dtfinmn':   last_day_of_month.strftime('%Y-%m-%d'),
        'finmois':   last_day_of_month.strftime('%Y-%m-%d'),
    }

    return dates

def write_to_layer(
    df,
    config,
    layer: str,
    filename: str,
    vision: str,
    logger=None,
    optimize: bool = False,
    zorder_cols: Optional[List[str]] = None
) -> None:
    """
    Generic datalake writer for silver/gold layers.
    Supports Delta, Parquet, CSV.

    Features:
    - Dynamic overwrite/append mode from config.yml
    - Delta-safe overwrite (no duplicate partitions)
    - Optional OPTIMIZE + ZORDER per-table (not global)
    - Optional VACUUM cleanup
    - Coalesce gold files
    
    Args:
        df: DataFrame to write
        config: ConfigLoader instance
        layer: 'silver' or 'gold'
        filename: Output filename (without extension for parquet/csv)
        vision: YYYYMM format
        logger: Optional logger
        optimize: If True, run OPTIMIZE + ZORDER after Delta write
        zorder_cols: Columns to ZORDER by (only if optimize=True)
    """

    # Resolve config
    base_path = config.get("datalake.base_path")
    path_template = config.get("datalake.path_template")
    output_format = config.get("output.format", "delta").lower()
    compression = config.get("output.compression", "snappy")
    mode = config.get("output.mode", "overwrite").lower()
    vacuum_hours = config.get("output.vacuum_hours", 168)
    clean = config.get("output.clean", False)  # NEW: Delete before write

    # Build path
    year, month = extract_year_month_int(vision)
    layer_path = path_template.format(
        base_path=base_path,
        layer=layer,
        year=year,
        month=f"{month:02d}"
    )

    # Delta = directory, others = file with extension
    if output_format == "delta":
        output_path = f"{layer_path}/{filename}"
    else:
        output_path = f"{layer_path}/{filename}.{output_format}"

    if logger:
        logger.info(f"Writing to {output_path}")
        logger.info(f"Format={output_format}, Mode={mode}, Layer={layer}, Clean={clean}")

    # ================================================================
    # CLEAN: DELETE EXISTING FILES/DIRECTORY BEFORE WRITE
    # ================================================================
    if clean:
        import shutil
        from pathlib import Path
        
        path_obj = Path(output_path)
        if path_obj.exists():
            if path_obj.is_dir():
                shutil.rmtree(output_path)
                if logger:
                    logger.info(f"🗑️  Cleaned directory: {output_path}")
            else:
                path_obj.unlink()
                if logger:
                    logger.info(f"🗑️  Cleaned file: {output_path}")

    # ================================================================
    # COALESCE OPTIMIZATION FOR GOLD LAYER
    # ================================================================
    if layer == "gold":
        try:
            row_count = df.count()
            if row_count < 1_000_000:
                df = df.coalesce(1)
                if logger:
                    logger.debug(f"Coalesced to 1 partition ({row_count:,} rows)")
            elif row_count < 10_000_000:
                df = df.coalesce(4)
                if logger:
                    logger.debug(f"Coalesced to 4 partitions ({row_count:,} rows)")
        except Exception as e:
            if logger:
                logger.warning(f"Coalesce skipped: {e}")

    # ================================================================
    # WRITE LOGIC BY FORMAT
    # ================================================================
    writer = df.write.mode(mode)

    # PARQUET
    if output_format == "parquet":
        writer.option("compression", compression).parquet(output_path)
        if logger:
            logger.success(f"Parquet write complete: {output_path}")

    # CSV
    elif output_format == "csv":
        writer.option("header", True).option("delimiter", ";").csv(output_path)
        if logger:
            logger.success(f"CSV write complete: {output_path}")

    # DELTA LAKE
    elif output_format == "delta":
        
        # ✅ FIX: Local config only (not global session config)
        writer = writer.format("delta")
        
        # Overwrite schema only in overwrite mode
        if mode == "overwrite":
            writer = writer.option("overwriteSchema", "true")
            # Static overwrite mode (replaces entire table, not individual partitions)
            writer = writer.option("partitionOverwriteMode", "static")
        
        # Execute write
        writer.save(output_path)

        if logger:
            logger.info("Delta write complete")

        # ================================================================
        # DELTA POST-WRITE OPTIMIZATIONS
        # ================================================================
        
        # Import Delta only when needed
        try:
            from delta.tables import DeltaTable
        except ImportError:
            if logger:
                logger.warning("Delta Lake not available - skipping optimizations")
            return
        
        # VACUUM cleanup (remove old file versions)
        try:
            delta_tbl = DeltaTable.forPath(df.sparkSession, output_path)
            delta_tbl.vacuum(vacuum_hours)
            if logger:
                logger.info(f"VACUUM completed (retention={vacuum_hours}h)")
        except Exception as e:
            if logger:
                logger.warning(f"VACUUM skipped: {e}")

        # OPTIMIZE + ZORDER (optional, per-table)
        if optimize:
            try:
                spark = df.sparkSession
                optimize_sql = f"OPTIMIZE delta.`{output_path}`"

                if zorder_cols:
                    # Validate columns exist
                    invalid_cols = [c for c in zorder_cols if c not in df.columns]
                    if invalid_cols:
                        if logger:
                            logger.warning(f"ZORDER cols not found: {invalid_cols}")
                        # Only use valid columns
                        valid_zorder = [c for c in zorder_cols if c in df.columns]
                        if valid_zorder:
                            optimize_sql += " ZORDER BY (" + ", ".join(valid_zorder) + ")"
                    else:
                        optimize_sql += " ZORDER BY (" + ", ".join(zorder_cols) + ")"

                spark.sql(optimize_sql)

                if logger:
                    logger.success(f"OPTIMIZE completed: {output_path}")

            except Exception as e:
                if logger:
                    logger.warning(f"OPTIMIZE failed: {e}")

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