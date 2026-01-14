"""
Generic bronze layer reader for Construction Data Pipeline.
Reads files from bronze with column selection and filtering.
"""

import json
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame # type: ignore
from pyspark.sql.functions import col, input_file_name, lit # type: ignore
import fnmatch
from typing import List, Dict, Optional, Any
from utils.loaders.config_loader import ConfigLoader
from utils.helpers import build_layer_path


class BronzeReader:
    """Read files from bronze layer with column selection and filtering."""

    def __init__(
        self,
        spark: SparkSession,
        config: ConfigLoader,
        reading_config_path: Optional[str] = None,
        verify_schemas: bool = False,
        logger = None
    ):
        """
        Initialize bronze reader.

        Args:
            spark: SparkSession instance
            config: ConfigLoader instance
            reading_config_path: Path to reading_config.json (optional)
            verify_schemas: If True, verify schemas against actual CSV columns (default: False)
            logger: Optional logger instance for verification messages
        """
        self.spark = spark
        self.config = config
        self.verify_schemas = verify_schemas
        self.logger = logger

        # Load reading configuration
        if reading_config_path is None:
            project_root = Path(__file__).parent.parent
            reading_config_path = project_root / "config" / "reading_config.json"

        with open(reading_config_path, 'r', encoding='LATIN9') as f:
            self.reading_config = json.load(f)

        # Import schema registry
        from config.schemas import get_schema
        self.get_schema = get_schema

    def read_file_group(
        self,
        file_group: str,
        vision: str,
        custom_filters: Optional[List[Dict[str, Any]]] = None
    ) -> DataFrame:
        """
        Read a file group from bronze layer.
        
        IMPORTANT: All columns are automatically converted to lowercase.

        Args:
            file_group: File group key from reading_config.json (e.g., 'ipf_az')
            vision: Vision in YYYYMM format
            custom_filters: Optional additional filters to apply

        Returns:
            DataFrame with selected columns and filters applied (all columns lowercase)

        Raises:
            ValueError: If file_group not found in configuration
            FileNotFoundError: If no matching files found in bronze layer
        """
        # Get file group configuration
        if file_group not in self.reading_config['file_groups']:
            raise ValueError(
                f"File group '{file_group}' not found in reading_config.json"
            )

        group_config = self.reading_config['file_groups'][file_group]

        # Build bronze path based on location_type (monthly vs reference)
        base_path = self.config.get('datalake.base_path')
        location_type = group_config.get('location_type', 'monthly')

        if location_type == 'reference':
            # Reference data in bronze/ref/
            bronze_path = f"{base_path}/bronze/ref"
        else:
            # Monthly data in bronze/{YYYY}/{MM}/
            bronze_path = build_layer_path(base_path, 'bronze', vision)

        # Get configuration
        file_patterns = group_config['file_patterns']
        schema_name = group_config.get('schema')
        filters = group_config.get('filters', [])
        read_options = group_config.get('read_options', {})

        # Note: Encoding fallback logic (inspired by 02_TRANSCODIFICATION_ABS.sas)
        # PySpark CSV reader uses LATIN9 by default (read_options.encoding='LATIN9')
        # If encoding issues occur, Spark will handle them gracefully
        # For explicit Latin9 fallback, you would need to:
        # 1. Try LATIN9 first
        # 2. Catch encoding errors
        # 3. Retry with charset='ISO-8859-15' (Latin9)
        # This is handled at Spark DataFrame level and is already robust

        # Get schema if defined
        schema = self.get_schema(schema_name) if schema_name else None
        
        # Read files matching patterns
        dfs = []
        for pattern in file_patterns:
            full_pattern = f"{bronze_path}/{pattern}"
            try:
                # Determine file format from pattern
                file_format = self._detect_format(pattern, read_options)
                
                # Read based on format
                df_temp = self._read_by_format(
                    full_pattern,
                    file_format,
                    schema,
                    read_options,
                    schema_name=schema_name  # Pass schema name for verification
                )

                # Add source file name for tracking origin (e.g., PTF16 vs PTF36)
                df_temp = df_temp.withColumn("_source_file", input_file_name())

                # Add dynamic columns if there are some.
                dynamic_cols = group_config.get("dynamic_columns", [])

                # Extract source file name once
                source_file = df_temp.select("_source_file").first()[0].lower()

                for rule in dynamic_cols:
                    dyn_pattern = rule["pattern"].lower()
                    cols_to_add = rule["columns"]

                    if fnmatch.fnmatch(source_file, dyn_pattern):
                        for col_name, col_value in cols_to_add.items():
                            df_temp = df_temp.withColumn(col_name, lit(col_value))

                # Convert to lowercase BEFORE applying filters
                from utils.transformations import lowercase_all_columns
                df_temp = lowercase_all_columns(df_temp)

                df_temp = self._apply_read_filters(df_temp, filters)
                
                # Apply custom filters if provided (also before union)
                if custom_filters:
                    df_temp = self._apply_read_filters(df_temp, custom_filters)

                dfs.append(df_temp)
            except Exception:
                # File pattern not found - skip
                continue
        
        if not dfs:
            raise FileNotFoundError(
                f"No files found for pattern {file_patterns} in {bronze_path}"
            )
        
        # Union all matching files (already filtered)
        if len(dfs) == 1:
            df = dfs[0]
        else:
            df = dfs[0]
            for df_next in dfs[1:]:
                df = df.unionByName(df_next, allowMissingColumns=True)
        
        # Note: Filters already applied before union (see above)
        # Columns already lowercased before union (see above)

        return df

    def _detect_format(
        self,
        pattern: str,
        read_options: Dict[str, Any]
    ) -> str:
        """
        Detect file format from pattern or read_options.

        Args:
            pattern: File pattern (e.g., "*.csv.gz", "*.parquet")
            read_options: Read options dictionary

        Returns:
            Format string: 'csv', 'parquet', 'json', 'text'
        """
        # Check if format explicitly specified
        if 'format' in read_options:
            return read_options['format']
        
        # Detect from extension
        pattern_lower = pattern.lower()
        if pattern_lower.endswith('.parquet'):
            return 'parquet'
        elif pattern_lower.endswith('.json'):
            return 'json'
        elif pattern_lower.endswith('.txt'):
            return 'text'
        elif pattern_lower.endswith('.csv.gz') or pattern_lower.endswith('.csv'):
            return 'csv'
        else:
            # Default to CSV
            return 'csv'

    def _verify_schema(
        self,
        actual_columns: List[str],
        schema_columns: List[str],
        schema_name: str,
        file_path: str
    ) -> Dict[str, Any]:
        """
        Verify schema columns against actual CSV columns.
        Silently logs warnings for missing columns and info for case corrections.
        
        Args:
            actual_columns: Actual columns from CSV file
            schema_columns: Expected columns from schema
            schema_name: Name of schema for reporting
            file_path: Path to file for reporting
            
        Returns:
            Dictionary with verification results
        """
        # Build case-insensitive comparison maps
        actual_map = {c.lower(): c for c in actual_columns}
        schema_map = {c.lower(): c for c in schema_columns}
        
        # Find matches, mismatches, missing
        perfect_matches = []
        case_mismatches = []
        missing = []
        
        for schema_col in schema_columns:
            schema_lower = schema_col.lower()
            if schema_lower in actual_map:
                actual_col = actual_map[schema_lower]
                if actual_col == schema_col:
                    perfect_matches.append(schema_col)
                else:
                    case_mismatches.append((schema_col, actual_col))
            else:
                missing.append(schema_col)
        
        # Extra columns not in schema
        extra = [c for c in actual_columns if c.lower() not in schema_map]
        
        # Log issues (silent unless there are problems)
        if self.logger:
            if missing:
                # Warning for missing columns (columns in schema but not in CSV - should be removed from schema)
                self.logger.warning(
                    f"Schema '{schema_name}': {len(missing)} columns missing in CSV (remove from schema): "
                    f"{', '.join(missing)}"
                )
            
            if case_mismatches:
                # Info for case corrections (columns exist but wrong case - will be auto-corrected)
                for schema_col, actual_col in case_mismatches:
                    self.logger.info(
                        f"Schema '{schema_name}': Auto-correcting case '{schema_col}' → '{actual_col}'"
                    )
        
        return {
            'perfect': len(perfect_matches),
            'case_mismatch': len(case_mismatches),
            'missing': len(missing),
            'extra': len(extra),
            'mismatches': case_mismatches,
            'missing_list': missing
        }

    def _read_by_format(
        self,
        path: str,
        file_format: str,
        schema: Optional[Any],
        read_options: Dict[str, Any],
        schema_name: str = None
    ) -> DataFrame:
        """
        Read file based on format with schema enforcement.

        For CSV/JSON: schemas are applied BY NAME, not by position.
        This allows selecting a subset of columns (e.g., 120 from 800)
        without requiring strict column ordering in schema definitions.

        Args:
            path: File path or pattern
            file_format: Format string ('csv', 'parquet', 'json', 'text')
            schema: PySpark StructType schema (optional)
            read_options: Read options passed directly to Spark reader
            schema_name: Schema name for verification reporting (optional)

        Returns:
            DataFrame with schema columns selected and types enforced
        """
        # Remove 'format' from options (not a Spark read option)
        spark_options = {k: v for k, v in read_options.items() if k != 'format'}

        if file_format == 'csv':
            # Always read with header=True so Spark uses column names
            df_all = (
                self.spark.read
                    .options(**spark_options)
                    .csv(path, header=True, inferSchema=False)
            )
            
            # Verify schema if enabled
            if self.verify_schemas and schema and schema_name:
                schema_columns = [field.name for field in schema.fields]
                self._verify_schema(
                    df_all.columns,
                    schema_columns,
                    schema_name,
                    path
                )

            if schema:
                from pyspark.sql.functions import col
                select_exprs = [
                    col(field.name).cast(field.dataType).alias(field.name)
                    for field in schema.fields
                    if field.name in df_all.columns
                ]
                df = df_all.select(select_exprs) if select_exprs else self.spark.createDataFrame([], schema)
            else:
                df = df_all

        elif file_format == 'parquet':
            df = self.spark.read.options(**spark_options).parquet(path)

        elif file_format == 'json':
            df_all = self.spark.read.options(**spark_options).json(path)

            if schema:
                from pyspark.sql.functions import col
                select_exprs = [
                    col(field.name).cast(field.dataType).alias(field.name)
                    for field in schema.fields
                    if field.name in df_all.columns
                ]
                df = df_all.select(select_exprs) if select_exprs else self.spark.createDataFrame([], schema)
            else:
                df = df_all

        elif file_format == 'text':
            df = self.spark.read.options(**spark_options).text(path)

        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        return df


    def _apply_read_filters(
        self,
        df: DataFrame,
        filters: List[Dict[str, Any]]
    ) -> DataFrame:
        """
        Apply simple filters during data reading (bronze → silver).

        Used for basic data quality filters at ingestion time.
        For business logic filters, use apply_business_filters() from
        utils.transformations.generic_transforms instead.

        Args:
            df: Input DataFrame (columns already lowercase)
            filters: List of filter dictionaries with keys:
                - column: Column name
                - operator: Comparison operator (==, !=, in, not_in, >, <, >=, <=)
                - value: Value or list of values to filter

        Returns:
            Filtered DataFrame
        """
        for filter_spec in filters:
            column_name = filter_spec['column'].lower()  # Lowercase for consistency
            operator = filter_spec['operator']
            value = filter_spec['value']
            
            # Skip if column doesn't exist
            if column_name not in df.columns:
                continue
            
            # Apply filter based on operator
            if operator == "==":
                df = df.filter(col(column_name) == value)
            elif operator == "!=":
                df = df.filter(col(column_name) != value)
            elif operator == "in":
                df = df.filter(col(column_name).isin(value))
            elif operator == "not_in":
                df = df.filter(~col(column_name).isin(value))
            elif operator == ">":
                df = df.filter(col(column_name) > value)
            elif operator == "<":
                df = df.filter(col(column_name) < value)
            elif operator == ">=":
                df = df.filter(col(column_name) >= value)
            elif operator == "<=":
                df = df.filter(col(column_name) <= value)
        
        return df

    def list_available_file_groups(self) -> List[str]:
        """
        List all available file groups from configuration.

        Returns:
            List of file group keys
        """
        return list(self.reading_config['file_groups'].keys())


class SilverReader:
    """Read parquet files from silver layer."""

    def __init__(
        self,
        spark: SparkSession,
        config: ConfigLoader
    ):
        """
        Initialize silver reader.

        Args:
            spark: SparkSession instance
            config: ConfigLoader instance
        """
        self.spark = spark
        self.config = config

    def read_silver_file(
        self,
        filename: str,
        vision: str
    ) -> DataFrame:
        """
        Read a file from silver layer using the configured format (delta, parquet, csv).
        
        Automatically detects the format from config, allowing seamless migration
        from Parquet to Delta without modifying pipeline code.

        Args:
            filename: File name without extension (e.g., 'mvt_const_ptf')
            vision: Vision in YYYYMM format

        Returns:
            DataFrame from silver layer (all columns lowercase)

        Example:
            >>> reader = SilverReader(spark, config)
            >>> df = reader.read_silver_file('mvt_const_ptf', '202509')
            # Reads delta or parquet based on config.output.format
        """
        base_path = self.config.get('datalake.base_path')
        silver_path = build_layer_path(base_path, 'silver', vision)
        
        # Get format from config (default to parquet for backward compatibility)
        output_format = self.config.get('output.format', 'parquet').lower()
        
        # Build full file path (Delta = directory, others = file with extension)
        if output_format == "delta":
            full_path = f"{silver_path}/{filename}"
            df = self.spark.read.format("delta").load(full_path)
            
        elif output_format == "parquet":
            full_path = f"{silver_path}/{filename}.parquet"
            df = self.spark.read.parquet(full_path)
            
        elif output_format == "csv":
            full_path = f"{silver_path}/{filename}.csv"
            df = self.spark.read.option("header", True).option("delimiter", ";").csv(full_path)
            
        else:
            raise ValueError(f"Unsupported silver format: {output_format}")
        
        # Ensure columns are lowercase (they should already be from silver write)
        from utils.transformations import lowercase_all_columns
        df = lowercase_all_columns(df)
        
        return df

    def read_multiple_silver_files(
        self,
        filenames: List[str],
        vision: str
    ) -> Dict[str, DataFrame]:
        """
        Read multiple parquet files from silver layer.

        Args:
            filenames: List of file names without extensions
            vision: Vision in YYYYMM format

        Returns:
            Dictionary mapping filename → DataFrame

        Example:
            >>> reader = SilverReader(spark, config)
            >>> dfs = reader.read_multiple_silver_files(['mvt_const_ptf', 'azec_ptf'], '202509')
            >>> df_az = dfs['mvt_const_ptf']
            >>> df_azec = dfs['azec_ptf']
        """
        result = {}
        for filename in filenames:
            result[filename] = self.read_silver_file(filename, vision)
        
        return result

