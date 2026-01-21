# -*- coding: utf-8 -*-
"""
Generic bronze layer reader for Construction Data Pipeline.
Reads files from bronze with column selection and filtering.

Design principles
-----------------
- Read RAW files (CSV with inferSchema=False => strings), Parquet/JSON keep native types.
- Lowercase column names early for consistency.
- Apply *projection by name* (optional) to keep only schema-declared columns (no cast at read time).
- Union all fragments FIRST.
- Clean SAS NULL markers (".", "", whitespace) → real Spark NULLs (on string columns only) ONCE.
- Cast (if a schema is provided) in a single pass, after cleaning.
- Apply filters ONCE, after clean/cast (NULL-safe).
- Keep the implementation simple and predictable (baseline).

Why this order?
---------------
Cleaning before casting ensures values like "." and space-only strings become true NULLs
that Spark recognizes (isNull/isNotNull), and comparisons/casts behave correctly.
Applying filters after clean avoids dropping rows prematurely due to NULL semantics.
"""

import json
import fnmatch
from pathlib import Path
from typing import List, Dict, Optional, Any

from pyspark.sql import SparkSession, DataFrame  # type: ignore
from pyspark.sql.functions import (
    col, input_file_name, lit, trim, regexp_replace, when, to_date  # type: ignore
)
from pyspark.sql.types import StringType, DateType  # type: ignore

from utils.loaders.config_loader import ConfigLoader
from utils.helpers import build_layer_path
from utils.transformations import lowercase_all_columns


class BronzeReader:
    """
    Read files from the bronze layer with projection, SAS-null cleanup, casting and filtering.

    This class is intentionally kept simple:
    - It reads all matching files per group (e.g., IPF16 + IPF36),
    - Lowercases columns and optionally keeps only columns declared in a schema,
    - Unions all fragments once,
    - Cleans SAS-style NULLs once (string-only),
    - Casts to target types once (if a schema is provided),
    - Applies filters once (NULL-aware for conditions that compare to ".").

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    config : ConfigLoader
        Pipeline configuration accessor.
    reading_config_path : str, optional
        Path to reading_config.json. If None, loads from project config directory.
    logger : logging.Logger, optional
        Optional logger; if provided, the reader will log info/warnings.

    Notes
    -----
    * CSV files are always read with inferSchema=False, so all columns start as strings.
      Parquet/JSON keep their native types; cleanup runs only on string columns safely.
    * Projection-by-name at read time (no cast) keeps this reader robust to column reordering.
    * Dynamic columns can be injected based on the filename pattern (vectorized by _source_file).
    """


    def __init__(
        self,
        spark: SparkSession,
        config: ConfigLoader,
        reading_config_path: Optional[str] = None,
        logger=None
    ):
        self.spark = spark
        self.config = config
        self.logger = logger

        # Load reading configuration
        if reading_config_path is None:
            project_root = Path(__file__).parent.parent
            reading_config_path = project_root / "config" / "reading_config.json"

        # ✅ Use UTF-8 (safer for JSON)
        with open(reading_config_path, 'r', encoding='utf-8') as f:
            self.reading_config = json.load(f)

        # Schema registry
        from config.schemas import get_schema
        self.get_schema = get_schema

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------
    def read_file_group(
        self,
        file_group: str,
        vision: str,
        custom_filters: Optional[List[Dict[str, Any]]] = None
    ) -> DataFrame:
        """
        Read a file group from the bronze layer with:
        lowercasing → per-fragment → SAS cleanup → cast (optional) → filters (optional) → union.

        Parameters
        ----------
        file_group : str
            File group key from reading_config.json (e.g., 'ipf_az').
        vision : str
            Period in YYYYMM format.
        custom_filters : list[dict], optional
            Additional filters to apply after cleanup/cast.

        Returns
        -------
        DataFrame
            Cleaned and optionally casted DataFrame with filters applied (columns all lowercase).

        Raises
        ------
        ValueError
            If file_group is not found in the reading configuration.
        FileNotFoundError
            If no files matching the patterns are found in the bronze layer.
        """

        if file_group not in self.reading_config['file_groups']:
            raise ValueError(f"File group '{file_group}' not found in reading_config.json")

        group_cfg = self.reading_config['file_groups'][file_group]

        # Resolve bronze path based on location type
        base_path = self.config.get('datalake.base_path')
        location_type = group_cfg.get('location_type', 'monthly')
        if location_type == 'reference':
            bronze_path = f"{base_path}/bronze/ref"
        else:
            bronze_path = build_layer_path(base_path, 'bronze', vision)

        file_patterns: List[str] = group_cfg['file_patterns']
        schema_name: Optional[str] = group_cfg.get('schema')
        filters: List[Dict[str, Any]] = group_cfg.get('filters', [])
        read_options: Dict[str, Any] = group_cfg.get('read_options', {})
        dynamic_columns: List[Dict[str, Any]] = group_cfg.get('dynamic_columns', [])

        schema = self.get_schema(schema_name) if schema_name else None

        # -------------------------------------------
        # 1) Read fragments RAW (no cast), add _source_file, dynamic columns
        # -------------------------------------------
        fragments: List[DataFrame] = []

        for pattern in file_patterns:
            full_pattern = f"{bronze_path}/{pattern}"
            try:
                fmt = self._detect_format(pattern, read_options)
                df_part = self._read_by_format(
                    full_pattern,
                    fmt,
                    schema,
                    read_options,
                    schema_name=schema_name
                )

                # Track origin file
                df_part = df_part.withColumn("_source_file", input_file_name())

                # Apply dynamic columns (vectorized by filename pattern)
                if dynamic_columns:
                    df_part = self._apply_dynamic_columns(df_part, dynamic_columns)

                # Ensure lowercase (already done in _read_by_format; re-assert here if needed)
                df_part = lowercase_all_columns(df_part)

                # -------------------------------------------
                # 2) CLEAN + CAST + FILTERS **PER FRAGMENT** (SAS WHERE per SELECT)
                # -------------------------------------------
                df_part = self._clean_sas_nulls(df_part)

                if schema:
                    df_part = self._safe_cast(df_part, schema)

                if filters:
                    df_part = self._apply_read_filters(df_part, filters)

                if custom_filters:
                    df_part = self._apply_read_filters(df_part, custom_filters)

                fragments.append(df_part)

            except Exception as e:
                if self.logger:
                    self.logger.debug(f"[BronzeReader] Pattern {pattern} skipped: {e}")
                continue

        if not fragments:
            raise FileNotFoundError(
                f"No files found for pattern {file_patterns} in {bronze_path}"
            )

        # -------------------------------------------
        # 3) UNION all **already filtered** fragments
        # -------------------------------------------
        df = fragments[0]
        for nxt in fragments[1:]:
            df = df.unionByName(nxt, allowMissingColumns=True)

        # Nothing else to do (clean/cast/filters done per fragment)
        return df


    def list_available_file_groups(self) -> List[str]:
        """
        List all available file groups from configuration.

        Returns
        -------
        list[str]
            File group keys available in reading_config.json.
        """
        return list(self.reading_config['file_groups'].keys())

    # -------------------------------------------------------------------------
    # Internals
    # -------------------------------------------------------------------------
    def _detect_format(self, pattern: str, read_options: Dict[str, Any]) -> str:
        """
        Detect file format from pattern or read options.

        Returns
        -------
        str
            One of: 'csv', 'parquet', 'json', 'text'
        """
        if 'format' in read_options:
            return str(read_options['format']).lower()

        p = pattern.lower()
        if p.endswith('.parquet'):
            return 'parquet'
        if p.endswith('.json'):
            return 'json'
        if p.endswith('.txt'):
            return 'text'
        return 'csv'  # default

    def _read_by_format(
        self,
        path: str,
        file_format: str,
        schema: Optional[Any],
        read_options: Dict[str, Any],
        schema_name: str = None
    ) -> DataFrame:
        """
        Read a fragment based on format and project columns by name (no cast).

        For CSV/JSON: projection-by-name allows selecting only needed columns (e.g., 120 from 800)
        without relying on order. Casting is explicitly deferred until after UNION + CLEAN.

        Parameters
        ----------
        path : str
            Source path or pattern.
        file_format : str
            'csv' | 'parquet' | 'json' | 'text'
        schema : StructType, optional
            Optional target schema to pick the column names (no cast here).
        read_options : dict
            Options passed directly to Spark reader (e.g., sep, encoding).
        schema_name : str, optional
            Unused here (kept for compatibility).

        Returns
        -------
        DataFrame
            Lowercased column names, projected by name if a schema is provided.
        """
        spark_options = {k: v for k, v in read_options.items() if k != 'format'}

        if file_format == 'csv':
            df_all = (
                self.spark.read
                    .options(**spark_options)
                    .csv(path, header=True, inferSchema=False)
            )
        elif file_format == 'parquet':
            df_all = self.spark.read.options(**spark_options).parquet(path)
        elif file_format == 'json':
            df_all = self.spark.read.options(**spark_options).json(path)
        elif file_format == 'text':
            df_all = self.spark.read.options(**spark_options).text(path)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        # Normalize lowercase once here
        df_all = lowercase_all_columns(df_all)

        # Projection by name (no cast)
        if schema:
            wanted = [f.name.lower() for f in schema.fields]
            available = [c for c in df_all.columns if c in wanted]
            return df_all.select(*available) if available else df_all.limit(0)

        return df_all

    def _apply_dynamic_columns(self, df: DataFrame, rules: List[Dict[str, Any]]) -> DataFrame:
        """
        Add dynamic columns based on source filename pattern (vectorized per row).

        Config example (reading_config.json):
        -------------------------------------
        "dynamic_columns": [
          {"pattern": "ipf16*.csv*", "columns": {"cdpole": "3"}},
          {"pattern": "ipf36*.csv*", "columns": {"cdpole": "1"}}
        ]

        Parameters
        ----------
        df : DataFrame
            Input with '_source_file' column available.
        rules : list[dict]
            Each rule has 'pattern' (glob) and 'columns' (dict of name->value).

        Returns
        -------
        DataFrame
            DataFrame with added/updated columns according to matching rules.
        """
        if "_source_file" not in df.columns or not rules:
            return df

        # Convert fnmatch glob to regex and apply vectorized conditions per rule
        for rule in rules:
            pattern = rule.get("pattern", "")
            cols_to_add: Dict[str, Any] = rule.get("columns", {})
            regex = fnmatch.translate(pattern)
            # remove trailing \Z if present to allow substring matches on full paths
            if regex.endswith(r"\Z"):
                regex = regex[:-2]

            cond = col("_source_file").rlike(regex)
            for k, v in cols_to_add.items():
                k_low = k.lower()
                if k_low in df.columns:
                    df = df.withColumn(k_low, when(cond, lit(v)).otherwise(col(k_low)))
                else:
                    df = df.withColumn(k_low, when(cond, lit(v)))

        # Ensure column names are lowercase after additions
        df = lowercase_all_columns(df)
        return df

    def _clean_sas_nulls(self, df: DataFrame) -> DataFrame:
        """
        Convert SAS-style NULL markers to real Spark NULLs (string columns only):

        Rules
        -----
        - Normalize NBSP (\\u00A0) to normal space (defensive for odd exports),
        - TRIM leading/trailing whitespace,
        - "."      -> NULL
        - ""       -> NULL

        Parameters
        ----------
        df : DataFrame
            Input DataFrame (mixed types allowed).

        Returns
        -------
        DataFrame
            Same schema, with string columns cleaned and true NULLs written.
        """
        string_cols = [c for c, t in df.dtypes if t == "string"]
        if not string_cols:
            return df

        # Build a single select projection to apply all operations efficiently
        select_exprs = []
        for c in df.columns:
            if c in string_cols:
                tmp = trim(regexp_replace(col(c), u"\u00A0", " "))
                expr = when((tmp == ".") | (tmp == ""), lit(None).cast(StringType())) \
                       .otherwise(tmp) \
                       .alias(c)
                select_exprs.append(expr)
            else:
                select_exprs.append(col(c))

        return df.select(*select_exprs)


    def _safe_cast(self, df: DataFrame, schema) -> DataFrame:
        """
        Cast columns to target types defined in 'schema' in a single pass.

        - DateType: attempts a single default format ('yyyy-MM-dd').
          (If your sources have multiple date formats, extend this method
           to coalesce several to_date() calls.)
        - Other types: cast() as given by the schema.

        Parameters
        ----------
        df : DataFrame
            Cleaned DataFrame.
        schema : StructType
            Target schema (names & types).

        Returns
        -------
        DataFrame
            DataFrame with columns cast to target types where applicable.
        """
        # Build a cast map {col_name_lower: dataType}
        cast_map = {f.name.lower(): f.dataType for f in schema.fields}

        select_exprs = []
        for c in df.columns:
            dtype = cast_map.get(c)
            if dtype is None:
                select_exprs.append(col(c))
            elif isinstance(dtype, DateType):
                select_exprs.append(to_date(col(c), "yyyy-MM-dd").alias(c))
            else:
                select_exprs.append(col(c).cast(dtype).alias(c))

        return df.select(*select_exprs)

    def _apply_read_filters(
        self,
        df: DataFrame,
        filters: List[Dict[str, Any]]
    ) -> DataFrame:
        """
        Apply filters AFTER union and AFTER cleaning/casting.
        
        CRITICAL: Uses SAS NULL semantics (not PySpark defaults):
        - equals (==):     col == value           → NULL excluded (same as PySpark)
        - not_equals (!=): col IS NULL OR col != value  → NULL INCLUDED (SAS behavior!)
        - in:              col IS NOT NULL AND col IN (...)  → NULL excluded
        - not_in:          col IS NULL OR col NOT IN (...)   → NULL INCLUDED (SAS behavior!)
        - comparisons:     standard PySpark (NULL excluded)
        
        This matches SAS WHERE clause behavior exactly.

        Supported operators:
        - '==', '!='
        - 'in', 'not_in'
        - '>', '<', '>=', '<='
        """
        for f in filters:
            colname = f.get('column', '').lower()
            operator = f.get('operator')
            value = f.get('value')

            # Skip if column doesn't exist or operator missing
            if not colname or colname not in df.columns or not operator:
                continue

            # SAS semantics: equals excludes NULL (same as PySpark default)
            if operator == "==":
                df = df.filter(col(colname) == value)
            
            # SAS semantics: not_equals INCLUDES NULL (different from PySpark!)
            elif operator == "!=":
                df = df.filter(col(colname).isNull() | (col(colname) != value))
            
            # SAS semantics: in excludes NULL (need explicit isNotNull)
            elif operator == "in":
                vals = value if isinstance(value, list) else [value]
                df = df.filter(col(colname).isNotNull() & col(colname).isin(vals))
            
            # SAS semantics: not_in INCLUDES NULL (different from PySpark!)
            elif operator == "not_in":
                vals = value if isinstance(value, list) else [value]
                df = df.filter(col(colname).isNull() | (~col(colname).isin(vals)))
            
            # Comparisons: standard PySpark behavior (NULL excluded)
            elif operator == ">":
                df = df.filter(col(colname) > value)
            elif operator == "<":
                df = df.filter(col(colname) < value)
            elif operator == ">=":
                df = df.filter(col(colname) >= value)
            elif operator == "<=":
                df = df.filter(col(colname) <= value)

        return df


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