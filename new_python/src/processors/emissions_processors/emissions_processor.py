"""
Emissions Processor.

Processes One BI premium emissions data (rf_fr1_prm_dtl_midcorp_m):
- Applies business filters (intermediaries, products, guarantees, categories)
- Assigns distribution channel (CDPOLE) from CD_NIV_2_STC
- Calculates current/prior year split (EXERCICE)
- Extracts guarantee code (CGARP)
- Enriches with segmentation
- Creates two outputs:
  1. PRIMES_EMISES_{vision}_POL_GARP (by guarantee)
  2. PRIMES_EMISES_{vision}_POL (aggregated by policy)

Based on: EMISSIONS_RUN.sas (308 lines)
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, sum as _sum, coalesce, broadcast, when
from pyspark.sql.types import StringType
from config.constants import MARKET_CODE
from src.processors.base_processor import BaseProcessor
from utils.helpers import extract_year_month_int
from utils.loaders.config_loader import ConfigLoader
from utils.processor_helpers import enrich_segmentation


class EmissionsProcessor(BaseProcessor):
    """
    Process One BI emissions data: Bronze → Silver → Gold.
    
    Workflow:
    1. Read: rf_fr1_prm_dtl_midcorp_m from bronze
    2. Transform:
       - Apply business filters
       - Assign CDPOLE, calculate EXERCICE, extract CGARP
       - Calculate premiums (primes_x, primes_n)
       - Enrich segmentation
    3. Write: 
       - primes_emises_{vision}_pol_garp.parquet to gold (by guarantee)
       - primes_emises_{vision}_pol.parquet to gold (aggregated)
    """
    
    def __init__(self, spark, config: ConfigLoader, logger):
        """
        Initialize Emissions Processor.
        
        Args:
            spark: SparkSession
            config: ConfigLoader instance
            logger: Logger instance
        """
        super().__init__(spark, config, logger)
        self.logger.info("Emissions Processor initialized")
    
    def read(self, vision: str) -> DataFrame:
        """
        Read One BI premium data from bronze layer.
        
        Args:
            vision: Vision in YYYYMM format
        
        Returns:
            DataFrame from rf_fr1_prm_dtl_midcorp_m
        """
        self.logger.info(f"Reading One BI emissions data for vision {vision}")
        
        from src.reader import BronzeReader
        from pathlib import Path
        
        reading_config_path = self.config.get('config_files.reading_config', 'config/reading_config.json')
        
        # Convert to absolute path if relative
        if not Path(reading_config_path).is_absolute():
            reading_config_path = str(self.get_project_root() / reading_config_path)
        
        reader = BronzeReader(self.spark, self.config, reading_config_path)
        
        # Read One BI premium data
        df = reader.read_file_group('rf_fr1_prm_dtl_midcorp_m', vision)
        
        self.logger.success(f"Read {df.count():,} records from One BI (bronze)")
        return df
    
    def transform(self, df: DataFrame, vision: str) -> tuple:
        """
        Apply Emissions transformations.
        
        Steps:
        1. Lowercase columns
        2. Apply emissions business filters
        3. Assign CDPOLE from CD_NIV_2_STC
        4. Calculate EXERCICE (current/prior year split)
        5. Extract guarantee code (CGARP)
        6. Calculate premiums (primes_x for all, primes_n for current year)
        7. Enrich with segmentation
        8. Create two aggregated outputs
        
        Args:
            df: Input DataFrame from read()
            vision: Vision in YYYYMM format
        
        Returns:
            Tuple of (df_pol_garp, df_pol) - two aggregated DataFrames
        """
        self.logger.info("Starting Emissions transformations")
        
        year_int, month_int = extract_year_month_int(vision)
        
        # Step 1: Lowercase columns
        self.logger.step(1, "Lowercasing all columns")
        from utils.transformations import lowercase_all_columns
        df = lowercase_all_columns(df)
        
        # Step 2: Apply emissions business filters
        self.logger.step(2, "Applying business filters")
        import json
        from pathlib import Path
        
        emissions_config_path = Path('config/transformations/emissions_config.json')
        
        # Convert to absolute path if relative
        if not emissions_config_path.is_absolute():
            emissions_config_path = self.get_project_root() / emissions_config_path
        
        with open(emissions_config_path, 'r') as f:
            emissions_config = json.load(f)
        
        from utils.transformations import apply_emissions_filters
        df = apply_emissions_filters(df, emissions_config, vision, self.logger)
        
        # Step 3: Assign CDPOLE from CD_NIV_2_STC
        self.logger.step(3, "Assigning distribution channel (CDPOLE)")
        from utils.transformations import assign_distribution_channel
        df = assign_distribution_channel(df)
        self.logger.info("CDPOLE assigned: '1' (Agent) for DCAG/DCPS/DIGITAL, '3' (Courtage) for BROKDIV")
        
        # Step 4: Calculate EXERCICE (current/prior year split)
        self.logger.step(4, "Calculating EXERCICE (current/prior year split)")
        from utils.transformations import calculate_exercice_split
        df = calculate_exercice_split(df, vision)
        self.logger.info("EXERCICE calculated: 'cou' (current) if nu_ex_ratt_cts >= year, else 'ant' (prior)")
        
        # Step 5: Extract guarantee code (CGARP)
        self.logger.step(5, "Extracting guarantee code (CGARP)")
        from utils.transformations import extract_guarantee_code
        df = extract_guarantee_code(df)
        self.logger.info("CGARP extracted from cd_gar_prospctiv (chars 3-5)")
        
        # Step 6: Transform columns and calculate premiums
        self.logger.step(6, "Transforming columns and calculating premiums")
        
        # Rename columns to match output schema
        df = df.withColumnRenamed('nu_cnt_prm', 'nopol')
        df = df.withColumnRenamed('cd_prd_prm', 'cdprod')
        df = df.withColumnRenamed('cd_int_stc', 'noint')
        df = df.withColumnRenamed('mt_cms_cts', 'mtcom')
        
        # Add DIRCOM
        df = df.withColumn('dircom', lit('AZ '))
        
        # Add VISION
        df = df.withColumn('vision', lit(vision))
        
        # Calculate primes_n (current year premiums) using intermediate column
        # SAS L214-221: Inner query filters WHERE EXERCICE='cou'
        df_current = df.filter(col('exercice') == 'cou')
        
        df_current_agg = df_current.groupBy(
            'cdpole', 'cdprod', 'nopol', 'noint', 'cd_gar_princ', 'cd_gar_prospctiv', 'cd_cat_min', 'dircom'
        ).agg(
            _sum('mt_ht_cts').alias('primes_n_temp')
        )
        
        # Join primes_n back to main df
        df = df.join(
            df_current_agg,
            on=['cdpole', 'cdprod', 'nopol', 'noint', 'cd_gar_princ', 'cd_gar_prospctiv', 'cd_cat_min', 'dircom'],
            how='left'
        )
        
        # Rename and handle nulls
        df = df.withColumn('primes_n', coalesce(col('primes_n_temp'), lit(0.0)))
        df = df.drop('primes_n_temp')
        
        self.logger.info("Premiums calculated: primes_x (all years), primes_n (current year only)")
        
        # Step 7: Enrich with segmentation (using helper)
        self.logger.step(7, "Enriching with segmentation")
        from src.reader import BronzeReader
        from pathlib import Path
        
        reading_config_path = self.config.get('config_files.reading_config', 'config/reading_config.json')
        
        # Convert to absolute path if relative
        if not Path(reading_config_path).is_absolute():
            reading_config_path = str(self.get_project_root() / reading_config_path)
        
        reader = BronzeReader(self.spark, self.config, reading_config_path)
        
        # Use enrich_segmentation helper to replace 21 lines of duplicate code
        df = enrich_segmentation(df, reader, vision, logger=self.logger)
        
        # Filter for construction market
        if 'cmarch' in df.columns:
            df = df.filter(col('cmarch') == MARKET_CODE.MARKET)
            self.logger.info(f"After cmarch='6' filter: {df.count():,} records")
        
        # Step 8: Create aggregated outputs
        self.logger.step(8, "Creating aggregated outputs")
        
        # Keep necessary columns
        df = df.select(
            'nopol', 'cdprod', 'noint', 'cgarp', 'cmarch', 'cseg', 'cssseg',
            'cdpole', 'vision', 'dircom', 'cd_cat_min', 'mt_ht_cts', 'primes_n', 'mtcom'
        )
        
        # Output 1: Aggregate by policy + guarantee (POL_GARP)
        # Using helper function to avoid code duplication
        from utils.transformations import aggregate_by_policy_guarantee
        
        group_cols_garp = [
            'vision', 'dircom', 'cdpole', 'nopol', 'cdprod', 'noint', 'cgarp',
            'cmarch', 'cseg', 'cssseg', 'cd_cat_min'
        ]
        
        df_pol_garp = aggregate_by_policy_guarantee(df, group_cols_garp)
        
        self.logger.info(f"POL_GARP aggregation: {df_pol_garp.count():,} records")
        
        # Output 2: Aggregate by policy only (POL) - from POL_GARP
        group_cols_pol = [
            'vision', 'dircom', 'nopol', 'noint', 'cdpole', 'cdprod',
            'cmarch', 'cseg', 'cssseg'
        ]
        
        df_pol = df_pol_garp.groupBy(*group_cols_pol).agg(
            _sum('primes_x').alias('primes_x'),
            _sum('primes_n').alias('primes_n'),
            _sum('mtcom_x').alias('mtcom_x')
        )
        
        self.logger.info(f"POL aggregation: {df_pol.count():,} records")
        
        self.logger.success("Emissions transformations completed")
        return df_pol_garp, df_pol
    
    def write(self, dfs: tuple, vision: str) -> None:
        """
        Write transformed data to gold layer.
        
        Args:
            dfs: Tuple of (df_pol_garp, df_pol)
            vision: Vision in YYYYMM format
        """
        df_pol_garp, df_pol = dfs
        
        from utils.helpers import write_to_layer
        
        # Write POL_GARP (by guarantee)
        self.logger.info(f"Writing POL_GARP to gold for vision {vision}")
        output_name_garp = f"primes_emises_{vision}_pol_garp"
        write_to_layer(df_pol_garp, self.config, 'gold', output_name_garp, vision, self.logger)
        self.logger.success(f"Wrote {df_pol_garp.count():,} records to gold: {output_name_garp}.parquet")
        
        # Write POL (aggregated)
        self.logger.info(f"Writing POL to gold for vision {vision}")
        output_name_pol = f"primes_emises_{vision}_pol"
        write_to_layer(df_pol, self.config, 'gold', output_name_pol, vision, self.logger)
        self.logger.success(f"Wrote {df_pol.count():,} records to gold: {output_name_pol}.parquet")
