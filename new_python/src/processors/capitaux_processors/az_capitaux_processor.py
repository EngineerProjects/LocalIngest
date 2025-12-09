"""
AZ Capitaux Processor.

Processes AZ channel capital data (Agent + Courtage):
- Extracts capitals WITH and WITHOUT indexation
- Normalizes to 100% basis
- Applies business rules
- Enriches with segmentation

Based on: CAPITAUX_AZ_MACRO.sas (313 lines)
"""

from pyspark.sql import DataFrame # type: ignore
from src.processors.base_processor import BaseProcessor
from utils.helpers import extract_year_month_int
from utils.loaders.config_loader import ConfigLoader
from utils.processor_helpers import enrich_segmentation


class AZCapitauxProcessor(BaseProcessor):
    """
    Process AZ capital data: IPFE16 + IPFE36 → Silver layer.
    
    Workflow:
    1. Read: IPF Agent + Courtage files
    2. Transform: 
       - Apply business filters
       - Extract capitals (indexed + non-indexed)
       - Normalize to 100%
       - Apply business rules
       - Enrich segmentation
    3. Write: az_capitaux_{vision}.parquet to silver
    """
    
    def __init__(self, spark, config: ConfigLoader, logger):
        """
        Initialize AZ Capitaux Processor.
        
        Args:
            spark: SparkSession
            config: ConfigLoader instance
            logger: Logger instance
        """
        super().__init__(spark, config, logger)
        self.logger.info("AZ Capitaux Processor initialized")
    
    def read(self, vision: str) -> DataFrame:
        """
        Read IPF files (Agent + Courtage) from bronze layer.
        
        Args:
            vision: Vision in YYYYMM format
        
        Returns:
            Combined DataFrame from IPFE16 + IPFE36
        """
        self.logger.info(f"Reading AZ capital data for vision {vision}")
        
        from src.reader import BronzeReader
        
        reading_config_path = self.config.get('config_files.reading_config', 'config/reading_config.json')
        reader = BronzeReader(self.spark, self.config, reading_config_path)
        
        # Read IPF files (combines Agent IPFE16 + Courtage IPFE36)
        df = reader.read_file_group('ipf_az', vision)
        
        self.logger.success(f"Read {df.count():,} records from bronze (AZ)")
        return df
    
    def transform(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Apply AZ capital transformations.
        
        Steps:
        1. Business filters
        2. Column configuration
        3. Extract capitals WITH indexation
        4. Extract capitals WITHOUT indexation
        5. Normalize to 100%
        6. Apply business rules
        7. Enrich segmentation
        
        Args:
            df: Input DataFrame from read()
            vision: Vision in YYYYMM format
        
        Returns:
            Transformed DataFrame ready for silver
        """
        self.logger.info("Starting AZ capital transformations")
        
        year_int, month_int = extract_year_month_int(vision)
        
        # Load configurations
        from utils.loaders.transformation_loader import get_default_loader
        loader = get_default_loader()
        
        # Use AZ config as base
        az_config = loader.get_az_config()
        
        # Load capital extraction config
        import json
        from pathlib import Path
        config_path = Path('config/transformations/capitaux_extraction_config.json')
        with open(config_path, 'r') as f:
            capital_config = json.load(f)
        
        # Step 1: Apply business filters
        self.logger.step(1, "Applying business filters")
        from utils.transformations import apply_business_filters
        filters = az_config.get('business_filters', {}).get('filters', [])
        df = apply_business_filters(df, {'filters': filters}, self.logger)
        self.logger.info(f"After filters: {df.count():,} records")
        
        # Step 2: Column configuration (rename csegt→cseg, etc.)
        self.logger.step(2, "Applying column configuration")
        from utils.transformations import lowercase_all_columns, apply_column_config
        df = lowercase_all_columns(df)
        column_config = az_config['column_selection']
        df = apply_column_config(df, column_config, vision, year_int, month_int)
        
        # Add DIRCOM and initialize capital columns
        from pyspark.sql.functions import lit
        df = df.withColumn('dircom', lit('az'))  # AZ direction commerciale
        
        # Step 3: Extract capitals WITH indexation
        self.logger.step(3, "Extracting capitals WITH indexation")
        
        # Apply indexation to mtcapi1-14
        from utils.transformations.operations.indexation import index_capitals
        df = index_capitals(
            df,
            num_capitals=14,
            date_col='dtechann',
            contract_start_col='dtefsitt',
            capital_prefix='mtcapi',
            nature_prefix='cdprvb',
            index_prefix='prprvc',
            reference_date=None,  # Use current date
            index_table_df=None,  # Will use prprvc coefficients
            logger=self.logger
        )
        
        # Extract indexed capitals (mtcapi1i-14i)
        from utils.transformations import extract_capitals_extended
        df = extract_capitals_extended(
            df,
            capital_config,
            num_capitals=14,
            indexed=True  # Use indexed columns
        )
        
        self.logger.info("Indexed capitals extracted: smp_100_ind, lci_100_ind, perte_exp_100_ind, etc.")
        
        # Step 4: Extract capitals WITHOUT indexation
        self.logger.step(4, "Extracting capitals WITHOUT indexation")
        
        # Extract non-indexed capitals (mtcapi1-14)
        df = extract_capitals_extended(
            df,
            capital_config,
            num_capitals=14,
            indexed=False  # Use non-indexed columns
        )
        
        self.logger.info("Non-indexed capitals extracted: smp_100, lci_100, perte_exp_100, etc.")
        
        # Step 5: Normalize to 100% basis
        self.logger.step(5, "Normalizing capitals to 100%")
        
        from utils.transformations import normalize_capitals_to_100
        
        # List of all capital columns to normalize
        indexed_cols = [
            'smp_100_ind', 'lci_100_ind', 'perte_exp_100_ind', 'risque_direct_100_ind',
            'limite_rc_100_par_sin_ind', 'limite_rc_100_par_sin_tous_dom_ind', 'limite_rc_100_par_an_ind',
            'smp_pe_100_ind', 'smp_rd_100_ind'
        ]

        non_indexed_cols = [
            'smp_100', 'lci_100', 'perte_exp_100', 'risque_direct_100',
            'limite_rc_100_par_sin', 'limite_rc_100_par_sin_tous_dom', 'limite_rc_100_par_an',
            'smp_pe_100', 'smp_rd_100'
        ]
        
        all_capital_cols = indexed_cols + non_indexed_cols
        
        df = normalize_capitals_to_100(df, all_capital_cols, 'prcdcie')
        
        self.logger.info("Capitals normalized to 100% technical basis")
        
        # Step 6: Apply business rules
        self.logger.step(6, "Applying business rules")
        
        from utils.transformations import apply_capitaux_business_rules
        
        # Apply for indexed and non-indexed
        df = apply_capitaux_business_rules(df, indexed=True)
        df = apply_capitaux_business_rules(df, indexed=False)
        
        self.logger.info("Business rules applied: SMP completion, RC limits")
        
        # Step 7: Enrich with segmentation (using helper)
        self.logger.step(7, "Enriching with segmentation")
        
        from src.reader import BronzeReader
        reading_config_path = self.config.get('config_files.reading_config', 'config/reading_config.json')
        reader = BronzeReader(self.spark, self.config, reading_config_path)
        
        # Use enrich_segmentation helper to replace 14 lines of duplicate code
        df = enrich_segmentation(df, reader, vision, logger=self.logger)
        
        self.logger.success("AZ capital transformations completed")
        return df
    
    def write(self, df: DataFrame, vision: str) -> None:
        """
        Write transformed data to silver layer.
        
        Args:
            df: Transformed DataFrame
            vision: Vision in YYYYMM format
        """
        self.logger.info(f"Writing AZ capital data to silver for vision {vision}")
        
        from utils.helpers import write_to_layer
        
        output_name = f"az_capitaux_{vision}"
        write_to_layer(df, self.config, 'silver', output_name, vision, self.logger)
        
        self.logger.success(f"Wrote {df.count():,} records to silver: {output_name}.parquet")
