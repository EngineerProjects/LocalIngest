"""
AZEC Capitaux Processor.

Processes AZEC channel capital data:
- Processes CAPITXCU (SMP/LCI by branch)
- Aggregates INCENDCU (PE/RD)
- Enriches with segmentation

Based on: CAPITAUX_AZEC_MACRO.sas (149 lines)
"""

from pyspark.sql import DataFrame # type: ignore
from pyspark.sql.functions import col, broadcast, lit, coalesce, when
from pyspark.sql.types import StringType, DoubleType
from config.constants import MARKET
from src.processors.base_processor import BaseProcessor
from utils.helpers import extract_year_month_int
from utils.loaders.config_loader import ConfigLoader
from utils.processor_helpers import enrich_segmentation
from pathlib import Path



class AZECCapitauxProcessor(BaseProcessor):
    """
    Process AZEC capital data: CAPITXCU + INCENDCU → Silver layer.
    
    Workflow:
    1. Read: CAPITXCU + INCENDCU files
    2. Transform:
       - Process CAPITXCU (SMP/LCI by branch)
       - Aggregate INCENDCU (PE/RD)
       - Join PE/RD data
       - Enrich segmentation
       - Filter construction market
    3. Write: azec_capitaux_{vision}.parquet to silver
    """
    
    def __init__(self, spark, config: ConfigLoader, logger):
        """
        Initialize AZEC Capitaux Processor.
        
        Args:
            spark: SparkSession
            config: ConfigLoader instance
            logger: Logger instance
        """
        super().__init__(spark, config, logger)
        self.logger.info("AZEC Capitaux Processor initialized")
    
    def read(self, vision: str) -> DataFrame:
        """
        Read CAPITXCU from bronze layer.
        
        Args:
            vision: Vision in YYYYMM format
        
        Returns:
            CAPITXCU DataFrame
        """
        self.logger.info(f"Reading AZEC capital data for vision {vision}")
        
        from src.reader import BronzeReader
        
        reading_config_path = self.config.get('config_files.reading_config', 'config/reading_config.json')
        
        # Convert to absolute path if relative
        if not Path(reading_config_path).is_absolute():
            reading_config_path = str(self.get_project_root() / reading_config_path)
        
        reader = BronzeReader(self.spark, self.config, reading_config_path)

        
        # Read CAPITXCU
        df_capitxcu = reader.read_file_group('capitxcu_azec', vision)
        
        self.logger.success(f"Read {df_capitxcu.count():,} records from CAPITXCU")
        return df_capitxcu
    
    def transform(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Apply AZEC capital transformations.
        
        Steps:
        1. Process CAPITXCU (SMP/LCI by branch)
        2. Aggregate INCENDCU (PE/RD)
        3. Join PE/RD data
        4. Enrich segmentation
        5. Filter construction market
        
        Args:
            df: CAPITXCU DataFrame from read()
            vision: Vision in YYYYMM format
        
        Returns:
            Transformed DataFrame ready for silver
        """
        self.logger.info("Starting AZEC capital transformations")
        
        year_int, month_int = extract_year_month_int(vision)
        
        # Step 1: Process CAPITXCU
        self.logger.step(1, "Processing CAPITXCU (SMP/LCI by branch)")
        from utils.transformations import process_azec_capitals
        df = process_azec_capitals(df)
        
        # Step 2: Read and aggregate INCENDCU
        self.logger.step(2, "Reading and aggregating INCENDCU (PE/RD)")
        from src.reader import BronzeReader
        
        reading_config_path = self.config.get('config_files.reading_config', 'config/reading_config.json')
        
        # Convert to absolute path if relative
        if not Path(reading_config_path).is_absolute():
            reading_config_path = str(self.get_project_root() / reading_config_path)
        
        reader = BronzeReader(self.spark, self.config, reading_config_path)

        
        try:
            df_incendcu = reader.read_file_group('incendcu_azec', vision)
            
            from utils.transformations import aggregate_azec_pe_rd
            df_pe_rd = aggregate_azec_pe_rd(df_incendcu)
            
            # Step 3: Join PE/RD data
            self.logger.step(3, "Joining PE/RD data")
            df = df.join(df_pe_rd, on=['nopol', 'cdprod'], how='left')
        except Exception as e:
            self.logger.warning(f"INCENDCU not available: {e}")
            self.logger.info("Initializing PE/RD columns to 0")
            from pyspark.sql.functions import lit
            df = df.withColumn('perte_exp_100_ind', lit(0.0))
            df = df.withColumn('risque_direct_100_ind', lit(0.0))
            df = df.withColumn('value_insured_100_ind', lit(0.0))
        
        # Step 4: Enrich with segmentation (using helper)
        self.logger.step(4, "Enriching with segmentation")
        
        # Use enrich_segmentation helper to replace 21 lines of duplicate code
        df = enrich_segmentation(df, reader, vision, logger=self.logger)

        # Step 5: Filter construction market
        self.logger.step(5, "Filtering construction market (CMARCH=6)")
        from pyspark.sql.functions import col

        # SAS L141-145: WHERE CMARCH = "6"
        if 'cmarch' in df.columns:
            df = df.filter(col('cmarch') == MARKET.CONSTRUCTION)
            self.logger.info(f"After CMARCH=6 filter: {df.count():,} rows")
        else:
            self.logger.warning("CMARCH column not found - skipping construction market filter")
        
        self.logger.success("AZEC capital transformations completed")
        return df
    
    def write(self, df: DataFrame, vision: str) -> None:
        """
        Write transformed data to silver layer.
        
        Args:
            df: Transformed DataFrame
            vision: Vision in YYYYMM format
        """
        self.logger.info(f"Writing AZEC capital data to silver for vision {vision}")
        
        from utils.helpers import write_to_layer
        
        output_name = f"azec_capitaux_{vision}"
        write_to_layer(df, self.config, 'silver', output_name, vision, self.logger)
        
        self.logger.success(f"Wrote {df.count():,} records to silver: {output_name}.parquet")
