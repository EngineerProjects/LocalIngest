"""
Capitaux Consolidation Processor.

Consolidates AZ and AZEC capital data into unified gold layer output.

Based on: CAPITAUX_RUN.sas consolidation_az_azec_capitaux macro (L14-71)
"""

from pyspark.sql import DataFrame # type: ignore
from src.processors.base_processor import BaseProcessor
from utils.loaders.config_loader import ConfigLoader


class CapitauxConsolidationProcessor(BaseProcessor):
    """
    Consolidate AZ + AZEC capital data: Silver → Gold layer.
    
    Workflow:
    1. Read: AZ and AZEC silver outputs
    2. Transform:
       - Harmonize AZ schema
       - Harmonize AZEC schema
       - Union AZ + AZEC
    3. Write: az_azec_capitaux_{vision}.parquet to gold
    """
    
    def __init__(self, spark, config: ConfigLoader, logger):
        """
        Initialize Capitaux Consolidation Processor.
        
        Args:
            spark: SparkSession
            config: ConfigLoader instance
            logger: Logger instance
        """
        super().__init__(spark, config, logger)
        self.logger.info("Capitaux Consolidation Processor initialized")
    
    def read(self, vision: str) -> DataFrame:
        """
        Read AZ silver data (AZEC read in transform).

        Args:
            vision: Vision in YYYYMM format

        Returns:
            AZ silver DataFrame
        """
        self.logger.info(f"Reading AZ capitaux from silver for vision {vision}")
        
        from src.reader import SilverReader
        
        reader = SilverReader(self.spark, self.config)
        df_az = reader.read_silver_file(f"az_capitaux_{vision}", vision)
        
        self.logger.success(f"Read {df_az.count():,} AZ capital records from silver")
        return df_az
    
    def transform(self, df_az: DataFrame, vision: str) -> DataFrame:
        """
        Consolidate AZ + AZEC capital data.
        
        Steps:
        1. Read AZEC silver
        2. Harmonize AZ schema (add DIRCOM, select columns)
        3. Harmonize AZEC schema (add DIRCOM, CDPOLE, select columns)
        4. Union AZ + AZEC
        
        Args:
            df_az: AZ DataFrame from read()
            vision: Vision in YYYYMM format
        
        Returns:
            Consolidated DataFrame ready for gold
        """
        self.logger.info("Starting consolidation of AZ + AZEC capital data")
        
        # Step 1: Read AZEC silver
        self.logger.step(1, "Reading AZEC capitaux from silver")
        from src.reader import SilverReader
        reader = SilverReader(self.spark, self.config)
        
        df_azec = None # Initialize df_azec
        try:
            df_azec = reader.read_silver_file(f"azec_capitaux_{vision}", vision)
            self.logger.success(f"Read {df_azec.count():,} AZEC capital records from silver")
        except Exception as e:
            self.logger.warning(f"AZEC data not available: {e}")
            self.logger.info("Consolidation will use AZ data only")
            df_azec = None
        
        # Step 2: Harmonize AZ schema
        self.logger.step(2, "Harmonizing AZ schema")
        from pyspark.sql.functions import lit
        df_az = df_az.withColumn('dircom', lit('AZ'))
        
        # Calculate VALUE_INSURED columns (PE + RD)
        from pyspark.sql.functions import col, coalesce
        if all(c in df_az.columns for c in ['perte_exp_100_ind', 'risque_direct_100_ind']):
            df_az = df_az.withColumn(
                'value_insured_100_ind',
                coalesce(col('perte_exp_100_ind'), lit(0.0)) + 
                coalesce(col('risque_direct_100_ind'), lit(0.0))
            )
        
        if all(c in df_az.columns for c in ['perte_exp_100', 'risque_direct_100']):
            df_az = df_az.withColumn(
                'value_insured_100',
                coalesce(col('perte_exp_100'), lit(0.0)) + 
                coalesce(col('risque_direct_100'), lit(0.0))
            )
        
        # Step 3: Harmonize AZEC schema (if available)
        if df_azec is not None:
            self.logger.step(3, "Harmonizing AZEC schema")

            # SAS L47: "AZEC" AS DIRCOM
            df_azec = df_azec.withColumn('dircom', lit('AZEC'))

            # SAS L49: "3" AS CDPOLE (AZEC always courtage)
            df_azec = df_azec.withColumn('cdpole', lit('3'))

            # SAS L57-66: Set non-indexed columns to NULL (AZEC only has indexed)
            # AZ has both indexed (_ind) and non-indexed, AZEC only has indexed
            non_indexed_columns = [
                'limite_rc_100_par_sin', 'limite_rc_100_par_sin_tous_dom', 'limite_rc_100_par_an',
                'limite_rc_100', 'perte_exp_100', 'risque_direct_100',
                'value_insured_100', 'smp_100', 'lci_100'
            ]

            from utils.processor_helpers import add_null_columns
            from pyspark.sql.types import DoubleType
            null_cols = {col_name: DoubleType for col_name in non_indexed_columns}
            df_azec = add_null_columns(df_azec, null_cols)

            # Note: AZEC has _CIE columns (smp_cie, lci_cie) which AZ doesn't have
            # unionByName with allowMissingColumns will handle this gracefully
        
        # Step 4: Union AZ + AZEC
        self.logger.step(4, "Unioning AZ + AZEC")
        if df_azec is not None:
            df_consolidated = df_az.unionByName(df_azec, allowMissingColumns=True)
        else:
            df_consolidated = df_az
        
        self.logger.success(f"Consolidation completed: {df_consolidated.count():,} total records")
        return df_consolidated
    
    def write(self, df: DataFrame, vision: str) -> None:
        """
        Write consolidated data to gold layer.
        
        Args:
            df: Consolidated DataFrame
            vision: Vision in YYYYMM format
        """
        self.logger.info(f"Writing consolidated capital data to gold for vision {vision}")
        
        from utils.helpers import write_to_layer
        
        output_name = f"az_azec_capitaux_{vision}"
        write_to_layer(
            df, self.config, 'gold', output_name, vision, self.logger
        )
        
        self.logger.success(f"Wrote {df.count():,} records to gold: {output_name}.parquet")
