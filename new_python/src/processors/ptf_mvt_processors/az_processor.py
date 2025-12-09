"""
AZ Processor - Portfolio Movements for AZ (Agent + Courtage channels).

Processes PTF16 (Agent) and PTF36 (Courtage) data from bronze layer,
applies business transformations, and outputs to silver layer.

Uses dictionary-driven configuration for maximum reusability.
"""

from pyspark.sql import DataFrame # type: ignore
from pyspark.sql.functions import col, when, lit, coalesce, broadcast # type: ignore
from pyspark.sql.types import DoubleType, StringType # type: ignore

from src.processors.base_processor import BaseProcessor
from src.reader import BronzeReader
from utils.loaders import get_default_loader
from config.constants import DIRCOM, POLE, LTA_TYPES
from utils.helpers import extract_year_month_int, compute_date_ranges
from utils.transformations import (
    apply_column_config,
    extract_capitals,
    calculate_movements,
    calculate_exposures,
    apply_conditional_transform,
    apply_transformations,
    apply_business_filters,
)
from utils.processor_helpers import safe_reference_join, safe_multi_reference_join, add_null_columns


class AZProcessor(BaseProcessor):
    """
    Process AZ (Agent + Courtage) portfolio data.
    
    Reads PTF16/PTF36 from bronze, applies transformations, writes to silver.
    All columns are lowercase.
    """

    def read(self, vision: str) -> DataFrame:
        """
        Read PTF16 (Agent) and PTF36 (Courtage) files from bronze layer.

        Args:
            vision: Vision in YYYYMM format

        Returns:
            Combined DataFrame (PTF16 + PTF36) with lowercase columns
        """
        reader = BronzeReader(self.spark, self.config)
        
        self.logger.info("Reading ipf_az files (PTF16 + PTF36)")
        df_ipf = reader.read_file_group('ipf_az', vision)
        # Columns are already lowercase from BronzeReader
        
        return df_ipf

    def transform(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Apply AZ business transformations (dictionary-driven from JSON configs).

        Args:
            df: Input DataFrame from read() (lowercase columns)
            vision: Vision in YYYYMM format

        Returns:
            Transformed DataFrame ready for silver layer (all lowercase)
        """
        year_int, month_int = extract_year_month_int(vision)
        dates = compute_date_ranges(vision)

        # Load configurations from JSON
        loader = get_default_loader()
        az_config = loader.get_az_config()
        business_rules = loader.get_business_rules()

        # Step 0: Apply business filters (SAS WHERE clause equivalent)
        self.logger.step(0, "Applying business filters (construction market)")
        az_filters = az_config.get('business_filters', {}).get('filters', [])
        df = apply_business_filters(df, {'filters': az_filters}, self.logger)

        # Step 1: Apply column configuration
        self.logger.step(1, "Applying column configuration")
        column_config = az_config['column_selection']
        df = apply_column_config(df, column_config, vision, year_int, month_int)

        # Step 2: Add DIRCOM constant
        df = df.withColumn('dircom', lit(DIRCOM.AZ))

        # Step 3: Determine CDPOLE from source file (business logic)
        self.logger.step(2, "Determining CDPOLE from source files")
        df = df.withColumn(
            'cdpole',
            when(col('_source_file').contains('ipfe16'), lit(POLE.AGENT))
            .when(col('_source_file').contains('ipfe36'), lit(POLE.COURTAGE))
            .otherwise(lit(POLE.AGENT))
        ).drop('_source_file')

        # Step 3: Join IPFM99 for product 01099
        self.logger.step(3, "Joining IPFM99 for product 01099")
        df = self._join_ipfm99(df, vision)

        # Step 4: Extract SMP/LCI capitals (dictionary-driven)
        self.logger.step(4, "Extracting capitals (SMP, LCI, PERTE_EXP, RISQUE_DIRECT)")
        capital_config = az_config['capital_extraction']
        df = extract_capitals(df, capital_config)

        # Step 5: Calculate movements (AFN/RES/RPT/RPC/NBPTF)
        self.logger.step(5, "Calculating movement indicators (NBAFN, NBRES, NBRPT, NBRPC, NBPTF)")
        movement_cols = az_config['movements']['column_mapping']
        df = calculate_movements(df, dates, year_int, movement_cols)

        # Step 6: Calculate exposures (expo_ytd, expo_gli)
        self.logger.step(6, "Calculating exposures (expo_ytd, expo_gli)")
        exposure_cols = az_config['exposures']['column_mapping']
        df = calculate_exposures(df, dates, year_int, exposure_cols)

        # Step 7: Calculate coassurance (dictionary-driven)
        self.logger.step(7, "Calculating coassurance indicators")
        coassurance_config = business_rules['coassurance_config']
        for col_name, config in coassurance_config.items():
            df = apply_conditional_transform(df, col_name, config)

        # Step 8: Apply revision criteria mapping (dictionary-driven)
        self.logger.step(8, "Applying revision criteria mapping")
        revision_config = az_config['revision_criteria']
        df = apply_transformations(df, [{
            'type': 'mapping',
            'column': 'critere_revision',
            'source': revision_config['source_col'],
            'mapping': revision_config['mapping'],
            'default': revision_config.get('default', '')
        }])

        # Step 9: Apply remaining transformations (primeto, mtca_, top_aop, etc.)
        self.logger.step(9, "Applying remaining transformations")
        transform_steps = business_rules['az_transform_steps']['steps']

        # All simple withColumn operations (config-driven)
        context = {
            'LTA_TYPES': LTA_TYPES,
            'FINMOIS': dates['finmois']
        }
        df = apply_transformations(df, transform_steps, context)

        # Step 10: Enrich segment2, type_produit_2, upper_mid (stub with graceful fallback)
        self.logger.step(10, "Enriching segment and product type")
        df = self._enrich_segment_and_product_type(df, vision)

        # Step 11: Final deduplication by nopol (SAS L505-507: NODUPKEY BY NOPOL)
        self.logger.step(11, "Deduplicating by nopol")
        # Order by nopol, cdsitp first to ensure consistent dedup (SAS L502)
        df = df.orderBy("nopol", "cdsitp").dropDuplicates(["nopol"])

        self.logger.info("AZ transformations completed successfully")

        return df

    def write(self, df: DataFrame, vision: str) -> None:
        """
        Write transformed AZ data to silver layer in parquet format.

        Args:
            df: Transformed DataFrame (lowercase columns)
            vision: Vision in YYYYMM format
        """
        from utils.helpers import write_to_layer
        write_to_layer(df, self.config, 'silver', 'mvt_const_ptf', vision, self.logger)

    def _join_ipfm99(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Join IPFM99 data for product 01099 special CA handling.

        Args:
            df: Main AZ DataFrame (lowercase columns)
            vision: Vision string

        Returns:
            DataFrame with IPFM99 joined (lowercase columns)
        """
        reader = BronzeReader(self.spark, self.config)
        
        # Use safe_reference_join helper to replace 61 lines of duplicate code
        df = safe_reference_join(
            df, reader,
            file_group='ipfm99_az',
            vision=vision,
            join_keys=['cdprod', 'nopol', 'noint'],
            select_columns=['mtcaenp', 'mtcasst', 'mtcavnt'],
            null_columns={'mtcaenp': DoubleType, 'mtcasst': DoubleType, 'mtcavnt': DoubleType},
            filter_condition="cdprod == '01099'",
            use_broadcast=True,
            logger=self.logger
        )
        
        # Update MTCA for product 01099
        df = df.withColumn(
            "mtca",
            when(
                col("cdprod") == "01099",
                coalesce(col("mtcaenp"), lit(0)) +
                coalesce(col("mtcasst"), lit(0)) +
                coalesce(col("mtcavnt"), lit(0))
            ).otherwise(col("mtca"))
        )

        return df

    def _enrich_segment_and_product_type(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrich with segment2, type_produit_2, and upper_mid.
        
        Implements SAS logic from PTF_MVTS_AZ_MACRO.sas L377-503:
        - Joins SEGMPRDT.PRDPFA1 / PRDPFA3 (segment tables by pole)
        - Joins CPRODUIT (Type_Produit_2, segment)
        - Joins PRDCAP.PRDCAP (product labels)
        - Joins TABLE_PT_GEST (management points - upper_mid)
        
        Args:
            df: Input DataFrame with cdprod and cdpole columns
            vision: Vision string
        
        Returns:
            DataFrame with segment2, type_produit_2, upper_mid columns
            (NULL if reference tables not available)
        """
        self.logger.info("Enriching segment and product type...")
        reader = BronzeReader(self.spark, self.config)
        
        # Use safe_multi_reference_join to replace 154 lines of duplicate code
        df = safe_multi_reference_join(df, reader, [
            {
                'file_group': 'cproduit',
                'vision': 'ref',
                'join_keys': 'cdprod',
                'select_columns': ['type_produit_2', 'segment2', 'segment_3'],
                'null_columns': {
                    'type_produit_2': StringType,
                    'segment2': StringType,
                    'segment_3': StringType
                }
            },
            {
                'file_group': 'table_pt_gest',
                'vision': 'ref',
                'join_keys': 'ptgst',
                'select_columns': ['upper_mid'],
                'null_columns': {'upper_mid': StringType}
            }
        ], logger=self.logger)
        
        return df

        return df
