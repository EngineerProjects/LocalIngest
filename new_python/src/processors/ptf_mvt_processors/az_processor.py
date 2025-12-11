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
    apply_business_filters,
    extract_capitals,
    calculate_movements,
    calculate_exposures,
    rename_columns,
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
        Apply AZ business transformations following SAS order.

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

        # =======================================================================
        # STEP 0: Apply business filters FIRST (before any transformations)
        # SAS: WHERE clause in SELECT (L135, L149)
        # =======================================================================
        self.logger.step(0, "Applying business filters (construction market)")
        az_filters = business_rules.get('business_filters', {}).get('az', {}).get('filters', [])
        df = apply_business_filters(df, {'filters': az_filters}, self.logger)

        # =======================================================================
        # STEP 1: Rename columns (SAS: AS clauses in SELECT, L55-115)
        # =======================================================================
        self.logger.step(1, "Renaming columns") 
        from utils.transformations import rename_columns
        
        rename_map = {
            'posacta': 'posacta_ri',
            'rueacta': 'rueacta_ri',
            'cediacta': 'cediacta_ri',
            'csegt': 'cseg',
            'cssegt': 'cssseg',
            'cdtpcoa': 'cdcoas',
            'prcdcie': 'prcie',
            'fncmaca': 'mtca'
        }
        df = rename_columns(df, rename_map)

        # =======================================================================
        # STEP 2: Initialize columns to 0 (SAS: L106-115)
        # =======================================================================
        self.logger.step(2, "Initializing columns to 0")
        
        # Initialize numeric columns
        for col_name in ['primeto', 'primes_ptf', 'primes_afn', 'primes_res', 
                          'primes_rpt', 'primes_rpc', 'expo_ytd', 'expo_gli',
                          'cotis_100', 'mtca_', 'perte_exp', 'risque_direct',
                          'value_insured', 'smp_100', 'lci_100']:
            df = df.withColumn(col_name, lit(0.0))
        
        # Initialize integer flags
        for col_name in ['nbptf', 'nbafn', 'nbres', 'nbrpt', 'nbrpc', 'top_temp',
                          'top_lta', 'top_aop', 'nbafn_anticipe', 'nbres_anticipe']:
            df = df.withColumn(col_name, lit(0))
        
        # Initialize date columns
        df = df.withColumn('dt_deb_expo', lit(None).cast('date'))
        df = df.withColumn('dt_fin_expo', lit(None).cast('date'))
        
        # Initialize other columns needed for calculations (SAS L132-133: CTDUREE = .)
        df = df.withColumn('ctduree', lit(None).cast('double'))

        # =======================================================================
        # STEP 3: Add computed columns (SAS: L60-97)
        # =======================================================================
        self.logger.step(3, "Adding computed columns (tx, top_coass, coass, partcie)")
        
        # TX: coalesce txcede to 0 (SAS L60)
        df = df.withColumn('tx', coalesce(col('txcede'), lit(0)))
        
        # TOP_COASS: flag if cdpolqpl = '1' (SAS L63)
        df = df.withColumn('top_coass', when(col('cdpolqpl') == '1', lit(1)).otherwise(lit(0)))
        
        # COASS: coassurance type (SAS L64-70)
        df = df.withColumn('coass',
            when((col('cdpolqpl') == '1') & col('cdcoas').isin('3', '6'), lit('APERITION'))
            .when((col('cdpolqpl') == '1') & col('cdcoas').isin('4', '5'), lit('COASS. ACCEPTEE'))
            .when((col('cdpolqpl') == '1') & (col('cdcoas') == '8'), lit('ACCEPTATION INTERNATIONALE'))
            .when((col('cdpolqpl') == '1') & ~col('cdcoas').isin('3', '4', '5', '6', '8'), lit('AUTRES'))
            .otherwise(lit('SANS COASSURANCE'))
        )
        
        # PARTCIE: company share (SAS L71-74)
        df = df.withColumn('partcie',
            when(col('cdpolqpl') != '1', lit(1.0))
            .otherwise(col('prcie') / 100.0)
        )
        
        # TOP_REVISABLE: revision flag (SAS L77)
        df = df.withColumn('top_revisable', when(col('cdpolrvi') == '1', lit(1)).otherwise(lit(0)))
        
        # CRITERE_REVISION: revision criteria mapping (SAS L78-96)
        revision_config = az_config['revision_criteria']
        revision_expr = col('cdgrev')
        for code, label in revision_config['mapping'].items():
            revision_expr = when(col('cdgrev') == code, lit(label)).otherwise(revision_expr)
        df = df.withColumn('critere_revision', revision_expr)

        # =======================================================================
        # STEP 4: Add metadata columns (SAS: L128-133, L142-147)
        # =======================================================================
        self.logger.step(4, "Adding metadata columns (vision, dircom, cdpole)")
        
        df = df.withColumn('dircom', lit(DIRCOM.AZ))
        df = df.withColumn('vision', lit(vision))
        df = df.withColumn('exevue', lit(year_int))
        df = df.withColumn('moisvue', lit(month_int))
        
        # Determine CDPOLE from input file name (SAS uses different libraries PTF16 vs PTF36)
        # Note: BronzeReader adds _source_file column for this purpose
        if '_source_file' in df.columns:
            df = df.withColumn(
                'cdpole',
                when(col('_source_file').contains('ipf16'), lit(POLE.AGENT))
                .when(col('_source_file').contains('ipf36'), lit(POLE.COURTAGE))
                .otherwise(lit(POLE.AGENT))
            ).drop('_source_file')
        else:
            # Fallback: assume Agent if source file info missing
            df = df.withColumn('cdpole', lit(POLE.AGENT))

        # =======================================================================
        # STEP 5: Join IPFM99 for product 01099 (SAS: L157-187)
        # =======================================================================
        self.logger.step(5, "Joining IPFM99 for product 01099")
        df = self._join_ipfm99(df, vision)

        # =======================================================================
        # STEP 6: Extract capitals (SAS: L195-231)
        # =======================================================================
        self.logger.step(6, "Extracting capitals (SMP, LCI, PERTE_EXP, RISQUE_DIRECT)")
        capital_config = az_config['capital_extraction']
        
        # Only extract the 4 main capital types (matching SAS)
        capital_targets = {
            'smp_100': capital_config['smp_100'],
            'lci_100': capital_config['lci_100'],
            'perte_exp': capital_config['perte_exp'],
            'risque_direct': capital_config['risque_direct']
        }
        df = extract_capitals(df, capital_targets)

        # =======================================================================
        # STEP 7: Calculate premium indicators (SAS: L238-243)
        # =======================================================================
        self.logger.step(7, "Calculating premium indicators")
        
        # PRIMETO: mtprprto * (1 - tx/100) (SAS L238-239)
        df = df.withColumn('primeto', col('mtprprto') * (lit(1) - col('tx') / lit(100)))
        
        # TOP_LTA: long-term contracts (SAS L241-243)
        df = df.withColumn('top_lta',
            when((col('ctduree') > 1) & col('tydris1').isin(LTA_TYPES), lit(1))
            .otherwise(lit(0))
        )

        # =======================================================================
        # STEP 8: Calculate movements (AFN/RES/RPT/RPC/NBPTF) (SAS: L250-291)
        # =======================================================================
        self.logger.step(8, "Calculating movement indicators")
        movement_cols = az_config['movements']['column_mapping']
        df = calculate_movements(df, dates, year_int, movement_cols)

        # =======================================================================
        # STEP 9: Calculate exposures (expo_ytd, expo_gli) (SAS: L298-311)
        # =======================================================================
        self.logger.step(9, "Calculating exposures")
        exposure_cols = az_config['exposures']['column_mapping']
        df = calculate_exposures(df, dates, year_int, exposure_cols)

        # =======================================================================
        # STEP 10: Calculate cotisation and CA (SAS: L317-332)
        # =======================================================================
        self.logger.step(10, "Calculating cotisation 100% and CA")
        
        # Set PRCIE to 100 if missing (SAS L318-319)
        df = df.withColumn('prcie', 
            when((col('prcie').isNull()) | (col('prcie') == 0), lit(100.0))
            .otherwise(col('prcie'))
        )
        
        # Cotis_100: Technical premium at 100% (SAS L322-327)
        df = df.withColumn('cotis_100', col('mtprprto'))
        df = df.withColumn('cotis_100',
            when((col('top_coass') == 1) & col('cdcoas').isin('4', '5'),
                 col('mtprprto') * 100 / col('prcie'))
            .otherwise(col('cotis_100'))
        )
        
        # MTCA: Centralize CA in one column (SAS L330-332)
        df = df.withColumn('mtca', coalesce(col('mtca'), lit(0)))
        df = df.withColumn('mtcaf', coalesce(col('mtcaf'), lit(0)))
        df = df.withColumn('mtca_', col('mtcaf') + col('mtca'))

        # =======================================================================
        # STEP 11: Apply business rules (SAS: L339-355)
        # =======================================================================
        self.logger.step(11, "Applying business rules")
        
        # TOP_AOP (SAS L339-341)
        df = df.withColumn('top_aop', 
            when(col('opapoffr') == 'O', lit(1)).otherwise(lit(0))
        )
        
        # AFN/RES anticipés (SAS L347-355)
        df = df.withColumn('nbafn_anticipe',
            when((col('dteffan') > lit(dates['finmois'])) | (col('dtcrepol') > lit(dates['finmois'])),
                 when(~(col('cdtypli1').isin('RE') | col('cdtypli2').isin('RE') | col('cdtypli3').isin('RE')), lit(1))
                 .otherwise(lit(0)))
            .otherwise(lit(0))
        )
        
        df = df.withColumn('nbres_anticipe',
            when(col('dtresilp') > lit(dates['finmois']),
                 when(~((col('cdtypli1').isin('RP')) | (col('cdtypli2').isin('RP')) | 
                       (col('cdtypli3').isin('RP')) | (col('cdmotres') == 'R') | (col('cdcasres') == '2R')), lit(1))
                 .otherwise(lit(0)))
            .otherwise(lit(0))
        )

        # =======================================================================
        # STEP 12: Data cleanup (SAS: L362-370)
        # =======================================================================
        self.logger.step(12, "Data cleanup")
        
        # Reset expo dates if expo_ytd = 0 (SAS L364-367)
        df = df.withColumn('dt_deb_expo',
            when(col('expo_ytd') == 0, lit(None).cast('date')).otherwise(col('dt_deb_expo'))
        )
        df = df.withColumn('dt_fin_expo',
            when(col('expo_ytd') == 0, lit(None).cast('date')).otherwise(col('dt_fin_expo'))
        )
        
        # If NMCLT is blank, use NMACTA (SAS L368-369)
        df = df.withColumn('nmclt',
            when((col('nmclt').isNull()) | (col('nmclt') == ' '), col('nmacta'))
            .otherwise(col('nmclt'))
        )

        # =======================================================================
        # STEP 13: Enrich segmentation (SAS: L492-502)
        # =======================================================================
        self.logger.step(13, "Enriching segment and product type")
        df = self._enrich_segment_and_product_type(df, vision)

        # =======================================================================
        # STEP 14: Final deduplication (SAS: L505-507)
        # =======================================================================
        self.logger.step(14, "Deduplicating by nopol")
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
        # Check for duplicate columns before writing
        from collections import Counter
        col_counts = Counter(df.columns)
        duplicates = {col: count for col, count in col_counts.items() if count > 1}
        
        if duplicates:
            self.logger.error(f"❌ DUPLICATE COLUMNS DETECTED: {duplicates}")
            self.logger.error(f"Total columns: {len(df.columns)}")
            self.logger.error(f"Unique columns: {len(set(df.columns))}")
            raise ValueError(f"Cannot write DataFrame with duplicate columns: {list(duplicates.keys())}")
        
        self.logger.info(f"✓ No duplicate columns ({len(df.columns)} unique columns)")
        
        # Print schema for debugging
        self.logger.info("DataFrame Schema:")
        df.printSchema()
        
        # Show sample of first row to verify data
        self.logger.info("Sample data (first row):")
        try:
            first_row = df.first()
            if first_row:
                self.logger.info(f"First row keys: {first_row.asDict().keys()}")
        except Exception as e:
            self.logger.warning(f"Could not fetch first row: {e}")
        
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