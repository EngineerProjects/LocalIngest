"""
AZ Processor - Portfolio Movements for AZ (Agent + Courtage channels).

Processes PTF16 (Agent) and PTF36 (Courtage) data from bronze layer,
applies business transformations, and outputs to silver layer.

Uses dictionary-driven configuration for maximum reusability.
"""

from pyspark.sql import DataFrame # type: ignore
from pyspark.sql.functions import col, when, lit, coalesce, broadcast # type: ignore
from pyspark.sql.types import DoubleType, StringType, DateType # type: ignore

from src.processors.base_processor import BaseProcessor
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
from utils.processor_helpers import (safe_reference_join, safe_multi_reference_join, 
                                      add_null_columns, get_bronze_reader)


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
        reader = get_bronze_reader(self)
        
        self.logger.info("Reading ipf_az files (PTF16 + PTF36)")
        return reader.read_file_group('ipf_az', vision)

    def transform(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Apply AZ business transformations following SAS logic.

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

        # STEP 0: Apply business filters (SAS L135, L149)
        self.logger.step(0, "Applying business filters (construction market)")
        az_filters = business_rules.get('business_filters', {}).get('az', {}).get('filters', [])
        df = apply_business_filters(df, {'filters': az_filters}, self.logger)

        # STEP 1: Rename columns (SAS L55-115)
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

        # STEP 2: Initialize columns (SAS L106-115)
        self.logger.step(2, "Initializing columns to 0")
        

        init_columns = {
            # Numeric columns (float)
            **{col: lit(0.0) for col in ['primeto', 'primes_ptf', 'primes_afn', 'primes_res',
                                          'primes_rpt', 'primes_rpc', 'expo_ytd', 'expo_gli',
                                          'cotis_100', 'mtca_', 'perte_exp', 'risque_direct',
                                          'value_insured', 'smp_100', 'lci_100']},
            # Integer flags
            **{col: lit(0) for col in ['nbptf', 'nbafn', 'nbres', 'nbrpt', 'nbrpc', 'top_temp',
                                        'top_lta', 'top_aop', 'nbafn_anticipe', 'nbres_anticipe']},
            # Date columns and other
            'dt_deb_expo': lit(None).cast(DateType()),
            'dt_fin_expo': lit(None).cast(DateType()),
            'ctduree': lit(None).cast('double')
        }
        for col_name, col_value in init_columns.items():
            df = df.withColumn(col_name, col_value)

        # STEP 3: Add computed columns (SAS L60-97)
        self.logger.step(3, "Adding computed columns (tx, top_coass, coass, partcie)")
        
        # TX (SAS L60)
        df = df.withColumn('tx', coalesce(col('txcede'), lit(0)))
        
        # TOP_COASS (SAS L63)
        df = df.withColumn('top_coass', when(col('cdpolqpl') == '1', lit(1)).otherwise(lit(0)))
        
        # COASS (SAS L64-70)
        df = df.withColumn('coass',
            when((col('cdpolqpl') == '1') & col('cdcoas').isin('3', '6'), lit('APERITION'))
            .when((col('cdpolqpl') == '1') & col('cdcoas').isin('4', '5'), lit('COASS. ACCEPTEE'))
            .when((col('cdpolqpl') == '1') & (col('cdcoas') == '8'), lit('ACCEPTATION INTERNATIONALE'))
            .when((col('cdpolqpl') == '1') & ~col('cdcoas').isin('3', '4', '5', '6', '8'), lit('AUTRES'))
            .otherwise(lit('SANS COASSURANCE'))
        )
        
        # PARTCIE (SAS L71-74)
        df = df.withColumn('partcie',
            when(col('cdpolqpl') != '1', lit(1.0))
            .otherwise(col('prcie') / 100.0)
        )
        
        # TOP_REVISABLE (SAS L77)
        df = df.withColumn('top_revisable', when(col('cdpolrvi') == '1', lit(1)).otherwise(lit(0)))
        
        # CRITERE_REVISION (SAS L78-96)
        revision_config = az_config['revision_criteria']
        revision_expr = col('cdgrev')
        for code, label in revision_config['mapping'].items():
            revision_expr = when(col('cdgrev') == code, lit(label)).otherwise(revision_expr)
        df = df.withColumn('critere_revision', revision_expr)

        # STEP 4: Add metadata columns (SAS L128-147)
        self.logger.step(4, "Adding metadata columns (vision, dircom, cdpole)")
        

        metadata_cols = {
            'dircom': lit(DIRCOM.AZ),
            'vision': lit(vision),
            'exevue': lit(year_int),
            'moisvue': lit(month_int)
        }
        for col_name, col_value in metadata_cols.items():
            df = df.withColumn(col_name, col_value)
        
        # Determine CDPOLE from source file
        if '_source_file' in df.columns:
            df = df.withColumn(
                'cdpole',
                when(col('_source_file').contains('ipf16'), lit(POLE.AGENT))
                .when(col('_source_file').contains('ipf36'), lit(POLE.COURTAGE))
                .otherwise(lit(POLE.AGENT))
            ).drop('_source_file')
        else:

            df = df.withColumn('cdpole', lit(POLE.AGENT))

        # STEP 5: Join IPFM99 for product 01099 (SAS L157-187)
        self.logger.step(5, "Joining IPFM99 for product 01099")
        df = self._join_ipfm99(df, vision)

        # STEP 6: Extract capitals (SAS L195-231)
        self.logger.step(6, "Extracting capitals (SMP, LCI, PERTE_EXP, RISQUE_DIRECT)")
        capital_config = az_config['capital_extraction']
        

        capital_targets = {
            'smp_100': capital_config['smp_100'],
            'lci_100': capital_config['lci_100'],
            'perte_exp': capital_config['perte_exp'],
            'risque_direct': capital_config['risque_direct']
        }
        df = extract_capitals(df, capital_targets)

        # STEP 7: Calculate premium indicators (SAS L238-243)
        self.logger.step(7, "Calculating premium indicators")
        
        # PRIMETO (SAS L238-239)
        df = df.withColumn('primeto', col('mtprprto') * (lit(1) - col('tx') / lit(100)))
        
        # TOP_LTA (SAS L241-243)
        df = df.withColumn('top_lta',
            when((col('ctduree') > 1) & col('tydris1').isin(LTA_TYPES), lit(1))
            .otherwise(lit(0))
        )

        # STEP 8: Calculate movements (SAS L250-291)
        self.logger.step(8, "Calculating movement indicators")
        movement_cols = az_config['movements']['column_mapping']
        df = calculate_movements(df, dates, year_int, movement_cols)
        
        # STEP 8.5: Set TOP_TEMP flag (SAS L288-291)
        self.logger.step(8.5, "Setting TOP_TEMP flag for temporary policies")
        df = df.withColumn('top_temp',
            when(col('cdnatp') == 'T', lit(1)).otherwise(lit(0))
        )

        # STEP 9: Calculate exposures (SAS L298-311)
        self.logger.step(9, "Calculating exposures")
        exposure_cols = az_config['exposures']['column_mapping']
        df = calculate_exposures(df, dates, year_int, exposure_cols)

        # STEP 10: Calculate cotisation and CA (SAS L317-332)
        self.logger.step(10, "Calculating cotisation 100% and CA")
        

        df = df.withColumn('prcie', 
            when((col('prcie').isNull()) | (col('prcie') == 0), lit(100.0))
            .otherwise(col('prcie'))
        )
        
        # Cotis_100 (SAS L322-327)
        df = df.withColumn('cotis_100', col('mtprprto'))
        df = df.withColumn('cotis_100',
            when((col('top_coass') == 1) & col('cdcoas').isin('4', '5'),
                 col('mtprprto') * 100 / col('prcie'))
            .otherwise(col('cotis_100'))
        )
        
        # MTCA (SAS L330-332)
        df = df.withColumn('mtca', coalesce(col('mtca'), lit(0)))
        df = df.withColumn('mtcaf', coalesce(col('mtcaf'), lit(0)))
        df = df.withColumn('mtca_', col('mtcaf') + col('mtca'))

        # STEP 11: Apply business rules (SAS L339-355)
        self.logger.step(11, "Applying business rules")
        

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

        # STEP 12: Data cleanup (SAS L362-370)
        self.logger.step(12, "Data cleanup")
        

        df = df.withColumn('dt_deb_expo',
            when(col('expo_ytd') == 0, lit(None).cast(DateType())).otherwise(col('dt_deb_expo'))
        )
        df = df.withColumn('dt_fin_expo',
            when(col('expo_ytd') == 0, lit(None).cast(DateType())).otherwise(col('dt_fin_expo'))
        )
        
        # NMCLT fallback (SAS L368-369)
        df = df.withColumn('nmclt',
            when((col('nmclt').isNull()) | (col('nmclt') == ' '), col('nmacta'))
            .otherwise(col('nmclt'))
        )

        # STEP 13: Enrich segmentation (SAS L492-502)
        self.logger.step(13, "Enriching segment and product type")
        df = self._enrich_segment_and_product_type(df, vision)

        # STEP 14: Final deduplication (SAS L505-507)
        self.logger.step(14, "Deduplicating by nopol")
        df = df.orderBy("nopol", "cdsitp").dropDuplicates(["nopol"])

        self.logger.info("AZ transformations completed successfully")

        return df

    def write(self, df: DataFrame, vision: str) -> None:
        """
        Write transformed AZ data to silver layer.

        Args:
            df: Transformed DataFrame (lowercase columns)
            vision: Vision in YYYYMM format
        """
        from utils.helpers import write_to_layer
        write_to_layer(
            df, self.config, 'silver', f'mvt_const_ptf_{vision}', vision, self.logger
        )


    def _join_ipfm99(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Join IPFM99 data for product 01099 special CA handling.

        Args:
            df: Main AZ DataFrame (lowercase columns)
            vision: Vision string

        Returns:
            DataFrame with IPFM99 joined (lowercase columns)
        """
        reader = get_bronze_reader(self)
        
        # IPFM99 is optional (SAS L157-187)
        df = safe_reference_join(
            df, reader,
            file_group='ipfm99_az',
            vision=vision,
            join_keys=['cdprod', 'nopol', 'noint'],
            select_columns=['mtcaenp', 'mtcasst', 'mtcavnt'],
            null_columns={'mtcaenp': DoubleType, 'mtcasst': DoubleType, 'mtcavnt': DoubleType},
            filter_condition="cdprod == '01099'",
            use_broadcast=True,
            logger=self.logger,
            required=False
        )
        

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
        
        Implements SAS logic from PTF_MVTS_AZ_MACRO.sas L377-503.
        cproduit is REQUIRED - processing will fail if table is missing.
        table_pt_gest is optional - will add NULL if missing.
        
        Args:
            df: Input DataFrame with cdprod and cdpole columns
            vision: Vision string
        
        Returns:
            DataFrame with segment2, type_produit_2, upper_mid columns
        
        Raises:
            RuntimeError: If required reference data (cproduit) is unavailable
        """
        self.logger.info("Enriching segment and product type...")
        reader = get_bronze_reader(self)
        
        # CPRODUIT - REQUIRED (SAS L413-435)
        try:
            df_cproduit = reader.read_file_group('cproduit', 'ref').cache()  # Cache for reuse across transforms
        except FileNotFoundError as e:
            self.logger.error("CRITICAL: cproduit reference table is REQUIRED for AZ processing")
            self.logger.error(f"Cannot find cproduit: {e}")
            self.logger.error("This matches SAS behavior which would fail with 'File does not exist'")
            raise RuntimeError("Missing required reference data: cproduit. Cannot process AZ without product segmentation.") from e
        
        if df_cproduit is None:
            self.logger.error("CRITICAL: cproduit returned None (table exists but empty or unreadable)")
            raise RuntimeError("cproduit reference data is unavailable")
        
        # Join cproduit
        df_cproduit = df_cproduit.select(
            col('cprod').alias('cdprod'),
            col('Type_Produit_2').alias('type_produit_2'),
            col('segment').alias('segment2'),
            col('Segment_3').alias('segment_3')
        )
        df = df.join(broadcast(df_cproduit), on='cdprod', how='left')
        self.logger.info("✓ Successfully joined cproduit reference data")
        
        # TABLE_PT_GEST - OPTIONAL (SAS L477-503)
        # SAS uses version-specific logic:
        # - If vision <= 201112: use PTGST_201201
        # - Else: use %derniere_version(PT_GEST, ptgst_, &vision., TABLE_PT_GEST)
        # 
        # Python implementation: Try vision-specific first, fallback to 'ref'
        year_int, month_int = extract_year_month_int(vision)
        
        # Determine which PT_GEST version to use
        if year_int < 2011 or (year_int == 2011 and month_int <= 12):
            # For very old visions (<= 201112), use fixed version 201201 (SAS L478-481)
            pt_gest_vision = '201201'
            self.logger.info(f"Using PT_GEST version 201201 (vision {vision} <= 201112)")
        else:
            # For recent visions, use vision-specific or fallback to 'ref' (SAS L483-485)
            pt_gest_vision = vision
            self.logger.info(f"Using PT_GEST version {vision} (vision-specific)")
        
        # Try to load version-specific TABLE_PT_GEST
        df = safe_reference_join(
            df, reader,
            file_group='table_pt_gest',
            vision=pt_gest_vision,  # Use computed version instead of fixed 'ref'
            join_keys='ptgst',
            select_columns=['upper_mid'],
            null_columns={'upper_mid': StringType},
            use_broadcast=True,
            logger=self.logger,
            required=False  # Fallback to NULL if not found
        )
        
        # Debug: Check if upper_mid was successfully enriched
        if 'upper_mid' in df.columns:
            upper_mid_count = df.filter(col('upper_mid').isNotNull()).count()
            total_count = df.count()
            self.logger.info(f"✓ upper_mid enrichment: {upper_mid_count}/{total_count} non-null ({100*upper_mid_count/total_count:.1f}%)")
        else:
            self.logger.warning("⚠️ upper_mid column not found after join!")
        
        return df
