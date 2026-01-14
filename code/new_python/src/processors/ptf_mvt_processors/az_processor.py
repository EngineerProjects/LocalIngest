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
        
        Business filters are now applied BEFORE union (SAS L135, L149).
        SAS applies filters in the WHERE clause of each SELECT statement before OUTER UNION.
        Python must do the same - filter each file BEFORE unionByName.

        Args:
            vision: Vision in YYYYMM format

        Returns:
            Combined DataFrame (PTF16 + PTF36) with lowercase columns and filters applied
        """
        reader = get_bronze_reader(self)
        
        # Load business filters (SAS: &filtres_ptf in WHERE clause)
        loader = get_default_loader()
        business_rules = loader.get_business_rules()
        az_filters = business_rules.get('business_filters', {}).get('az', {}).get('filters', [])
        
        # Convert business_rules filter format to reader's custom_filter format
        # business_rules uses: {type: "equals", column: "cmarch", value: "6"}
        # reader expects: {operator: "==", column: "cmarch", value: "6"}
        custom_filters = self._convert_filters_to_reader_format(az_filters)
        
        self.logger.info(f"Reading ipf_az files (PTF16 + PTF36) with {len(custom_filters)} filters applied BEFORE union")
        return reader.read_file_group('ipf_az', vision, custom_filters=custom_filters)

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

        # STEP 0: Business filters now applied in read() BEFORE union (SAS L135, L149)
        # This matches SAS logic where filters are in WHERE clause of each SELECT

        # Note: Computed columns (tx, top_coass, coass, partcie, critere_revision) 
        # and column renames are ALL handled by JSON config in STEP 5 (_apply_computed_fields)
        # SAS does these in SELECT clause (L55-97), Python does them in transformation step

        # STEP 1: Initialize indicators (SAS L106-115)
        self.logger.step(1, "Initializing indicator columns to 0")
        
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

        # STEP 2: Add metadata columns (SAS L128-147)
        self.logger.step(2, "Adding metadata columns (vision, dircom, cdpole)")
        
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

        # STEP 3: Join IPFM99 for product 01099 (SAS L157-187)
        self.logger.step(3, "Joining IPFM99 for product 01099")
        df = self._join_ipfm99(df, vision)

        # STEP 4: Extract capitals (SAS L195-231)
        self.logger.step(4, "Extracting capitals (SMP, LCI, PERTE_EXP, RISQUE_DIRECT)")
        capital_config = az_config['capital_extraction']
        
        capital_targets = {
            'smp_100': capital_config['smp_100'],
            'lci_100': capital_config['lci_100'],
            'perte_exp': capital_config['perte_exp'],
            'risque_direct': capital_config['risque_direct']
        }
        df = extract_capitals(df, capital_targets)

        # STEP 5: Apply computed fields from JSON config (SAS L55-97 + L238-243)
        # This handles:
        # - Column renames (posacta->posacta_ri, etc.)
        # - Computed columns (tx, top_coass, coass, partcie, critere_revision)
        # - Business fields (primeto, top_lta, top_aop, top_temp, top_revisable)
        # NOTE: cotis_100/mtca_ calculated in Step 7.5 (after exposures), not in JSON
        # NOTE: nbafn_anticipe/nbres_anticipe calculated in Step 6.5 (not in JSON)
        self.logger.step(5, "Applying computed fields from config (renames + computed + business)")
        df = self._apply_computed_fields(df, az_config)

        # STEP 6: Calculate movements (SAS L250-291)
        self.logger.step(6, "Calculating movement indicators")
        movement_cols = az_config['movements']['column_mapping']
        df = calculate_movements(df, dates, year_int, movement_cols)
        
        # STEP 6.5: Calculate anticipated movements (SAS L347-355)
        # CRITICAL FIX: These must be calculated manually (like AZEC) because they need
        # access to finmois from dates dict, which is not available as a DataFrame column
        self.logger.step(6.5, "Calculating anticipated movements (nbafn_anticipe, nbres_anticipe)")
        finmois = dates['finmois']
        
        # nbafn_anticipe: AFN with effective date or creation date AFTER month end (SAS L347-350)
        # Exclude replacements (RE types)
        df = df.withColumn(
            "nbafn_anticipe",
            when(
                ((col("dteffan") > lit(finmois)) | (col("dtcrepol") > lit(finmois))) &
                ~((col("cdtypli1") == "RE") | (col("cdtypli2") == "RE") | (col("cdtypli3") == "RE")),
                lit(1)
            ).otherwise(lit(0))
        )
        
        # nbres_anticipe: RES with termination date AFTER month end (SAS L352-355)
        # Exclude replaced contracts (RP types) and specific cancellation reasons
        df = df.withColumn(
            "nbres_anticipe",
            when(
                (col("dtresilp") > lit(finmois)) &
                ~((col("cdtypli1") == "RP") | (col("cdtypli2") == "RP") | (col("cdtypli3") == "RP") | 
                  (col("cdmotres") == "R") | (col("cdcasres") == "2R")),
                lit(1)
            ).otherwise(lit(0))
        )
        
        # STEP 7: Calculate exposures (SAS L298-311)
        self.logger.step(7, "Calculating exposures")
        exposure_cols = az_config['exposures']['column_mapping']
        df = calculate_exposures(df, dates, year_int, exposure_cols)

        # STEP 7.5: Calculate cotis_100 and mtca_ (SAS L318-332)
        # CRITICAL FIX: These MUST be calculated AFTER exposures to match exact SAS order
        self.logger.step(7.5, "Calculating cotis_100 and mtca_ (premium/revenue metrics)")
        
        # Normalize prcdcie: set to 100 if null or 0 (SAS L318-319)
        df = df.withColumn(
            'prcdcie_normalized',
            when((col('prcdcie').isNull()) | (col('prcdcie') == 0), lit(100))
            .otherwise(col('prcdcie'))
        )
        
        # Calculate cotis_100 (SAS L322-327)
        # For accepted co-insurance (cdcoas in ['4', '5']), gross up by company share
        df = df.withColumn(
            'cotis_100',
            when(
                (col('top_coass') == 1) & (col('cdcoas').isin(['4', '5'])),
                (col('mtprprto') * 100) / col('prcdcie_normalized')
            ).otherwise(col('mtprprto'))
        )
        
        # Calculate mtca_: Total CA (Revenue) = MTCAF + MTCA (SAS L330-332)
        df = df.withColumn(
            'mtca_',
            coalesce(col('mtcaf'), lit(0)) + coalesce(col('mtca'), lit(0))
        )

        # STEP 8: Enrich with segmentation (segment2, type_produit_2, upper_mid) (SAS L377-503)
        self.logger.step(8, "Enriching with segmentation and management point")
        df = self._enrich_segment_and_product_type(df, vision)
        
        # STEP 9: Final data cleanup (SAS L362-370)
        self.logger.step(9, "Applying final data cleanup")
        df = self._finalize_data_cleanup(df)

        # STEP 10: Final deduplication (SAS L505-507)
        self.logger.step(10, "Deduplicating by nopol")
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


    def _apply_computed_fields(self, df: DataFrame, config: dict) -> DataFrame:
        """
        Apply computed fields from configuration.
        
        Handles three types of computed fields:
        - expression: Direct PySpark SQL expressions
        - conditional: Cascading when() conditions
        - flag_equality: Simple equality flags
        
        Args:
            df: Input DataFrame
            config: computed_fields configuration section
            
        Returns:
            DataFrame with computed fields added
        """
        from pyspark.sql.functions import expr
        
        computed = config.get('computed_fields', {})
        if not computed:
            return df
        
        self.logger.info(f"Applying {len(computed) - 1} computed fields from config")  # -1 for description
        
        for field_name, field_config in computed.items():
            if field_name == 'description':
                continue
                
            field_type = field_config.get('type')
            
            if field_type == 'expression':
                # Direct PySpark expression
                formula = field_config['formula']
                df = df.withColumn(field_name, expr(formula))
                self.logger.debug(f"  ✓ {field_name}: {formula}")
                
            elif field_type == 'conditional':
                # Cascading when() conditions
                conditions = field_config['conditions']
                default = field_config['default']
                
                # Build expression from default backwards
                if isinstance(default, str) and default not in ['0', '1']:
                    result_expr = expr(default)
                else:
                    result_expr = lit(float(default) if '.' in str(default) else int(default))
                
                # Apply conditions in reverse order (last condition has highest priority)
                for cond in reversed(conditions):
                    check_expr = expr(cond['check'])
                    result_val = cond['result']
                    
                    if isinstance(result_val, str) and result_val not in ['0', '1']:
                        result_val = expr(result_val)
                    else:
                        result_val = lit(int(result_val))
                    
                    result_expr = when(check_expr, result_val).otherwise(result_expr)
                
                df = df.withColumn(field_name, result_expr)
                self.logger.debug(f"  ✓ {field_name}: conditional with {len(conditions)} condition(s)")
                
            elif field_type == 'flag_equality':
                # Simple equality flag
                source = field_config['source_col']
                value = field_config['value']
                df = df.withColumn(field_name, when(col(source) == value, lit(1)).otherwise(lit(0)))
                self.logger.debug(f"  ✓ {field_name}: {source} == '{value}'")
        
        return df

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
        # SAS joins on (CDPOLE, CDPROD, NOPOL, NOINT) - 4 keys
        # Must include cdpole to prevent incorrect matches between Pole 1 and Pole 3
        df = safe_reference_join(
            df, reader,
            file_group='ipfm99_az',
            vision=vision,
            join_keys=['cdpole', 'cdprod', 'nopol', 'noint'],  # FIXED: Added cdpole
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
        
        Now matches SAS logic exactly (PTF_MVTS_AZ_MACRO.sas L377-503):
        1. Read PRDPFA1 (Agent) and PRDPFA3 (Courtage) separately
        2. Add reseau column ('1' or '3')
        3. Union them into Segment table
        4. Join with CPRODUIT for Type_Produit_2
        5. Join with main DF on cdprod AND cdpole=reseau (CRITICAL!)
        
        This ensures Agent products don't get Courtage segmentation and vice-versa.
        
        Args:
            df: Input DataFrame with cdprod and cdpole columns
            vision: Vision string
        
        Returns:
            DataFrame with segment2, type_produit_2, upper_mid columns
        
        Raises:
            RuntimeError: If required reference data is unavailable
        """
        self.logger.info("Enriching segment and product type (using PRDPFA1/PRDPFA3)...")
        reader = get_bronze_reader(self)
        
        # STEP 1: Read PRDPFA1 (Agent segmentation) - SAS L377-392
        try:
            df_prdpfa1 = reader.read_file_group('segmprdt_prdpfa1', 'ref')
        except FileNotFoundError as e:
            self.logger.error("CRITICAL: PRDPFA1 reference table is REQUIRED for AZ processing")
            raise RuntimeError("Missing required reference data: PRDPFA1. Cannot process AZ without product segmentation.") from e
        
        # Filter construction market and add reseau='1' (Agent)
        df_segment1 = df_prdpfa1.filter(col('cmarch') == '6') \
            .select(
                col('cprod'),
                col('lprod'),
                col('cseg'),
                col('cssseg'),
                (col('cseg') + lit('_') + col('lseg')).alias('lseg2'),
                (col('cssseg') + lit('_') + col('lssseg')).alias('lssseg2'),
                col('cmarch'),
                (col('cmarch') + lit('_') + col('lmarch')).alias('lmarch2'),
                lit('1').alias('reseau')
            ) \
            .dropDuplicates(['cprod'])
        
        self.logger.info(f"✓ Loaded PRDPFA1 (Agent): {df_segment1.count()} products")
        
        # STEP 2: Read PRDPFA3 (Courtage segmentation) - SAS L395-410
        try:
            df_prdpfa3 = reader.read_file_group('segmprdt_prdpfa3', 'ref')
        except FileNotFoundError as e:
            self.logger.error("CRITICAL: PRDPFA3 reference table is REQUIRED for AZ processing")
            raise RuntimeError("Missing required reference data: PRDPFA3. Cannot process AZ without product segmentation.") from e
        
        # Filter construction market and add reseau='3' (Courtage)
        df_segment3 = df_prdpfa3.filter(col('cmarch') == '6') \
            .select(
                col('cprod'),
                col('lprod'),
                col('cseg'),
                col('cssseg'),
                (col('cseg') + lit('_') + col('lseg')).alias('lseg2'),
                (col('cssseg') + lit('_') + col('lssseg')).alias('lssseg2'),
                col('cmarch'),
                (col('cmarch') + lit('_') + col('lmarch')).alias('lmarch2'),
                lit('3').alias('reseau')
            ) \
            .dropDuplicates(['cprod'])
        
        self.logger.info(f"✓ Loaded PRDPFA3 (Courtage): {df_segment3.count()} products")
        
        # STEP 3: Union PRDPFA1 + PRDPFA3 - SAS L422-428
        df_segment = df_segment1.unionByName(df_segment3, allowMissingColumns=True)
        self.logger.info(f"✓ Combined segmentation: {df_segment.count()} total products")
        
        # STEP 4: Read CPRODUIT and join for Type_Produit_2 - SAS L413-435
        try:
            df_cproduit = reader.read_file_group('cproduit', 'ref')
        except FileNotFoundError:
            self.logger.warning("⚠️ CPRODUIT not found, segment2/type_produit_2 will be from PRDPFAx only")
            df_cproduit = None
        
        if df_cproduit is not None:
            # Merge Segment with Cproduit for Type_Produit_2 and segment (SAS L431-435)
            df_cproduit_enrichment = df_cproduit.select(
                col('cprod'),
                col('Type_Produit_2').alias('type_produit_2'),
                col('segment').alias('segment_from_cproduit'),
                col('Segment_3').alias('segment_3')
            )
            
            df_segment = df_segment.join(
                df_cproduit_enrichment,
                on='cprod',
                how='left'
            )
            self.logger.info("✓ Enriched segmentation with CPRODUIT Type_Produit_2")
        
        # STEP 5: Read PRDCAP for product labels - SAS L442-468
        try:
            df_prdcap = reader.read_file_group('prdcap', 'ref')
            df_prdcap = df_prdcap.select(
                col('cdprod').alias('cprod'),
                col('lbtprod').alias('lprod_prdcap')
            ).dropDuplicates(['cprod'])
            
            # Update lprod with PRDCAP where available (SAS L454-468)
            df_segment = df_segment.join(
                df_prdcap,
                on='cprod',
                how='left'
            ).withColumn(
                'lprod',
                when(col('lprod_prdcap').isNotNull(), col('lprod_prdcap')).otherwise(col('lprod'))
            ).drop('lprod_prdcap')
            
            self.logger.info("✓ Enriched product labels from PRDCAP")
        except FileNotFoundError:
            self.logger.warning("⚠️ PRDCAP not found, using lprod from PRDPFAx only")
        
        # STEP 6: Prepare segmentation for join (SAS L470-472: BY reseau CPROD)
        df_segment = df_segment.dropDuplicates(['reseau', 'cprod'])
        
        # STEP 7: Join with main DataFrame - CRITICAL FIX! (SAS L500)
        # SAS: left join segment b on a.cdprod = b.cprod and a.CDPOLE = b.reseau
        df = df.join(
            broadcast(df_segment.select(
                col('cprod').alias('cdprod'),
                col('reseau').alias('cdpole'),  # CRITICAL: Must match cdpole!
                col('segment_from_cproduit').alias('segment2') if 'segment_from_cproduit' in df_segment.columns else lit(None).alias('segment2'),
                col('type_produit_2') if 'type_produit_2' in df_segment.columns else lit(None).alias('type_produit_2')
            )),
            on=['cdprod', 'cdpole'],  # Join on BOTH cdprod AND cdpole!
            how='left'
        )
        
        self.logger.info("✓ Successfully joined segmentation with CDPROD + CDPOLE")
        
        # STEP 8: Enrich with TABLE_PT_GEST for UPPER_MID - SAS L477-503
        year_int, month_int = extract_year_month_int(vision)
        
        # Determine which PT_GEST version to use
        if year_int < 2011 or (year_int == 2011 and month_int <= 12):
            pt_gest_vision = '201201'
            self.logger.info(f"Using PT_GEST version 201201 (vision {vision} <= 201112)")
        else:
            pt_gest_vision = vision
            self.logger.info(f"Using PT_GEST version {vision} (vision-specific)")
        
        # Try to load version-specific TABLE_PT_GEST
        df = safe_reference_join(
            df, reader,
            file_group='table_pt_gest',
            vision=pt_gest_vision,
            join_keys='ptgst',
            select_columns=['upper_mid'],
            null_columns={'upper_mid': StringType},
            use_broadcast=True,
            logger=self.logger,
            required=False  # Fallback to NULL if not found
        )
        
        # Debug: Check enrichment results
        if 'upper_mid' in df.columns:
            upper_mid_count = df.filter(col('upper_mid').isNotNull()).count()
            total_count = df.count()
            self.logger.info(f"✓ upper_mid enrichment: {upper_mid_count}/{total_count} non-null ({100*upper_mid_count/total_count:.1f}%)")
        
        # Check segment2 enrichment
        if 'segment2' in df.columns:
            segment2_count = df.filter(col('segment2').isNotNull()).count()
            total_count = df.count()
            self.logger.info(f"✓ segment2 enrichment: {segment2_count}/{total_count} non-null ({100*segment2_count/total_count:.1f}%)")
        
        return df
    
    def _finalize_data_cleanup(self, df: DataFrame) -> DataFrame:
        """
        Final data cleanup step matching SAS L362-370.
        
        Cleanup rules:
        1. If EXPO_YTD = 0, reset DT_DEB_EXPO and DT_FIN_EXPO to NULL
        2. If NMCLT is empty/null, replace with NMACTA
        
        Args:
            df: Input DataFrame after all transformations
        
        Returns:
            DataFrame with cleaned data
        """
        self.logger.info("Applying final data cleanup (SAS L362-370)...")
        
        # Cleanup EXPO dates if exposure is zero
        if 'expo_ytd' in df.columns and 'dt_deb_expo' in df.columns and 'dt_fin_expo' in df.columns:
            df = df.withColumn(
                'dt_deb_expo',
                when(col('expo_ytd') == 0, lit(None)).otherwise(col('dt_deb_expo'))
            ).withColumn(
                'dt_fin_expo',
                when(col('expo_ytd') == 0, lit(None)).otherwise(col('dt_fin_expo'))
            )
            self.logger.debug("  ✓ Cleaned DT_DEB_EXPO and DT_FIN_EXPO when EXPO_YTD = 0")
        
        # Replace empty NMCLT with NMACTA
        if 'nmclt' in df.columns and 'nmacta' in df.columns:
            df = df.withColumn(
                'nmclt',
                when(
                    (col('nmclt').isNull()) | (col('nmclt') == '') | (col('nmclt') == ' '),
                    col('nmacta')
                ).otherwise(col('nmclt'))
            )
            self.logger.debug("  ✓ Replaced empty NMCLT with NMACTA")
        
        return df
    
    def _convert_filters_to_reader_format(self, business_filters: list) -> list:
        """
        Convert business_rules filter format to reader's custom_filter format.
        
        Business rules format:
            {"type": "equals", "column": "cmarch", "value": "6"}
            {"type": "not_in", "column": "noint", "value": ["H90061", ...]}
        
        Reader format:
            {"operator": "==", "column": "cmarch", "value": "6"}
            {"operator": "not_in", "column": "noint", "value": ["H90061", ...]}
        
        Args:
            business_filters: List of filter dicts in business_rules format
        
        Returns:
            List of filter dicts in reader format
        """
        reader_filters = []
        
        for f in business_filters:
            filter_type = f.get('type')
            
            # Map business_rules type to reader operator
            operator_map = {
                'equals': '==',
                'not_equals': '!=',
                'in': 'in',
                'not_in': 'not_in',
                'greater_than': '>',
                'less_than': '<',
                'greater_equal': '>=',
                'less_equal': '<='
            }
            
            operator = operator_map.get(filter_type, filter_type)
            
            reader_filters.append({
                'operator': operator,
                'column': f.get('column'),
                'value': f.get('value') if 'value' in f else f.get('values')  # Handle both 'value' and 'values'
            })
        
        return reader_filters
