"""
AZEC Processor - Portfolio Movements for AZEC (Construction AZEC).

Processes POLIC_CU and CAPITXCU data from bronze layer,
applies AZEC-specific transformations, and outputs to silver layer.

Uses dictionary-driven configuration for maximum reusability.
"""

from config.variables import AZEC_CAPITAL_MAPPING
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, coalesce, year, datediff, greatest, least,
    make_date, broadcast, create_map, sum as spark_sum
)
from pyspark.sql.types import (
    DoubleType, IntegerType, DateType, StringType
)

from src.processors.base_processor import BaseProcessor
from utils.loaders import get_default_loader
from config.constants import DIRCOM, MARKET_CODE
from utils.helpers import build_layer_path, extract_year_month_int, compute_date_ranges
from utils.transformations import (
    apply_column_config,
    apply_conditional_transform,
    apply_transformations,
    apply_business_filters,
)
from utils.processor_helpers import get_bronze_reader


class AZECProcessor(BaseProcessor):
    """
    Process AZEC (Construction AZEC) portfolio data.
    
    Reads POLIC_CU/CAPITXCU from bronze, applies transformations, writes to silver.
    All columns are lowercase.
    """

    def read(self, vision: str) -> DataFrame:
        """
        Read POLIC_CU from bronze layer and calculate DTECHANN.

        Args:
            vision: Vision in YYYYMM format

        Returns:
            POLIC_CU DataFrame with DTECHANN calculated (lowercase columns)
        """
        reader = get_bronze_reader(self)

        self.logger.info("Reading POLIC_CU file")
        df = reader.read_file_group('polic_cu_azec', vision)

        # Calculate DTECHANN from ECHEANMM and ECHEANJJ
        # SAS L61: mdy(echeanmm, echeanjj, &annee.) AS DTECHANN
        year_int, _ = extract_year_month_int(vision)
        df = df.withColumn(
            "dtechann",  # SAS L61: DTECHANN (not dtechanm!)
            when(
                col("echeanmm").isNotNull() & col("echeanjj").isNotNull(),
                make_date(lit(year_int), col("echeanmm"), col("echeanjj"))
            ).otherwise(lit(None).cast(DateType()))
        )

        return df


    def transform(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Apply AZEC business transformations following SAS L52-487 strict order.

        Args:
            df: POLIC_CU DataFrame from read() (lowercase columns)
            vision: Vision in YYYYMM format

        Returns:
            Transformed DataFrame ready for silver layer (all lowercase)
        """
        year_int, month_int = extract_year_month_int(vision)
        dates = compute_date_ranges(vision)

        # Load configurations from JSON
        loader = get_default_loader()
        azec_config = loader.get_azec_config()
        business_rules = loader.get_business_rules()

        # ============================================================
        # STEP 1: Column Selection (SAS L52-85)
        # ============================================================
        self.logger.step(1, "Applying column configuration")
        column_config = azec_config['column_selection']
        df = apply_column_config(df, column_config, vision, year_int, month_int)

        # Add DIRCOM constant
        df = df.withColumn('dircom', lit(DIRCOM.AZEC))

        # Apply business filters (SAS WHERE clause)
        self.logger.step(2, "Applying business filters")
        azec_filters = business_rules.get('business_filters', {}).get('azec', {}).get('filters', [])
        df = apply_business_filters(df, {'filters': azec_filters}, self.logger)

        # ============================================================
        # STEP 2: Migration Handling (SAS L94-106)
        # ============================================================
        self.logger.step(3, "Handling AZEC migration")
        df = self._handle_migration(df, vision, azec_config)

        # ============================================================
        # STEP 3: Data Quality Updates (SAS L113-137)
        # ============================================================
        self.logger.step(4, "Updating dates and policy states")
        df = self._update_dates_and_states(df, dates, year_int, month_int)

        # ============================================================
        # STEP 4: AFN/RES/PTF Indicators (SAS L144-182)
        # ============================================================
        self.logger.step(5, "Calculating movement indicators")
        df = self._calculate_movements(df, dates, year_int, month_int)

        # ============================================================
        # STEP 5: Exposure Calculation (SAS L189-203)
        # ============================================================
        self.logger.step(6, "Calculating suspension periods")
        from utils.transformations.operations.business_logic import calculate_azec_suspension
        df = calculate_azec_suspension(df, dates)

        self.logger.step(7, "Calculating exposures")
        df = self._calculate_exposures(df, dates, vision)

        # ============================================================
        # STEP 6: Segmentation + Premiums (SAS L210-265)
        # ============================================================
        self.logger.step(8, "Enriching segmentation (LOB)")
        df = self._enrich_segmentation(df)

        # Calculate premiums AFTER segmentation (CSSSEG needed)
        self.logger.step(9, "Calculating premiums and flags")
        df = self._calculate_premiums(df)

        # CRITICAL: Force expo_ytd = 0 when PRIMES_PTF <= 0 (SAS L263-265)
        # SAS: UPDATE PTF_AZEC SET expo_ytd = 0 WHERE PRIMES_PTF <= 0;
        df = df.withColumn(
            "expo_ytd",
            when(col("primes_ptf") <= 0, lit(0)).otherwise(col("expo_ytd"))
        )

        # ============================================================
        # STEP 7: NAF Codes (SAS L272-302)
        # ============================================================
        self.logger.step(10, "Enriching NAF codes")
        df = self._enrich_naf_codes(df, vision)

        # ============================================================
        # STEP 8: Formulas (SAS L309-332)
        # ============================================================
        self.logger.step(11, "Enriching formulas")
        df = self._enrich_formulas(df, vision)

        # ============================================================
        # STEP 9: CA Turnover (SAS L339-357)
        # ============================================================
        self.logger.step(12, "Enriching CA turnover")
        df = self._enrich_ca(df, vision)

        # ============================================================
        # STEP 10: PE/RD/VI Capitals + Cleanup (SAS L364-380)
        # ============================================================
        self.logger.step(13, "Enriching PE/RD/VI capitals")
        df = self._enrich_pe_rd_vi(df, vision)

        # Cleanup expo dates AFTER PE/RD (SAS L374-380)
        df = df.withColumn('dt_deb_expo',
            when(col('expo_ytd') == 0, lit(None).cast(DateType())).otherwise(col('dt_deb_expo'))
        )
        df = df.withColumn('dt_fin_expo',
            when(col('expo_ytd') == 0, lit(None).cast(DateType())).otherwise(col('dt_fin_expo'))
        )

        # ============================================================
        # STEP 11: SMP/LCI Capitals (SAS L387-420)
        # ============================================================
        self.logger.step(14, "Joining SMP/LCI capital data")
        df = self._join_capitals(df, vision)

        # ============================================================
        # STEP 12: Business Adjustments (SAS L449-466)
        # ============================================================
        self.logger.step(15, "Applying business adjustments")
        df = self._adjust_nbres(df)

        # ============================================================
        # STEP 13: Final Enrichment (SAS L476-487)
        # ============================================================
        self.logger.step(16, "Final enrichment (UPPER_MID)")
        df = self._enrich_region(df)
        df = self._enrich_constrcu_site_data(df, vision)

        # ORDER BY police (SAS L487)
        df = df.orderBy('police')

        self.logger.info("AZEC transformations completed successfully")
        return df

    def write(self, df: DataFrame, vision: str) -> None:
        """
        Write transformed AZEC data to silver layer.

        Args:
            df: Transformed DataFrame (lowercase columns)
            vision: Vision in YYYYMM format
        """
        from utils.helpers import write_to_layer
        write_to_layer(
            df, self.config, 'silver', f'azec_ptf_{vision}', vision, self.logger
        )

    def _handle_migration(self, df: DataFrame, vision: str, azec_config: dict) -> DataFrame:
        """
        Handle AZEC migration for visions > 202009.
        
        ref_mig_azec_vs_ims is REQUIRED for vision > 202009.
        Processing will fail if table is missing (matching SAS behavior).
        
        Args:
            df: POLIC_CU DataFrame
            vision: Vision in YYYYMM format
            azec_config: AZEC configuration dict
        
        Returns:
            DataFrame with nbptf_non_migres_azec column
        
        Raises:
            RuntimeError: If required migration table is unavailable for vision > 202009
        """
        
        migration_config = azec_config['migration_handling']
        if int(vision) > migration_config['vision_threshold']:
            reader = get_bronze_reader(self)
            
            try:
                df_mig = reader.read_file_group('ref_mig_azec_vs_ims', vision='ref')
            except FileNotFoundError as e:
                self.logger.error(f"CRITICAL: ref_mig_azec_vs_ims is REQUIRED for vision > {migration_config['vision_threshold']}")
                self.logger.error(f"Cannot find migration table: {e}")
                self.logger.error("This matches SAS behavior which would fail with 'File does not exist'")
                raise RuntimeError(f"Missing required reference data: ref_mig_azec_vs_ims for vision {vision}") from e
            
            if df_mig is None:
                self.logger.error("CRITICAL: ref_mig_azec_vs_ims returned None")
                raise RuntimeError("ref_mig_azec_vs_ims reference data is unavailable")
            
            # Join with migration table
            df_mig_select = df_mig.select(
                col('nopol_azec').alias('_mig_nopol')
            ).dropDuplicates(['_mig_nopol'])
            
            df = df.alias('a').join(
                df_mig_select.alias('m'),
                col('a.police') == col('m._mig_nopol'),
                how='left'
            ).select(
                'a.*',
                when(col('m._mig_nopol').isNull(), lit(1))
                .otherwise(lit(0)).alias('nbptf_non_migres_azec')
            )
            self.logger.info("Migration table joined - NBPTF_NON_MIGRES_AZEC calculated")
        else:
            df = df.withColumn('nbptf_non_migres_azec', lit(1))
        
        return df


    def _update_dates_and_states(
        self,
        df: DataFrame,
        dates: dict,
        annee: int,
        mois: int
    ) -> DataFrame:
        """Update DATEXPIR, ETATPOL, DATFIN for specific cases (config-driven)."""
        from config.variables import AZEC_DATE_STATE_UPDATES
        from utils.transformations import apply_transformations
        from datetime import datetime

        # Calculate DATE_ONE_YEAR_AGO: mdy(mois, 01, annee - 1)
        # This is the first day of the same month, one year ago
        date_one_year_ago = datetime(annee - 1, mois, 1).strftime('%Y-%m-%d')

        context = {
            'DTFIN': dates['DTFIN'],
            'DATE_ONE_YEAR_AGO': date_one_year_ago
        }

        return apply_transformations(df, AZEC_DATE_STATE_UPDATES, context)

    def _calculate_movements(
        self,
        df: DataFrame,
        dates: dict,
        annee: int,
        mois: int
    ) -> DataFrame:
        """
        Calculate NBAFN, NBRES, NBPTF indicators (AZEC-specific logic).
        
        Args:
            df: AZEC DataFrame
            dates: Date range dictionary
            annee: Year as integer
            mois: Month as integer
        
        Returns:
            DataFrame with movement indicators calculated
        """
        from utils.transformations.operations.business_logic import calculate_azec_movements

        # Use AZEC-specific movement calculation (not the generic AZ one)
        df = calculate_azec_movements(df, dates, annee, mois)

        # AZEC-specific: Anticipated movements (SAS L67-70)
        finmois = dates['finmois']
        df = df.withColumn(
            "nbafn_anticipe",
            when(col("effetpol") > lit(finmois), lit(1)).otherwise(lit(0))
        )
        df = df.withColumn(
            "nbres_anticipe",
            when(col("datfin") > lit(finmois), lit(1)).otherwise(lit(0))
        )

        return df

    def _calculate_exposures(self, df: DataFrame, dates: dict, vision: str) -> DataFrame:
        """
        Calculate exposure metrics (config-driven).
        
        Args:
            df: AZEC DataFrame
            dates: Date range dictionary
            vision: Vision in YYYYMM format
        
        Returns:
            DataFrame with exposure metrics calculated
        """
        from config.variables import EXPOSURE_COLUMN_MAPPING
        from utils.transformations import calculate_exposures
        
        year_int, _ = extract_year_month_int(vision)
        return calculate_exposures(df, dates, year_int, EXPOSURE_COLUMN_MAPPING['azec'])


    def _join_capitals(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Join capital data from CAPITXCU.
        
        CAPITXCU is REQUIRED - capitals are critical for AZEC analysis.
        Processing will fail if table is missing (matching SAS behavior).
        
        Args:
            df: AZEC DataFrame
            vision: Vision in YYYYMM format
        
        Returns:
            DataFrame with SMP/LCI capitals
        
        Raises:
            RuntimeError: If required CAPITXCU table is unavailable
        """
        reader = get_bronze_reader(self)

        try:
            df_cap = reader.read_file_group('capitxcu_azec', vision)
        except FileNotFoundError as e:
            self.logger.error("CRITICAL: CAPITXCU is REQUIRED for AZEC processing")
            self.logger.error(f"Cannot find CAPITXCU: {e}")
            self.logger.error("This matches SAS behavior which would fail with 'File does not exist'")
            raise RuntimeError("Missing required capital data: CAPITXCU") from e
        
        if df_cap is None:
            self.logger.error("CRITICAL: CAPITXCU returned None")
            raise RuntimeError("CAPITXCU capital data is unavailable")

        # Select only needed columns
        # CRITICAL: Must include 'produit' for groupBy(police, produit) later (SAS L411-420)
        needed_cols = set(["police", "produit", "smp_sre", "brch_rea"])
        for mapping in AZEC_CAPITAL_MAPPING:
            needed_cols.add(mapping['source'])

        existing_needed_cols = [c for c in needed_cols if c in df_cap.columns]
        df_cap = df_cap.select(*existing_needed_cols)

        # Calculate branch-specific capitals
        for mapping in AZEC_CAPITAL_MAPPING:
            df_cap = df_cap.withColumn(
                mapping['target'],
                when(
                    (col("smp_sre") == mapping['smp_sre']) &
                    (col("brch_rea") == mapping['brch_rea']),
                    coalesce(col(mapping['source']), lit(0))
                ).otherwise(lit(0))
            )

        # CRITICAL FIX: Aggregate by (police, produit) not just police
        # SAS: GROUP BY POLICE, PRODUIT (PTF_MVTS_AZEC_MACRO.sas L411-421)
        # This prevents summing capitals across different products for the same police
        agg_columns = [mapping['target'] for mapping in AZEC_CAPITAL_MAPPING]
        df_cap_agg = df_cap.groupBy("police", "produit").agg(  # FIXED: Added produit
            *[spark_sum(col_name).alias(col_name) for col_name in agg_columns]
        )

        # Calculate global totals
        df_cap_agg = df_cap_agg.withColumn("lci_100", col("lci_pe_100") + col("lci_dd_100"))
        df_cap_agg = df_cap_agg.withColumn("lci_cie", col("lci_pe_cie") + col("lci_dd_cie"))
        df_cap_agg = df_cap_agg.withColumn("smp_100", col("smp_pe_100") + col("smp_dd_100"))
        df_cap_agg = df_cap_agg.withColumn("smp_cie", col("smp_pe_cie") + col("smp_dd_cie"))

        # FIXED: Left join on (police, produit) not just police
        df = df.alias("a").join(
            df_cap_agg.alias("b"),
            (col("a.police") == col("b.police")) & 
            (col("a.produit") == col("b.produit")),  # FIXED: Added produit join condition
            how="left"
        ).select(
            "a.*",
            coalesce(col("b.smp_100"), lit(0)).alias("smp_100"),
            coalesce(col("b.smp_cie"), lit(0)).alias("smp_cie"),
            coalesce(col("b.lci_100"), lit(0)).alias("lci_100"),
            coalesce(col("b.lci_cie"), lit(0)).alias("lci_cie")
        )

        self.logger.info("✓ Capital data joined successfully")
        return df
    
    def _adjust_nbres(self, df: DataFrame) -> DataFrame:
        """
        Apply AZEC-specific NBRES and NBAFN adjustments (SAS L448-466).
        
        Args:
            df: AZEC DataFrame
        
        Returns:
            DataFrame with adjusted NBRES and NBAFN
        """
        # Build NBRES expression with all conditions
        excluded_products = col("produit").isin(['DO0', 'TRC', 'CTR', 'CNR'])  # FIXED: Changed D00 to DO0

        nbres_adjusted = (
            when(excluded_products, lit(0))
            .when(
                (col("nbres") == 1) &
                (col("rmplcant").isNotNull()) &
                (col("rmplcant") != "") &
                col("motifres").isin(['RP']),  # FIXED: Changed from 'HP' to 'RP' to match SAS
                lit(0)
            )
            .when(
                (col("nbres") == 1) & col("motifres").isin(['SE', 'SA']),
                lit(0)
            )
            .otherwise(col("nbres"))
        )

        # Build NBAFN expression
        if 'cssseg' in df.columns:
            nbafn_adjusted = when(
                (col("nbafn") == 1) & (col("cssseg") == "5"),
                lit(0)
            ).otherwise(col("nbafn"))
        else:
            nbafn_adjusted = col("nbafn")

        # Build NBPTF expressions
        # FIXED: Ensure excluded products stay at 0 even after recalculation
        nbptf_final = when(
            excluded_products, lit(0)  # Keep excluded products at 0
        ).when(
            (nbafn_adjusted == 0) & (nbres_adjusted == 0),
            lit(1)
        ).otherwise(lit(0))

        # FIXED: Use withColumn to avoid COLUMN_ALREADY_EXISTS error
        df = df.withColumn("nbafn", nbafn_adjusted)
        df = df.withColumn("nbres", nbres_adjusted)
        df = df.withColumn("nbptf", nbptf_final)

        return df

    def _enrich_segmentation(self, df: DataFrame) -> DataFrame:
        """
        Add SEGMENT, CMARCH, CSEG, CSSSEG from LOB reference table.
        Also enriches Type_Produit from CONSTRCU_AZEC.
        
        CRITICAL FIX: Uses LOB table (construction products) instead of
        TABLE_SEGMENTATION_AZEC_MML to match SAS behavior.
        
        Based on: REF_segmentation_azec.sas L77-336
        
        Args:
            df: AZEC DataFrame with produit column
        
        Returns:
            DataFrame with segmentation columns (cmarch, cseg, cssseg, segment, type_produit_2)
        
        Raises:
            RuntimeError: If LOB table is unavailable (required reference data)
        """
        reader = get_bronze_reader(self)
        
        # ================================================================
        # STEP 1: Read LOB table (SAS L132-135)
        # ================================================================
        # SAS uses HASH table from LOB dataset filtered for construction (cmarch='6')
        # Python equivalent: read LOB and join on produit
        
        try:
            lob_ref = reader.read_file_group('lob', 'ref')
        except FileNotFoundError as e:
            self.logger.error("CRITICAL: LOB table is REQUIRED for AZEC segmentation")
            self.logger.error(f"Cannot find LOB: {e}")
            self.logger.error("This matches SAS behavior which would fail with 'File does not exist'")
            raise RuntimeError("Missing required LOB reference table for segmentation") from e
        
        if lob_ref is None:
            self.logger.error("CRITICAL: LOB returned None")
            raise RuntimeError("LOB reference data is unavailable")
        
        # Filter for construction market only (SAS L134: IF cmarch IN ('6'))
        lob_ref = lob_ref.filter(col('cmarch') == MARKET_CODE.MARKET)
        
        # OPTIMIZATION: Store lob_ref for CONSTRCU enrichment to avoid re-reading
        # SAS creates HASH table once and reuses it (L227: %SEGMENTA)
        self._lob_construction_ref = lob_ref  # Cache for CONSTRCU enrichment
        
        # Select segmentation columns from LOB (SAS L80-83)
        # LOB provides: CDPROD, CPROD, cmarch, lmarch, cseg, lseg, cssseg, lssseg, segment
        lob_select = lob_ref.select(
            'produit',  # Join key
            'cdprod',   # Product code
            'cprod',    # Product code variant
            'cmarch',   # Market code (='6' for construction)
            'lmarch',   # Market label
            'cseg',     # Segment code
            'lseg',     # Segment label
            'cssseg',   # Sub-segment code
            'lssseg',   # Sub-segment label
            'segment'   # Segment name
        ).dropDuplicates(['produit'])
        
        # ================================================================
        # STEP 2: Join LOB on PRODUIT (SAS L228-230, L288-289)
        # ================================================================
        # SAS: Uses HASH table lookup in DATA step
        # Python: LEFT JOIN to preserve all AZEC records
        # OPTIMIZATION: LOB is small reference table - use broadcast join
        
        from pyspark.sql.functions import broadcast
        
        df = df.alias('a').join(
            broadcast(lob_select.alias('l')),  # Broadcast small LOB table
            col('a.produit') == col('l.produit'),
            how='left'
        ).select(
            'a.*',  # Keep all AZEC columns
            # CRITICAL: Do NOT select 'l.cdprod' here!
            # AZEC keeps 'produit' which gets renamed to 'cdprod' during consolidation
            # Adding 'cdprod' here causes duplication error during union with AZ
            'l.cprod', 'l.cmarch', 'l.lmarch',
            'l.cseg', 'l.lseg', 'l.cssseg', 'l.lssseg', 'l.segment'
        )
        
        self.logger.info("✓ LOB segmentation joined (cmarch, cseg, cssseg, segment)")
        
        # ================================================================
        # CRITICAL: Filter for construction market ONLY (SAS L134)
        # ================================================================
        # After LOB join, filter to keep only construction market products
        # SAS does this via LOB hash table that only contains cmarch='6' products
        # Python: Explicit filter after join
        
        rows_before = df.count()
        df = df.filter(col('cmarch') == MARKET_CODE.MARKET)
        rows_after = df.count()
        
        self.logger.info(f"✓ Construction market filter applied: {rows_before:,} → {rows_after:,} rows ({100*(rows_before-rows_after)/rows_before:.1f}% filtered)")
        
        # ================================================================        
        # ================================================================
        # STEP 3: Enrich Type_Produit from CONSTRCU (built on-the-fly)
        # ================================================================
        # Instead of reading a preprocessed CONSTRCU_AZEC file, we build it automatically
        # This matches SAS REF_segmentation_azec.sas logic but done in the pipeline
        
        try:
            # Read raw CONSTRCU data
            constrcu_raw = reader.read_file_group('constrcu_azec', vision='ref')
            
            if constrcu_raw is not None:
                self.logger.info("Building CONSTRCU enrichment on-the-fly (SAS REF_segmentation_azec.sas)")
                
                # OPTIMIZATION: Reuse LOB reference from above instead of reading again
                # SAS does this efficiently with HASH table (L227: %SEGMENTA) - created once, used multiple times
                # Python: Reuse self._lob_construction_ref that was already filtered for construction market
                
                # Join CONSTRCU with LOB to get CDPROD and SEGMENT (SAS L227: %SEGMENTA)
                constrcu_enriched = constrcu_raw.alias('c').join(
                    self._lob_construction_ref.alias('l').select('produit', 'cdprod', 'segment', 'lssseg'),
                    col('c.produit') == col('l.produit'),
                    how='left'
                ).select(
                    col('c.police'),
                    col('c.produit'),
                    col('l.cdprod'),     # From LOB
                    col('l.segment'),    # From LOB
                    col('l.lssseg')      # For TYPE_PRODUIT calculation
                )
                
                # Calculate TYPE_PRODUIT (SAS L329-333)
                constrcu_enriched = constrcu_enriched.withColumn('type_produit',
                    when(col('lssseg') == 'TOUS RISQUES CHANTIERS', lit('TRC'))
                    .when(col('lssseg') == 'DOMMAGES OUVRAGES', lit('DO'))
                    .when(col('produit') == 'RCC', lit('Entreprises'))
                    .otherwise(lit('Autres'))
                )
                
                # Select final columns for join
                constrcu_select = constrcu_enriched.select(
                    'police',
                    'cdprod',
                    col('type_produit').alias('type_produit_constr'),
                    col('segment').alias('segment_constr')
                ).dropDuplicates(['police', 'cdprod'])
                
                # Left join: AZEC.produit = CONSTRCU.cdprod (SAS L484)
                df = df.alias('a').join(
                    constrcu_select.alias('c'),
                    (col('a.police') == col('c.police')) & 
                    (col('a.produit') == col('c.cdprod')),
                    how='left'
                ).select(
                    'a.*',
                    col('c.type_produit_constr').alias('type_produit_2'),
                    col('c.segment_constr').alias('segment_2')
                )
                
                self.logger.info("✓ CONSTRCU enrichment built and applied (type_produit_2, segment_2)")
                
            else:
                self.logger.warning("CONSTRCU not available - type_produit_2 will be NULL")
                from utils.processor_helpers import add_null_columns
                df = add_null_columns(df, {
                    'type_produit_2': StringType,
                    'segment_2': StringType
                })
                
        except Exception as e:
            self.logger.warning(f"CONSTRCU enrichment failed: {e}")
            self.logger.info("Adding NULL columns for type_produit_2 and segment_2")
            from utils.processor_helpers import add_null_columns
            df = add_null_columns(df, {
                'type_produit_2': StringType,
                'segment_2': StringType
            })
        
        self.logger.info("✓ AZEC segmentation complete (LOB + CONSTRCU_AZEC)")
        return df

    def _enrich_region(self, df: DataFrame) -> DataFrame:
        """
        SAS L482-487: Enrich UPPER_MID from TABLE_PT_GEST.
        
        CRITICAL: Only UPPER_MID is enriched in AZEC (not REGION, not P_NUM)
        SAS L482: d.UPPER_MID
        SAS L486: left join TABLE_PT_GEST as d ON a.POINGEST = d.PTGST
        
        Join: a.poingest = d.ptgst
        
        Args:
            df: AZEC DataFrame with poingest column
        
        Returns:
            DataFrame enriched with upper_mid column ONLY
        """
        reader = get_bronze_reader(self)

        try:
            df_ref = reader.read_file_group("table_pt_gest", vision="ref")

            if df_ref is None:
                self.logger.warning("TABLE_PT_GEST unavailable - applying fallback (NULL upper_mid)")
                return df.withColumn("upper_mid", lit(None).cast(StringType()))

            # Only UPPER_MID (SAS L482)
            df_ref = df_ref.select(
                col("ptgst"),
                col("upper_mid") if "upper_mid" in df_ref.columns else lit(None).cast(StringType()).alias("upper_mid")
            ).dropDuplicates(["ptgst"])

            # LEFT join a.poingest == ref.ptgst (SAS L486)
            df = df.alias("a").join(
                df_ref.alias("r"),
                col("a.poingest") == col("r.ptgst"),
                how="left"
            ).select(
                "a.*",
                col("r.upper_mid")
            )

            self.logger.info("✓ TABLE_PT_GEST joined (upper_mid ONLY) - SAS L482 compliant")
            return df

        except Exception as e:
            self.logger.warning(f"TABLE_PT_GEST enrichment failed: {e} - applying fallback")
            return df.withColumn("upper_mid", lit(None).cast(StringType()))


    def _enrich_naf_codes(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrich AZEC data with NAF codes using FULL OUTER JOIN strategy.
        
        CRITICAL: Reproduces SAS L272-302 EXACTLY.
        SAS uses FULL JOIN of 4 tables (INCENDCU, MPACU, RCENTCU, RISTECCU)
        to ensure ALL policies from ALL tables are captured.
        
        Args:
            df: AZEC DataFrame
            vision: Vision in YYYYMM format
        
        Returns:
            DataFrame enriched with NAF codes (cdnaf, cdtre)
        """
        reader = get_bronze_reader(self)
        
        # ================================================================
        # STEP 1: FULL OUTER JOIN of 4 tables for NAF codes (SAS L272-279)
        # ================================================================
        # SAS: COALESCEC(t1.POLICE, t2.POLICE, t3.POLICE, t4.POLICE)
        #      COALESCEC(t1.COD_NAF, t2.cod_naf, t3.COD_NAF, t4.COD_NAF)
        
        try:
            # Read all 4 tables
            df_incend = reader.read_file_group('incendcu_azec', vision)
            df_mpacu = reader.read_file_group('mpacu_azec', vision)
            df_rcentcu = reader.read_file_group('rcentcu_azec', vision)
            df_risteccu = reader.read_file_group('risteccu_azec', vision)
            
            # Prepare each table: select (police, cod_naf) and deduplicate
            tables = []
            
            if df_incend is not None:
                tables.append(df_incend.select(
                    col('police'), 
                    col('cod_naf').alias('naf_incend')
                ).dropDuplicates(['police']))
            
            if df_mpacu is not None:
                tables.append(df_mpacu.select(
                    col('police'),
                    col('cod_naf').alias('naf_mpacu')
                ).dropDuplicates(['police']))
            
            if df_rcentcu is not None:
                tables.append(df_rcentcu.select(
                    col('police'),
                    col('cod_naf').alias('naf_rcentcu')
                ).dropDuplicates(['police']))
            
            if df_risteccu is not None:
                tables.append(df_risteccu.select(
                    col('police'),
                    col('cod_naf').alias('naf_risteccu')
                ).dropDuplicates(['police']))
            
            if not tables:
                self.logger.warning("No NAF tables available - skipping NAF enrichment")
                from utils.processor_helpers import add_null_columns
                return add_null_columns(df, {'cdnaf': StringType, 'cdtre': StringType})
            
            # FULL OUTER JOIN all tables (SAS L277-279)
            df_naf_all = tables[0]
            for df_table in tables[1:]:
                df_naf_all = df_naf_all.join(df_table, on='police', how='full_outer')
            
            # COALESCEC: Take first non-null NAF code (SAS L275)
            df_naf_all = df_naf_all.withColumn(
                'code_naf',
                coalesce(
                    col('naf_incend') if 'naf_incend' in df_naf_all.columns else lit(None),
                    col('naf_mpacu') if 'naf_mpacu' in df_naf_all.columns else lit(None),
                    col('naf_rcentcu') if 'naf_rcentcu' in df_naf_all.columns else lit(None),
                    col('naf_risteccu') if 'naf_risteccu' in df_naf_all.columns else lit(None)
                )
            )
            
            # Keep only (police, code_naf)
            df_naf_all = df_naf_all.select('police', 'code_naf')
            
            # GROUP BY + MIN to deduplicate (SAS L281-284)
            from pyspark.sql.functions import min as spark_min
            df_code_naf = df_naf_all.groupBy('police').agg(
                spark_min('code_naf').alias('code_naf')
            )
            
            self.logger.info(f"NAF codes consolidated from {len(tables)} tables using FULL OUTER JOIN")
            
        except Exception as e:
            self.logger.warning(f"NAF FULL JOIN failed: {e} - using empty NAF")
            df_code_naf = self.spark.createDataFrame([], schema="police string, code_naf string")
        
        # ================================================================
        # STEP 2: COD_TRE from INCENDCU only (SAS L286-289)
        # ================================================================
        try:
            if df_incend is not None:
                df_code_tre = df_incend.select('police', 'cod_tre').dropDuplicates(['police'])
                
                # GROUP BY + MIN (SAS L287-289)
                from pyspark.sql.functions import min as spark_min
                df_code_tre = df_code_tre.groupBy('police').agg(
                    spark_min('cod_tre').alias('code_tre')
                )
                
                self.logger.info("COD_TRE extracted from INCENDCU")
            else:
                df_code_tre = self.spark.createDataFrame([], schema="police string, code_tre string")
        except Exception as e:
            self.logger.warning(f"COD_TRE extraction failed: {e}")
            df_code_tre = self.spark.createDataFrame([], schema="police string, code_tre string")
        
        # ================================================================
        # STEP 3: FULL OUTER JOIN CODE_TRE + CODE_NAF (SAS L291-297)
        # ================================================================
        df_naf_final = df_code_tre.join(df_code_naf, on='police', how='full_outer')
        
        # ================================================================
        # STEP 4: LEFT JOIN to main DataFrame (SAS L300-302)
        # ================================================================
        df = df.join(df_naf_final, on='police', how='left')
        
        # Rename to match expected columns
        if 'code_naf' in df.columns:
            df = df.withColumn('cdnaf', col('code_naf')).drop('code_naf')
        else:
            df = df.withColumn('cdnaf', lit(None).cast(StringType()))
        
        if 'code_tre' in df.columns:
            df = df.withColumn('cdtre', col('code_tre')).drop('code_tre')
        else:
            df = df.withColumn('cdtre', lit(None).cast(StringType()))
        
        self.logger.info("✓ NAF codes enriched with FULL OUTER JOIN (SAS-compliant)")
        return df



    def _calculate_premiums(self, df: DataFrame) -> DataFrame:
        """
        Calculate premiums and related fields AFTER segmentation (SAS L217-248).
        
        CRITICAL: Must be called AFTER _enrich_segmentation() to have CSSSEG available.
        
        Args:
            df: AZEC DataFrame with segmentation (CSSSEG column)
        
        Returns:
            DataFrame with premiums and flags calculated
        """
        # SAS L62: partcie = partbrut / 100
        df = df.withColumn('partcie', col('partbrut') / 100.0)
        
        # SAS L217: primecua = (prime * partbrut / 100 + cpcua)
        # SAS L227: primeto = (prime * partbrut / 100 + cpcua)  [Same formula]
        primeto_expr = (col('prime') * col('partbrut') / 100.0) + col('cpcua')
        df = df.withColumn('primeto', primeto_expr)
        df = df.withColumn('primecua', primeto_expr)  # Alias
        
        # SAS L229: cotis_100 = CASE WHEN partbrut=0 THEN prime ELSE (prime + (cpcua/partcie)) END
        df = df.withColumn('cotis_100',
            when(col('partbrut') == 0, col('prime'))
            .otherwise(col('prime') + (col('cpcua') / col('partcie')))
        )
        
        # SAS L231-238: COASS logic
        df = df.withColumn('coass',
            when(col('codecoas') == '0', lit('SANS COASSURANCE'))
            .when(col('codecoas') == 'A', lit('APERITION'))
            .when(col('codecoas') == 'C', lit('COASS. ACCEPTEE'))
            .when((col('typcontr') == 'A') & (col('codecoas') == 'R'), lit('REASS. ACCEPTEE'))
            .otherwise(lit(None))
        )
        
        # SAS L240-245: CDNATP logic
        df = df.withColumn('cdnatp',
            when((col('duree') == '00') & col('produit').isin(['CNR', 'CTR', 'DO0']), lit('C'))
            .when((col('duree') == '00') & (col('produit') == 'TRC'), lit('T'))
            .when(col('duree').isin(['01', '02', '03']), lit('R'))
            .otherwise(lit(''))
        )
        
        # SAS L247: TOP_COASS
        df = df.withColumn('top_coass',
            when(col('codecoas') == '0', lit(0)).otherwise(lit(1))
        )
        
        # SAS L76: TOP_LTA
        df = df.withColumn('top_lta',
            when(~col('duree').isin(['00', '01', '', ' ']), lit(1)).otherwise(lit(0))
        )
        
        # SAS L77: TOP_REVISABLE
        if 'indregul' in df.columns:
            df = df.withColumn('top_revisable',
                when(col('indregul') == 'O', lit(1)).otherwise(lit(0))
            )
        else:
            df = df.withColumn('top_revisable', lit(0))
        
        # Initialize empty critere_revision and cdgrev
        df = df.withColumn('critere_revision', lit(''))
        df = df.withColumn('cdgrev', lit(''))
        
        # SAS L248: TYPE_AFFAIRE = TYPCONTR
        df = df.withColumn('type_affaire', col('typcontr'))
        
        # SAS L223-225: PRIMES_AFN, PRIMES_RES, PRIMES_PTF
        # CSSSEG filtering for PRIMES_AFN and PRIMES_RES
        cssseg_filter = (col("cssseg") != "5") if "cssseg" in df.columns else lit(True)

        df = df.withColumn('primes_afn',
            when((col("nbafn") == 1) & cssseg_filter, col("primecua")).otherwise(lit(0))
        )
        df = df.withColumn('primes_res',
            when((col("nbres") == 1) & cssseg_filter, col("primecua")).otherwise(lit(0))
        )
        df = df.withColumn('primes_ptf',
            when(col("nbptf") == 1, col("primecua")).otherwise(lit(0))
        )
        
        self.logger.info("✓ Premiums and flags calculated (primeto, cotis_100, coass, cdnatp, top_*, primes_*)")
        return df

    def _enrich_formulas(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrich with product formulas from RCENTCU and RISTECCU (SAS L309-332).
        
        Args:
            df: AZEC DataFrame
            vision: Vision in YYYYMM format
        
        Returns:
            DataFrame enriched with formulas (formule, formule2, formule3, formule4)
        """
        reader = get_bronze_reader(self)
        
        # Initialize formula columns if not present
        from utils.processor_helpers import add_null_columns
        existing_cols = set(df.columns)
        for col_name in ['formule', 'formule2', 'formule3', 'formule4']:
            if col_name not in existing_cols:
                df = df.withColumn(col_name, lit(None).cast(StringType()))
        
        # RCENTCU formulas (SAS L310 line 1)
        try:
            df_rcentcu = reader.read_file_group('rcentcu_azec', vision)
            if df_rcentcu is not None:
                df_rcentcu_select = df_rcentcu.select(
                    'police',
                    col('formule').alias('formule_rc'),
                    col('formule2').alias('formule2_rc'),
                    col('formule3').alias('formule3_rc'),
                    col('formule4').alias('formule4_rc')
                ).dropDuplicates(['police'])
                
                df = df.join(df_rcentcu_select, on='police', how='left')
                
                # Coalesce with RC fallback
                df = df.withColumn('formule', coalesce(col('formule'), col('formule_rc')))
                df = df.withColumn('formule2', coalesce(col('formule2'), col('formule2_rc')))
                df = df.withColumn('formule3', coalesce(col('formule3'), col('formule3_rc')))
                df = df.withColumn('formule4', coalesce(col('formule4'), col('formule4_rc')))
                df = df.drop('formule_rc', 'formule2_rc', 'formule3_rc', 'formule4_rc')
                
                self.logger.info("RCENTCU formulas joined")
        except Exception as e:
            self.logger.debug(f"RCENTCU not available: {e}")
        
        # RISTECCU formulas (SAS L311 line 2 - UNION)
        try:
            df_risteccu = reader.read_file_group('risteccu_azec', vision)
            if df_risteccu is not None:
                df_risteccu_select = df_risteccu.select(
                    'police',
                    col('formule').alias('formule_ris'),
                    col('formule2').alias('formule2_ris'),
                    col('formule3').alias('formule3_ris'),
                    col('formule4').alias('formule4_ris')
                ).dropDuplicates(['police'])
                
                df = df.join(df_risteccu_select, on='police', how='left')
                
                # Coalesce with RISTECCU fallback
                df = df.withColumn('formule', coalesce(col('formule'), col('formule_ris')))
                df = df.withColumn('formule2', coalesce(col('formule2'), col('formule2_ris')))
                df = df.withColumn('formule3', coalesce(col('formule3'), col('formule3_ris')))
                df = df.withColumn('formule4', coalesce(col('formule4'), col('formule4_ris')))
                df = df.drop('formule_ris', 'formule2_ris', 'formule3_ris', 'formule4_ris')
                
                self.logger.info("RISTECCU formulas joined")
        except Exception as e:
            self.logger.debug(f"RISTECCU not available: {e}")
        
        self.logger.info("✓ Formulas enriched (formule × 4)")
        return df

    def _enrich_ca(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrich with CA turnover from MULPROCU (SAS L339-357).
        
        Args:
            df: AZEC DataFrame
            vision: Vision in YYYYMM format
        
        Returns:
            DataFrame enriched with MTCA (turnover)
        """
        reader = get_bronze_reader(self)
        
        try:
            df_mulprocu = reader.read_file_group('mulprocu_azec', vision)
            if df_mulprocu is not None:
                # SAS L340: SELECT POLICE, SUM(CHIFFAFF) AS MTCA GROUP BY POLICE
                from pyspark.sql.functions import sum as spark_sum
                df_mulprocu_agg = df_mulprocu.groupBy('police').agg(
                    spark_sum('chiffaff').alias('mtca')
                )
                
                # Left join on police
                df = df.join(df_mulprocu_agg, on='police', how='left')
                
                self.logger.info("MULPROCU CA data joined")
        except Exception as e:
            self.logger.warning(f"MULPROCU not available: {e}")
            if 'mtca' not in df.columns:
                df = df.withColumn('mtca', lit(None).cast(DoubleType()))
        
        self.logger.info("✓ CA turnover enriched (mtca)")
        return df

    def _enrich_pe_rd_vi(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrich with PE/RD/VI capitals from INCENDCU (SAS L364-371).
        
        Args:
            df: AZEC DataFrame
            vision: Vision in YYYYMM format
        
        Returns:
            DataFrame enriched with PERTE_EXP, RISQUE_DIRECT, VALUE_INSURED
        """
        reader = get_bronze_reader(self)
        
        try:
            df_incendcu = reader.read_file_group('incendcu_azec', vision)
            if df_incendcu is not None:
                # SAS L365-370: Aggregate by (POLICE, PRODUIT)
                from pyspark.sql.functions import sum as spark_sum
                df_pe_rd = df_incendcu.groupBy('police', 'produit').agg(
                    spark_sum('mt_baspe').alias('perte_exp'),
                    spark_sum('mt_basdi').alias('risque_direct')
                ).withColumn('value_insured',
                    coalesce(col('perte_exp'), lit(0)) + coalesce(col('risque_direct'), lit(0))
                )
                
                # Left join on (police, produit) - CRITICAL: Both keys!
                df = df.alias('a').join(
                    df_pe_rd.alias('b'),
                    (col('a.police') == col('b.police')) & (col('a.produit') == col('b.produit')),
                    how='left'
                ).select(
                    'a.*',
                    coalesce(col('b.perte_exp'), lit(0)).alias('perte_exp'),
                    coalesce(col('b.risque_direct'), lit(0)).alias('risque_direct'),
                    coalesce(col('b.value_insured'), lit(0)).alias('value_insured')
                )
                
                self.logger.info("PE/RD/VI capitals joined")
        except Exception as e:
            self.logger.warning(f"INCENDCU not available: {e}")
            from utils.processor_helpers import add_null_columns
            df = add_null_columns(df, {
                'perte_exp': DoubleType,
                'risque_direct': DoubleType,
                'value_insured': DoubleType
            })
        
        self.logger.info("✓ PE/RD/VI capitals enriched")
        return df

    def _enrich_constrcu_site_data(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrich with construction site data from CONSTRCU (SAS L481).
        
        Adds: DATOUVCH, LDESTLOC, MNT_GLOB, DATRECEP, DEST_LOC, DATFINCH, LQUALITE.
        
        Note: UPPER_MID is now enriched in _enrich_region() for efficiency
        (single join instead of two separate joins).
        
        Args:
            df: AZEC DataFrame
            vision: Vision in YYYYMM format
        
        Returns:
            DataFrame enriched with construction site details
        """
        reader = get_bronze_reader(self)
        
        # CONSTRCU site data (SAS L481: c.DATOUVCH, c.LDESTLOC, ...)
        try:
            df_constrcu = reader.read_file_group('constrcu_azec', vision='ref')
            if df_constrcu is not None:
                # Select site data columns
                site_cols = ['police', 'produit']
                for col_name in ['datouvch', 'ldestloc', 'mnt_glob', 'datrecep', 'dest_loc', 'datfinch', 'lqualite']:
                    if col_name in df_constrcu.columns:
                        site_cols.append(col_name)
                
                df_site = df_constrcu.select(*site_cols).dropDuplicates(['police', 'produit'])
                
                # Left join on (police, produit) - SAS L485
                df = df.alias('a').join(
                    df_site.alias('c'),
                    (col('a.police') == col('c.police')) & (col('a.produit') == col('c.produit')),
                    how='left'
                ).select('a.*', *[f'c.{c}' for c in site_cols if c not in ['police', 'produit']])
                
                self.logger.info("✓ CONSTRCU site data joined")
        except Exception as e:
            self.logger.warning(f"CONSTRCU not available: {e}")
        
        return df

