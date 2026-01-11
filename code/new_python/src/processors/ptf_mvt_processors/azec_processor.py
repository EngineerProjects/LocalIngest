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
        Read POLIC_CU from bronze layer and calculate DTECHANM.

        Args:
            vision: Vision in YYYYMM format

        Returns:
            POLIC_CU DataFrame with DTECHANM calculated (lowercase columns)
        """
        reader = get_bronze_reader(self)

        self.logger.info("Reading POLIC_CU file")
        df = reader.read_file_group('polic_cu_azec', vision)

        # Calculate DTECHANM from ECHEANMM and ECHEANJJ
        # SAS: mdy(echeanmm, echeanjj, &annee.)
        year_int, _ = extract_year_month_int(vision)
        df = df.withColumn(
            "dtechanm",
            when(
                col("echeanmm").isNotNull() & col("echeanjj").isNotNull(),
                make_date(lit(year_int), col("echeanmm"), col("echeanjj"))
            ).otherwise(lit(None).cast(DateType()))
        )

        return df

    def transform(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Apply AZEC business transformations (config-driven from JSON).

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

        # Step 1: Apply column configuration
        self.logger.step(1, "Applying column configuration")
        column_config = azec_config['column_selection']
        df = apply_column_config(df, column_config, vision, year_int, month_int)

        # Step 2: Add DIRCOM constant
        df = df.withColumn('dircom', lit(DIRCOM.AZEC))

        # Step 3: Apply business filters (SAS WHERE clause equivalent)
        self.logger.step(2, "Applying business filters")
        azec_filters = azec_config.get('business_filters', {}).get('filters', [])
        df = apply_business_filters(df, {'filters': azec_filters}, self.logger)

        # Step 4: Handle migration logic (vision > 202009)
        self.logger.step(3, "Handling AZEC migration")
        df = self._handle_migration(df, vision, azec_config)

        # Step 5: Update dates and policy states
        self.logger.step(4, "Updating dates and policy states")
        df = self._update_dates_and_states(df, dates, year_int, month_int)

        # Step 6: Calculate movements (AZEC-specific)
        self.logger.step(5, "Calculating movements")
        df = self._calculate_movements(df, dates, year_int, month_int)

        # Step 7: Calculate suspension periods (SAS L118-126)
        self.logger.step(6, "Calculating suspension periods (nbj_susp_ytd)")
        from utils.transformations.operations.business_logic import calculate_azec_suspension
        df = calculate_azec_suspension(df, dates)

        # Step 8: Calculate exposures
        self.logger.step(7, "Calculating exposures")
        df = self._calculate_exposures(df, dates, vision)
        
        # Step 8.5: Cleanup DT_DEB_EXPO/DT_FIN_EXPO when EXPO_YTD = 0 (SAS L374-380)
        self.logger.step(7.5, "Cleaning up exposure dates when EXPO_YTD = 0")
        df = df.withColumn('dt_deb_expo',
            when(col('expo_ytd') == 0, lit(None).cast(DateType())).otherwise(col('dt_deb_expo'))
        )
        df = df.withColumn('dt_fin_expo',
            when(col('expo_ytd') == 0, lit(None).cast(DateType())).otherwise(col('dt_fin_expo'))
        )


        # Step 10: Join capitals from CAPITXCU
        self.logger.step(8, "Joining capital data (CAPITXCU)")
        df = self._join_capitals(df, vision)

        # Step 10.5: Calculate AZEC coassurance (uses CODECOAS, not cdpolgp1 like AZ)
        # Based on SAS PTF_MVTS_AZEC_MACRO.sas L232-247
        self.logger.step(9, "Calculating AZEC coassurance")
        
        # COASS: Coassurance type (SAS L232-238)
        df = df.withColumn('coass',
            when(col('codecoas') == '0', lit('SANS COASSURANCE'))
            .when(col('codecoas') == 'A', lit('APERITION'))
            .when(col('codecoas') == 'C', lit('COASS. ACCEPTEE'))
            .when((col('typcontr') == 'A') & (col('codecoas') == 'R'), lit('REASS. ACCEPTEE'))
            .otherwise(lit('AUTRES'))
        )
        
        # TOP_COASS: Binary flag (SAS L247)
        df = df.withColumn('top_coass',
            when(col('codecoas') == '0', lit(0)).otherwise(lit(1))
        )
        
        # CDNATP: Reprocessing logic (SAS L240-245)
        df = df.withColumn('cdnatp',
            when((col('duree') == '00') & col('produit').isin(['CNR', 'CTR', 'DO0']), lit('C'))
            .when((col('duree') == '00') & col('produit').isin(['TRC']), lit('T'))
            .when(col('duree').isin(['01', '02', '03']), lit('R'))
            .otherwise(lit(''))
        )
        
        # TYPE_AFFAIRE: Alias for TYPCONTR (SAS L248)
        df = df.withColumn('type_affaire', col('typcontr'))

        # Step 11: NAF code enrichment from INCENDCU, MPACU, RCENTCU, RISTECCU
        self.logger.step(10, "Enriching NAF codes and PE/RD capitals (INCENDCU)")
        df = self._enrich_naf_codes(df, vision)

        # Step 12: Product formulas and CA from CONSTRCU and MULPROCU
        self.logger.step(10.5, "Enriching formulas and CA (CONSTRCU, MULPROCU)")
        df = self._enrich_formules_and_ca(df, vision)

        # Step 13: Enrichissement segmentation AZEC
        self.logger.step(11, "Adding AZEC segmentation (SEGMENT)")
        df = self._enrich_segmentation(df)

        # Step 13.5: Enrich REGION from PTGST_STATIC (SAS REF_segmentation_azec.sas L59-75)
        self.logger.step(11.5, "Enriching REGION from management points (PTGST_STATIC)")
        df = self._enrich_region(df)

        # Step 14: Calculate primes (optimized: single select instead of 5 withColumns)
        self.logger.step(11, "Calculating primes")
        primeto_expr = col("prime") * col("partcie")
        primecua_expr = (col("prime") * col("partbrut") / 100.0) + col("cpcua")  # SAS L217
        
        # Cotis_100: SAS L229 - conditional logic
        # CASE WHEN PARTBRUT = 0 THEN PRIME ELSE (PRIME + (CPCUA/PARTCIE)) END
        cotis_100_expr = when(col("partbrut") == 0, col("prime")) \
                        .otherwise(col("prime") + (col("cpcua") / col("partcie")))

        # FIXED: Add CSSSEG filtering for PRIMES_AFN and PRIMES_RES
        cssseg_filter = (col("cssseg") != "5") if "cssseg" in df.columns else lit(True)

        df = df.select(
            "*",
            primeto_expr.alias("primeto"),
            primecua_expr.alias("primecua"),  # SAS L217
            cotis_100_expr.alias("cotis_100"),  # SAS L229 - FIXED
            when(col("nbptf") == 1, primeto_expr).otherwise(lit(0)).alias("primes_ptf"),
            when((col("nbafn") == 1) & cssseg_filter, primecua_expr).otherwise(lit(0)).alias("primes_afn"),  # SAS L223
            when((col("nbres") == 1) & cssseg_filter, primecua_expr).otherwise(lit(0)).alias("primes_res")   # SAS L224
        )

        # Step 15: NBRES adjustments (AZEC-specific)
        df = self._adjust_nbres(df)

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
        needed_cols = set(["police", "smp_sre", "brch_rea"])
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

        # Aggregate by police
        agg_columns = [mapping['target'] for mapping in AZEC_CAPITAL_MAPPING]
        df_cap_agg = df_cap.groupBy("police").agg(
            *[spark_sum(col_name).alias(col_name) for col_name in agg_columns]
        )

        # Calculate global totals
        df_cap_agg = df_cap_agg.withColumn("lci_100", col("lci_pe_100") + col("lci_dd_100"))
        df_cap_agg = df_cap_agg.withColumn("lci_cie", col("lci_pe_cie") + col("lci_dd_cie"))
        df_cap_agg = df_cap_agg.withColumn("smp_100", col("smp_pe_100") + col("smp_dd_100"))
        df_cap_agg = df_cap_agg.withColumn("smp_cie", col("smp_pe_cie") + col("smp_dd_cie"))

        # Left join
        df = df.alias("a").join(
            df_cap_agg.alias("b"),
            col("a.police") == col("b.police"),
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

    def _apply_business_filters(self, df: DataFrame) -> DataFrame:
        """
        Apply business filters to AZEC data.
        Filters are now loaded from azec_transformations.json for easier maintenance.
        
        Based on SAS: PTF_MVTS_AZEC_MACRO.sas L37-39, L81-85
        
        Args:
            df: AZEC DataFrame
        
        Returns:
            Filtered DataFrame
        """
        # Read exclusion lists from config (SAS L37)
        business_filters = self.transformations.get('business_filters', {})
        excluded_intermed = business_filters.get('excluded_intermed', {}).get('values', [])
        excluded_police = business_filters.get('excluded_police', {}).get('values', [])
        
        self.logger.info(f"Excluding {len(excluded_intermed)} intermediaries, {len(excluded_police)} policies")
        
        # Apply filters (SAS L37-39)
        df = df.filter(~col('intermed').isin(excluded_intermed))
        df = df.filter(~col('police').isin(excluded_police))
        
        # Standard filters (SAS L81-85)
        df = df.filter(
            ~((col('duree') == '00') & ~col('produit').isin(['DO0', 'TRC', 'CTR', 'CNR']))
        )
        df = df.filter(col('datfin') != col('effetpol'))
        df = df.filter(
            (col('gestsit') != 'MIGRAZ') | 
            ((col('gestsit') == 'MIGRAZ') & (col('etatpol') == 'R'))
        )
        
        self.logger.info("✓ AZEC business filters applied")
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
        Enrich AZEC data with REGION and P_Num from PTGST_STATIC.
        
        Args:
            df: AZEC DataFrame with poingest column
        
        Returns:
            DataFrame enriched with region and p_num columns
        """
        reader = get_bronze_reader(self)
        
        try:
            df_ptgst = reader.read_file_group('ptgst_static', vision='ref')
            
            if df_ptgst is not None:
                # Select needed columns and prepare for join
                # Note: BronzeReader lowercases all columns
                df_ptgst_select = df_ptgst.select(
                    col("ptgst"),
                    col("region"),
                    col("p_num") if "p_num" in df_ptgst.columns else lit(None).cast(StringType()).alias("p_num")
                ).dropDuplicates(["ptgst"])
                
                # Left join on poingest = ptgst
                # Note: poingest in POLIC_CU maps to PTGST in reference table
                df = df.alias("a").join(
                    df_ptgst_select.alias("p"),
                    col("a.poingest") == col("p.ptgst"),
                    how="left"
                ).select(
                    "a.*",
                    # If no match found, set REGION = 'Autres' (SAS L70)
                    when(col("p.region").isNull(), lit("Autres"))
                    .otherwise(col("p.region")).alias("region"),
                    col("p.p_num")
                )
                
                self.logger.info("PTGST_STATIC joined successfully - REGION and P_Num added")
            else:
                self.logger.warning("PTGST_STATIC not available - setting REGION to 'Autres'")
                from utils.processor_helpers import add_null_columns
                df = df.withColumn('region', lit('Autres'))
                df = add_null_columns(df, {'p_num': StringType})

        except Exception as e:
            self.logger.warning(f"PTGST_STATIC enrichment failed: {e} - setting REGION to 'Autres'")
            from utils.processor_helpers import add_null_columns
            df = df.withColumn('region', lit('Autres'))
            df = add_null_columns(df, {'p_num': StringType})
        
        return df

    def _enrich_naf_codes(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrich AZEC data with NAF codes and capitals from INCENDCU, MPACU, RCENTCU, RISTECCU.
        
        Args:
            df: AZEC DataFrame
            vision: Vision in YYYYMM format
        
        Returns:
            DataFrame enriched with NAF codes and capitals
        """
        reader = get_bronze_reader(self)
        
        # Initialize enrichment columns upfront
        enrichment_cols = {
            'cdnaf': StringType,
            'cdtre': StringType,
            'perte_exp': DoubleType,
            'risque_direct': DoubleType,
            'nbj_acti': IntegerType
        }
        
        from utils.processor_helpers import add_null_columns
        existing_cols = set(df.columns)
        cols_to_add = {k: v for k, v in enrichment_cols.items() if k not in existing_cols}
        if cols_to_add:
            df = add_null_columns(df, cols_to_add)

        # 1. INCENDCU: NAF codes and PE/RD capitals
        try:
            df_incend = reader.read_file_group('incendcu_azec', vision)
            if df_incend is not None:
                # Select needed columns
                df_incend_select = df_incend.select(
                    'police',
                    col('cod_naf').alias('cod_naf'),
                    col('cod_tre').alias('cod_tre'),
                    col('mt_baspe').alias('mt_baspe'),
                    col('mt_basdi').alias('mt_basdi')
                ).dropDuplicates(['police'])

                # Left join on police
                df = df.join(df_incend_select, on='police', how='left')

                # Update columns with coalesce (use INCENDCU if main is null)
                df = df.withColumn('cdnaf', coalesce(col('cdnaf'), col('cod_naf')))
                df = df.withColumn('cdtre', coalesce(col('cdtre'), col('cod_tre')))
                df = df.withColumn('perte_exp', coalesce(col('perte_exp'), col('mt_baspe'), lit(0)))
                df = df.withColumn('risque_direct', coalesce(col('risque_direct'), col('mt_basdi'), lit(0)))

                # Drop temporary join columns
                df = df.drop('cod_naf', 'cod_tre', 'mt_baspe', 'mt_basdi')

                self.logger.info("INCENDCU joined successfully - NAF codes and PE/RD capitals enriched")
        except Exception as e:
            self.logger.warning(f"INCENDCU not available: {e}. Using default values.")

        # 2. MPACU: Additional NAF codes (SAS L277: FULL JOIN with MPACU.MPACU)
        try:
            df_mpacu = reader.read_file_group('mpacu_azec', vision)
            if df_mpacu is not None:  # OPTIMIZED: Removed count() check
                # Select needed columns for NAF fallback
                df_mpacu_select = df_mpacu.select(
                    'police',
                    col('cod_naf').alias('cod_naf_mpacu')
                ).dropDuplicates(['police'])
                
                # Left join on police
                df = df.join(df_mpacu_select, on='police', how='left')
                
                # Coalesce CDNAF with MPACU fallback (SAS L274-275)
                df = df.withColumn(
                    'cdnaf',
                    coalesce(col('cdnaf'), col('cod_naf_mpacu'))
                ).drop('cod_naf_mpacu')
                
                self.logger.info("MPACU NAF codes joined successfully")
        except Exception as e:
            self.logger.debug(f"MPACU not available: {e}")

        # 3. RCENTCU: Formulas for RC policies (SAS L309-312)
        try:
            df_rcentcu = reader.read_file_group('rcentcu_azec', vision)
            if df_rcentcu is not None:  # OPTIMIZED: Removed count() check
                # Select formula columns (SAS L310)
                df_rcentcu_select = df_rcentcu.select(
                    'police',
                    col('formule').alias('formule_rc'),
                    col('formule2').alias('formule2_rc'),
                    col('formule3').alias('formule3_rc'),
                    col('formule4').alias('formule4_rc'),
                    col('cod_naf').alias('cod_naf_rcentcu')
                ).dropDuplicates(['police'])
                
                # Left join on police
                df = df.join(df_rcentcu_select, on='police', how='left')
                
                # Coalesce formulas and NAF (prioritize existing if not null)
                df = df.withColumn('formule', coalesce(col('formule'), col('formule_rc')))
                df = df.withColumn('formule2', coalesce(col('formule2'), col('formule2_rc')))
                df = df.withColumn('formule3', coalesce(col('formule3'), col('formule3_rc')))
                df = df.withColumn('formule4', coalesce(col('formule4'), col('formule4_rc')))
                df = df.withColumn('cdnaf', coalesce(col('cdnaf'), col('cod_naf_rcentcu')))
                
                # Drop temporary columns
                df = df.drop('formule_rc', 'formule2_rc', 'formule3_rc', 'formule4_rc', 'cod_naf_rcentcu')
                
                self.logger.info("RCENTCU formulas joined successfully")
        except Exception as e:
            self.logger.debug(f"RCENTCU not available: {e}")

        # 4. RISTECCU: Technical risk formulas (SAS L312)
        try:
            df_risteccu = reader.read_file_group('risteccu_azec', vision)
            if df_risteccu is not None:  # OPTIMIZED: Removed count() check
                # Select formula columns
                df_risteccu_select = df_risteccu.select(
                    'police',
                    col('formule').alias('formule_ris'),
                    col('formule2').alias('formule2_ris'),
                    col('formule3').alias('formule3_ris'),
                    col('formule4').alias('formule4_ris'),
                    col('cod_naf').alias('cod_naf_risteccu')
                ).dropDuplicates(['police'])
                
                # Left join on police
                df = df.join(df_risteccu_select, on='police', how='left')
                
                # Coalesce formulas and NAF with RISTECCU as fallback
                df = df.withColumn('formule', coalesce(col('formule'), col('formule_ris')))
                df = df.withColumn('formule2', coalesce(col('formule2'), col('formule2_ris')))
                df = df.withColumn('formule3', coalesce(col('formule3'), col('formule3_ris')))
                df = df.withColumn('formule4', coalesce(col('formule4'), col('formule4_ris')))
                df = df.withColumn('cdnaf', coalesce(col('cdnaf'), col('cod_naf_risteccu')))
                
                # Drop temporary columns
                df = df.drop('formule_ris', 'formule2_ris', 'formule3_ris', 'formule4_ris', 'cod_naf_risteccu')
                
                self.logger.info("RISTECCU formulas joined successfully")
        except Exception as e:
            self.logger.debug(f"RISTECCU not available: {e}")

        return df


    def _enrich_formules_and_ca(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrich AZEC data with product formulas from CONSTRCU and CA from MULPROCU.
        
        Args:
            df: AZEC DataFrame
            vision: Vision in YYYYMM format
        
        Returns:
            DataFrame enriched with formulas and CA
        """
        reader = get_bronze_reader(self)

        # 1. CONSTRCU: Product formulas and construction site details
        try:
            df_CONSTRCU = reader.read_file_group('constrcu_azec', vision)
            if df_CONSTRCU is not None:  # OPTIMIZED: Removed count() check
                # Select formula columns
                CONSTRCU_cols = ['police']

                # Formula columns
                for col_name in ['formule', 'formule2', 'formule3', 'formule4']:
                    if col_name in df_CONSTRCU.columns:
                        CONSTRCU_cols.append(col_name)

                # Construction site details
                for col_name in ['datouvch', 'ldestloc', 'datrecep', 'datnot',
                                'loclieu', 'adrchant', 'vilchant', 'codpchant']:
                    if col_name in df_CONSTRCU.columns:
                        CONSTRCU_cols.append(col_name)

                df_CONSTRCU_select = df_CONSTRCU.select(*CONSTRCU_cols)

                # Left join on police
                df = df.join(df_CONSTRCU_select, on='police', how='left')

                self.logger.info("CONSTRCU joined successfully - formulas and construction site details added")
        except Exception as e:
            self.logger.warning(f"CONSTRCU not available: {e}. Skipping formula enrichment.")
            # Initialize formula columns if not present
            from utils.processor_helpers import add_null_columns
            existing_cols = set(df.columns)
            null_cols_needed = {
                col_name: StringType
                for col_name in ['formule', 'formule2', 'formule3', 'formule4']
                if col_name not in existing_cols
            }
            if null_cols_needed:
                df = add_null_columns(df, null_cols_needed)

        # 2. MULPROCU: Turnover (CA) data
        try:
            df_mulprocu = reader.read_file_group('mulprocu_azec', vision)
            if df_mulprocu is not None:  # OPTIMIZED: Removed count() check
                # Select CA column
                # Aggregate CHIFFAFF to get MTCA (SAS L340: SUM(CHIFFAFF) AS MTCA)
                from pyspark.sql.functions import sum as spark_sum
                df_mulprocu_agg = df_mulprocu.groupBy('police').agg(
                    spark_sum('chiffaff').alias('mtca_mulpro')
                )

                # Left join on police
                df = df.join(df_mulprocu_agg, on='police', how='left')

                # Priority logic (SAS L344-355):
                # If MTCA is null and specific formulas, use MTCA_MULPRO
                # For now, use simple coalesce (can be refined based on formula conditions)
                if 'mtca' in df.columns:
                    df = df.withColumn(
                        'mtca',
                        when(
                            col('mtca').isNull() & col('mtca_mulpro').isNotNull(),
                            col('mtca_mulpro')
                        ).otherwise(col('mtca'))
                    )
                else:
                    df = df.withColumn('mtca', coalesce(col('mtca_mulpro'), lit(0)))

                # Drop temporary column
                df = df.drop('mtca_mulpro')

                self.logger.info("MULPROCU joined successfully - CA data enriched")
        except Exception as e:
            self.logger.warning(f"MULPROCU not available: {e}. Skipping CA enrichment.")
            # Initialize mtca if not present
            if 'mtca' not in df.columns:
                df = df.withColumn('mtca', lit(0).cast(DoubleType()))

        return df
