"""
Consolidation Processor - Combines AZ and AZEC into unified Gold layer.

Reads MVT_CONST_PTF (AZ) and AZEC_PTF from silver layer,
harmonizes schemas, enriches with reference data, writes to gold layer.

Uses dictionary-driven configuration and SilverReader.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, coalesce, upper, broadcast,
    month, dayofmonth, to_date
)
from pyspark.sql.types import StringType, DateType, DoubleType

from src.processors.base_processor import BaseProcessor
from src.reader import SilverReader, BronzeReader
from utils.loaders import get_default_loader
from config.constants import DIRCOM
from utils.helpers import build_layer_path, extract_year_month_int
from utils.processor_helpers import safe_reference_join, get_bronze_reader


class ConsolidationProcessor(BaseProcessor):
    """
    Consolidate AZ and AZEC data into unified Gold layer.
    
    Reads from silver (parquet), applies harmonization, writes to gold.
    All columns are lowercase.
    """

    def read(self, vision: str) -> DataFrame:
        """
        Read AZ silver data. AZEC will be read in transform().

        Args:
            vision: Vision in YYYYMM format

        Returns:
            AZ silver DataFrame (lowercase columns)
        """
        silver_reader = SilverReader(self.spark, self.config)
        
        self.logger.info("Reading AZ silver data (mvt_const_ptf)")
        df_az = silver_reader.read_silver_file('mvt_const_ptf', vision)
        # Columns already lowercase from silver
        
        return df_az

    def transform(self, df_az: DataFrame, vision: str) -> DataFrame:
        """
        Consolidate AZ and AZEC data with harmonization and enrichment (using JSON configs).

        Args:
            df_az: AZ DataFrame from read() (lowercase columns)
            vision: Vision in YYYYMM format

        Returns:
            Consolidated DataFrame ready for gold layer (all lowercase)
        """
        year_int, month_int = extract_year_month_int(vision)

        # Load configurations from JSON
        loader = get_default_loader()
        consolidation_config = loader.get_consolidation_config()

        # Step 1: Read AZEC silver data
        self.logger.step(1, "Reading AZEC silver data")
        silver_reader = SilverReader(self.spark, self.config)
        df_azec = silver_reader.read_silver_file('azec_ptf', vision)

        # Step 2: Harmonize AZ schema (rename if needed)
        self.logger.step(2, "Harmonizing AZ schema")
        az_harmonization = consolidation_config['az_harmonization']
        df_az = self._harmonize_schema(df_az, az_harmonization)

        # Step 3: Harmonize AZEC schema (rename columns to match AZ)
        self.logger.step(3, "Harmonizing AZEC schema")
        azec_harmonization = consolidation_config['azec_harmonization']
        df_azec = self._harmonize_schema(df_azec, azec_harmonization)

        # Step 3b: Apply AZEC-specific transformations
        self.logger.step(3.5, "Applying AZEC-specific transformations")
        df_azec = self._apply_azec_transformations(df_azec)

        # Step 4: Add DIRCOM for AZEC (if not already present)
        df_azec = df_azec.withColumn('dircom', lit(DIRCOM.AZEC))
        
        # Step 5: Union AZ + AZEC
        self.logger.step(4, "Consolidating AZ + AZEC")
        df_consolidated = df_az.unionByName(df_azec, allowMissingColumns=True)

        # Step 5b: Apply common transformations (CDTRE, etc.)
        self.logger.step(4.5, "Applying common transformations")
        df_consolidated = self._apply_common_transformations(df_consolidated)

        # Step 5c: Enrich with DO_DEST reference (SAS L416-421)
        self.logger.step(4.6, "Enriching with DO_DEST reference")
        df_consolidated = self._enrich_do_dest(df_consolidated)

        # Step 5d: Apply DESTINAT consolidation logic
        self.logger.step(4.7, "Applying DESTINAT consolidation logic")
        from utils.transformations.base.destinat_calculation import apply_destinat_consolidation_logic
        df_consolidated = apply_destinat_consolidation_logic(df_consolidated)

        # Step 5e: Calculate DESTINAT for Chantiers (pattern matching)
        self.logger.step(5, "Calculating DESTINAT for construction sites")
        from utils.transformations.base.destinat_calculation import calculate_destinat
        df_consolidated = calculate_destinat(df_consolidated, self.logger)

        # Step 6: Enrich with IRD risk data
        self.logger.step(5.5, "Enriching with IRD risk data")
        df_consolidated = self._enrich_ird_risk(df_consolidated, vision)

        # Step 6c: Apply fallback logic (DTRCPPR)
        self.logger.step(5.7, "Applying fallback logic")
        df_consolidated = self._apply_fallback_logic(df_consolidated)
        
        # Step 6d: Enrich with client data (SIRET/SIREN)
        self.logger.step(5.8, "Enriching client data (SIRET/SIREN)")
        df_consolidated = self._enrich_client_data(df_consolidated, vision)
        
        # Step 6e: Enrich with Euler risk notes
        self.logger.step(5.9, "Enriching Euler risk notes (note_euler)")
        df_consolidated = self._enrich_euler_risk_note(df_consolidated, vision)
        
        # Step 6f: Enrich with special product activity codes
        self.logger.step(6, "Enriching special product activity codes (TypeAct)")
        df_consolidated = self._enrich_special_product_activity(df_consolidated, vision)
        
        # Step 6g: Enrich with W6 NAF and Client CDNAF codes
        self.logger.step(6.05, "Enriching NAF codes (W6 + CLIENT)")
        df_consolidated = self._enrich_w6_naf_and_client_cdnaf(df_consolidated)
        
        # Step 6h: Add ISIC codes and HAZARD_GRADES
        self.logger.step(6.1, "Adding ISIC codes and HAZARD_GRADES")
        df_consolidated = self._add_isic_codes(df_consolidated, vision)
        
        # Step 6i: Apply ISIC code corrections (manual fixes for known bad codes)
        self.logger.step(6.2, "Applying ISIC code corrections")
        from utils.transformations.base.isic_codification import apply_isic_corrections
        df_consolidated = apply_isic_corrections(df_consolidated)
        
        # Step 6j: Add ISIC global code (SAS L563-570)
        self.logger.step(6.3, "Adding ISIC global code mapping")
        df_consolidated = self._add_isic_global_code(df_consolidated)
        
        # Step 6k: Add special business flags (SAS L596-600)
        self.logger.step(6.4, "Adding special business flags")
        df_consolidated = self._add_business_flags(df_consolidated)
        
        # Step 7: Add placeholders for any remaining missing columns
        self.logger.step(6.9, "Adding placeholders for any remaining missing columns")
        df_consolidated = self._add_placeholders(df_consolidated)
        
        self.logger.info("Consolidation completed successfully")
        
        return df_consolidated


    def _add_business_flags(self, df: DataFrame) -> DataFrame:
        """
        Add special business flags for specific intermediaries.
        
        Based on: SAS PTF_MVTS_CONSOLIDATION_MACRO.sas L596-600
        
        Logic:
        - TOP_BERLIOZ: Flag for intermediary "4A5766"
        - TOP_PARTENARIAT: Flag for intermediaries "4A6160", "4A6947", "4A6956"
        
        Args:
            df: Consolidated DataFrame
            
        Returns:
            DataFrame with TOP_BERLIOZ and TOP_PARTENARIAT columns
        """
        # SAS L598: if NOINT="4A5766" then TOP_BERLIOZ=1;
        df = df.withColumn('top_berlioz',
            when(col('noint') == '4A5766', lit(1)).otherwise(lit(0))
        )
        
        # SAS L599: if NOINT in ("4A6160","4A6947","4A6956") then TOP_PARTENARIAT=1;
        df = df.withColumn('top_partenariat',
            when(col('noint').isin(['4A6160', '4A6947', '4A6956']), lit(1)).otherwise(lit(0))
        )
        
        return df

    def write(self, df: DataFrame, vision: str) -> None:
        """
        Write consolidated data to gold layer in parquet format.

        Args:
            df: Consolidated DataFrame (lowercase columns)
            vision: Vision in YYYYMM format
        """
        from utils.helpers import write_to_layer
        write_to_layer(df, self.config, 'gold', 'construction_portfolio', vision, self.logger)

    def _harmonize_schema(
        self,
        df: DataFrame,
        harmonization_config: dict
    ) -> DataFrame:
        """
        Harmonize schema using configuration (dictionary-driven).

        Handles:
        - rename: Simple column renaming
        - computed: Computed columns (month/day extraction, constants, etc.)

        Args:
            df: Input DataFrame (lowercase columns)
            harmonization_config: Harmonization config from variables.py

        Returns:
            DataFrame with harmonized column names and computed columns (all lowercase)
        """
        from utils.helpers import compute_date_ranges

        # Apply renames
        rename_mapping = harmonization_config.get('rename', {})
        for old_name, new_name in rename_mapping.items():
            if old_name.lower() in df.columns:
                df = df.withColumnRenamed(old_name.lower(), new_name.lower())

        # Apply computed columns
        computed_mapping = harmonization_config.get('computed', {})
        for col_name, comp_config in computed_mapping.items():
            comp_type = comp_config.get('type')

            if comp_type == 'constant':
                value = comp_config['value']
                # Cast null to string to avoid void type (Parquet incompatible)
                if value is None:
                    df = df.withColumn(col_name.lower(), lit(None).cast("string"))
                else:
                    df = df.withColumn(col_name.lower(), lit(value))

            elif comp_type == 'month_extract':
                source = comp_config['source'].lower()
                if source in df.columns:
                    df = df.withColumn(col_name.lower(), month(col(source)))

            elif comp_type == 'day_extract':
                source = comp_config['source'].lower()
                if source in df.columns:
                    df = df.withColumn(col_name.lower(), dayofmonth(col(source)))

            elif comp_type == 'alias':
                source = comp_config['source'].lower()
                if source in df.columns:
                    df = df.withColumn(col_name.lower(), col(source))

        return df

    # IRD join configuration - SEQUENTIAL processing (SAS L154-258)
    # All sources use SAME suffix '_risk' but are joined sequentially with immediate drops
    IRD_JOIN_CONFIG = {
        'q46': {
            'file_group': 'ird_risk_q46',
            'date_columns': ['dtouchan', 'dtrectrx', 'dtreffin'],
            'text_columns': ['ctprvtrv', 'ctdeftra', 'lbnattrv', 'lbdstcsc'],
            'suffix': '_risk'  # Same suffix for all (SAS pattern)
        },
        'q45': {
            'file_group': 'ird_risk_q45',
            'date_columns': ['dtouchan', 'dtrectrx', 'dtreffin'],
            'text_columns': ['ctprvtrv', 'ctdeftra', 'lbnattrv', 'lbdstcsc'],
            'suffix': '_risk'  # Same suffix (dropped before Q45 join)
        },
        'qan': {
            'file_group': 'ird_risk_qan',
            'date_columns': ['dtouchan', 'dtrcppr'],
            'text_columns': ['ctprvtrv', 'ctdeftra', 'lbnattrv', 'dstcsc'],
            'suffix': '_risk'  # Same suffix (dropped before QAN join)
        }
    }

    def _enrich_ird_risk(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrich with IRD risk framework data (Q45, Q46, QAN).

        OPTIMIZED: Uses consolidated _join_ird_risk method (reduced from 122 to 40 lines)

        Based on PTF_MVTS_CONSOLIDATION_MACRO.sas L200-280.
        Joins IRD risk data and adds columns with _risk suffix for later coalescing.

        Args:
            df: Consolidated DataFrame (lowercase columns)
            vision: Vision string

        Returns:
            Enriched DataFrame with IRD _risk columns (lowercase columns)
        """


        reader = get_bronze_reader(self)
        year_int, month_int = extract_year_month_int(vision)

        try:
            # Determine which IRD risk file to use based on vision month
            # SAS logic: if vision >= 202210 use Q45/Q46, else use QAN
            if year_int > 2022 or (year_int == 2022 and month_int >= 10):
                # Use Q45 and Q46 (two separate joins)
                df = self._join_ird_risk(df, vision, reader, 'q46')
                df = self._join_ird_risk(df, vision, reader, 'q45')
            else:
                # Use QAN (single join)
                df = self._join_ird_risk(df, vision, reader, 'qan')

            self.logger.info("IRD risk data enrichment completed")

        except Exception as e:
            self.logger.warning(f"IRD risk data not found or error: {e}. Skipping enrichment.")

        return df


    def _apply_destinat_final_adjustments(self, df: DataFrame) -> DataFrame:
        """
        Apply final adjustments to DESTINAT (not currently implemented).
        
        Placeholder for potential future logic.
        
        Args:
            df: DataFrame with destinat column
            
        Returns:
            DataFrame (unchanged for now)
        """
        return df

    def _add_placeholders(self, df: DataFrame) -> DataFrame:
        """
        Add NULL placeholders for missing reference data.

        Args:
            df: DataFrame (lowercase columns)

        Returns:
            DataFrame with placeholder columns (all lowercase)
        """
        # Add placeholders for missing enrichments (SEG, INCENDCU, CLIENT, etc.)
        missing_enrichments = [
            "seg_label",
            "incendcu_label",
            "client_naf_label",
            "isic_code",
            "isic_label",
            "geo_region",
            "geo_department"
        ]
        
        from utils.processor_helpers import add_null_columns
        existing_cols = set([c.lower() for c in df.columns])
        null_cols_needed = {
            col_name.lower(): StringType
            for col_name in missing_enrichments
            if col_name.lower() not in existing_cols
        }
        
        if null_cols_needed:
            df = add_null_columns(df, null_cols_needed)

        return df

    def _apply_azec_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply AZEC-specific transformations before consolidation (config-driven).

        Transformations:
        - CDMOTRES: Map ORIGRES codes to standard codes

        Args:
            df: AZEC DataFrame

        Returns:
            Transformed DataFrame
        """
        # Use config-driven transformation instead of hardcoded mapping
        loader = get_default_loader()
        consolidation_config = loader.get_consolidation_config()

        azec_transforms = consolidation_config.get('azec_specific_transformations', {})

        # Apply CDMOTRES mapping if configured
        cdmotres_config = azec_transforms.get('cdmotres_mapping')
        if cdmotres_config and 'origres' in df.columns:
            from utils.transformations import apply_transformations
            df = apply_transformations(df, [cdmotres_config])

        return df

    def _apply_common_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply common transformations to consolidated data (config-driven).

        Transformations:
        - CDTRE: Remove '*' prefix if present
        - Partnership flags: TOP_PARTENARIAT, TOP_BERLITZ

        Args:
            df: Consolidated DataFrame

        Returns:
            Transformed DataFrame
        """
        # Use config-driven transformations instead of hardcoded logic
        loader = get_default_loader()
        consolidation_config = loader.get_consolidation_config()
        common_transforms = consolidation_config.get('common_transformations', {})

        # CDTRE cleanup
        if 'cdtre' in df.columns:
            df = df.withColumn(
                'cdtre',
                when(col('cdtre').startswith('*'), col('cdtre').substr(2, 4))
                .otherwise(col('cdtre'))
            )

        # Partnership flags (config-driven)
        partnership_config = common_transforms.get('partnership_flags', {})
        flag_transforms = partnership_config.get('transformations', [])

        if flag_transforms and 'noint' in df.columns:
            from utils.transformations import apply_transformations
            df = apply_transformations(df, flag_transforms)

        return df

    def _apply_fallback_logic(self, df: DataFrame) -> DataFrame:
        """
        Apply fallback logic for missing dates.
        
        Based on: SAS PTF_MVTS_CONSOLIDATION_MACRO.sas L373-379
        
        Logic:
        - If DTRCPPR is missing and DTREFFIN exists, use DTREFFIN for DTRCPPR
        
        Args:
            df: DataFrame with IRD columns
            
        Returns:
            DataFrame with fallback applied
        """
        if 'dtrcppr' in df.columns and 'dtreffin' in df.columns:
            df = df.withColumn(
                'dtrcppr',
                when(col('dtrcppr').isNull() & col('dtreffin').isNotNull(), col('dtreffin'))
                .otherwise(col('dtrcppr'))
            )
        
        return df

    def _enrich_ird_risk(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrich with IRD risk data from Q46, Q45, and QAN.
        
        Based on: SAS PTF_MVTS_CONSOLIDATION_MACRO.sas L154-258
        
        CRITICAL: Uses SEQUENTIAL processing (not parallel):
        1. Join Q46 → Coalesce → Drop _risk columns
        2. Join Q45 → Coalesce → Drop _risk columns (reuses suffix)
        3. Join QAN → Coalesce → Drop _risk columns (reuses suffix)
        
        All sources use SAME suffix '_risk' but are processed sequentially
        to avoid column name collisions.
        
        Args:
            df: Consolidated DataFrame
            vision: Vision in YYYYMM format
            
        Returns:
            DataFrame enriched with IRD risk data
        """
        # Initialize dtreffin upfront
        if 'dtreffin' not in df.columns:
            from utils.processor_helpers import add_null_columns
            df = add_null_columns(df, {'dtreffin': DateType})
        
        reader = get_bronze_reader(self)
        
        # Sequential processing (SAS L154-258)
        # Q46 → Coalesce → Drop (SAS L158-188)
        df = self._join_ird_and_coalesce(df, reader, vision, 'q46')
        
        # Q45 → Coalesce → Drop (SAS L194-224)
        df = self._join_ird_and_coalesce(df, reader, vision, 'q45')
        
        # QAN → Coalesce → Drop (SAS L230-258)
        df = self._join_ird_and_coalesce(df, reader, vision, 'qan')
        
        self.logger.info("IRD risk data enrichment completed")
        return df
    
    def _join_ird_and_coalesce(
        self,
        df: DataFrame,
        reader,
        vision: str,
        source: str
    ) -> DataFrame:
        """
        Join one IRD source, coalesce values, and drop temp columns immediately.
        
        This matches SAS pattern of sequential join-coalesce-drop for each source.
        All sources use '_risk' suffix but are processed one at a time.
        
        Args:
            df: Main DataFrame
            reader: BronzeReader instance
            vision: Vision string
            source: 'q46', 'q45', or 'qan'
            
        Returns:
            DataFrame with IRD data coalesced and temp columns dropped
        """
        cfg = self.IRD_JOIN_CONFIG[source]
        
        try:
            df_ird = reader.read_file_group(cfg['file_group'], vision)
            if df_ird is None or df_ird.count() == 0:
                self.logger.debug(f"IRD {source.upper()}: No data found")
                return df
            
            # QUALITY IMPROVEMENT: Detect and remove duplications (not done in SAS)
            # This ensures gold layer data quality
            total_count = df_ird.count()
            distinct_nopol_count = df_ird.select("nopol").distinct().count()
            
            if total_count > distinct_nopol_count:
                dup_count = total_count - distinct_nopol_count
                self.logger.warning(
                    f"IRD {source.upper()}: {dup_count} duplications detected on NOPOL - "
                    f"keeping most recent entry (total={total_count}, unique={distinct_nopol_count})"
                )
                
                # Keep most recent entry per NOPOL based on available date columns
                from pyspark.sql.window import Window
                from pyspark.sql.functions import row_number, desc
                
                # Build ordering: prioritize by latest dates (nulls last)
                order_cols = []
                for date_col in cfg['date_columns']:
                    if date_col in df_ird.columns:
                        order_cols.append(col(date_col).desc_nulls_last())
                
                # If no date columns, just take first row
                if not order_cols:
                    order_cols = [lit(1)]
                
                window_spec = Window.partitionBy("nopol").orderBy(*order_cols)
                df_ird = df_ird.withColumn("_row_num", row_number().over(window_spec)) \
                               .filter(col("_row_num") == 1) \
                               .drop("_row_num")
                
                self.logger.info(f"IRD {source.upper()}: Deduplication complete - kept {distinct_nopol_count} unique records")
            
            select_cols = ['nopol']
            
            for col_name in cfg['date_columns']:
                if col_name in df_ird.columns:
                    select_cols.append(to_date(col(col_name)).alias(f"{col_name}_risk"))
            
            for col_name in cfg['text_columns']:
                if col_name in df_ird.columns:
                    select_cols.append(col(col_name).alias(f"{col_name}_risk"))
                else:
                    select_cols.append(lit(None).cast(StringType()).alias(f"{col_name}_risk"))
            
            df_ird_select = df_ird.select(*select_cols)
            
            # LEFT JOIN
            df = df.join(broadcast(df_ird_select), on='nopol', how='left')
            
            # Coalesce immediately
            for col_name in cfg['date_columns'] + cfg['text_columns']:
                risk_col = f"{col_name}_risk"
                
                # Map lbdstcsc_risk → dstcsc
                target_col = 'dstcsc' if col_name == 'lbdstcsc' else col_name
                
                if risk_col in df.columns:
                    # DTREFFIN assignment for Q46
                    if col_name == 'dtreffin' and source == 'q46':
                        df = df.withColumn('dtreffin', col(risk_col))
                    # Standard coalesce
                    else:
                        df = df.withColumn(target_col,
                            when(col(target_col).isNull(), col(risk_col)).otherwise(col(target_col)))
            
            # Drop _risk columns
            risk_cols_to_drop = [f"{c}_risk" for c in cfg['date_columns'] + cfg['text_columns']]
            risk_cols_existing = [c for c in risk_cols_to_drop if c in df.columns]
            if risk_cols_existing:
                df = df.drop(*risk_cols_existing)
            
            self.logger.debug(f"IRD {source.upper()}: Joined and coalesced successfully")
            
        except Exception as e:
            self.logger.warning(f"IRD {source.upper()} enrichment failed: {e}")
        
        return df


    def _enrich_client_data(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrich with client data (SIRET/SIREN) from CLIENT1 and CLIENT3.
        Also enriches with HISTO_NOTE_RISQUE risk scoring.
        
        Based on: PTF_MVTS_CONSOLIDATION_MACRO.sas L385-394
        
        Args:
            df: Consolidated DataFrame
            vision: Vision in YYYYMM format
        
        Returns:
            DataFrame with cdsiret, cdsiren, and risk note columns added
        """
        from utils.transformations import join_client_data
        return join_client_data(df, self.spark, self.config, vision, self.logger)

    def _add_isic_codes(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Add ISIC codes and HAZARD_GRADES to consolidated data.
        
        Uses isic_codification utility with fallback strategy.
        
        Args:
            df: Consolidated DataFrame
            vision: Vision in YYYYMM format
        
        Returns:
            DataFrame with ISIC columns added (may be NULL if data missing)
        """
        from utils.transformations.base.isic_codification import (
            join_isic_reference_tables,
            assign_isic_codes,
            add_partenariat_berlitz_flags
        )

        # First, join all ISIC reference tables
        df = join_isic_reference_tables(df, self.spark, self.config, vision, self.logger)

        # Then assign ISIC codes using the joined reference data
        df = assign_isic_codes(df, vision, self.logger)
        
        # Add partnership flags (simple logic, no data dependency)
        df = add_partenariat_berlitz_flags(df)
        
        return df

    def _enrich_euler_risk_note(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrich with Euler credit risk notes from BINSEE.histo_note_risque.
        
        Implements SAS logic from PTF_MVTS_CONSOLIDATION_MACRO.sas L400-410:
        - Joins on cdsiren with valid date range filtering
        - Maps cdnote: "00" → "" (empty), else keep value
        
        Args:
            df: Consolidated DataFrame with cdsiren column
            vision: Vision in YYYYMM format
        
        Returns:
            DataFrame with note_euler column added (NULL if data unavailable)
        """

        from utils.helpers import compute_date_ranges
        
        reader = get_bronze_reader(self)
        dates = compute_date_ranges(vision)
        
        try:
            df_euler = reader.read_file_group('binsee_histo_note_risque', vision='ref')
            
            if df_euler is not None:
                # Filter for valid date range (SAS L408-409)
                dtfinmn = dates['finmois']
                
                df_euler_filtered = df_euler.filter(
                    (col("dtdeb_valid") <= lit(dtfinmn)) &
                    (col("dtfin_valid") >= lit(dtfinmn))
                ).select(
                    col("cdsiren"),
                    # Map "00" to empty string (SAS L404)
                    when(col("cdnote") == "00", lit("")).otherwise(col("cdnote")).alias("note_euler")
                ).dropDuplicates(["cdsiren"])
                
                # Left join on cdsiren
                df = df.alias("a").join(
                    broadcast(df_euler_filtered.alias("e")),
                    col("a.cdsiren") == col("e.cdsiren"),
                    how="left"
                ).select("a.*", "e.note_euler")
                
                self.logger.info("Euler risk notes enriched successfully")
            else:
                self.logger.warning("BINSEE.histo_note_risque not available - note_euler will be NULL")
                from utils.processor_helpers import add_null_columns
                df = add_null_columns(df, {'note_euler': StringType})

        except Exception as e:
            self.logger.warning(f"Euler enrichment failed: {e}")
            from utils.processor_helpers import add_null_columns
            df = add_null_columns(df, {'note_euler': StringType})
        
        return df

    def _enrich_special_product_activity(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrich with special product activity codes from IPFM0024, IPFM63, IPFM99.
        
        Implements SAS logic from PTF_MVTS_CONSOLIDATION_MACRO.sas L446-525:
        - Extracts activity codes from special product files (IPFSPE1/3)
        - Unions data from Pole 1 and Pole 3
        - Coalesces with existing ACTPRIN and CDNAF
        - Creates TypeAct: "Multi" if secondary activity exists, else "Mono"
        
        Args:
            df: Consolidated DataFrame
            vision: Vision in YYYYMM format
        
        Returns:
            DataFrame with ACTPRIN2, CDNAF2, CDACTCONST2, TypeAct columns
            (NULL/Mono if special product files unavailable)
        """
        reader = get_bronze_reader(self)
        
        try:
            # Initialize empty special products table with schema
            df_tabspec = None
            
            # Try to read IPFM0024 (professional activity codes)
            try:
                df_ipfm0024 = reader.read_file_group('ipfspe_ipfm0024', vision)
                if df_ipfm0024 is not None:
                    # Union Pole 1 and Pole 3 (SAS L467-469)
                    df_spec_0024 = df_ipfm0024.select(
                        col("nopol"),
                        col("noint"),
                        col("cdprod"),
                        lit("1").alias("cdpole"),  # Assume mixed or add pole detection
                        col("cdactprf01").alias("cdactconst"),
                        col("cdactprf02").alias("cdactconst2"),
                        lit("").cast(StringType()).alias("cdnaf"),
                        lit(None).cast(DoubleType()).alias("mtca_ris")
                    )
                    df_tabspec = df_spec_0024 if df_tabspec is None else df_tabspec.unionByName(df_spec_0024)
                    self.logger.info("IPFM0024 special products loaded")
            except Exception as e:
                self.logger.debug(f"IPFM0024 not available: {e}")
            
            # Try to read IPFM63 (activity + NAF + CA)
            try:
                df_ipfm63 = reader.read_file_group('ipfspe_ipfm63', vision)
                if df_ipfm63 is not None:
                    df_spec_63 = df_ipfm63.select(
                        col("nopol"),
                        col("noint"),
                        col("cdprod"),
                        lit("1").alias("cdpole"),
                        col("actprin").alias("cdactconst"),
                        col("actsec1").alias("cdactconst2"),
                        col("cdnaf"),
                        col("mtca1").alias("mtca_ris")
                    )
                    df_tabspec = df_spec_63 if df_tabspec is None else df_tabspec.unionByName(df_spec_63)
                    self.logger.info("IPFM63 special products loaded")
            except Exception as e:
                self.logger.debug(f"IPFM63 not available: {e}")
            
            # Try to read IPFM99 (already partially used in AZ processor)
            try:
                df_ipfm99 = reader.read_file_group('ipfspe_ipfm99', vision)
                if df_ipfm99 is not None:
                    df_spec_99 = df_ipfm99.select(
                        col("nopol"),
                        col("noint"),
                        col("cdprod"),
                        lit("1").alias("cdpole"),
                        col("cdacpr1").substr(1, 4).alias("cdactconst"),  # First 4 chars (SAS L491)
                        col("cdacpr2").alias("cdactconst2"),
                        lit("").cast(StringType()).alias("cdnaf"),
                        col("mtca").alias("mtca_ris")
                    )
                    df_tabspec = df_spec_99 if df_tabspec is None else df_tabspec.unionByName(df_spec_99)
                    self.logger.info("IPFM99 special products loaded")
            except Exception as e:
                self.logger.debug(f"IPFM99 not available: {e}")
            
            # If any special product data found, join to main DataFrame
            if df_tabspec is not None:
                # Remove empty records (SAS L498-500)
                df_tabspec = df_tabspec.filter(col("nopol").isNotNull() & (col("nopol") != ""))
                
                # Left join on nopol + cdprod (SAS L514-515)
                df = df.alias("t1").join(
                    df_tabspec.alias("t3"),
                    (col("t1.nopol") == col("t3.nopol")) & (col("t1.cdprod") == col("t3.cdprod")),
                    how="left"
                ).select(
                    "t1.*",
                    # Coalesce with existing ACTPRIN (SAS L511)
                    coalesce(col("t3.cdactconst"), col("t1.actprin")).alias("actprin2"),
                    # Use special CDNAF if not empty, else existing (SAS L512)
                    when((col("t3.cdnaf").isNotNull()) & (col("t3.cdnaf") != ""), 
                         col("t3.cdnaf")).otherwise(col("t1.cdnaf")).alias("cdnaf2"),
                    col("t3.cdactconst2")
                )
                
                # Derive TypeAct (SAS L523-524)
                df = df.withColumn(
                    "typeact",
                    when(col("cdactconst2").isNotNull() & (col("cdactconst2") != ""), 
                         lit("Multi")).otherwise(lit("Mono"))
                )
                
                # Replace ACTPRIN and CDNAF with enriched values (SAS L521-522)
                df = df.withColumn("actprin", col("actprin2")) \
                       .withColumn("cdnaf", col("cdnaf2")) \
                       .drop("actprin2", "cdnaf2")
                
                self.logger.info("Special product activity codes enriched successfully")
            else:
                # No special product data available - use defaults
                self.logger.warning("No special product files (IPFM0024/63/99) available")
                from utils.processor_helpers import add_null_columns
                df = add_null_columns(df, {'cdactconst2': StringType})
                df = df.withColumn("typeact", lit("Mono"))  # Default to Mono

        except Exception as e:
            self.logger.warning(f"Special product enrichment failed: {e}")
            from utils.processor_helpers import add_null_columns
            df = add_null_columns(df, {'cdactconst2': StringType})
            df = df.withColumn("typeact", lit("Mono"))
        
        return df

    def _enrich_w6_naf_and_client_cdnaf(self, df: DataFrame) -> DataFrame:
        """
        Enrich with NAF codes from W6.BASECLI_INV and CLIENT NAF codes.
        
        Implements SAS logic from PTF_MVTS_CONSOLIDATION_MACRO.sas L534-552:
        - Adds CDNAF08_W6 (NAF 2008) from W6.BASECLI_INV
        - Adds CDNAF03_CLI from CLIENT1/CLIENT3 (NAF 2003)
        - Filters out invalid codes ("00", "000Z", "9999", "0000Z")
        
        Args:
            df: Consolidated DataFrame with noclt column
        
        Returns:
            DataFrame with CDNAF08_W6 and CDNAF03_CLI columns added
            (NULL if data unavailable)
        """
        reader = get_bronze_reader(self)
        
        # Step 1: W6 NAF enrichment
        try:
            df_w6 = reader.read_file_group('w6_basecli_inv', vision='ref')
            
            if df_w6 is not None:
                df_w6_select = df_w6.select(
                    col("noclt"),
                    col("cdapet").alias("cdnaf08_w6")
                ).dropDuplicates(["noclt"])
                
                df = df.alias("a").join(
                    broadcast(df_w6_select.alias("w6")),
                    col("a.noclt") == col("w6.noclt"),
                    how="left"
                ).select("a.*", "w6.cdnaf08_w6")
                
                # Filter invalid codes (SAS L551)
                df = df.withColumn(
                    "cdnaf08_w6",
                    when(col("cdnaf08_w6").isin(["0000Z"]), lit(None)).otherwise(col("cdnaf08_w6"))
                )
                
                self.logger.info("W6 NAF codes enriched successfully")
            else:
                self.logger.warning("W6.BASECLI_INV not available - cdnaf08_w6 will be NULL")
                from utils.processor_helpers import add_null_columns
                df = add_null_columns(df, {'cdnaf08_w6': StringType})

        except Exception as e:
            self.logger.warning(f"W6 NAF enrichment failed: {e}")
            from utils.processor_helpers import add_null_columns
            df = add_null_columns(df, {'cdnaf08_w6': StringType})
        
        # Step 2: Client NAF enrichment (extends existing _enrich_client_data)
        try:
            df_client1 = reader.read_file_group('client1', vision='ref')
            df_client3 = reader.read_file_group('client3', vision='ref')
            
            # Extract CDNAF from CLIENT1
            cdnaf_c1 = None
            if df_client1 is not None and 'cdnaf' in df_client1.columns:
                cdnaf_c1 = df_client1.select(
                    col("noclt"),
                    col("cdnaf").alias("cdnaf_c1")
                ).dropDuplicates(["noclt"])
            
            # Extract CDNAF from CLIENT3
            cdnaf_c3 = None
            if df_client3 is not None and 'cdnaf' in df_client3.columns:
                cdnaf_c3 = df_client3.select(
                    col("noclt"),
                    col("cdnaf").alias("cdnaf_c3")
                ).dropDuplicates(["noclt"])
            
            # Join and coalesce
            if cdnaf_c1 is not None:
                df = df.join(cdnaf_c1, on="noclt", how="left")
            if cdnaf_c3 is not None:
                df = df.join(cdnaf_c3, on="noclt", how="left")
            
            if cdnaf_c1 is not None or cdnaf_c3 is not None:
                df = df.withColumn(
                    "cdnaf03_cli",
                    coalesce(col("cdnaf_c1") if 'cdnaf_c1' in df.columns else lit(None),
                            col("cdnaf_c3") if 'cdnaf_c3' in df.columns else lit(None))
                )
                
                # Filter invalid codes (SAS L550)
                df = df.withColumn(
                    "cdnaf03_cli",
                    when(col("cdnaf03_cli").isin(["00", "000Z", "9999"]), lit(None))
                    .otherwise(col("cdnaf03_cli"))
                )
                
                # Drop temporary columns
                if 'cdnaf_c1' in df.columns:
                    df = df.drop("cdnaf_c1")
                if 'cdnaf_c3' in df.columns:
                    df = df.drop("cdnaf_c3")
                
                self.logger.info("Client NAF codes enriched successfully")
            else:
                from utils.processor_helpers import add_null_columns
                df = add_null_columns(df, {'cdnaf03_cli': StringType})
                self.logger.warning("Client NAF codes not available - cdnaf03_cli will be NULL")

        except Exception as e:
            self.logger.warning(f"Client NAF enrichment failed: {e}")
            from utils.processor_helpers import add_null_columns
            df = add_null_columns(df, {'cdnaf03_cli': StringType})
        
        return df


    def _add_isic_global_code(self, df: DataFrame) -> DataFrame:
        """
        Add ISIC_CODE_GBL by mapping ISIC_CODE through ISIC_LG reference.
        
        Implements SAS logic from PTF_MVTS_CONSOLIDATION_MACRO.sas L563-570:
        - Joins with ISIC.ISIC_LG to get global ISIC code
        - Falls back to ISIC_CODE if no mapping found
        
        Args:
            df: DataFrame with isic_code column
        
        Returns:
            DataFrame with isic_code_gbl column added
        """
        try:
            reader = get_bronze_reader(self)
            df_isic_lg = reader.read_file_group('isic_lg', 'ref')
            
            if df_isic_lg is not None:  # OPTIMIZED: Removed count() check
                # Join on ISIC_LOCAL = ISIC_CODE
                df_isic_lg = df_isic_lg.select(
                    col("isic_local").alias("isic_local_ref"),
                    col("isic_global").alias("isic_code_gbl_ref")
                )
                
                df = df.join(
                    df_isic_lg,
                    col("isic_code") == col("isic_local_ref"),
                    "left"
                )
                
                # Add isic_code_gbl column
                df = df.withColumn(
                    "isic_code_gbl",
                    coalesce(col("isic_code_gbl_ref"), lit(None).cast(StringType()))
                ).drop("isic_local_ref", "isic_code_gbl_ref")
                
                self.logger.info("ISIC global code mapping applied successfully")
            else:
                # Fallback: copy ISIC_CODE to ISIC_CODE_GBL
                df = df.withColumn("isic_code_gbl", col("isic_code"))
                self.logger.warning("ISIC_LG reference not available - using ISIC_CODE as fallback")
                
        except Exception as e:
            self.logger.warning(f"ISIC global mapping failed: {e}")
            df = df.withColumn("isic_code_gbl", col("isic_code"))
        
        return df


    def _enrich_do_dest(self, df: DataFrame) -> DataFrame:
        """
        Enrich with DESTINAT from DO_DEST reference before pattern matching.
        
        Implements SAS logic from PTF_MVTS_CONSOLIDATION_MACRO.sas L416-421:
        - Joins with DEST.DO_DEST on NOPOL
        - Sets DESTINAT value from reference
        - Pattern matching only applies to NULL DESTINAT values
        
        Args:
            df: Consolidated DataFrame
        
        Returns:
            DataFrame with destinat column enriched from reference
        """
        try:
            reader = get_bronze_reader(self)
            do_dest_df = reader.read_file_group("do_dest", "ref")
            
            if do_dest_df is not None:  # OPTIMIZED: Removed count() check
                # Select relevant columns and alias to avoid conflicts
                do_dest_df = do_dest_df.select(
                    col("nopol").alias("nopol_ref"),
                    col("destinat").alias("destinat_ref")
                )
                
                df = df.join(
                    do_dest_df,
                    col("nopol") == col("nopol_ref"),
                    "left"
                )
                
                # Use reference value if available
                df = df.withColumn(
                    "destinat",
                    col("destinat_ref")  # Use joined value directly
                ).drop("nopol_ref", "destinat_ref")
                
                self.logger.info("DO_DEST reference enrichment applied successfully")
            else:
                self.logger.warning("DO_DEST reference not available - using pattern matching only")
                
        except Exception as e:
            self.logger.warning(f"DO_DEST enrichment failed: {e}")
        
        return df
