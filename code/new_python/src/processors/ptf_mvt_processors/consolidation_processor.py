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
        """
        self.logger.info("Reading AZ silver data (mvt_const_ptf)")
        
        from src.reader import SilverReader
        reader = SilverReader(self.spark, self.config)
        
        # Read AZ silver file (with vision suffix)
        df_az = reader.read_silver_file(f"mvt_const_ptf_{vision}", vision)
        # Columns already lowercase from silver
        
        return df_az

    def transform(self, df_az: DataFrame, vision: str) -> DataFrame:
        """
        Consolidate AZ and AZEC data with harmonization and enrichment (JSON-driven).

        SAS parity:
        - If vision >= 201211 → AZ + AZEC
        - If vision <  201211 → AZ only
        - OUTER UNION CORR parity → unionByName + dedup on NOPOL with AZ priority
        """
        year_int, month_int = extract_year_month_int(vision)

        # Load configurations from JSON
        loader = get_default_loader()
        consolidation_config = loader.get_consolidation_config()

        silver_reader = SilverReader(self.spark, self.config)

        # -----------------------------
        # Step A — Read AZEC if allowed
        # -----------------------------
        if int(vision) >= 201211:
            self.logger.step(1, f"Reading AZEC silver data (vision >= 201211 → include AZEC)")
            df_azec = silver_reader.read_silver_file(f"azec_ptf_{vision}", vision)  # suffix vision
        else:
            self.logger.step(1, f"Vision {vision} < 201211 → AZ only (no AZEC)")
            df_azec = None

        # -----------------------------------
        # Step B — Harmonize AZ (rename/alias)
        # -----------------------------------
        self.logger.step(2, "Harmonizing AZ schema (JSON)")
        az_harmonization = consolidation_config["az_harmonization"]
        df_az = self._harmonize_schema(df_az, az_harmonization)

        # --------------------------------------
        # Step C — Harmonize AZEC (if applicable)
        # --------------------------------------
        if df_azec is not None:
            self.logger.step(3, "Harmonizing AZEC schema (JSON)")
            azec_harmonization = consolidation_config["azec_harmonization"]
            df_azec = self._harmonize_schema(df_azec, azec_harmonization)
            df_azec = df_azec.withColumn("dircom", lit(DIRCOM.AZEC))

            # -----------------------------
            # Step D — AZ + AZEC consolidation
            # -----------------------------
            self.logger.step(4, "Union AZ + AZEC (OUTER UNION CORR parity)")
            df_consolidated = df_az.unionByName(df_azec, allowMissingColumns=True)

            # Deduplicate with AZ priority
            priority = when(col("dircom").isin("AZ", "AZ "), lit(1)).otherwise(lit(0))
            df_consolidated = (
                df_consolidated
                .withColumn("_priority_dircom", priority)
                .orderBy(col("_priority_dircom").desc(), col("nopol"))
                .dropDuplicates(["nopol"])
                .drop("_priority_dircom")
            )
        else:
            df_consolidated = df_az

        # ----------------------------------------------------------
        # Step E — Extract MOIS/JOUR ECHEANCE from DTECHANN (common)
        # ----------------------------------------------------------
        self.logger.step(4.1, "Extracting MOIS_ECHEANCE / JOUR_ECHEANCE from DTECHANN")
        if "dtechann" in df_consolidated.columns:
            df_consolidated = df_consolidated.withColumn("mois_echeance", month(col("dtechann")))
            df_consolidated = df_consolidated.withColumn("jour_echeance", dayofmonth(col("dtechann")))

        # ----------------------------------------------
        # Step F — Common transformations (e.g., CDTRE *)
        # ----------------------------------------------
        self.logger.step(4.5, "Applying common transformations")
        df_consolidated = self._apply_common_transformations(df_consolidated)

        # ------------------------------------------------
        # Step G — DO_DEST (monthly or fixed reference)
        # ------------------------------------------------
        self.logger.step(4.6, "Enriching with DO_DEST reference (monthly)")
        df_consolidated = self._enrich_do_dest(df_consolidated, vision)

        # -------------------------------------------------------------
        # Step H — DESTINAT consolidation and pattern matching (SAS)
        # -------------------------------------------------------------
        self.logger.step(4.7, "Applying DESTINAT consolidation logic")
        from utils.transformations.enrichment import apply_destinat_consolidation_logic
        df_consolidated = apply_destinat_consolidation_logic(df_consolidated)

        self.logger.step(5, "Calculating DESTINAT for construction sites")
        from utils.transformations.enrichment import calculate_destinat
        df_consolidated = calculate_destinat(df_consolidated, self.logger)

        # -----------------------------------------
        # Step I — IRD Risk Q46/Q45/QAN enrichment
        # -----------------------------------------
        self.logger.step(5.5, "Enriching with IRD risk data")
        df_consolidated = self._enrich_ird_risk(df_consolidated, vision)

        # -------------------------------------------------
        # Step J — Fallbacks (DTRCPPR from DTREFFIN, etc.)
        # -------------------------------------------------
        self.logger.step(5.7, "Applying fallback logic")
        df_consolidated = self._apply_fallback_logic(df_consolidated)

        # ---------------------------------------
        # Step K — Client (SIREN/SIRET) enrichment
        # ---------------------------------------
        self.logger.step(5.8, "Enriching client data (SIRET/SIREN)")
        df_consolidated = self._enrich_client_data(df_consolidated, vision)

        # --------------------------------------
        # Step L — Euler credit risk notes join
        # --------------------------------------
        self.logger.step(5.9, "Enriching Euler risk notes (note_euler)")
        df_consolidated = self._enrich_euler_risk_note(df_consolidated, vision)

        # ---------------------------------------------------
        # Step M — Special product activity (IPFM0024/63/99)
        # ---------------------------------------------------
        self.logger.step(6, "Enriching special product activity codes (TypeAct)")
        df_consolidated = self._enrich_special_product_activity(df_consolidated, vision)

        # ---------------------------------------------------
        # Step N — W6 & Client NAF codes, ISIC + corrections
        # ---------------------------------------------------
        self.logger.step(6.05, "Enriching NAF codes (W6 + CLIENT)")
        df_consolidated = self._enrich_w6_naf_and_client_cdnaf(df_consolidated)

        self.logger.step(6.1, "Adding ISIC codes and HAZARD_GRADES")
        df_consolidated = self._add_isic_codes(df_consolidated, vision)

        self.logger.step(6.2, "Adding ISIC global code mapping")
        df_consolidated = self._add_isic_global_code(df_consolidated)

        # ---------------------------------------------------------
        # Step O — Business flags (Berlioz / Partenariat)
        # ---------------------------------------------------------
        self.logger.step(6.4, "Adding special business flags")
        df_consolidated = self._add_business_flags(df_consolidated)

        # ---------------------------------------------------------
        # Step P — Placeholders for optional enrichments
        # ---------------------------------------------------------
        self.logger.step(6.9, "Adding placeholders for remaining missing columns")
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
        Write consolidated data to gold layer with exact SAS column schema.

        - Selects only SAS output columns in correct order (GOLD_COLUMNS_PTF_MVT)
        - Convention entreprise : nom de fichier suffixé par la vision
        """
        from config.constants import GOLD_COLUMNS_PTF_MVT

        # Renommage éventuel de 'desti_isic' → 'destinat_isic'
        if "desti_isic" in df.columns:
            self.logger.info("✓ Renaming 'desti_isic' → 'destinat_isic'")
            df = df.withColumnRenamed("desti_isic", "destinat_isic")

        # Colonnes finales
        existing_gold_cols = [c for c in GOLD_COLUMNS_PTF_MVT if c in df.columns]

        # Logs
        missing_cols = set(GOLD_COLUMNS_PTF_MVT) - set(df.columns)
        if missing_cols:
            self.logger.warning(f"Missing expected GOLD columns (NULL in output): {sorted(list(missing_cols))[:10]} ...")

        extra_cols = set(df.columns) - set(GOLD_COLUMNS_PTF_MVT)
        if extra_cols:
            self.logger.info(f"Dropping {len(extra_cols)} intermediate columns (e.g., {list(extra_cols)[:5]})")

        df_final = df.select(existing_gold_cols)

        # Stats
        row_count = df_final.count()
        self.logger.info(f"Final gold output: {len(existing_gold_cols)} columns, {row_count:,} rows")

        from utils.helpers import write_to_layer
        write_to_layer(
            df=df_final,
            config=self.config,
            layer="gold",
            filename=f"ptf_mvt_{vision}",
            vision=vision,
            logger=self.logger
        )

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
        Apply fallback logic for missing dates (SAS L373–379):
        - If DTRCPPR is missing and DTREFFIN exists → DTRCPPR = DTREFFIN
        """
        if "dtrcppr" in df.columns and "dtreffin" in df.columns:
            df = df.withColumn(
                "dtrcppr",
                when(col("dtrcppr").isNull() & col("dtreffin").isNotNull(), col("dtreffin"))
                .otherwise(col("dtrcppr"))
            )
        return df

    def _enrich_ird_risk(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrich with IRD risk data from Q46, Q45, and QAN.
        
        Based on: SAS PTF_MVTS_CONSOLIDATION_MACRO.sas L154-258
        
        Uses refactored risk_enrichment module to eliminate code duplication.
        
        Args:
            df: Consolidated DataFrame
            vision: Vision in YYYYMM format
            
        Returns:
            DataFrame enriched with IRD risk data
        """
        from utils.transformations.enrichment.risk_enrichment import enrich_with_risk_data
        
        reader = get_bronze_reader(self)
        
        # Use refactored module (eliminates ~100 lines of duplicate code)
        df = enrich_with_risk_data(
            df, 
            risk_sources=['q46', 'q45', 'qan'],
            vision=vision,
            bronze_reader=reader,
            logger=self.logger
        )
        
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
        
        Uses refactored isic_codification module (assign_isic_codes is now self-contained).
        
        Based on: CODIFICATION_ISIC_CONSTRUCTION.sas (full pipeline)
        
        Args:
            df: Consolidated DataFrame
            vision: Vision in YYYYMM format
        
        Returns:
            DataFrame with ISIC columns added (isic_code, hazard_grades, etc.)
        """
        from utils.transformations.base.isic_codification import assign_isic_codes
        
        # assign_isic_codes now handles all reference table loading internally
        # New signature: (df, spark, config, vision, logger)
        df = assign_isic_codes(df, self.spark, self.config, vision, self.logger)
        
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
            # SAS L462-470: Union IPFSPE1.IPFM0024 (Pole 1) + IPFSPE3.IPFM0024 (Pole 3)
            try:
                # Read Pole 1 (Agents)
                df_ipfm0024_1 = reader.read_file_group('ipfm0024_1', vision)
                # Read Pole 3 (Brokers)
                df_ipfm0024_3 = reader.read_file_group('ipfm0024_3', vision)
                
                if df_ipfm0024_1 is not None or df_ipfm0024_3 is not None:
                    # Pole 1 processing
                    if df_ipfm0024_1 is not None:
                        df_spec_0024_p1 = df_ipfm0024_1.select(
                            col("nopol"),
                            col("noint"),
                            col("cdprod"),
                            lit("1").alias("cdpole"),  # Pole 1 = Agents
                            col("cdactprf01").alias("cdactconst"),
                            col("cdactprf02").alias("cdactconst2"),
                            lit("").cast(StringType()).alias("cdnaf"),
                            lit(None).cast(DoubleType()).alias("mtca_ris"),
                            lit("ipfm0024_1").alias("_source")  # Track source for dedup
                        )
                        df_tabspec = df_spec_0024_p1 if df_tabspec is None else df_tabspec.unionByName(df_spec_0024_p1)
                    
                    # Pole 3 processing
                    if df_ipfm0024_3 is not None:
                        df_spec_0024_p3 = df_ipfm0024_3.select(
                            col("nopol"),
                            col("noint"),
                            col("cdprod"),
                            lit("3").alias("cdpole"),  # Pole 3 = Brokers
                            col("cdactprf01").alias("cdactconst"),
                            col("cdactprf02").alias("cdactconst2"),
                            lit("").cast(StringType()).alias("cdnaf"),
                            lit(None).cast(DoubleType()).alias("mtca_ris"),
                            lit("ipfm0024_3").alias("_source")  # Track source for dedup
                        )
                        df_tabspec = df_spec_0024_p3 if df_tabspec is None else df_tabspec.unionByName(df_spec_0024_p3)
                    
                    self.logger.info("IPFM0024 special products loaded (Pole 1 + Pole 3)")
            except Exception as e:
                self.logger.debug(f"IPFM0024 not available: {e}")
            
            # Try to read IPFM63 (activity + NAF + CA)
            # SAS L474-482: Union IPFSPE1.IPFM63 (Pole 1) + IPFSPE3.IPFM63 (Pole 3)
            try:
                # Read Pole 1 (Agents)
                df_ipfm63_1 = reader.read_file_group('ipfm63_1', vision)
                # Read Pole 3 (Brokers)
                df_ipfm63_3 = reader.read_file_group('ipfm63_3', vision)
                
                if df_ipfm63_1 is not None or df_ipfm63_3 is not None:
                    # Pole 1 processing
                    if df_ipfm63_1 is not None:
                        df_spec_63_p1 = df_ipfm63_1.select(
                            col("nopol"),
                            col("noint"),
                            col("cdprod"),
                            lit("1").alias("cdpole"),  # Pole 1 = Agents
                            col("actprin").alias("cdactconst"),
                            col("actsec1").alias("cdactconst2"),
                            col("cdnaf"),
                            col("mtca1").alias("mtca_ris"),
                            lit("ipfm63_1").alias("_source")  # Track source for dedup
                        )
                        df_tabspec = df_spec_63_p1 if df_tabspec is None else df_tabspec.unionByName(df_spec_63_p1)
                    
                    # Pole 3 processing
                    if df_ipfm63_3 is not None:
                        df_spec_63_p3 = df_ipfm63_3.select(
                            col("nopol"),
                            col("noint"),
                            col("cdprod"),
                            lit("3").alias("cdpole"),  # Pole 3 = Brokers
                            col("actprin").alias("cdactconst"),
                            col("actsec1").alias("cdactconst2"),
                            col("cdnaf"),
                            col("mtca1").alias("mtca_ris"),
                            lit("ipfm63_3").alias("_source")  # Track source for dedup
                        )
                        df_tabspec = df_spec_63_p3 if df_tabspec is None else df_tabspec.unionByName(df_spec_63_p3)
                    
                    self.logger.info("IPFM63 special products loaded (Pole 1 + Pole 3)")
            except Exception as e:
                self.logger.debug(f"IPFM63 not available: {e}")
            
            # Try to read IPFM99 (already partially used in AZ processor)
            # SAS L486-494: Union IPFSPE1.IPFM99 (Pole 1) + IPFSPE3.IPFM99 (Pole 3)
            try:
                # Read Pole 1 (Agents)
                df_ipfm99_1 = reader.read_file_group('ipfm99_1', vision)
                # Read Pole 3 (Brokers)
                df_ipfm99_3 = reader.read_file_group('ipfm99_3', vision)
                
                if df_ipfm99_1 is not None or df_ipfm99_3 is not None:
                    # Pole 1 processing
                    if df_ipfm99_1 is not None:
                        df_spec_99_p1 = df_ipfm99_1.select(
                            col("nopol"),
                            col("noint"),
                            col("cdprod"),
                            lit("1").alias("cdpole"),  # Pole 1 = Agents
                            col("cdacpr1").substr(1, 4).alias("cdactconst"),  # First 4 chars (SAS L491)
                            col("cdacpr2").alias("cdactconst2"),
                            lit("").cast(StringType()).alias("cdnaf"),
                            col("mtca").alias("mtca_ris"),
                            lit("ipfm99_1").alias("_source")  # Track source for dedup
                        )
                        df_tabspec = df_spec_99_p1 if df_tabspec is None else df_tabspec.unionByName(df_spec_99_p1)
                    
                    # Pole 3 processing
                    if df_ipfm99_3 is not None:
                        df_spec_99_p3 = df_ipfm99_3.select(
                            col("nopol"),
                            col("noint"),
                            col("cdprod"),
                            lit("3").alias("cdpole"),  # Pole 3 = Brokers
                            col("cdacpr1").substr(1, 4).alias("cdactconst"),  # First 4 chars (SAS L493)
                            col("cdacpr2").alias("cdactconst2"),
                            lit("").cast(StringType()).alias("cdnaf"),
                            col("mtca").alias("mtca_ris"),
                            lit("ipfm99_3").alias("_source")  # Track source for dedup
                        )
                        df_tabspec = df_spec_99_p3 if df_tabspec is None else df_tabspec.unionByName(df_spec_99_p3)
                    
                    self.logger.info("IPFM99 special products loaded (Pole 1 + Pole 3)")
            except Exception as e:
                self.logger.debug(f"IPFM99 not available: {e}")
            
            # If any special product data found, join to main DataFrame
            if df_tabspec is not None:
                # Remove empty records (SAS L498-500)
                df_tabspec = df_tabspec.filter(col("nopol").isNotNull() & (col("nopol") != ""))
                
                # SAS: Sequential union → last source wins for duplicates
                # Python: Need explicit orderBy to make dropDuplicates deterministic
                # Priority: IPFM99_3 (highest) > IPFM99_1 > IPFM63_3 > IPFM63_1 > IPFM0024_3 > IPFM0024_1 (lowest)
                df_tabspec = (df_tabspec
                    .withColumn(
                        "_priority",
                        when(col("_source") == "ipfm99_3", lit(5))  # Highest priority (last in SAS)
                        .when(col("_source") == "ipfm99_1", lit(4))
                        .when(col("_source") == "ipfm63_3", lit(3))
                        .when(col("_source") == "ipfm63_1", lit(2))
                        .when(col("_source") == "ipfm0024_3", lit(1))
                        .otherwise(lit(0))  # ipfm0024_1
                    )
                    .orderBy(col("_priority").desc())  # Sort by priority BEFORE dedup
                    .dropDuplicates(['nopol', 'cdprod'])
                    .drop("_priority", "_source")  # Clean up tracking columns
                )
                
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
            df_client1 = reader.read_file_group('cliact14', vision='ref')
            df_client3 = reader.read_file_group('cliact3', vision='ref')
            
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


    def _enrich_do_dest(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrich with DESTINAT from DO_DEST reference.
        
        SAS L416-421 uses DEST.DO_DEST202110 (fixed October 2021 reference).
        However, if your data has monthly DO_DEST files, use vision parameter.
        
        CRITICAL: DO_DEST may contain date columns that need normalization.
        
        Args:
            df: Consolidated DataFrame
            vision: Vision in YYYYMM format
        
        Returns:
            DataFrame with destinat column enriched from DO_DEST reference
        """
        from pyspark.sql.functions import to_date, coalesce
        from pyspark.sql.types import DateType, StringType
        
        try:
            reader = get_bronze_reader(self)
            
            # CRITICAL: SAS uses DO_DEST202110 (fixed ref), but if you have monthly files, use vision
            # For SAS parity, you might want to use a fixed vision like "202110"
            # For now, using current vision as per user's data structure
            do_dest_df = reader.read_file_group("do_dest", vision)
            
            if do_dest_df is None:
                self.logger.warning(f"DO_DEST not available for vision {vision} - skipping destinat enrichment")
                # Ensure destinat column exists (NULL)
                if 'destinat' not in df.columns:
                    df = df.withColumn('destinat', lit(None).cast(StringType()))
                return df
            
            # Verify required columns exist
            if 'nopol' not in do_dest_df.columns or 'destinat' not in do_dest_df.columns:
                self.logger.warning("DO_DEST missing required columns (nopol/destinat) - cannot join")
                if 'destinat' not in df.columns:
                    df = df.withColumn('destinat', lit(None).cast(StringType()))
                return df
            
            # CRITICAL: Normalize date columns if present (prevents date format errors)
            # DO_DEST may contain: dtouvch, dtfinch, dtrecep
            date_cols_to_normalize = ['dtouvch', 'dtfinch', 'dtrecep']
            for col_name in date_cols_to_normalize:
                if col_name in do_dest_df.columns:
                    do_dest_df = do_dest_df.withColumn(
                        col_name,
                        coalesce(
                            to_date(col(col_name), 'yyyyMMdd'),     # Format 1: 20240115
                            to_date(col(col_name), 'yyyy-MM-dd'),   # Format 2: 2024-01-15
                            to_date(col(col_name), 'dd/MM/yyyy')    # Format 3: 15/01/2024
                        ).cast(DateType())
                    )
                    self.logger.debug(f"Normalized date column: {col_name}")
            
            # Prepare reference data (select only needed columns + dedup)
            do_dest_select = do_dest_df.select(
                col("nopol").alias("nopol_ref"),
                col("destinat").cast(StringType()).alias("destinat_ref")
            ).dropDuplicates(["nopol_ref"])
            
            # Check if main DF has nopol
            if 'nopol' not in df.columns:
                self.logger.warning("Main DataFrame missing 'nopol' column - cannot join DO_DEST")
                if 'destinat' not in df.columns:
                    df = df.withColumn('destinat', lit(None).cast(StringType()))
                return df
            
            # Left join on nopol
            df_joined = df.join(
                do_dest_select,
                col("nopol") == col("nopol_ref"),
                "left"
            )
            
            # CRITICAL: SAS logic (L418): "select t1.*, t2.DESTINAT"
            # This ADDS destinat column (or OVERWRITES if exists)
            # Python equivalent: Create destinat if missing, else fill NULLs only
            if 'destinat' in df.columns:
                # Destinat exists: fill NULLs with reference value
                df_result = df_joined.withColumn(
                    "destinat",
                    when(col("destinat").isNull(), col("destinat_ref"))
                    .otherwise(col("destinat"))
                )
            else:
                # Destinat doesn't exist: create from reference
                df_result = df_joined.withColumn("destinat", col("destinat_ref"))
            
            # Drop temporary join columns
            df_result = df_result.drop("nopol_ref", "destinat_ref")
            
            self.logger.info(f"✓ DO_DEST reference joined for vision {vision}")
            return df_result
            
        except Exception as e:
            self.logger.warning(f"DO_DEST enrichment failed: {e}")
            # Ensure destinat exists even on error
            if 'destinat' not in df.columns:
                df = df.withColumn('destinat', lit(None).cast(StringType()))
            return df
