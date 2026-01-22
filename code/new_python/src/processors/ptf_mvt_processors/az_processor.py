"""
AZ Processor - Portfolio Movements for AZ (Agent + Courtage channels).

Processes PTF16 (Agent) and PTF36 (Courtage) data from bronze layer,
applies business transformations, and outputs to silver layer.

Uses dictionary-driven configuration for maximum reusability.
"""


from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, coalesce, broadcast, to_date, expr, create_map, make_date
)
from pyspark.sql.types import DoubleType, StringType, DateType

# Base processor
from src.processors.base_processor import BaseProcessor

# Loader
from utils.loaders import get_default_loader

# Constants
from config.constants import DIRCOM, POLE, LTA_TYPES

# Helpers
from utils.helpers import extract_year_month_int, compute_date_ranges

# Business logic (corrected & final)
from utils.transformations.base.column_operations import apply_column_config
from utils.transformations.operations.business_logic import (
    extract_capitals,
    calculate_movements,   # alias AZ
    calculate_exposures,
)

# Processor helpers
from utils.processor_helpers import (
    safe_reference_join,
    safe_multi_reference_join,
    add_null_columns,
    get_bronze_reader
)

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

        from pyspark.sql.functions import (
            col, when, lit, coalesce, to_date, expr, create_map
        )

        year_int, month_int = extract_year_month_int(vision)
        dates = compute_date_ranges(vision)

        loader = get_default_loader()
        az_config = loader.get_az_config()

        # ============================================================
        # STEP 1 — Initialize indicator columns (SAS L106–115)
        #   ⚠️ Ne PAS écraser les colonnes si elles existent déjà !
        # ============================================================
        self.logger.step(1, "Initializing indicator columns")
        init_columns = {
            **{c: lit(0.0) for c in [
                'primeto', 'primes_ptf', 'primes_afn', 'primes_res',
                'primes_rpt', 'primes_rpc', 'expo_ytd', 'expo_gli',
                'cotis_100', 'mtca_', 'perte_exp', 'risque_direct',
                'value_insured', 'smp_100', 'lci_100'
            ]},
            **{c: lit(0) for c in [
                'nbptf', 'nbafn', 'nbres', 'nbrpt', 'nbrpc',
                'top_temp', 'top_lta', 'top_aop',
                'nbafn_anticipe', 'nbres_anticipe'
            ]},
            'dt_deb_expo': lit(None).cast(DateType()),
            'dt_fin_expo': lit(None).cast(DateType()),
            # 'ctduree' ne doit PAS être écrasée si déjà présente
            'ctduree': lit(None).cast('double'),
        }
        for name, value in init_columns.items():
            if name not in df.columns:
                df = df.withColumn(name, value)

        # ============================================================
        # STEP 2 — Metadata (vision, dircom, exevue, moisvue) + CDPOLE propre
        # ============================================================
        self.logger.step(2, "Adding metadata columns")
        metadata_cols = {
            'dircom': lit(DIRCOM.AZ),
            'vision': lit(vision),
            'exevue': lit(year_int),
            'moisvue': lit(month_int)
        }
        for name, value in metadata_cols.items():
            df = df.withColumn(name, value)

        # CDPOLE: produire des valeurs propres '1'/'3' (pas "  1")
        if '_source_file' in df.columns:
            df = df.withColumn(
                'cdpole',
                when(col('_source_file').contains('ipf16'), lit('1'))
                .when(col('_source_file').contains('ipf36'), lit('3'))
                .otherwise(lit('1'))
            ).drop('_source_file')
        else:
            df = df.withColumn('cdpole', lit('1'))

        df = df.withColumn("cdpole", col("cdpole").cast("string"))

        # ============================================================
        # STEP 3 — Apply renames from JSON (SAS SELECT renames)
        # ============================================================
        self.logger.step(3, "Applying renames")
        df = self._apply_renames(df, az_config)

        # 3.b — CAST des colonnes de dates (support European format dd/MM/yyyy)
        date_cols = [
            "dtcrepol", "dteffan", "dttraan", "dtresilp", "dttraar",
            "dttypli1", "dttypli2", "dttypli3",
            "dtouchan", "dtrcppr", "dtrectrx", "dtrcpre", "dtechann"
        ]
        for dc in date_cols:
            if dc in df.columns:
                # Try European format first (dd/MM/yyyy), then ISO formats
                # Source data uses dd/MM/yyyy (French/European format)
                df = df.withColumn(
                    dc,
                    coalesce(
                        to_date(col(dc), "dd/MM/yyyy"),   # European format (source)
                        to_date(col(dc), "yyyy-MM-dd"),   # ISO format
                        to_date(col(dc), "yyyyMMdd")      # Compact format
                    )
                )

        # ============================================================
        # STEP 4a — COASS + PARTCIE (comme dans le SELECT SAS)
        #   ✅ Standardiser sur cdcoass (double "s")
        #   (si la colonne s'appelle cdcoas, on la renomme à la volée)
        # ============================================================
        if 'cdcoass' not in df.columns and 'cdcoas' in df.columns:
            df = df.withColumnRenamed('cdcoas', 'cdcoass')

        df = df.withColumn(
            "coass",
            when((col("cdpolqpl") == "1") & col("cdcoass").isin("3", "6"), lit("APERITION"))
            .when((col("cdpolqpl") == "1") & col("cdcoass").isin("4", "5"), lit("COASS. ACCEPTEE"))
            .when((col("cdpolqpl") == "1") & (col("cdcoass") == "8"), lit("ACCEPTATION INTERNATIONALE"))
            .when((col("cdpolqpl") == "1"), lit("AUTRES"))
            .otherwise(lit("SANS COASSURANCE"))
        )

        df = df.withColumn(
            "partcie",
            when(col("cdpolqpl") != "1", lit(1.0))      # sans coassurance
            .otherwise(col("prcdcie") / 100.0)          # avec coassurance
        )

        # ============================================================
        # STEP 4b — SELECT-level computed fields (tx, top_coass, etc.)
        # ============================================================
        self.logger.step(4, "Applying SELECT-level computed fields")
        select_computed = az_config.get("column_selection", {}).get("computed", {})
        df = self._apply_computed_generic(df, select_computed)

        # CRITERE_REVISION depuis config
        rev_cfg = az_config.get("revision_criteria", {})
        src = rev_cfg.get("source_col", "cdgrev")
        mapping = rev_cfg.get("mapping", {})
        if mapping and src in df.columns:
            kv = []
            for k, v in mapping.items():
                kv += [lit(k), lit(v)]
            df = df.withColumn("critere_revision", create_map(kv)[col(src)])

        # ============================================================
        # STEP 5 — Join IPFM99 (special CA logic for product 01099)
        # ============================================================
        self.logger.step(5, "Joining IPFM99")
        df = self._join_ipfm99(df, vision)

        # ============================================================
        # STEP 6 — Capital extraction (SMP, LCI, PERTE_EXP, RISQUE_DIRECT)
        # ============================================================
        self.logger.step(6, "Extracting capital fields")
        capital_cfg = az_config['capital_extraction']
        df = extract_capitals(df, {
            'smp_100': capital_cfg['smp_100'],
            'lci_100': capital_cfg['lci_100'],
            'perte_exp': capital_cfg['perte_exp'],
            'risque_direct': capital_cfg['risque_direct']
        })

        # ============================================================
        # STEP 7 — UPDATE-level computed fields (primeto, top_lta, top_temp, top_revisable)
        # ============================================================
        self.logger.step(7, "Applying UPDATE-level computed fields")
        update_computed = az_config.get("computed_fields", {})
        df = self._apply_computed_generic(df, update_computed)

        # ============================================================
        # STEP 8 — Movement indicators (AFN, RES, RPC, RPT, NBPTF)
        # ============================================================
        self.logger.step(8, "Calculating movement indicators")
        
        # DIAGNOSTIC: Log preconditions for movement calculations
        try:
            total = df.count()
            self.logger.info("[AZ DIAG] Pre-movements diagnostics:")
            self.logger.info(f"[AZ DIAG] Total rows: {total:,}")
            
            # CSSSEG distribution (critical for NBPTF)
            cssseg_5 = df.filter(col('cssseg') == '5').count()
            cssseg_null = df.filter(col('cssseg').isNull()).count()
            self.logger.info(f"[AZ DIAG] CSSSEG='5': {cssseg_5:,} ({100*cssseg_5/total:.1%})")
            self.logger.info(f"[AZ DIAG] CSSSEG NULL: {cssseg_null:,} ({100*cssseg_null/total:.1%})")
            
            # CDSITP distribution (critical for NBPTF/NBRES)
            cdsitp_1 = df.filter(col('cdsitp') == '1').count()
            cdsitp_3 = df.filter(col('cdsitp') == '3').count()
            cdsitp_null = df.filter(col('cdsitp').isNull()).count()
            self.logger.info(f"[AZ DIAG] CDSITP='1': {cdsitp_1:,} ({100*cdsitp_1/total:.1%})")
            self.logger.info(f"[AZ DIAG] CDSITP='3': {cdsitp_3:,} ({100*cdsitp_3/total:.1%})")
            self.logger.info(f"[AZ DIAG] CDSITP NULL: {cdsitp_null:,} ({100*cdsitp_null/total:.1%})")
            
            # CDNATP distribution (critical for NBPTF/NBRES)
            cdnatp_ro = df.filter(col('cdnatp').isin('R', 'O')).count()
            cdnatp_c = df.filter(col('cdnatp') == 'C').count()
            cdnatp_null = df.filter(col('cdnatp').isNull()).count()
            self.logger.info(f"[AZ DIAG] CDNATP in(R,O): {cdnatp_ro:,} ({100*cdnatp_ro/total:.1%})")
            self.logger.info(f"[AZ DIAG] CDNATP='C': {cdnatp_c:,} ({100*cdnatp_c/total:.1%})")
            self.logger.info(f"[AZ DIAG] CDNATP NULL: {cdnatp_null:,} ({100*cdnatp_null/total:.1%})")
            
            # Date columns NULL ratios (critical for AFN/RES conditions)
            dtcrepol_null = df.filter(col('dtcrepol').isNull()).count()
            dteffan_null = df.filter(col('dteffan').isNull()).count()
            dttraan_null = df.filter(col('dttraan').isNull()).count()
            dtresilp_null = df.filter(col('dtresilp').isNull()).count()
            self.logger.info(f"[AZ DIAG] Date columns NULL ratios:")
            self.logger.info(f"[AZ DIAG]   dtcrepol: {total-dtcrepol_null:,}/{total:,} ({100*(total-dtcrepol_null)/total:.1%} non-NULL)")
            self.logger.info(f"[AZ DIAG]   dteffan: {total-dteffan_null:,}/{total:,} ({100*(total-dteffan_null)/total:.1%} non-NULL)")
            self.logger.info(f"[AZ DIAG]   dttraan: {total-dttraan_null:,}/{total:,} ({100*(total-dttraan_null)/total:.1%} non-NULL)")
            self.logger.info(f"[AZ DIAG]   dtresilp: {total-dtresilp_null:,}/{total:,} ({100*(total-dtresilp_null)/total:.1%} non-NULL)")
        except Exception as e:
            self.logger.warning(f"[AZ DIAG] Failed to compute diagnostics: {e}")
        
        movement_cols = az_config['movements']['column_mapping']
        df = calculate_movements(df, dates, year_int, movement_cols)

        # ============================================================
        # STEP 9 — Exposure calculations (expo_ytd, expo_gli)
        # ============================================================
        self.logger.step(9, "Calculating exposures")
        exposure_cols = az_config['exposures']['column_mapping']
        df = calculate_exposures(df, dates, year_int, exposure_cols)

        # ============================================================
        # STEP 10 — Cotisation 100% and MTCA_
        # ============================================================
        self.logger.step(10, "Calculating cotis_100 and mtca_")

        # ISO-SAS: PRCDCIE = 100 si NULL ou 0 (sur la COLONNE elle-même)
        df = df.withColumn(
            'prcdcie',
            when((col('prcdcie').isNull()) | (col('prcdcie') == 0), lit(100))
            .otherwise(col('prcdcie'))
        )

        # Cotisation technique à 100% (COASS ACCEPTEE => (mtprprto*100)/prcdcie)
        df = df.withColumn(
            'cotis_100',
            when(
                (col('top_coass') == 1) & (col('cdcoass').isin(['4', '5'])),
                (col('mtprprto') * 100) / col('prcdcie')
            ).otherwise(col('mtprprto'))
        )

        # ISO-SAS: MTCA / MTCAF à 0 si NULL puis MTCA_
        df = df.withColumn("mtca", coalesce(col("mtca"), lit(0.0))) \
            .withColumn("mtcaf", coalesce(col("mtcaf"), lit(0.0))) \
            .withColumn("mtca_", col("mtcaf") + col("mtca"))

        # ============================================================
        # STEP 11 — TOP_AOP flag (STRICT ORDER MATCH)
        # ============================================================
        self.logger.step(11, "Applying TOP_AOP flag")
        df = df.withColumn(
            'top_aop',
            when(col('opapoffr') == 'O', lit(1)).otherwise(lit(0))
        )

        # ============================================================
        # STEP 12 — Anticipated movements (AFN/RES anticipés)
        # ============================================================
        self.logger.step(12, "Calculating anticipated movements")
        finmois = dates['finmois']  # 'YYYY-MM-DD'
        df = df.withColumn("finmois", lit(finmois).cast("date"))

        df = df.withColumn(
            "nbafn_anticipe",
            when(
                ((col("dteffan") > col("finmois")) | (col("dtcrepol") > col("finmois"))) &
                ~((col("cdtypli1") == "RE") | (col("cdtypli2") == "RE") | (col("cdtypli3") == "RE")),
                lit(1)
            ).otherwise(lit(0))
        )

        df = df.withColumn(
            "nbres_anticipe",
            when(
                (col("dtresilp") > col("finmois")) &
                ~((col("cdtypli1") == "RP") | (col("cdtypli2") == "RP") | (col("cdtypli3") == "RP") |
                (col("cdmotres") == "R") | (col("cdcasres") == "2R")),
                lit(1)
            ).otherwise(lit(0))
        )

        # ============================================================
        # STEP 13 — Final cleanup (expo dates, nmclt)
        # ============================================================
        self.logger.step(13, "Final data cleanup")
        df = self._finalize_data_cleanup(df)

        # ============================================================
        # STEP 14 — Segmentation & PT_GEST enrichment
        # ============================================================
        self.logger.step(14, "Enriching segmentation and management point")
        df = self._enrich_segment_and_product_type(df, vision)

        # ============================================================
        # STEP 15 — Deduplication (SAS L505–507)
        # ============================================================
        self.logger.step(15, "Deduplicating by nopol")
        # orderBy required before dropDuplicates for deterministic behavior (SAS NODUPKEY)
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
            df=df,
            config=self.config,
            layer="silver",
            filename=f'mvt_const_ptf_{vision}',
            vision=vision,
            logger=self.logger,
            optimize=True,
        )

    def _apply_renames(self, df, az_config):
        column_selection = az_config.get("column_selection", {})
        rename_map = column_selection.get("rename", {})
        self.logger.info(f"Rename map loaded: {rename_map}")
        for old, new in rename_map.items():
            if old in df.columns:
                df = df.withColumnRenamed(old, new)
        return df

    def _apply_computed_generic(self, df: DataFrame, computed_config: dict) -> DataFrame:
        """
        Generic computed-field engine.
        Supports:
        - coalesce_default
        - flag_equality
        - expression
        - conditional
        Works for both:
        - column_selection.computed (SELECT SAS)
        - computed_fields (UPDATE SAS)
        """

        from pyspark.sql.functions import col, lit, when, expr

        if not computed_config:
            return df

        # --- helper: normalize any JSON literal to a Spark Column ---
        def _normalize(value):
            # NULL explicit
            if value is None:
                return lit(None).cast("string")

            # string-based
            if isinstance(value, str):
                cleaned = value.strip()

                # SAS missing markers → NULL
                if cleaned in ["", ".", "NULL", "NA"]:
                    return lit(None).cast("string")

                # numeric strings
                if cleaned.replace(".", "", 1).isdigit():
                    if "." in cleaned:
                        return lit(float(cleaned))
                    else:
                        return lit(int(cleaned))

                # otherwise: treat as a literal string (SAFE)
                return lit(cleaned)

            # numeric Python literal
            if isinstance(value, (int, float)):
                return lit(value)

            # fallback
            return lit(str(value))

        # --- main processing ---
        for field_name, field_def in computed_config.items():
            if field_name == "description":
                continue

            field_type = field_def.get("type")

            # 1. coalesce_default
            if field_type == "coalesce_default":
                source = field_def["source_col"]
                default = field_def["default"]
                df = df.withColumn(
                    field_name,
                    when(col(source).isNull(), _normalize(default)).otherwise(col(source))
                )

            # 2. flag_equality
            elif field_type == "flag_equality":
                source = field_def["source_col"]
                value = field_def["value"]
                df = df.withColumn(
                    field_name,
                    when(col(source) == value, lit(1)).otherwise(lit(0))
                )

            # 3. expression
            elif field_type == "expression":
                formula = field_def["formula"]
                df = df.withColumn(field_name, expr(formula))

            # 4. conditional
            elif field_type == "conditional":
                conditions = field_def["conditions"]
                default_v = field_def["default"]

                # start from normalized default
                result_expr = _normalize(default_v)

                # apply conditions in reverse order (last wins)
                for cond in reversed(conditions):
                    check_expr = expr(cond["check"])  # condition always SQL
                    result_val = _normalize(cond["result"])

                    result_expr = when(check_expr, result_val).otherwise(result_expr)

                df = df.withColumn(field_name, result_expr)

            else:
                self.logger.warning(f"Unknown computed field type: {field_type}")

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
        
        # Trim join keys (SAS does implicit trim, PySpark requires explicit)
        from pyspark.sql.functions import trim
        df = df.withColumn('cdpole', trim(col('cdpole'))) \
               .withColumn('cdprod', trim(col('cdprod')))
        
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
