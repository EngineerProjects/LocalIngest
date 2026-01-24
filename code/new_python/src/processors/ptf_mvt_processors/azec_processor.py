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
    make_date, broadcast, create_map, sum as spark_sum,
    to_date
)
from pyspark.sql.types import (
    DoubleType, IntegerType, DateType, StringType
)

from src.processors.base_processor import BaseProcessor
from utils.loaders import get_default_loader
from config.constants import DIRCOM
from utils.helpers import build_layer_path, extract_year_month_int, compute_date_ranges
from utils.transformations import (
    apply_column_config,
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
        # Dates are already cast by BronzeReader._safe_cast() using dateFormat from reading_config.json
        # No need to re-cast here

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
        # ------------------------------------------------------------
        # Préparations vision / dates / config
        # ------------------------------------------------------------
        year_int, month_int = extract_year_month_int(vision)
        dates = compute_date_ranges(vision)  # clés en lowercase (dtfin, dtdeb_an, dtfinmn, dtfinmm1, finmois, …)

        loader = get_default_loader()
        azec_config = loader.get_azec_config()

        self.logger.info(f"[AZEC CFG] keys: {list(azec_config.keys())}")
        try:
            mapping = azec_config['capital_mapping']['mappings']
            self.logger.info(f"[AZEC CFG] capital_mapping size = {len(mapping)} | first={mapping[0] if mapping else None}")
        except Exception as e:
            self.logger.error(f"[AZEC CFG] capital_mapping missing or invalid: {e}")

        # ============================================================
        # STEP 01: Column Selection (SAS L52–85)
        # ============================================================
        self.logger.step(1, "Applying column configuration")
        column_config = azec_config['column_selection']
        df = apply_column_config(df, column_config, vision, year_int, month_int)

        # Constante DIRCOM
        df = df.withColumn('dircom', lit(DIRCOM.AZEC))

        # ============================================================
        # STEP 1.1 — Initialize columns (safe defaults like AZ)
        # ============================================================
        self.logger.step(1.1, "Initialize columns (safe defaults like AZ)")

        init_double = [
            "primecua", "primeto", "primes_ptf", "primes_afn", "primes_res",
            "expo_ytd", "expo_gli", "cotis_100",
            "smp_100", "smp_cie", "lci_100", "lci_cie",
            "perte_exp", "risque_direct", "value_insured",
            "mtca"
        ]
        init_int = [
            "nbptf", "nbafn", "nbres",
            "nbafn_anticipe", "nbres_anticipe",
            "top_coass", "top_lta", "top_revisable"
        ]
        init_date = ["dt_deb_expo", "dt_fin_expo"]
        init_str  = ["critere_revision", "cdgrev", "type_affaire"]

        for c in init_double:
            if c not in df.columns:
                df = df.withColumn(c, lit(0.0).cast(DoubleType()))
        for c in init_int:
            if c not in df.columns:
                df = df.withColumn(c, lit(0).cast(IntegerType()))
        for c in init_date:
            if c not in df.columns:
                df = df.withColumn(c, lit(None).cast(DateType()))
        for c in init_str:
            if c not in df.columns:
                df = df.withColumn(c, lit(None).cast(StringType()))

        # ============================================================
        # STEP 02: Business Filters (SAS WHERE)
        # ============================================================
        self.logger.step(2, "Applying business filters")
        
        initial_count = df.count()
        self.logger.info(f"[ROW COUNT] Before filters: {initial_count:,} rows")

        # Ces deux venaient du JSON
        df = df.filter(~col("intermed").isin("24050", "40490"))
        count_1 = df.count()
        self.logger.info(f"[ROW COUNT] After intermed filter: {count_1:,} rows (removed {initial_count - count_1:,})")
        
        df = df.filter(~col("police").isin("012684940"))
        count_2 = df.count()
        self.logger.info(f"[ROW COUNT] After police filter: {count_2:,} rows")

        # Filtre temporaires (Version SAS)
        df = df.filter(
            ~(
                (col("duree") == "00") &
                (~col("produit").isin("DO0", "TRC", "CTR", "CNR"))
            )
        )
        count_3 = df.count()
        self.logger.info(f"[ROW COUNT] After duree filter: {count_3:,} rows")

        # SAS L84: datfin ne effetpol (excludes when both equal)
        df = df.filter(
            col("datfin").isNull() |
            col("effetpol").isNull() |
            (col("datfin") != col("effetpol"))
        )
        count_4 = df.count()
        self.logger.info(f"[ROW COUNT] After datfin filter: {count_4:,} rows")

        df = df.filter(
            (~col("gestsit").isin("MIGRAZ")) |
            ((col("gestsit") == "MIGRAZ") & (col("etatpol") == "R"))
        )
        count_5 = df.count()
        self.logger.info(f"[ROW COUNT] After gestsit filter: {count_5:,} rows")
        
        if count_5 == 0:
            self.logger.error("[ZERO ROWS] All data lost after GESTSIT filter!")


        # ============================================================
        # STEP 03: Migration Handling (SAS L94–106)
        # ============================================================
        self.logger.step(3, "Handling AZEC migration")
        df = self._handle_migration(df, vision, azec_config)
        count_after_migration = df.count()
        self.logger.info(f"[ROW COUNT] After migration join: {count_after_migration:,} rows")

        # ============================================================
        # STEP 04: Data Quality Updates (SAS L113–137)
        # ============================================================
        self.logger.step(4, "Updating dates and policy states")
        df = self._update_dates_and_states(df, dates, year_int, month_int)
        count_after_dates = df.count()
        self.logger.info(f"[ROW COUNT] After date updates: {count_after_dates:,} rows")

        # ============================================================
        # STEP 05: Movement Indicators (SAS L144–182)
        # ============================================================
        self.logger.step(5, "Calculating movement indicators (NBPTF/NBAFN/NBRES)")
        df = self._calculate_movements(df, dates, year_int, month_int)
        count_after_movements = df.count()
        self.logger.info(f"[ROW COUNT] After movements: {count_after_movements:,} rows")

        # ============================================================
        # STEP 06: Suspension Days (SAS L189–194)
        # ============================================================
        self.logger.step(6, "Calculating suspension periods")
        from utils.transformations.operations.business_logic import calculate_azec_suspension
        df = calculate_azec_suspension(df, dates)

        # ============================================================
        # STEP 07: Exposures (AZEC) (SAS L195–203)
        # ============================================================
        self.logger.step(7, "Calculating exposures (AZEC)")
        from utils.transformations import calculate_exposures_azec
        df = calculate_exposures_azec(df, dates)

        # ============================================================
        # STEP 08: Segmentation - Simplified (use pre-built TABLE_SEGMENTATION_AZEC_MML)
        # SAS: LEFT JOIN REF.TABLE_SEGMENTATION_AZEC_MML t2 ON (t1.PRODUIT = t2.PRODUIT)
        # ============================================================
        self.logger.step(8, "Enriching LOB segmentation (AZEC)")
        
        # Read pre-built segmentation reference (like SAS REF.TABLE_SEGMENTATION_AZEC_MML)
        reader = get_bronze_reader(self)
        df_seg = reader.read_file_group('table_segmentation_azec_mml', 'ref')
        
        if df_seg is None or df_seg.count() == 0:
            raise RuntimeError(
                "TABLE_SEGMENTATION_AZEC_MML is required for AZEC segmentation. "
                "Ensure table_segmentation_azec_mml.csv exists in bronze/ref/"
            )
        
        # Dedup by PRODUIT (defensive, should already be unique)
        df_seg = df_seg.dropDuplicates(["produit"])
        
        # LEFT JOIN on PRODUIT (SAS: LEFT JOIN t2 ON t1.PRODUIT = t2.PRODUIT)
        df = df.alias("a").join(
            broadcast(df_seg.alias("s")),
            col("a.produit") == col("s.produit"),
            how="left"
        ).select(
            "a.*",
            col("s.segment"),
            col("s.cmarch"),
            col("s.cseg"),
            col("s.cssseg"),
            col("s.lmarch"),
            col("s.lseg"),
            col("s.lssseg")
        )
        
        # SAS WHERE: t2.CMARCH in ("6") and t1.GESTSIT <> 'MIGRAZ'
        # Deux conditions combinées (SAS L260)
        initial_count = df.count()
        df = df.filter(
            (col("cmarch") == "6") &
            (col("gestsit") != "MIGRAZ")
        )
        filtered_count = df.count()
        self.logger.info(f"Segmentation: {initial_count:,} → {filtered_count:,} rows (CMARCH='6' AND GESTSIT!='MIGRAZ')")
        
        # No CONSTRCU cache needed (simplified flow)
        self._cached_constrcu_ref = None

        # ============================================================
        # STEP 09: Premiums & Flags (SAS L210–265)
        # ============================================================
        self.logger.step(9, "Calculating premiums and flags")
        df = self._calculate_premiums(df)

        # ============================================================
        # NOTE: ISIC codification is NOT part of AZEC SAS baseline
        # (only used in AZ/CONSOLIDATION pipelines)
        # Previously caused data explosion with 6 superfluous LEFT JOINs
        # ============================================================

        # ============================================================
        # STEP 10: NAF Codes (SAS L272–302)
        # ============================================================
        self.logger.step(10, "Enriching NAF codes (FULL OUTER JOIN)")
        df = self._enrich_naf_codes(df, vision)

        # ============================================================
        # STEP 11: Formulas (SAS L309–332)
        # ============================================================
        self.logger.step(11, "Enriching formulas")
        df = self._enrich_formulas(df, vision)

        # ============================================================
        # STEP 12: Turnover (MULPROCU) (SAS L339–357)
        # ============================================================
        self.logger.step(12, "Enriching CA turnover")
        df = self._enrich_ca(df, vision)

        # ============================================================
        # STEP 13: PE/RD/VI Capitals (INCENDCU) (SAS L364–371)
        # ============================================================
        self.logger.step(13, "Enriching PE/RD/VI capitals")
        df = self._enrich_pe_rd_vi(df, vision)

        # ============================================================
        # STEP 14: Cleanup expo dates when expo_ytd=0 (SAS L374–380)
        # ============================================================
        self.logger.step(14, "Cleaning exposure dates when expo_ytd = 0")
        df = df.withColumn(
            'dt_deb_expo',
            when(col('expo_ytd') == 0, lit(None).cast(DateType())).otherwise(col('dt_deb_expo'))
        ).withColumn(
            'dt_fin_expo',
            when(col('expo_ytd') == 0, lit(None).cast(DateType())).otherwise(col('dt_fin_expo'))
        )

        # ============================================================
        # STEP 15: SMP/LCI Capitals (CAPITXCU) (SAS L387–420)
        # ============================================================
        self.logger.step(15, "Joining SMP/LCI capital data")
        df = self._join_capitals(df, vision)

        # ============================================================
        # STEP 16: Business Adjustments (SAS L449–466)
        # ============================================================
        self.logger.step(16, "Applying business adjustments (NBRES/NBAFN/NBPTF)")
        df = self._adjust_nbres(df)

        # ============================================================
        # STEP 17: Final Enrichment (CONSTRCU_AZEC + SITE + UPPER_MID) (SAS L484–487)
        # ============================================================
        self.logger.step(17, "Final enrichment (segment2/type_produit_2, site data, UPPER_MID)")
        df = self._enrich_final_constrcu_data(df, vision)

        # ORDER BY police (SAS L487)
        df = df.orderBy('police')

        self.logger.info("AZEC transformations completed successfully")
        return df

    def write(self, df: DataFrame, vision: str) -> None:
        # Guard: pas de colonnes dupliquées (case-insensitive)
        cols = df.columns
        lower = list(map(str.lower, cols))
        dups = [c for c in set(lower) if lower.count(c) > 1]
        if dups:
            raise RuntimeError(f"Duplicate column names before write: {dups}")

        from utils.helpers import write_to_layer
        write_to_layer(
            df=df,
            config=self.config,
            layer="silver",
            filename=f'azec_ptf_{vision}',
            vision=vision,
            logger=self.logger,
            optimize=True,
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

    def _update_dates_and_states(self, df, dates, annee, mois):
        """
        SAS-faithful reproduction of updates L113–137,
        but refactored using a clean Python dictionary-driven pattern
        (NOT config-driven, because SAS order must be preserved).
        
        Sequence implemented:
        1) datexpir = datfin when datfin > datexpir and etatpol in ('X','R')
        2) Anticipated flags for AFN/RES relative to FINMOIS
        3) Tacit renewals not invoiced for > 1 year → mark as terminated at DATTERME
        4) Temporaries → mark as terminated at FINPOL
        """
        from pyspark.sql.functions import col, lit, when, to_date
        from datetime import datetime

        def apply_overwrites(df_, condition, updates: dict):
            for cname, expr in updates.items():
                df_ = df_.withColumn(cname, when(condition, expr).otherwise(col(cname)))
            return df_

        # (1) DATEEXPIR correction
        cond_datexpir = (
            col("datfin").isNotNull()
            & col("datexpir").isNotNull()
            & (col("datfin") > col("datexpir"))
            & col("etatpol").isin("X", "R")
        )
        df = apply_overwrites(df, cond_datexpir, {"datexpir": col("datfin")})

        # (2) AFN/RES anticipés
        finmois = to_date(lit(dates["finmois"]))
        df = apply_overwrites(df, col("effetpol") > finmois, {"nbafn_anticipe": lit(1)})
        df = apply_overwrites(df, col("datfin") > finmois, {"nbres_anticipe": lit(1)})

        # (3) Tacite reconduction > 1 an
        python_boundary = datetime(annee - 1, mois, 1).strftime("%Y-%m-%d")
        boundary = to_date(lit(python_boundary))

        cond_tacite = (
            (col("duree") == "01")
            & col("finpol").isNull()
            & col("datterme").isNotNull()
            & (col("datterme") < boundary)
        )

        df = apply_overwrites(df, cond_tacite, {
            "etatpol": lit("R"),
            "datfin": col("datterme"),
            "datresil": col("datterme"),
        })

        # (4) Temporaires
        cond_temp = col("finpol").isNotNull() & col("datfin").isNull()
        df = apply_overwrites(df, cond_temp, {
            "etatpol": lit("R"),
            "datfin": col("finpol"),
            "datresil": col("finpol"),
        })

        return df

    def _calculate_movements(
        self,
        df: DataFrame,
        dates: dict,
        year: int,
        month: int
    ) -> DataFrame:
        """
        Compute AZEC movement indicators (NBPTF, NBAFN, NBRES) exactly as SAS (L144–182).

        Implementation notes:
        - Delegates the core SAS logic to:
            utils.transformations.operations.business_logic.calculate_azec_movements
            which handles:
            * Product list vs. out-of-list date windows (effetpol/datafn, datfin/datresil)
            * ETATPOL='R' precondition for AFN/RES
            * NBPTF_NON_MIGRES_AZEC=1 gating
            * Exclusions for specific product codes (e.g., 'CNR','DO0','TRC','CTR')
            * Current-month windows using dtfinmn and dtfinmn1
        - Adds anticipated flags using the month-end boundary (FINMOIS).
        - Ensures 0/1 integer outputs for flags to match SAS expectations.
        - Creates missing critical columns as NULL defensively (prevents runtime errors in tests).
        """
        from pyspark.sql.functions import col, lit
        from utils.transformations.operations.business_logic import calculate_azec_movements

        # Ensure critical columns exist
        for c in ["etatpol", "produit", "effetpol", "datafn", "datfin", "datresil"]:
            if c not in df.columns:
                df = df.withColumn(c, lit(None))

        # Default migration flag if missing (only happens in isolated tests)
        if "nbptf_non_migres_azec" not in df.columns:
            df = df.withColumn("nbptf_non_migres_azec", lit(1))
        
        # ========== DIAGNOSTIC: WHY NBAFN/NBRES = 0? ==========
        from pyspark.sql.functions import sum as _sum, count as _count, when
        
        self.logger.info("[AZEC DIAG] Pre-movements diagnostics:")
        
        # 1. Check ETATPOL distribution (AFN/RES need etatpol='R')
        try:
            r_count = df.filter(col("etatpol") == "R").count()
            self.logger.info(f"[AZEC DIAG] Rows with etatpol='R': {r_count:,} / {df.count():,}")
        except Exception as e:
            self.logger.warning(f"[AZEC DIAG] etatpol check failed: {e}")
        
        # 2. Check nbptf_non_migres_azec flag
        try:
            mig1_count = df.filter(col("nbptf_non_migres_azec") == 1).count()
            self.logger.info(f"[AZEC DIAG] nbptf_non_migres_azec=1: {mig1_count:,} / {df.count():,}")
        except Exception as e:
            self.logger.warning(f"[AZEC DIAG] migration flag check failed: {e}")
        
        # 3. Check date columns non-NULL ratio
        try:
            date_stats = df.select(
                _count("*").alias("total"),
                _sum(when(col("datafn").isNotNull(), 1).otherwise(0)).alias("datafn_non_null"),
                _sum(when(col("effetpol").isNotNull(), 1).otherwise(0)).alias("effetpol_non_null")
            ).collect()[0]
            
            self.logger.info(f"[AZEC DIAG] Date columns: datafn={date_stats['datafn_non_null']:,}/{date_stats['total']:,}, effetpol={date_stats['effetpol_non_null']:,}/{date_stats['total']:,}")
        except Exception as e:
            self.logger.warning(f"[AZEC DIAG] date check failed: {e}")
        # ========== END DIAGNOSTIC ==========

        # ========== DEEP DIAGNOSTIC - MOVEMENT LOGIC ==========
        try:
            from test_azec_movements_diagnostic import diagnose_azec_movements
            self.logger.info("[DEEP DIAG] Running comprehensive movement diagnostics...")
            stats = diagnose_azec_movements(df, year, month, dates['dtdeb_an'], dates['dtfinmn'])
            self.logger.info(f"[DEEP DIAG] Expected NBAFN=1: {stats['expected_nbafn']:,} rows")
        except Exception as e:
            self.logger.warning(f"[DEEP DIAG] Diagnostic failed: {e}")
        # ========== END DEEP DIAGNOSTIC ==========

        # SAS AFN/RES/PTF logic
        df = calculate_azec_movements(df, dates, year, month)

        # Cast flags to int as SAS does
        for flag in ["nbptf", "nbafn", "nbres", "nbafn_anticipe", "nbres_anticipe"]:
            if flag in df.columns:
                df = df.withColumn(flag, col(flag).cast("int"))
            else:
                df = df.withColumn(flag, lit(0).cast("int"))

        # Optional stats
        try:
            total = df.count()
            self.logger.info(
                f"AZEC movements: NBPTF={df.filter(col('nbptf')==1).count():,} "
                f"NBAFN={df.filter(col('nbafn')==1).count():,} "
                f"NBRES={df.filter(col('nbres')==1).count():,} (total={total:,})"
            )
        except Exception:
            pass

        return df

    def _join_capitals(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Join capital data from CAPITXCU.

        CAPITXCU is REQUIRED - capitals are critical for AZEC analysis.
        Processing will fail if table is missing (matching SAS behavior).

        Note: Numeric precision tolerance of 0.01€ is acceptable when comparing
        PySpark results with SAS reference outputs due to floating-point arithmetic.

        Args:
            df: AZEC DataFrame
            vision: Vision in YYYYMM format

        Returns:
            DataFrame with SMP/LCI capitals

        Raises:
            RuntimeError: If required CAPITXCU table is unavailable
        """
        from pyspark.sql.functions import col, lit, coalesce, sum as spark_sum, when
        reader = get_bronze_reader(self)

        # 0) Fail-fast mapping vide
        if not AZEC_CAPITAL_MAPPING:
            raise RuntimeError(
                "AZEC_CAPITAL_MAPPING est vide. Vérifie config/transformations/azec_transformations.json "
                "→ 'capital_mapping.mappings'."
            )

        # 1) Lecture CAPITXCU
        try:
            df_cap = reader.read_file_group('capitxcu_azec', vision)
        except FileNotFoundError as e:
            self.logger.error("CRITICAL: CAPITXCU is REQUIRED for AZEC processing")
            raise RuntimeError("Missing required capital data: CAPITXCU") from e

        if df_cap is None or not df_cap.columns:
            raise RuntimeError("CAPITXCU est vide ou introuvable (aucune colonne). Vérifie reading_config.json (capitxcu_azec).")

        # 2) Colonnes clés attendues
        required_key_cols = {"police", "produit", "smp_sre", "brch_rea"}
        missing_keys = [c for c in required_key_cols if c not in df_cap.columns]
        if missing_keys:
            self.logger.error("[CAPITXCU] Colonnes clés manquantes: %s", missing_keys)
            self.logger.error("[CAPITXCU] Colonnes disponibles: %s", df_cap.columns)
            try:
                self.logger.error("[CAPITXCU] Sample(5): %s", df_cap.select(
                    *[c for c in required_key_cols if c in df_cap.columns]
                ).limit(5).toPandas().to_string(index=False))
            except Exception:
                pass
            raise RuntimeError(
                f"CAPITXCU ne contient pas les colonnes clés requises: {missing_keys}. "
                f"Vérifie le schéma de capitxcu_azec dans reading_config.json et le fichier source."
            )

        # 3) Vérifier au moins une source mappée
        mapped_sources = list({m["source"] for m in AZEC_CAPITAL_MAPPING})
        existing_sources = [s for s in mapped_sources if s in df_cap.columns]
        if not existing_sources:
            self.logger.error("[CAPITXCU] Aucune colonne source mappée trouvée. Attendues (extrait): %s", mapped_sources)
            self.logger.error("[CAPITXCU] Colonnes disponibles: %s", df_cap.columns)
            raise RuntimeError(
                "Aucune colonne source de la mapping n'existe dans CAPITXCU (ex: capx_100, capx_cua). "
                "Vérifie le schéma CAPITXCU."
            )

        # 4) Réduction du plan
        needed_cols = set(required_key_cols) | set(existing_sources)
        df_cap = df_cap.select(*[c for c in needed_cols if c in df_cap.columns])

        # 5) Créer les colonnes cibles (si source existe)
        created_targets = []
        for m in AZEC_CAPITAL_MAPPING:
            src = m["source"]
            tgt = m["target"]
            if src in df_cap.columns:
                df_cap = df_cap.withColumn(
                    tgt,
                    when(
                        (col("smp_sre") == m["smp_sre"]) & (col("brch_rea") == m["brch_rea"]),
                        coalesce(col(src), lit(0))
                    ).otherwise(lit(0))
                )
                created_targets.append(tgt)
            else:
                self.logger.debug(f"[CAPITXCU] Source absente, target ignorée: {tgt} (source={src})")

        if not created_targets:
            self.logger.error("[CAPITXCU] Aucune target créée. Sources existantes: %s", existing_sources)
            self.logger.error("[CAPITXCU] Mapping: %s", AZEC_CAPITAL_MAPPING)
            self.logger.error("[CAPITXCU] Colonnes: %s", df_cap.columns)
            raise RuntimeError(
                "Impossible de créer des colonnes cibles (targets) depuis CAPITXCU. "
                "Vérifie que les sources (capx_100, capx_cua) et smp_sre/brch_rea correspondent."
            )

        # 7) Agrégats par (police, produit)
        df_cap_agg = df_cap.groupBy("police", "produit").agg(
            *[spark_sum(c).alias(c) for c in created_targets]
        )

        # 8) Totaux globaux
        def safe_add(df_, a, b, out):
            a_col = col(a) if a in df_.columns else lit(0)
            b_col = col(b) if b in df_.columns else lit(0)
            return df_.withColumn(out, a_col + b_col)

        df_cap_agg = safe_add(df_cap_agg, "lci_pe_100", "lci_dd_100", "lci_100")
        df_cap_agg = safe_add(df_cap_agg, "lci_pe_cie", "lci_dd_cie", "lci_cie")
        df_cap_agg = safe_add(df_cap_agg, "smp_pe_100", "smp_dd_100", "smp_100")
        df_cap_agg = safe_add(df_cap_agg, "smp_pe_cie", "smp_dd_cie", "smp_cie")

        # 9) Join (police, produit) via helper — évite toute duplication
        df = self.coalesce_from_right(
            df,
            df_cap_agg,
            keys=["police", "produit"],
            cols=["smp_100", "smp_cie", "lci_100", "lci_cie"]
        )

        self.logger.info("✓ Capital data joined successfully (fail-fast mode)")
        return df

    def _adjust_nbres(self, df: DataFrame) -> DataFrame:
        """
        Apply AZEC-specific NBRES and NBAFN adjustments (SAS L448-466).
        
        SAS L452-456: Sets NBPTF=0 and NBRES=0 for excluded products (DO0/TRC/CTR/CNR)
        SAS L459: Exclude replacements from RES (RMPLCANT not empty AND MOTIFRES='RP')
        SAS L462: Exclude specific termination reasons from RES (MOTIFRES in 'SE','SA')
        SAS L465: Exclude CSSSEG='5' from AFN
        
        Args:
            df: AZEC DataFrame
        
        Returns:
            DataFrame with adjusted NBRES, NBAFN, and NBPTF
        """
        excluded_products = col("produit").isin(['DO0', 'TRC', 'CTR', 'CNR'])

        # Adjust NBRES (SAS L452-456, L459, L462)
        nbres_adjusted = (
            when(excluded_products, lit(0))  # SAS L452-456: Excluded products
            .when(
                (col("nbres") == 1) &
                (col("rmplcant").isNotNull()) &
                (col("rmplcant") != "") &
                col("motifres").isin(['RP']),
                lit(0)  # SAS L459: Replacements
            )
            .when(
                (col("nbres") == 1) &
                col("motifres").isin(['SE', 'SA']),
                lit(0)  # SAS L462: Specific termination reasons
            )
            .otherwise(col("nbres"))
        )

        # Adjust NBAFN (SAS L465)
        if "cssseg" in df.columns:
            nbafn_adjusted = when(
                (col("nbafn") == 1) & (col("cssseg") == "5"),
                lit(0)
            ).otherwise(col("nbafn"))
        else:
            nbafn_adjusted = col("nbafn")

        # Adjust NBPTF (SAS L452-456 ONLY - no recalculation)
        # SAS simply sets NBPTF=0 for excluded products, nothing else
        nbptf_adjusted = when(excluded_products, lit(0)).otherwise(col("nbptf"))

        df = df.withColumn("nbres", nbres_adjusted)
        df = df.withColumn("nbafn", nbafn_adjusted)
        df = df.withColumn("nbptf", nbptf_adjusted)

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

                # ⬇️ Coalesce depuis la droite sans dupliquer la colonne 'mtca'
                df = self.coalesce_from_right(
                    df,                  # left: AZEC DF courant
                    df_mulprocu_agg,     # right: CA agrégé par police
                    keys=["police"],
                    cols=["mtca"]
                )
                self.logger.info("MULPROCU CA data joined")
        except Exception as e:
            self.logger.warning(f"MULPROCU not available: {e}")
            if 'mtca' not in df.columns:
                df = df.withColumn('mtca', lit(None).cast(DoubleType()))

        self.logger.info("✓ CA turnover enriched (mtca)")
        return df 

    def coalesce_from_right(self, df, right_df, keys, cols):
        """
        Join df with right_df on `keys` and coalesce target `cols` from right to left,
        without ever referencing the left side by alias (prevents ambiguous `l.key`).
        - Right columns are temporarily renamed with a __r_ prefix,
        then coalesced into left columns and dropped.
        """
        import pyspark.sql.functions as F

        # 1) Renommer les colonnes cibles du RIGHT pour éviter tout écrasement de nom
        r = right_df
        for c in cols:
            if c in r.columns:
                r = r.withColumnRenamed(c, f"__r_{c}")

        # 2) Join sur les clés (using join). Les colonnes de clé restent au niveau racine.
        out = df.join(r, on=keys, how="left")

        # 3) Coalesce: priorité à la valeur right (__r_col) si présente, sinon left (col)
        for c in cols:
            rc = f"__r_{c}"
            out = out.withColumn(c, F.coalesce(F.col(rc), F.col(c)))

        # 4) Nettoyage: drop des colonnes temporaires du RIGHT
        out = out.drop(*[f"__r_{c}" for c in cols])

        return out

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
                from pyspark.sql.functions import sum as spark_sum, col, lit, coalesce
                df_pe_rd = (df_incendcu
                    .groupBy('police', 'produit')
                    .agg(
                        spark_sum('mt_baspe').alias('perte_exp'),
                        spark_sum('mt_basdi').alias('risque_direct')
                    )
                    .withColumn(
                        'value_insured',
                        coalesce(col('perte_exp'), lit(0)) + coalesce(col('risque_direct'), lit(0))
                    )
                )

                # ⬇️ Coalesce depuis la droite sur (police, produit)
                df = self.coalesce_from_right(
                    df,
                    df_pe_rd,
                    keys=["police", "produit"],
                    cols=["perte_exp", "risque_direct", "value_insured"]
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

    def _enrich_final_constrcu_data(self, df: DataFrame, vision: str) -> DataFrame:
        """
        SAS L484-486: Final enrichment with triple LEFT JOIN:
          1. CONSTRCU_AZEC → segment2, type_produit_2
          2. CONSTRCU.CONSTRCU raw → site data (datouvch, ldestloc, etc.)
          3. PT_GEST → upper_mid
        
        Uses cached CONSTRCU reference from step 08 to avoid recomputation.
        
        Args:
            df: Main AZEC DataFrame
            vision: Vision in YYYYMM format
            
        Returns:
            DataFrame with segment2, type_produit_2, site data, and upper_mid
        """
        from utils.processor_helpers import get_bronze_reader
        from types import SimpleNamespace
        from pyspark.sql.functions import col
        
        reader = get_bronze_reader(SimpleNamespace(
            spark=self.spark,
            config=self.config,
            logger=self.logger
        ))
        
        # ============================================================
        # 1. CONSTRCU_AZEC join (segment2/type_produit_2)
        # ============================================================
        # Build CONSTRCU_AZEC (SAS REF_segmentation_azec.sas L341-344)
        # SAS creates CONSTRCU_AZEC with: KEEP POLICE CDPROD SEGMENT TYPE_PRODUIT
        # We use load_constrcu_reference() to compute SEGMENT/TYPE_PRODUIT
        try:
            from utils.transformations.enrichment.segmentation_enrichment import load_constrcu_reference
            
            lob_ref = reader.read_file_group("lob", "ref").filter(col("cmarch") == "6")
            
            df_constrcu_azec = load_constrcu_reference(
                spark=self.spark,
                config=self.config,
                vision=vision,
                lob_ref=lob_ref,
                logger=self.logger
            )
            
            if df_constrcu_azec is not None:
                self.logger.debug(f"[CONSTRCU_AZEC] Built reference: {df_constrcu_azec.count():,} rows")
            else:
                raise RuntimeError("load_constrcu_reference returned None")
                
        except Exception as e:
            self.logger.warning(f"[CONSTRCU_AZEC] Failed to build reference: {e}")
            from utils.processor_helpers import add_null_columns
            from pyspark.sql.types import StringType
            df = add_null_columns(df, {
                'segment2': StringType,
                'type_produit_2': StringType
            })
            df_constrcu_azec = None
        
        # If CONSTRCU_AZEC loaded, join on (police, produit)
        if df_constrcu_azec is not None:
            # CSV contains 'produit' (renamed from SAS CDPROD), need to alias to cdprod for join
            available_cols = df_constrcu_azec.columns
            
            # Check if segment/type_produit exist (may be missing if file is raw CONSTRCU, not CONSTRCU_AZEC)
            if "segment" in available_cols and "type_produit" in available_cols:
                df_constrcu_azec_small = df_constrcu_azec.select(
                    col("police"),
                    col("produit").alias("cdprod"),
                    col("segment").alias("segment2"),
                    col("type_produit").alias("type_produit_2")
                ).dropDuplicates(["police", "cdprod"])
            else:
                self.logger.warning(f"[CONSTRCU_AZEC] Missing segment/type_produit columns. Available: {available_cols}")
                # Create empty DataFrame if columns missing
                from pyspark.sql.types import StructType, StructField, StringType
                schema = StructType([
                    StructField("police", StringType(), True),
                    StructField("cdprod", StringType(), True),
                    StructField("segment2", StringType(), True),
                    StructField("type_produit_2", StringType(), True)
                ])
                df_constrcu_azec_small = self.spark.createDataFrame([], schema)
        else:
            # Create empty DataFrame with correct schema if CONSTRCU_AZEC not available
            from pyspark.sql.types import StructType, StructField, StringType
            schema = StructType([
                StructField("police", StringType(), True),
                StructField("cdprod", StringType(), True),
                StructField("segment2", StringType(), True),
                StructField("type_produit_2", StringType(), True)
            ])
            df_constrcu_azec_small = self.spark.createDataFrame([], schema)
            self.logger.debug("[CONSTRCU_AZEC] Using empty DataFrame (reference unavailable)")
        
        # ============================================================
        # 2. CONSTRCU.CONSTRCU raw join (site data)
        # ============================================================
        # SAS L485: CONSTRCU.CONSTRCU (permanent library, static reference)
        df_const = reader.read_file_group("constrcu", "ref")
        
        df_const = df_const.select(
            col("police"),
            col("produit"),
            col("datouvch"),
            col("ldestloc"),
            col("mnt_glob"),
            col("datrecep"),
            col("dest_loc"),
            col("datfinch"),
            col("lqualite")
        ).dropDuplicates(["police", "produit"])
        
        # ============================================================
        # 3. PT_GEST join (UPPER_MID)
        # ============================================================
        # SAS L486: TABLE_PT_GEST (versioned table with Upper_Mid)
        df_ptgest = reader.read_file_group("table_pt_gest", vision)
        
        df_ptgest = df_ptgest.select(
            col("ptgst").alias("_ptgst_key"),
            col("upper_mid")
        ).dropDuplicates(["_ptgst_key"])
        
        # ============================================================
        # 4. Triple LEFT JOIN (SAS L484-486)
        # ============================================================
        df = (
            df.alias("a")
            .join(
                df_constrcu_azec_small.alias("b"),
                (col("a.police") == col("b.police")) & (col("a.produit") == col("b.cdprod")),
                "left"
            )
            .join(
                df_const.alias("c"),
                (col("a.police") == col("c.police")) & (col("a.produit") == col("c.produit")),
                "left"
            )
            .join(
                df_ptgest.alias("d"),
                col("a.poingest") == col("d._ptgst_key"),
                "left"
            )
        )
        
        # Select all 'a' columns + new enrichment columns
        df = df.select(
            "a.*",
            col("b.segment2"),
            col("b.type_produit_2"),
            col("c.datouvch"),
            col("c.ldestloc"),
            col("c.mnt_glob"),
            col("c.datrecep"),
            col("c.dest_loc"),
            col("c.datfinch"),
            col("c.lqualite"),
            col("d.upper_mid")
        )
        
        # ============================================================
        # 5. Cleanup: Drop LOB columns not in AZEC schema
        # ============================================================
        # SAS consolidation (L75): PRODUIT AS CDPROD (AZEC keeps 'produit')
        # SAS AZEC SELECT (L55-79): Uses CMARCH, CSEG, CSSSEG but NOT other LOB fields
        # Drop LOB enrichment columns added by enrich_segmentation_sas that are NOT in AZEC schema
        lob_cols_to_drop = [
            "cdprod",    # AZEC uses 'produit', not 'cdprod'
            "cprod",     # Internal LOB code, not in AZEC
            "lmarch",    # Label, not needed
            "lmarch2",   # Derived label, not needed
            "lseg",      # Segment label, not in AZEC (only cseg)
            "lssseg",    # Sub-segment label, not in AZEC (only cssseg)  
            "lprod",     # Product label, not in AZEC
            "activite"   # Only used for type_produit mapping, not in final AZEC
        ]
        cols_to_drop = [c for c in lob_cols_to_drop if c in df.columns]
        if cols_to_drop:
            df = df.drop(*cols_to_drop)
            self.logger.debug(f"Dropped LOB columns not in AZEC schema: {cols_to_drop}")
        
        self.logger.info("✓ Final CONSTRCU_AZEC + site data + UPPER_MID enrichment completed")
        
        return df