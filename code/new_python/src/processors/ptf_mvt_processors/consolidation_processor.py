# -*- coding: utf-8 -*-
"""
===============================================================================
PROCESSEUR DE CONSOLIDATION (Mouvements de Portefeuille)
===============================================================================

Ce processeur fusionne les flux de mouvements "AZ" (Agents & Courtiers) et "AZEC" (Construction)
pour produire une vision consolidée "Silver" et finalement "Gold".

OBJECTIFS DU PROCESSEUR :
------------------------
1. Lire les sorties Silver des processeurs AZ et AZEC.
2. Harmoniser les schémas (renommage, alignement des types).
3. Fusionner les deux flux en un seul DataFrame.
4. Enrichir avec des données transverses (Clients, Risques IRD, Codes ISIC).
5. Appliquer les règles de gestion finales (partenariats, redressements).
6. Produire le fichier Gold final.

FLUX DE TRAITEMENT :
-------------------
Le traitement est conditionnel à la vision traitée :
- Si Vision >= 201211 : Fusion AZ + AZEC.
- Si Vision < 201211 : AZ uniquement (AZEC n'existait pas ou n'est pas périmètré).
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    when,
    lit,
    coalesce,
    upper,
    broadcast,
    month,
    dayofmonth,
    row_number,
    trim,
    lpad,
)
from pyspark.sql.types import StringType, DateType, DoubleType
from pyspark.sql.window import Window
from src.processors.base_processor import BaseProcessor
from src.reader import SilverReader, BronzeReader
from utils.loaders import get_default_loader
from config.constants import DIRCOM
from utils.helpers import build_layer_path, extract_year_month_int
from utils.processor_helpers import safe_reference_join


class ConsolidationProcessor(BaseProcessor):
    """
    =======================================================================
    CONSOLIDATION DES MOUVEMENTS (SILVER → GOLD)
    =======================================================================

    Fusionne, harmonise et enrichit les flux AZ et AZEC.

    WORKFLOW DÉTAILLÉ :
    ------------------
    1. READ : Lecture du flux AZ (toujours présent).
    2. TRANSFORM :
       - Lecture conditionnelle du flux AZEC.
       - Harmonisation des noms de colonnes (via configuration).
       - Union des deux flux (Priorité AZ en cas de doublon police).
       - Calculs communs (Mois/Jour échéance).
       - Enrichissements référentiels (DO_DEST, Risques IRD, Clients, Euler, ISIC).
       - Redressements et flags business (Partenariats).
    3. WRITE : Écriture en couche Gold avec schéma strict.
    """

    def read(self, vision: str) -> DataFrame:
        """
        Lit les données Silver AZ.

        Note : Les données AZEC sont lues dans l'étape Transform car leur présence
        dépend de la date de vision.

        PARAMÈTRES :
        -----------
        vision : str
            Vision (Période) à traiter

        RETOUR :
        -------
        DataFrame
            Données AZ Silver
        """
        self.logger.info("Lecture des données Silver AZ (mvt_const_ptf)")

        from src.reader import SilverReader

        reader = SilverReader(self.spark, self.config)

        # Lecture du fichier AZ
        df_az = reader.read_silver_file(f"mvt_const_ptf_{vision}", vision)

        return df_az

    def transform(self, df_az: DataFrame, vision: str) -> DataFrame:
        """
        Orchestre la consolidation AZ + AZEC et les enrichissements finaux.

        PARAMÈTRES :
        -----------
        df_az : DataFrame
            Données AZ
        vision : str
            Vision traitée

        RETOUR :
        -------
        DataFrame
            Données consolidées et enrichies
        """
        year_int, month_int = extract_year_month_int(vision)

        # Chargement de la configuration de consolidation
        loader = get_default_loader()
        consolidation_config = loader.get_consolidation_config()

        silver_reader = SilverReader(self.spark, self.config)

        # -----------------------------
        # ÉTAPE A : Lecture AZEC conditionnelle
        # -----------------------------
        # La fusion AZ+AZEC ne s'applique qu'à partir de Novembre 2012
        if int(vision) >= 201211:
            self.logger.step(1, f"Lecture AZEC (Vision {vision} >= 201211)")
            try:
                df_azec = silver_reader.read_silver_file(f"azec_ptf_{vision}", vision)
            except Exception as e:
                self.logger.warning(f"Fichier AZEC manquant pour vision éligible : {e}")
                df_azec = None
        else:
            self.logger.step(1, f"Pas de lecture AZEC (Vision {vision} < 201211)")
            df_azec = None

        # -----------------------------------
        # ÉTAPE B : Harmonisation Schéma AZ
        # -----------------------------------
        self.logger.step(2, "Harmonisation du schéma AZ")
        az_harmonization = consolidation_config["az_harmonization"]
        df_az = self._harmonize_schema(df_az, az_harmonization)

        # --------------------------------------
        # ÉTAPE C : Harmonisation Schéma AZEC
        # --------------------------------------
        if df_azec is not None:
            self.logger.step(3, "Harmonisation du schéma AZEC")
            azec_harmonization = consolidation_config["azec_harmonization"]
            df_azec = self._harmonize_schema(df_azec, azec_harmonization)

            # Ajout marqueur source AZEC
            df_azec = df_azec.withColumn("dircom", lit(DIRCOM.AZEC))

            # -----------------------------
            # ÉTAPE D : Fusion AZ + AZEC
            # -----------------------------
            self.logger.step(4, "Fusion des flux (Union + Déduplication)")

            # Union permissive (les colonnes manquantes sont mises à NULL)
            df_consolidated = df_az.unionByName(df_azec, allowMissingColumns=True)

            # Déduplication avec priorité AZ sur AZEC
            # Si une même police existe dans les deux flux, on garde la version AZ
            priority = when(col("dircom").isin("AZ", "AZ "), lit(1)).otherwise(lit(0))

            df_consolidated = (
                df_consolidated.withColumn("_priority_dircom", priority)
                .orderBy(col("_priority_dircom").desc(), col("nopol"))
                .dropDuplicates(["nopol"])
                .drop("_priority_dircom")
            )
        else:
            # Si pas d'AZEC, la consolidation est égale à AZ
            df_consolidated = df_az

        # ----------------------------------------------------------
        # ÉTAPE E : Calcul Mois/Jour Echéance
        # ----------------------------------------------------------
        self.logger.step(4.1, "Extraction Mois/Jour Echéance")
        if "dtechann" in df_consolidated.columns:
            # S'assurer que c'est bien 5 caractères avec leading zeros
            dtechann_str = lpad(col("dtechann"), 5, "0")

            # Extraire mois : les 2 premiers caractères
            df_consolidated = df_consolidated.withColumn(
                "mois_echeance",
                when(
                    col("dtechann").isNotNull(), dtechann_str.substr(1, 2).cast("int")
                ).otherwise(lit(None)),
            )

            # Extraire jour : les 2 derniers caractères
            df_consolidated = df_consolidated.withColumn(
                "jour_echeance",
                when(
                    col("dtechann").isNotNull(), dtechann_str.substr(4, 2).cast("int")
                ).otherwise(lit(None)),
            )

            # Extraire jour : les 2 derniers caractères
            df_consolidated = df_consolidated.withColumn(
                "jour_echeance",
                when(
                    col("dtechann").isNotNull(), dtechann_str.substr(4, 2).cast("int")
                ).otherwise(lit(None)),
            )

        # ----------------------------------------------
        # ÉTAPE F : Transformations Communes
        # ----------------------------------------------
        self.logger.step(4.5, "Application des transformations communes")
        df_consolidated = self._apply_common_transformations(df_consolidated)

        # ----------------------------------------------
        # ÉTAPE G : Enrichissement DO_DEST
        # ----------------------------------------------
        self.logger.step(4.6, "Enrichissement Référentiel DO_DEST")
        df_consolidated = self._enrich_do_dest(df_consolidated, vision)

        # -------------------------------------------------------------
        # ÉTAPE H : Consolidation DESTINAT
        # -------------------------------------------------------------
        self.logger.step(4.7, "Consolidation du champ DESTINAT")
        from utils.transformations.enrichment import apply_destinat_consolidation_logic

        df_consolidated = apply_destinat_consolidation_logic(df_consolidated)

        # -----------------------------------------
        # ÉTAPE I : Risques IRD (Q46/Q45/QAN)
        # -----------------------------------------
        self.logger.step(5.5, "Enrichissement Données Risques IRD")
        df_consolidated = self._enrich_ird_risk(df_consolidated, vision)

        # -------------------------------------------------
        # ÉTAPE J : Logique de Repli (Fallbacks)
        # -------------------------------------------------
        self.logger.step(5.7, "Application des valeurs par défaut (Fallbacks)")
        df_consolidated = self._apply_fallback_logic(df_consolidated)

        # ---------------------------------------
        # ÉTAPE K : Enrichissement Client (SIREN/SIRET)
        # ---------------------------------------
        self.logger.step(5.8, "Enrichissement Données Client")
        df_consolidated = self._enrich_client_data(df_consolidated, vision)

        # --------------------------------------
        # ÉTAPE L : Note de Risque Euler
        # --------------------------------------
        self.logger.step(5.9, "Enrichissement Note Euler")
        df_consolidated = self._enrich_euler_risk_note(df_consolidated, vision)

        # ---------------------------------------------------
        # ÉTAPE M : Activité Produit Spécial (IPFM...)
        # ---------------------------------------------------
        self.logger.step(6, "Enrichissement Activités Spéciales")
        df_consolidated = self._enrich_special_product_activity(df_consolidated, vision)

        # ---------------------------------------------------
        # ÉTAPE N : Codes NAF (W6 + Client) et ISIC
        # ---------------------------------------------------
        self.logger.step(6.05, "Correction Codes NAF (W6)")
        df_consolidated = self._enrich_w6_naf_and_client_cdnaf(df_consolidated)

        self.logger.step(6.1, "Codification ISIC et Hazard Grades")
        df_consolidated = self._add_isic_codes(df_consolidated, vision)

        self.logger.step(6.2, "Ajout Code ISIC Global")
        df_consolidated = self._add_isic_global_code(df_consolidated)

        self.logger.step(6.3, "Corrections Manuelles ISIC Global")
        df_consolidated = self._apply_isic_gbl_corrections(df_consolidated)

        # ---------------------------------------------------------
        # ÉTAPE O : Indicateurs Business (Partenariats)
        # ---------------------------------------------------------
        self.logger.step(6.4, "Calcul Indicateurs Partenariats (Berlioz...)")
        df_consolidated = self._add_business_flags(df_consolidated)

        # ---------------------------------------------------------
        # ÉTAPE P : Colonnes Manquantes (Placeholders)
        # ---------------------------------------------------------
        self.logger.step(6.9, "Ajout des colonnes techniques manquantes")
        df_consolidated = self._add_placeholders(df_consolidated)

        self.logger.success("Consolidation terminée avec succès")
        return df_consolidated

    def _add_business_flags(self, df: DataFrame) -> DataFrame:
        """
        Ajoute des indicateurs spécifiques pour certains intermédiaires.

        LOGIQUE METIER :
        - TOP_BERLIOZ : Intermédiaire "4A5766"
        - TOP_PARTENARIAT : Intermédiaires "4A6160", "4A6947", "4A6956"

        PARAMÈTRES :
        -----------
        df : DataFrame
            Données consolidées

        RETOUR :
        -------
        DataFrame
            Avec colonnes TOP_BERLIOZ et TOP_PARTENARIAT
        """
        # Flag Berlioz
        df = df.withColumn(
            "top_berlioz", when(col("noint") == "4A5766", lit(1)).otherwise(lit(0))
        )

        # Flag Partenariat
        df = df.withColumn(
            "top_partenariat",
            when(col("noint").isin(["4A6160", "4A6947", "4A6956"]), lit(1)).otherwise(
                lit(0)
            ),
        )

        return df

    def write(self, df: DataFrame, vision: str) -> None:
        """
        Écrit les données consolidées en couche Gold.

        Cette méthode garantit que le schéma de sortie respecte STRICTEMENT
        la définition du modèle Gold (ordre et présence des colonnes).

        PARAMÈTRES :
        -----------
        df : DataFrame
            Données consolidées
        vision : str
            Vision traitée
        """
        from config.column_definitions import GOLD_COLUMNS_PTF_MVT

        # Alignement nommage (desti_isic vs destinat_isic)
        if "desti_isic" in df.columns:
            df = df.withColumnRenamed("desti_isic", "destinat_isic")

        # Ajout des colonnes Hazard Grades manquantes comme placeholders (NULL)
        # Ces colonnes existent dans le modèle cible mais ne sont pas calculées
        # par la codification ISIC actuelle.
        missing_hazard_cols = [
            "hazard_grades_bi",
            "hazard_grades_do",
            "hazard_grades_fire",
            "hazard_grades_rca",
            "hazard_grades_rcd",
            "hazard_grades_rce",
            "hazard_grades_trc",
        ]
        for col_name in missing_hazard_cols:
            if col_name not in df.columns:
                df = df.withColumn(col_name, lit(None).cast("string"))

        # Filtrage strict sur les colonnes du modèle Gold
        existing_gold_cols = [c for c in GOLD_COLUMNS_PTF_MVT if c in df.columns]

        # Vérification Qualité : Colonnes manquantes
        missing_cols = set(GOLD_COLUMNS_PTF_MVT) - set(df.columns)
        if missing_cols:
            self.logger.warning(
                f"Colonnes GOLD manquantes (seront absentes du fichier) : {sorted(list(missing_cols))[:10]} ..."
            )

        # Nettoyage des colonnes techniques intermédiaires
        extra_cols = set(df.columns) - set(GOLD_COLUMNS_PTF_MVT)
        if extra_cols:
            self.logger.info(
                f"Suppression de {len(extra_cols)} colonnes intermédiaires avant écriture"
            )

        df_final = df.select(existing_gold_cols)

        from utils.helpers import write_to_layer

        write_to_layer(
            df=df_final,
            config=self.config,
            layer="gold",
            filename=f"ptf_mvt_{vision}",
            vision=vision,
            logger=self.logger,
        )

    def _harmonize_schema(self, df: DataFrame, harmonization_config: dict) -> DataFrame:
        """
        Harmonise le schéma des données entrantes via configuration.

        Gère le renommage et la création de colonnes calculées simples
        pour aligner AZ et AZEC sur le même modèle de données.

        PARAMÈTRES :
        -----------
        df : DataFrame
            Données en entrée
        harmonization_config : dict
            Configuration d'harmonisation (variables.py)

        RETOUR :
        -------
        DataFrame
            Données harmonisées
        """
        # 1. Renommage simple
        rename_mapping = harmonization_config.get("rename", {})
        for old_name, new_name in rename_mapping.items():
            old_l = old_name.lower()
            new_l = new_name.lower()

            if old_l == new_l:
                continue

            # Cas de collision : Source ET Cible existent
            # On garde la cible (supposée bonne), on supprime la source
            if old_l in df.columns and new_l in df.columns:
                df = df.drop(old_l)
                continue

            # Cas standard : Renommage
            if old_l in df.columns and new_l not in df.columns:
                df = df.withColumnRenamed(old_l, new_l)

        # 2. Colonnes calculées (Constantes, Extraction dates, Alias)
        computed_mapping = harmonization_config.get("computed", {})
        for col_name, comp_config in computed_mapping.items():
            comp_type = comp_config.get("type")

            if comp_type == "constant":
                value = comp_config["value"]
                # Cast nécessaire pour éviter le type Void (incompatible Parquet)
                if value is None:
                    df = df.withColumn(col_name.lower(), lit(None).cast("string"))
                else:
                    df = df.withColumn(col_name.lower(), lit(value))

            elif comp_type == "month_extract":
                source = comp_config["source"].lower()
                if source in df.columns:
                    df = df.withColumn(col_name.lower(), month(col(source)))

            elif comp_type == "day_extract":
                source = comp_config["source"].lower()
                if source in df.columns:
                    df = df.withColumn(col_name.lower(), dayofmonth(col(source)))

            elif comp_type == "alias":
                source = comp_config["source"].lower()
                if source in df.columns:
                    df = df.withColumn(col_name.lower(), col(source))

        return df

    def _add_placeholders(self, df: DataFrame) -> DataFrame:
        """
        Ajoute des colonnes vides (NULL) pour les enrichissements manquants.
        Permet de garantir la stabilité du schéma même si certains référentiels sont absents.
        """
        missing_enrichments = [
            "seg_label",
            "incendcu_label",
            "client_naf_label",
            "isic_code",
            "isic_label",
            "geo_region",
            "geo_department",
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

    def _apply_common_transformations(self, df: DataFrame) -> DataFrame:
        """
        Applique les transformations communes aux deux flux.
        """
        loader = get_default_loader()
        consolidation_config = loader.get_consolidation_config()
        common_transforms = consolidation_config.get("common_transformations", {})

        # Nettoyage CDTRE (suppression préfixe '*')
        if "cdtre" in df.columns:
            df = df.withColumn(
                "cdtre",
                when(col("cdtre").startswith("*"), col("cdtre").substr(2, 3)).otherwise(
                    col("cdtre")
                ),
            )

        # Application transformations configurées (Partenariats)
        partnership_config = common_transforms.get("partnership_flags", {})
        flag_transforms = partnership_config.get("transformations", [])

        if flag_transforms and "noint" in df.columns:
            from utils.transformations import apply_transformations

            df = apply_transformations(df, flag_transforms)

        return df

    def _apply_fallback_logic(self, df: DataFrame) -> DataFrame:
        """
        Applique des règles de repli pour les dates manquantes.
        Ex: Si DTRCPPR (Date Réception) est vide, on utilise DTREFFIN (Date Effet).
        """
        if "dtrcppr" in df.columns and "dtreffin" in df.columns:
            df = df.withColumn(
                "dtrcppr",
                when(
                    col("dtrcppr").isNull() & col("dtreffin").isNotNull(),
                    col("dtreffin"),
                ).otherwise(col("dtrcppr")),
            )
        return df

    def _enrich_ird_risk(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrichit avec les données de risques IRD (Q46, Q45, QAN).
        Utilise un module partagé pour la logique de jointure séquentielle.
        """
        from utils.transformations.enrichment.risk_enrichment import (
            enrich_with_risk_data,
        )

        reader = BronzeReader(self.spark, self.config)

        df = enrich_with_risk_data(
            df,
            risk_sources=["q46", "q45", "qan"],
            vision=vision,
            bronze_reader=reader,
            logger=self.logger,
        )

        return df

    def _enrich_client_data(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrichit avec les données Client (SIRET, SIREN) et Note de Risque Historique.
        """
        from utils.transformations import join_client_data

        return join_client_data(df, self.spark, self.config, vision, self.logger)

    def _add_isic_codes(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Ajoute la codification ISIC et Hazard Grades.
        """
        from utils.transformations.base.isic_codification import assign_isic_codes

        df = assign_isic_codes(df, self.spark, self.config, vision, self.logger)

        return df

    def _apply_isic_gbl_corrections(self, df: DataFrame) -> DataFrame:
        """
        Applique des corrections manuelles sur le Code ISIC Global.
        Certains codes spécifiques sont re-mappés pour cohérence historique.
        """
        if "isic_code" not in df.columns:
            return df
        if "isic_code_gbl" not in df.columns:
            df = df.withColumn("isic_code_gbl", lit(None).cast(StringType()))

        corrections = {
            "22000": "022000",
            "24021": "024000",
            "242025": "242005",
            "329020": "329000",
            "731024": "731000",
            "81020": "081000",
            "81023": "081000",
            "981020": "981000",
        }

        ic = trim(col("isic_code"))
        expr = col("isic_code_gbl")
        for src, tgt in corrections.items():
            expr = when(ic == src, lit(tgt)).otherwise(expr)

        return df.withColumn("isic_code_gbl", expr)

    def _enrich_euler_risk_note(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrichit avec la note de risque Euler (via BINSEE).
        Jointure sur SIREN avec filtrage par date de validité.
        """

        from utils.helpers import compute_date_ranges

        reader = BronzeReader(self.spark, self.config)
        dates = compute_date_ranges(vision)

        try:
            df_euler = reader.read_file_group("binsee_histo_note_risque", vision="ref")

            if df_euler is not None:
                dtfinmn = dates["finmois"]

                # Filtrage : Note valide à la date de fin de mois
                df_euler_filtered = (
                    df_euler.filter(
                        (col("dtdeb_valid") <= lit(dtfinmn))
                        & (col("dtfin_valid") >= lit(dtfinmn))
                    )
                    .select(
                        col("cdsiren"),
                        # Nettoyage : "00" devient NULL
                        when(col("cdnote") == "00", lit(None))
                        .otherwise(col("cdnote"))
                        .alias("note_euler"),
                    )
                    .dropDuplicates(["cdsiren"])
                )

                # Jointure Gauche
                df = (
                    df.alias("a")
                    .join(
                        broadcast(df_euler_filtered.alias("e")),
                        col("a.cdsiren") == col("e.cdsiren"),
                        how="left",
                    )
                    .select("a.*", "e.note_euler")
                )

                self.logger.info("Enrichissement Euler effectué")
            else:
                self.logger.warning("Fichier Euler non disponible")
                from utils.processor_helpers import add_null_columns

                df = add_null_columns(df, {"note_euler": StringType})

        except Exception as e:
            self.logger.warning(f"Echec enrichissement Euler: {e}")
            from utils.processor_helpers import add_null_columns

            df = add_null_columns(df, {"note_euler": StringType})

        return df

    def _enrich_special_product_activity(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrichit les activités pour les produits spéciaux (IPFM0024, IPFM63, IPFM99).

        Ces tables contiennent des informations d'activité spécifiques pour certains produits
        qui ne sont pas dans le flux standard.

        LOGIQUE:
        - Charge IPFM0024, IPFM63, IPFM99 (Pôle 1 et 3 chacun)
        - Harmonise vers colonnes : NOPOL, NOINT, CDPROD, CDPOLE, CDACTCONST, CDACTCONST2, CDNAF, MTCA_RIS
        - Union (OUTER UNION CORR) de toutes les sources
        - Jointure LEFT sur (NOPOL, CDPROD) avec les données principales
        - TypeAct = "Multi" si CDACTCONST2 non vide, sinon "Mono"
        - TOUJOURS crée TypeAct, même si TABSPEC est vide
        """
        from pyspark.sql.functions import substring as spark_substring

        reader = BronzeReader(self.spark, self.config)

        try:
            # Construction du référentiel TABSPEC par union successive
            tabspec_parts = []

            # ---------------------------------------------------------------
            # IPFM0024 : cdactprf01 → CDACTCONST, cdactprf02 → CDACTCONST2
            # CDNAF = NULL, MTCA_RIS = NULL
            # ---------------------------------------------------------------
            for file_group in ["ipfm0024_1", "ipfm0024_3"]:
                try:
                    df_src = reader.read_file_group(file_group, vision)
                    if df_src is not None and df_src.count() > 0:
                        df_part = df_src.select(
                            col("nopol"),
                            col("noint"),
                            col("cdprod"),
                            col("cdpole"),
                            col("cdactprf01").alias("cdactconst"),
                            col("cdactprf02").alias("cdactconst2"),
                            lit(None).cast("string").alias("cdnaf_tabspec"),
                            lit(None).cast("double").alias("mtca_ris"),
                        )
                        tabspec_parts.append(df_part)
                        self.logger.debug(f"TABSPEC: {file_group} chargé ({df_src.count()} lignes)")
                except Exception as e:
                    self.logger.debug(f"TABSPEC: {file_group} non disponible : {e}")

            # ---------------------------------------------------------------
            # IPFM63 : actprin → CDACTCONST, actsec1 → CDACTCONST2
            # cdnaf → CDNAF_TABSPEC, mtca1 → MTCA_RIS
            # ---------------------------------------------------------------
            for file_group in ["ipfm63_1", "ipfm63_3"]:
                try:
                    df_src = reader.read_file_group(file_group, vision)
                    if df_src is not None and df_src.count() > 0:
                        df_part = df_src.select(
                            col("nopol"),
                            col("noint"),
                            col("cdprod"),
                            col("cdpole"),
                            col("actprin").alias("cdactconst"),
                            col("actsec1").alias("cdactconst2"),
                            col("cdnaf").alias("cdnaf_tabspec"),
                            col("mtca1").cast("double").alias("mtca_ris"),
                        )
                        tabspec_parts.append(df_part)
                        self.logger.debug(f"TABSPEC: {file_group} chargé ({df_src.count()} lignes)")
                except Exception as e:
                    self.logger.debug(f"TABSPEC: {file_group} non disponible : {e}")

            # ---------------------------------------------------------------
            # IPFM99 : substr(cdacpr1, 1, 4) → CDACTCONST, cdacpr2 → CDACTCONST2
            # CDNAF = NULL, mtca → MTCA_RIS
            # ---------------------------------------------------------------
            for file_group in ["ipfm99_1", "ipfm99_3"]:
                try:
                    df_src = reader.read_file_group(file_group, vision)
                    if df_src is not None and df_src.count() > 0:
                        df_part = df_src.select(
                            col("nopol"),
                            col("noint"),
                            col("cdprod"),
                            col("cdpole"),
                            spark_substring(col("cdacpr1"), 1, 4).alias("cdactconst"),
                            col("cdacpr2").alias("cdactconst2"),
                            lit(None).cast("string").alias("cdnaf_tabspec"),
                            col("mtca").cast("double").alias("mtca_ris"),
                        )
                        tabspec_parts.append(df_part)
                        self.logger.debug(f"TABSPEC: {file_group} chargé ({df_src.count()} lignes)")
                except Exception as e:
                    self.logger.debug(f"TABSPEC: {file_group} non disponible : {e}")

            # ---------------------------------------------------------------
            # Union de toutes les parties et nettoyage (SAS : delete if missing(nopol))
            # ---------------------------------------------------------------
            if tabspec_parts:
                from functools import reduce

                df_tabspec = reduce(
                    lambda a, b: a.unionByName(b, allowMissingColumns=True),
                    tabspec_parts,
                )
                df_tabspec = df_tabspec.filter(col("nopol").isNotNull())

                if df_tabspec.count() > 0:
                    self.logger.info(f"TABSPEC construit : {df_tabspec.count()} enregistrements")

                    # Jointure LEFT sur (NOPOL, CDPROD) — SAS ligne 515
                    # NB : SAS joint sur (NOPOL, CDPROD) seulement, PAS CDPOLE
                    df = df.join(
                        df_tabspec.select(
                            col("nopol").alias("_ts_nopol"),
                            col("cdprod").alias("_ts_cdprod"),
                            "cdactconst",
                            "cdactconst2",
                            "cdnaf_tabspec",
                        ),
                        (df["nopol"] == col("_ts_nopol"))
                        & (df["cdprod"] == col("_ts_cdprod")),
                        how="left",
                    ).drop("_ts_nopol", "_ts_cdprod")

                    # Mise à jour des colonnes avec les valeurs TABSPEC
                    # Actprin2 = Coalesce(Tabspec.ActConst, Original.ActPrin)
                    df = df.withColumn(
                        "actprin2", coalesce(col("cdactconst"), col("actprin"))
                    )

                    # cdnaf2 = Coalesce(Tabspec.cdnaf_tabspec, Original.cdnaf)
                    df = df.withColumn(
                        "cdnaf2", coalesce(col("cdnaf_tabspec"), col("cdnaf"))
                    )

                    # TypeAct = "Multi" ou "Mono"
                    df = df.withColumn(
                        "typeact",
                        when(col("cdactconst2").isNotNull(), lit("Multi")).otherwise(
                            lit("Mono")
                        ),
                    )

                    # Écrasement final
                    df = (
                        df.withColumn("actprin", col("actprin2"))
                        .withColumn("cdnaf", col("cdnaf2"))
                        .drop(
                            "actprin2",
                            "cdnaf2",
                            "cdactconst",
                            "cdactconst2",
                            "cdnaf_tabspec",
                        )
                    )
                else:
                    self.logger.info("TABSPEC vide — typeact initialisé à 'Mono'")
                    df = df.withColumn("typeact", lit("Mono"))
            else:
                # Aucun fichier IPFSPE disponible — typeact = "Mono" par défaut
                # SAS crée TOUJOURS typeact même si TABSPEC est vide
                self.logger.info(
                    "Aucune source IPFSPE disponible — typeact initialisé à 'Mono'"
                )
                df = df.withColumn("typeact", lit("Mono"))

        except Exception as e:
            self.logger.warning(f"Enrichissement Activités Spéciales ignoré : {e}")
            # Garantir que typeact existe même en cas d'erreur
            if "typeact" not in df.columns:
                df = df.withColumn("typeact", lit("Mono"))

        return df

    def _add_isic_global_code(self, df: DataFrame) -> DataFrame:
        """
        Ajoute le code ISIC Global.

        Logique : Copie simplement le code ISIC calculé.
        """
        if "isic_code" in df.columns:
            df = df.withColumn("isic_code_gbl", col("isic_code"))
        return df

    def _enrich_w6_naf_and_client_cdnaf(self, df: DataFrame) -> DataFrame:
        """
        Enrichissement avec les codes NAF depuis W6 et Client, et nettoyage des valeurs invalides.

        LOGIQUE REPRODUITE :
        ------------------------
        1. Jointure W6.BASECLI_INV sur NOCLT -> cdnaf08_w6
        2. Jointure CLIENT1.CLACENT1 sur NOCLT -> cdnaf03_cli
        3. Jointure CLIENT3.CLACENT3 sur NOCLT -> cdnaf03_cli (fallback)
        4. Nettoyage :
           - Si cdnaf03_cli IN ("00", "000Z", "9999") -> ""
           - Si cdnaf08_w6 = "0000Z" -> ""

        PARAMÈTRES :
        -----------
        df : DataFrame
            Données consolidées

        RETOUR :
        -------
        DataFrame
            Données enrichies avec cdnaf08_w6 et cdnaf03_cli nettoyés
        """
        from pyspark.sql.functions import col, lit, when, coalesce, broadcast

        reader = BronzeReader(self.spark, self.config)

        # 1) Enrichissement W6 (CA minimal)
        try:
            df_w6 = reader.read_file_group("w6_basecli_inv", vision="ref")
            if df_w6 is not None:
                df_w6_select = df_w6.select(
                    col("noclt").alias("_w6_noclt"), col("cdapet").alias("cdnaf08_w6")
                ).dropDuplicates(["_w6_noclt"])

                df = (
                    df.alias("a")
                    .join(
                        df_w6_select.alias("w"),
                        col("a.noclt") == col("w._w6_noclt"),
                        how="left",
                    )
                    .select("a.*", col("w.cdnaf08_w6"))
                )
                self.logger.debug("Enrichissement W6 (CDNAF08) effectué")
        except Exception as e:
            self.logger.warning(f"Echec enrichissement W6: {e}")
            if "cdnaf08_w6" not in df.columns:
                df = df.withColumn("cdnaf08_w6", lit(None).cast(StringType()))

        # 2) Enrichissement Client NAF03 (deux sources)
        try:
            # CLIENT1 (prioritaire)
            df_client1 = reader.read_file_group("cliact14", vision="ref")
            if df_client1 is not None:
                df_client1_select = df_client1.select(
                    col("noclt").alias("_c1_noclt"), col("cdnaf").alias("cdnaf03_cli_1")
                ).dropDuplicates(["_c1_noclt"])

                df = (
                    df.alias("a")
                    .join(
                        df_client1_select.alias("c1"),
                        col("a.noclt") == col("c1._c1_noclt"),
                        how="left",
                    )
                    .select("a.*", col("c1.cdnaf03_cli_1").alias("cdnaf03_cli"))
                )

                # CLIENT3 (fallback) - ONLY si CLIENT1 pas de valeur
                df_client3 = reader.read_file_group("cliact3", vision="ref")
                if df_client3 is not None:
                    df_client3_select = df_client3.select(
                        col("noclt").alias("_c3_noclt"),
                        col("cdnaf").alias("cdnaf03_cli_3"),
                    ).dropDuplicates(["_c3_noclt"])

                    df = df.withColumnRenamed("cdnaf03_cli", "_cdnaf03_cli_prev")

                    df = (
                        df.alias("a")
                        .join(
                            df_client3_select.alias("c3"),
                            col("a.noclt") == col("c3._c3_noclt"),
                            how="left",
                        )
                        .select(
                            "a.*",
                            coalesce(
                                col("a._cdnaf03_cli_prev"), col("c3.cdnaf03_cli_3")
                            ).alias("cdnaf03_cli"),
                        )
                        .drop("_cdnaf03_cli_prev")
                    )

                self.logger.debug("Enrichissement Client (CDNAF03) effectué")
        except Exception as e:
            self.logger.warning(f"Echec enrichissement Client NAF: {e}")
            if "cdnaf03_cli" not in df.columns:
                df = df.withColumn("cdnaf03_cli", lit(None).cast(StringType()))

        # 3) NETTOYAGE des codes NAF invalides
        if "cdnaf03_cli" in df.columns:
            df = df.withColumn(
                "cdnaf03_cli",
                when(
                    col("cdnaf03_cli").isin("00", "000Z", "9999"),
                    lit(None).cast(StringType()),
                ).otherwise(col("cdnaf03_cli")),
            )

        # if CDNAF08_W6 in ("0000Z") then CDNAF08_W6 = ""
        if "cdnaf08_w6" in df.columns:
            df = df.withColumn(
                "cdnaf08_w6",
                when(
                    col("cdnaf08_w6") == "0000Z", lit(None).cast(StringType())
                ).otherwise(col("cdnaf08_w6")),
            )

        self.logger.info("Enrichissement et nettoyage NAF (W6 + Client) terminés")
        return df

    def _enrich_do_dest(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrichit avec DESTINAT depuis le référentiel DO_DEST.

        DO_DEST est une table de RÉFÉRENCE (pas vision-dépendante).

        LOGIQUE :
        --------
        - Lit do_dest.csv
        - Jointure LEFT sur NOPOL
        - Remplace TOUJOURS destinat par la valeur du référentiel

        PARAMÈTRES :
        -----------
        df : DataFrame
            Données consolidées
        vision : str
            Vision (non utilisée, DO_DEST est une référence)

        RETOUR :
        -------
        DataFrame
            Données avec colonne destinat enrichie
        """
        from pyspark.sql.functions import to_date, lit, coalesce
        from pyspark.sql.types import DateType, StringType

        try:
            reader = BronzeReader(self.spark, self.config)
            # DO_DEST est une table de référence (pas mensuelle)
            do_dest_df = reader.read_file_group("do_dest", "ref")

            if do_dest_df is None:
                # Si table absente, garantir existence de la colonne
                if "destinat" not in df.columns:
                    df = df.withColumn("destinat", lit(None).cast(StringType()))
                self.logger.warning("Référentiel DO_DEST non disponible")
                return df

            # Vérifier colonnes requises
            if (
                "nopol" not in do_dest_df.columns
                or "destinat" not in do_dest_df.columns
            ):
                if "destinat" not in df.columns:
                    df = df.withColumn("destinat", lit(None).cast(StringType()))
                self.logger.warning("DO_DEST : colonnes requises manquantes")
                return df

            # Préparer référence (déduplication sur NOPOL)
            do_dest_ref = do_dest_df.select(
                col("nopol").alias("nopol_ref"),
                col("destinat").cast(StringType()).alias("destinat_ref"),
            ).dropDuplicates(["nopol_ref"])

            if "nopol" not in df.columns:
                if "destinat" not in df.columns:
                    df = df.withColumn("destinat", lit(None).cast(StringType()))
                self.logger.warning("Colonne NOPOL manquante dans données principales")
                return df

            # Jointure LEFT
            df_join = df.join(do_dest_ref, df.nopol == col("nopol_ref"), "left")

            # TOUJOURS utiliser la valeur du référentiel
            df_final = df_join.withColumn("destinat", col("destinat_ref"))

            return df_final.drop("nopol_ref", "destinat_ref")

        except Exception as e:
            self.logger.warning(f"Échec enrichissement DO_DEST : {e}")
            if "destinat" not in df.columns:
                df = df.withColumn("destinat", lit(None).cast(StringType()))
            return df
