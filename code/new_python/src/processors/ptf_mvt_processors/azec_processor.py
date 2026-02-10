# -*- coding: utf-8 -*-
"""
===============================================================================
PROCESSEUR AZEC (Mouvements de Portefeuille - Construction)
===============================================================================

Ce processeur gère le traitement des mouvements de portefeuille pour le périmètre "AZEC",
qui correspond au marché spécifique de la Construction.

OBJECTIFS DU PROCESSEUR :
------------------------
1. Lire les données de base (Police et Capitaux) depuis la couche Bronze.
2. Appliquer les règles de gestion spécifiques à la Construction (filtres, redressements).
3. Enrichir les données avec les classifications NAF, le Chiffre d'Affaires, et la Segmentation.
4. Calculer les indicateurs clés (Primes, Expositions, Capitaux SMP/LCI).
5. Produire une vision consolidée "Silver" des mouvements AZEC.

SOURCES DE DONNÉES :
-------------------
- POLIC_CU : Données principales des polices.
- CAPITXCU : Données des capitaux (SMP, LCI).
- Tables de référence : REF_MIG_AZEC_VS_IMS, INCENDCU, MPACU, RCENTCU, RISTECCU, MULPROCU.
- Référentiels techniques : PTGST_STATIC, TABLE_SEGMENTATION_AZEC_MML, TABLE_PT_GEST.

FLUX DE TRAITEMENT :
-------------------
Le traitement suit une séquence précise pour garantir l'intégrité des calculs,
en particulier pour la gestion des capitaux et des expositions.
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
from src.reader import BronzeReader


class AZECProcessor(BaseProcessor):
    """
    =======================================================================
    PROCESSEUR DE MOUVEMENTS AZEC (CONSTRUCTION)
    =======================================================================
    
    Transforme les données brutes AZEC (Bronze) en couche Silver.
    
    WORKFLOW DÉTAILLÉ :
    ------------------
    1. READ : Lecture du fichier POLIC_CU et calcul de la date d'échéance.
    2. TRANSFORM :
       - Sélection et renommage des colonnes.
       - Filtres métier (exclusion des intermédiaires et polices spécifiques).
       - Gestion de la migration (identification des polices non migrées).
       - Mise à jour de la qualité des données (Dates, États des polices).
       - Calcul des indicateurs de mouvement (NBPTF, NBAFN, NBRES).
       - Gestion des périodes de suspension.
       - Calcul des expositions (YTD / GLI).
       - Enrichissement Segmentation et Primes.
       - Enrichissement Codes NAF (Fusion de 4 sources).
       - Enrichissement Formules de garanties.
       - Enrichissement Chiffre d'Affaires (MULPROCU).
       - Enrichissement Capitaux (INCENDCU et CAPITXCU).
       - Redressements métier finaux.
       - Enrichissement final (Données site et PT_GEST).
    3. WRITE : Écriture en couche Silver.
    """

    def read(self, vision: str) -> DataFrame:
        """
        Lit le fichier principal POLIC_CU depuis la couche Bronze.
        
        Calcul également la date d'échéance annuelle (DTECHANM) à partir
        du jour et du mois d'échéance fournis.
        
        PARAMÈTRES :
        -----------
        vision : str
            Vision (Période) à traiter au format YYYYMM
            
        RETOUR :
        -------
        DataFrame
            Données POLIC_CU avec DTECHANM calculée.
        """
        reader = BronzeReader(self.spark, self.config)

        self.logger.info("Lecture du fichier POLIC_CU")
        df = reader.read_file_group('polic_cu_azec', vision)

        # Calcul de DTECHANM (Date Echéance Annuelle)
        # Reconstitution d'une date à partir de l'année de vision, du mois et du jour d'échéance
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
        Exécute la chaîne complète de transformations AZEC.
        
        Cette méthode orchestre l'ensemble des règles métier spécifiques
        au marché de la Construction, en respectant l'ordre logique de dépendance des données.
        
        PARAMÈTRES :
        -----------
        df : DataFrame
            Données POLIC_CU lues
        vision : str
            Vision (Période) à traiter
            
        RETOUR :
        -------
        DataFrame
            Table de mouvements enrichie et consolidée.
        """
        # ------------------------------------------------------------
        # Préparation du contexte (Dates, Config)
        # ------------------------------------------------------------
        year_int, month_int = extract_year_month_int(vision)
        dates = compute_date_ranges(vision)

        loader = get_default_loader()
        azec_config = loader.get_azec_config()

        self.logger.info(f"Configuration AZEC chargée : {list(azec_config.keys())}")
        
        # ============================================================
        # ÉTAPE 1 : Sélection et Configuration des Colonnes
        # ============================================================
        self.logger.step(1, "Application de la configuration des colonnes")
        column_config = azec_config['column_selection']
        df = apply_column_config(df, column_config, vision, year_int, month_int)

        # Ajout des constantes de structure (DIRCOM = AZEC, CDPOLE = 3 Courtage)
        df = df.withColumn('dircom', lit(DIRCOM.AZEC))
        df = df.withColumn('cdpole', lit('3'))  # AZEC est toujours considéré comme du Courtage

        # ============================================================
        # ÉTAPE 1.1 : Initialisation des colonnes techniques
        # ============================================================
        self.logger.step(1.1, "Initialisation sécurisée des colonnes manquantes")

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

        # Initialisation par type pour éviter les erreurs de schéma
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
        # ÉTAPE 2 : Filtres Métier (Exclusions)
        # ============================================================
        self.logger.step(2, "Application des filtres d'exclusion métier")
        
        # Exclusion de certains intermédiaires spécifiques
        df = df.filter(~col("intermed").isin("24050", "40490"))
        
        # Exclusion de police spécifique (Test/Anomalie connue)
        df = df.filter(~col("police").isin("012684940"))
        
        # Exclusion des durées "00" sauf pour certains produits
        df = df.filter(
            ~(
                (col("duree") == "00") &
                (~col("produit").isin("DO0", "TRC", "CTR", "CNR"))
            )
        )
        
        # Consistence des dates : Exclusion si Fin = Effet (Sauf si NULL)
        df = df.filter(
            col("datfin").isNull() |
            col("effetpol").isNull() |
            (col("datfin") != col("effetpol"))
        )
        
        # Gestion des cas de migration "MIGRAZ" : On ne garde que ceux résiliés ("R")
        df = df.filter(
            (~col("gestsit").isin("MIGRAZ")) |
            ((col("gestsit") == "MIGRAZ") & (col("etatpol") == "R"))
        )

        # ============================================================
        # ÉTAPE 3 : Gestion de la Migration
        # ============================================================
        self.logger.step(3, "Gestion de la migration AZEC vs IMS")
        df = self._handle_migration(df, vision, azec_config)

        # ============================================================
        # ÉTAPE 4 : Mise à jour Qualité des Données (Dates/États)
        # ============================================================
        self.logger.step(4, "Redressement des dates et états de police")
        df = self._update_dates_and_states(df, dates, year_int, month_int)

        # ============================================================
        # ÉTAPE 5 : Indicateurs de Mouvement
        # ============================================================
        self.logger.step(5, "Calcul des indicateurs de mouvement (NBPTF/NBAFN/NBRES)")
        df = self._calculate_movements(df, dates, year_int, month_int)

        # ============================================================
        # ÉTAPE 6 : Gestion de la Suspension
        # ============================================================
        self.logger.step(6, "Calcul des périodes de suspension")
        from utils.transformations.operations.business_logic import calculate_azec_suspension
        df = calculate_azec_suspension(df, dates)

        # ============================================================
        # ÉTAPE 7 : Calcul des Expositions
        # ============================================================
        self.logger.step(7, "Calcul des expositions (Méthode AZEC)")
        from utils.transformations import calculate_exposures_azec
        df = calculate_exposures_azec(df, dates)

        # ============================================================
        # ÉTAPE 8 : Segmentation Simplifiée
        # ============================================================
        self.logger.step(8, "Enrichissement Segmentation (via TABLE_SEGMENTATION_AZEC_MML)")
        
        reader = BronzeReader(self.spark, self.config)
        df_seg = reader.read_file_group('table_segmentation_azec_mml', 'ref')
        
        if df_seg is None or df_seg.count() == 0:
            raise RuntimeError(
                "La table TABLE_SEGMENTATION_AZEC_MML est requise pour la segmentation AZEC. "
                "Vérifiez sa présence dans le répertoire 'bronze/ref/'."
            )
        
        # Sécurité : unicité par produit
        df_seg = df_seg.dropDuplicates(["produit"])
        
        # Jointure Gauche sur le Code Produit
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
        
        # Filtrage post-jointure : Uniquement Marché Construction (6)
        df = df.filter(
            (col("cmarch") == "6") &
            (col("gestsit") != "MIGRAZ")
        )
        
        # Pas de cache CONSTRCU nécessaire dans ce flux simplifié
        self._cached_constrcu_ref = None

        # ============================================================
        # ÉTAPE 9 : Calcul des Primes et Flags
        # ============================================================
        self.logger.step(9, "Calcul des primes et indicateurs techniques")
        df = self._calculate_premiums(df)

        # ============================================================
        # ÉTAPE 10 : Codes NAF (Fusion Multi-Sources)
        # ============================================================
        self.logger.step(10, "Enrichissement Codes NAF (Fusion 4 tables)")
        df = self._enrich_naf_codes(df, vision)

        # ============================================================
        # ÉTAPE 11 : Formules de Garantie
        # ============================================================
        self.logger.step(11, "Enrichissement Formules de garantie")
        df = self._enrich_formulas(df, vision)

        # ============================================================
        # ÉTAPE 12 : Chiffre d'Affaires (MULPROCU)
        # ============================================================
        self.logger.step(12, "Enrichissement Chiffre d'Affaires (MTCA)")
        df = self._enrich_ca(df, vision)

        # ============================================================
        # ÉTAPE 13 : Capitaux PE/RD (INCENDCU)
        # ============================================================
        self.logger.step(13, "Enrichissement Capitaux Perte Exp. & Risque Direct")
        df = self._enrich_pe_rd_vi(df, vision)

        # ============================================================
        # ÉTAPE 14 : Nettoyage final des dates d'exposition
        # ============================================================
        self.logger.step(14, "Nettoyage des dates d'exposition (si YTD = 0)")
        df = df.withColumn(
            'dt_deb_expo',
            when(col('expo_ytd') == 0, lit(None).cast(DateType())).otherwise(col('dt_deb_expo'))
        ).withColumn(
            'dt_fin_expo',
            when(col('expo_ytd') == 0, lit(None).cast(DateType())).otherwise(col('dt_fin_expo'))
        )

        # ============================================================
        # ÉTAPE 15 : Capitaux SMP/LCI (CAPITXCU)
        # ============================================================
        self.logger.step(15, "Jointure Capitaux SMP/LCI (CAPITXCU)")
        df = self._join_capitals(df, vision)

        # ============================================================
        # ÉTAPE 16 : Redressements Métier
        # ============================================================
        self.logger.step(16, "Application des redressements métier (NBRES/NBAFN)")
        df = self._adjust_nbres(df)

        # ============================================================
        # ÉTAPE 17 : Enrichissement Final (Site, PT_GEST)
        # ============================================================
        self.logger.step(17, "Enrichissement final (Données Site, UPPER_MID)")
        df = self._enrich_final_constrcu_data(df, vision)

        # Tri final par police pour une sortie déterministe
        df = df.orderBy('police')

        self.logger.success("Transformations AZEC terminées avec succès")
        return df

    def write(self, df: DataFrame, vision: str) -> None:
        """
        Écrit les données transformées en couche Silver.
        
        Effectue une vérification de non-duplication des colonnes avant écriture.
        
        PARAMÈTRES :
        -----------
        df : DataFrame
            Données finales transformées
        vision : str
            Vision traitée
        """
        # Sécurité : Vérification des doublons de noms de colonnes (case-insensitive)
        cols = df.columns
        lower = list(map(str.lower, cols))
        dups = [c for c in set(lower) if lower.count(c) > 1]
        
        if dups:
            raise RuntimeError(f"Noms de colonnes dupliqués avant écriture : {dups}")

        from utils.helpers import write_to_layer
        write_to_layer(
            df=df,
            config=self.config,
            layer="silver",
            filename=f'azec_ptf_{vision}',
            vision=vision,
            logger=self.logger,
            optimize=True, # Optimisation Z-ORDER pour lecture rapide
        )

    def _handle_migration(self, df: DataFrame, vision: str, azec_config: dict) -> DataFrame:
        """
        Gère la migration AZEC vs IMS pour les visions récentes (> Sept 2020).
        
        Identifie les polices qui n'ont pas encore migré vers le nouveau système.
        
        PARAMÈTRES :
        -----------
        df : DataFrame
            Données en cours
        vision : str
            Vision traitée
        azec_config : dict
            Configuration AZEC
        
        RETOUR :
        -------
        DataFrame
            Avec colonne 'nbptf_non_migres_azec' ajoutée
        """
        migration_config = azec_config['migration_handling']
        
        # La migration ne concerne que les données postérieures à Septembre 2020
        if int(vision) > migration_config['vision_threshold']:
            reader = BronzeReader(self.spark, self.config)
            
            try:
                df_mig = reader.read_file_group('ref_mig_azec_vs_ims', vision='ref')
            except FileNotFoundError as e:
                self.logger.error(f"Table de migration ref_mig_azec_vs_ims requise pour vision > {migration_config['vision_threshold']}")
                raise RuntimeError(f"Données de référence manquantes : ref_mig_azec_vs_ims") from e
            
            if df_mig is None:
                raise RuntimeError("La table de migration est vide.")
            
            # Jointure pour identifier les polices migrées
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
            self.logger.info("Table de migration appliquée : Indicateur NBPTF_NON_MIGRES_AZEC calculé")
        else:
            # Avant la migration, tout est considéré comme non migré (actif système actuel)
            df = df.withColumn('nbptf_non_migres_azec', lit(1))
        
        return df

    def _update_dates_and_states(self, df, dates, annee, mois):
        """
        Corrige la qualité des données de dates et d'états de police.
        
        LOGIQUE APPLIQUÉE :
        ------------------
        1. Correction DATEXPIR si incohérente avec DATFIN pour polices résiliées.
        2. Détection des AFN/RES anticipés (futurs par rapport à fin de mois).
        3. Gestion des Tacites Reconductions de > 1 an non facturées (Forçage RÉSILIATION).
        4. Gestion des polices Temporaires expirées (Forçage RÉSILIATION).
        
        Cette méthode centralise la logique de nettoyage temporel complexe.
        """
        from pyspark.sql.functions import col, lit, when, to_date
        from datetime import datetime

        def apply_overwrites(df_, condition, updates: dict):
            """Applique des mises à jour conditionnelles sur plusieurs colonnes."""
            for cname, expr in updates.items():
                df_ = df_.withColumn(cname, when(condition, expr).otherwise(col(cname)))
            return df_

        # (1) Correction DATEEXPIR
        # Si DATFIN > DATEXPIR pour une police résiliée/sans effet, on aligne DATEEXPIR
        cond_datexpir = (
            col("datfin").isNotNull()
            & col("datexpir").isNotNull()
            & (col("datfin") > col("datexpir"))
            & col("etatpol").isin("X", "R")
        )
        df = apply_overwrites(df, cond_datexpir, {"datexpir": col("datfin")})

        # (2) Drapeaux Anticipés (Mouvements futurs)
        finmois = to_date(lit(dates["finmois"]))
        df = apply_overwrites(df, col("effetpol") > finmois, {"nbafn_anticipe": lit(1)})
        df = apply_overwrites(df, col("datfin") > finmois, {"nbres_anticipe": lit(1)})

        # (3) Tacite reconduction > 1 an (Nettoyage vieilles polices)
        # Si une police annuelle a dépassé son terme d'un an sans facturation, on la résilie
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

        # (4) Polices Temporaires (Nettoyage)
        # Si une temporaire a une date de fin prévue (FINPOL) mais pas de DATFIN réelle, on la clôture
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
        Calcule les indicateurs de mouvement AZEC (NBPTF, NBAFN, NBRES).
        
        Utilise la logique métier centralisée dans `utils.transformations`.
        Gère également :
        - La conversion des drapeaux en entiers (0/1)
        - La création défensive des colonnes nécessaires
        
        PARAMÈTRES :
        -----------
        df : DataFrame
            Données en cours
        dates : dict
            Dictionnaire des dates clés (finmois, etc.)
            
        RETOUR :
        -------
        DataFrame
            Avec colonnes NBPTF, NBAFN, NBRES calculées.
        """
        from pyspark.sql.functions import col, lit
        from utils.transformations.operations.business_logic import calculate_azec_movements

        # Sécurité : Création des colonnes si absentes
        for c in ["etatpol", "produit", "effetpol", "datafn", "datfin", "datresil"]:
            if c not in df.columns:
                df = df.withColumn(c, lit(None))

        # Drapeau de migration par défaut
        if "nbptf_non_migres_azec" not in df.columns:
            df = df.withColumn("nbptf_non_migres_azec", lit(1))
            
        # Appel de la logique métier centrale
        df = calculate_azec_movements(df, dates, year, month)

        # Transtypage explicite en entier (Standardisation)
        for flag in ["nbptf", "nbafn", "nbres", "nbafn_anticipe", "nbres_anticipe"]:
            if flag in df.columns:
                df = df.withColumn(flag, col(flag).cast("int"))
            else:
                df = df.withColumn(flag, lit(0).cast("int"))

        return df

    def _join_capitals(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Jointure avec les données de capitaux (CAPITXCU).
        
        CRITIQUE :
        ---------
        Les capitaux SMP/LCI sont essentiels pour l'analyse AZEC.
        Le traitement échouera si la table ou le mapping est manquant.
        
        Gère la précision flottante et l'agrégation par police/produit.
        """
        from pyspark.sql.functions import col, lit, coalesce, sum as spark_sum, when
        reader = BronzeReader(self.spark, self.config)

        # 0) Vérification de la configuration
        if not AZEC_CAPITAL_MAPPING:
            raise RuntimeError(
                "AZEC_CAPITAL_MAPPING est vide. Vérifiez la configuration des transformations AZEC."
            )

        # 1) Lecture de la table des capitaux CAPITXCU
        try:
            df_cap = reader.read_file_group('capitxcu_azec', vision)
        except FileNotFoundError as e:
            self.logger.error("CAPITXCU requis pour le traitement AZEC")
            raise RuntimeError("Données de capitaux manquantes : CAPITXCU") from e

        if df_cap is None or not df_cap.columns:
            raise RuntimeError("CAPITXCU est vide ou introuvable.")

        # 2) Vérification des colonnes clés
        required_key_cols = {"police", "produit", "smp_sre", "brch_rea"}
        missing_keys = [c for c in required_key_cols if c not in df_cap.columns]
        if missing_keys:
            raise RuntimeError(
                f"CAPITXCU ne contient pas les colonnes clés requises: {missing_keys}"
            )

        # 3) Vérification de la présence des sources mappées
        mapped_sources = list({m["source"] for m in AZEC_CAPITAL_MAPPING})
        existing_sources = [s for s in mapped_sources if s in df_cap.columns]
        if not existing_sources:
            raise RuntimeError(
                "Aucune colonne source du mapping n'existe dans CAPITXCU. Vérifiez le schéma."
            )

        # 4) Optimisation : Sélection uniquement des colonnes nécessaires
        needed_cols = set(required_key_cols) | set(existing_sources)
        df_cap = df_cap.select(*[c for c in needed_cols if c in df_cap.columns])

        # 5) Création des colonnes cibles selon le mapping (Lignes -> Colonnes)
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
        
        if not created_targets:
            raise RuntimeError("Impossible de créer des colonnes cibles depuis CAPITXCU.")

        # 7) Agrégation par (police, produit) : Somme des montants
        df_cap_agg = df_cap.groupBy("police", "produit").agg(
            *[spark_sum(c).alias(c) for c in created_targets]
        )

        # 8) Calcul des Totaux (100% et Part Compagnie)
        def safe_add(df_, a, b, out):
            """Addition sécurisée avec gestion de l'existence des colonnes."""
            a_col = col(a) if a in df_.columns else lit(0)
            b_col = col(b) if b in df_.columns else lit(0)
            return df_.withColumn(out, a_col + b_col)

        df_cap_agg = safe_add(df_cap_agg, "lci_pe_100", "lci_dd_100", "lci_100")
        df_cap_agg = safe_add(df_cap_agg, "lci_pe_cie", "lci_dd_cie", "lci_cie")
        df_cap_agg = safe_add(df_cap_agg, "smp_pe_100", "smp_dd_100", "smp_100")
        df_cap_agg = safe_add(df_cap_agg, "smp_pe_cie", "smp_dd_cie", "smp_cie")

        # 9) Jointure sécurisée ("Coalesce from right") pour éviter doublons et NULLs
        df = self.coalesce_from_right(
            df,
            df_cap_agg,
            keys=["police", "produit"],
            cols=["smp_100", "smp_cie", "lci_100", "lci_cie"]
        )

        self.logger.info("✓ Données de capitaux jointes avec succès")
        return df

    def _adjust_nbres(self, df: DataFrame) -> DataFrame:
        """
        Applique les redressements métier finaux sur NBRES et NBAFN.
        
        RÈGLES DE REDRESSEMENT :
        -----------------------
        1. Produits exclus (DO0, TRC...) : NBPTF et NBRES forcés à 0.
        2. Remplacements : Si motif 'RP' et police remplaçante renseignée -> Ce n'est pas une vraie résiliation (NBRES=0).
        3. Motifs spécifiques : 'SE' (Sans Effet) et 'SA' (Sans Avenant) ne sont pas des résiliations (NBRES=0).
        4. Segment '5' (Prestations Accessoires) : Exclu des Affaires Nouvelles (NBAFN=0).
        """
        excluded_products = col("produit").isin(['DO0', 'TRC', 'CTR', 'CNR'])

        # Redressement NBRES
        nbres_adjusted = (
            when(excluded_products, lit(0))
            .when(
                (col("nbres") == 1) &
                (col("rmplcant").isNotNull()) &
                (col("rmplcant") != "") &
                col("motifres").isin(['RP']),
                lit(0)  # C'est un Remplacement, pas une chute sèche
            )
            .when(
                (col("nbres") == 1) &
                col("motifres").isin(['SE', 'SA']),
                lit(0)  # Sans Effet / Sans Avenant
            )
            .otherwise(col("nbres"))
        )

        # Redressement NBAFN (Exclusion segment 5)
        if "cssseg" in df.columns:
            nbafn_adjusted = when(
                (col("nbafn") == 1) & (col("cssseg") == "5"),
                lit(0)
            ).otherwise(col("nbafn"))
        else:
            nbafn_adjusted = col("nbafn")

        # Redressement NBPTF (Uniquement sur produits exclus)
        nbptf_adjusted = when(excluded_products, lit(0)).otherwise(col("nbptf"))

        df = df.withColumn("nbres", nbres_adjusted)
        df = df.withColumn("nbafn", nbafn_adjusted)
        df = df.withColumn("nbptf", nbptf_adjusted)

        return df

    def _enrich_region(self, df: DataFrame) -> DataFrame:
        """
        Enrichit les données avec la RÉGION via PTGST_STATIC.
        """
        reader = BronzeReader(self.spark, self.config)
        
        try:
            df_ptgst = reader.read_file_group('ptgst_static', vision='ref')
            
            if df_ptgst is not None:
                # Sélection et déduplication
                df_ptgst_select = df_ptgst.select(
                    col("ptgst"),
                    col("region"),
                    col("p_num") if "p_num" in df_ptgst.columns else lit(None).cast(StringType()).alias("p_num")
                ).dropDuplicates(["ptgst"])
                
                # Jointure gauche sur point de gestion
                df = df.alias("a").join(
                    df_ptgst_select.alias("p"),
                    col("a.poingest") == col("p.ptgst"),
                    how="left"
                ).select(
                    "a.*",
                    # Valeur par défaut 'Autres' si région inconnue
                    when(col("p.region").isNull(), lit("Autres"))
                    .otherwise(col("p.region")).alias("region"),
                    col("p.p_num")
                )
                
                self.logger.info("Enrichissement Région effectué")
            else:
                self.logger.warning("PTGST_STATIC non disponible - Région définie à 'Autres'")
                from utils.processor_helpers import add_null_columns
                df = df.withColumn('region', lit('Autres'))
                df = add_null_columns(df, {'p_num': StringType})

        except Exception as e:
            self.logger.warning(f"Echec enrichissement PTGST: {e} - Région définie à 'Autres'")
            from utils.processor_helpers import add_null_columns
            df = df.withColumn('region', lit('Autres'))
            df = add_null_columns(df, {'p_num': StringType})
        
        return df

    def _enrich_naf_codes(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrichit les données avec les codes NAF et TRE en fusionnant 4 tables sources.
        
        STRATÉGIE DE FUSION :
        --------------------
        On utilise un FULL OUTER JOIN sur 4 tables (INCENDCU, MPACU, RCENTCU, RISTECCU)
        pour être sûr de capturer toutes les polices, même si elles ne sont présentes que
        dans l'une des tables satellites.
        
        C'est une opération coûteuse mais nécessaire pour l'exhaustivité.
        """
        reader = BronzeReader(self.spark, self.config)
        
        # --- ÉTAPE 1 : Chargement et préparation des 4 tables ---
        try:
            df_incend = reader.read_file_group('incendcu_azec', vision)
            df_mpacu = reader.read_file_group('mpacu_azec', vision)
            df_rcentcu = reader.read_file_group('rcentcu_azec', vision)
            df_risteccu = reader.read_file_group('risteccu_azec', vision)
            
            tables = []
            
            if df_incend is not None:
                tables.append(df_incend.select(col('police'), col('cod_naf').alias('naf_incend')).dropDuplicates(['police']))
            if df_mpacu is not None:
                tables.append(df_mpacu.select(col('police'), col('cod_naf').alias('naf_mpacu')).dropDuplicates(['police']))
            if df_rcentcu is not None:
                tables.append(df_rcentcu.select(col('police'), col('cod_naf').alias('naf_rcentcu')).dropDuplicates(['police']))
            if df_risteccu is not None:
                tables.append(df_risteccu.select(col('police'), col('cod_naf').alias('naf_risteccu')).dropDuplicates(['police']))
            
            if not tables:
                self.logger.warning("Aucune table NAF disponible - Enrichissement ignoré")
                from utils.processor_helpers import add_null_columns
                return add_null_columns(df, {'cdnaf': StringType, 'cdtre': StringType})
            
            # --- ÉTAPE 2 : FULL OUTER JOIN Cascadé ---
            df_naf_all = tables[0]
            for df_table in tables[1:]:
                df_naf_all = df_naf_all.join(df_table, on='police', how='full_outer')
            
            # --- ÉTAPE 3 : COALESCE (Priorité indifférente, on prend le premier non-null) ---
            df_naf_all = df_naf_all.withColumn(
                'code_naf',
                coalesce(
                    col('naf_incend') if 'naf_incend' in df_naf_all.columns else lit(None),
                    col('naf_mpacu') if 'naf_mpacu' in df_naf_all.columns else lit(None),
                    col('naf_rcentcu') if 'naf_rcentcu' in df_naf_all.columns else lit(None),
                    col('naf_risteccu') if 'naf_risteccu' in df_naf_all.columns else lit(None)
                )
            ).select('police', 'code_naf')
            
            # Déduplication finale par sécurité (devrait être inutile grâce au dropDuplicates plus haut)
            from pyspark.sql.functions import min as spark_min
            df_code_naf = df_naf_all.groupBy('police').agg(spark_min('code_naf').alias('code_naf'))
            
        except Exception as e:
            self.logger.warning(f"Echec de la fusion NAF: {e}")
            df_code_naf = self.spark.createDataFrame([], schema="police string, code_naf string")
        
        # --- ÉTAPE 4 : Extraction COD_TRE (Uniquement depuis INCENDCU) ---
        try:
            if df_incend is not None:
                df_code_tre = df_incend.select('police', 'cod_tre').dropDuplicates(['police'])
                df_code_tre = df_code_tre.groupBy('police').agg(spark_min('cod_tre').alias('code_tre'))
            else:
                df_code_tre = self.spark.createDataFrame([], schema="police string, code_tre string")
        except Exception as e:
            self.logger.warning(f"Echec extraction COD_TRE: {e}")
            df_code_tre = self.spark.createDataFrame([], schema="police string, code_tre string")
        
        # --- ÉTAPE 5 : Fusion Finale et Jointure au Flux Principal ---
        df_naf_final = df_code_tre.join(df_code_naf, on='police', how='full_outer')
        
        df = df.join(df_naf_final, on='police', how='left')
        
        # Renommage final
        if 'code_naf' in df.columns:
            df = df.withColumn('cdnaf', col('code_naf')).drop('code_naf')
        else:
            df = df.withColumn('cdnaf', lit(None).cast(StringType()))
        
        if 'code_tre' in df.columns:
            df = df.withColumn('cdtre', col('code_tre')).drop('code_tre')
        else:
            df = df.withColumn('cdtre', lit(None).cast(StringType()))
        
        self.logger.info("✓ Codes NAF/TRE enrichis")
        return df

    def _calculate_premiums(self, df: DataFrame) -> DataFrame:
        """
        Calcule les montants de primes et les indicateurs associés.
        """
        # Part Compagnie = Part Brut / 100
        df = df.withColumn('partcie', col('partbrut') / 100.0)
        
        # Prime Totale et Prime CUA (même formule)
        # Prime * Part + CPCUA
        primeto_expr = (col('prime') * col('partbrut') / 100.0) + col('cpcua')
        df = df.withColumn('primeto', primeto_expr)
        df = df.withColumn('primecua', primeto_expr)
        
        # Cotisation à 100%
        # Si part brute = 0 -> Prime brute
        # Sinon -> Prime + (CPCUA / PartCie)
        df = df.withColumn('cotis_100',
            when(col('partbrut') == 0, col('prime'))
            .otherwise(col('prime') + (col('cpcua') / col('partcie')))
        )
        
        # Libellé Coassurance
        df = df.withColumn('coass',
            when(col('codecoas') == '0', lit('SANS COASSURANCE'))
            .when(col('codecoas') == 'A', lit('APERITION'))
            .when(col('codecoas') == 'C', lit('COASS. ACCEPTEE'))
            .when((col('typcontr') == 'A') & (col('codecoas') == 'R'), lit('REASS. ACCEPTEE'))
            .otherwise(lit(None))
        )
        
        # Type Nature Police (CDNATP)
        # T = Temporaire, R = Révisable/Abonnement, C = Chantier Unique
        df = df.withColumn('cdnatp',
            when((col('duree') == '00') & col('produit').isin(['CNR', 'CTR', 'DO0']), lit('C'))
            .when((col('duree') == '00') & (col('produit') == 'TRC'), lit('T'))
            .when(col('duree').isin(['01', '02', '03']), lit('R'))
            .otherwise(lit(''))
        )
        
        # Indicateurs binaires
        df = df.withColumn('top_coass',
            when(col('codecoas') == '0', lit(0)).otherwise(lit(1))
        )
        
        df = df.withColumn('top_lta',
            when(~col('duree').isin(['00', '01', '', ' ']), lit(1)).otherwise(lit(0))
        )
        
        if 'indregul' in df.columns:
            df = df.withColumn('top_revisable',
                when(col('indregul') == 'O', lit(1)).otherwise(lit(0))
            )
        else:
            df = df.withColumn('top_revisable', lit(0))
        
        # Initialisation champs texte vides
        df = df.withColumn('critere_revision', lit(''))
        df = df.withColumn('cdgrev', lit(''))
        df = df.withColumn('type_affaire', col('typcontr'))
        
        # Primes ventilées par mouvement (AFN / RES / PTF)
        # Exclusion du segment 5 (Accessoires) pour AFN/RES
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
        
        self.logger.info("✓ Primes et indicateurs calculés")
        return df

    def _enrich_formulas(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrichit avec les formules produits depuis RCENTCU et RISTECCU.
        Tentative sur RCENTCU puis fallback sur RISTECCU.
        """
        reader = BronzeReader(self.spark, self.config)
        
        # Initialisation
        from utils.processor_helpers import add_null_columns
        existing_cols = set(df.columns)
        for col_name in ['formule', 'formule2', 'formule3', 'formule4']:
            if col_name not in existing_cols:
                df = df.withColumn(col_name, lit(None).cast(StringType()))
        
        # 1. RCENTCU
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
                
                # Coalesce avec priorité RCENTCU
                df = df.withColumn('formule', coalesce(col('formule'), col('formule_rc')))
                df = df.withColumn('formule2', coalesce(col('formule2'), col('formule2_rc')))
                df = df.withColumn('formule3', coalesce(col('formule3'), col('formule3_rc')))
                df = df.withColumn('formule4', coalesce(col('formule4'), col('formule4_rc')))
                df = df.drop('formule_rc', 'formule2_rc', 'formule3_rc', 'formule4_rc')
        except Exception:
            pass
        
        # 2. RISTECCU (Fallback)
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
                
                # Coalesce avec priorité existant (RCENTCU) puis RISTECCU
                df = df.withColumn('formule', coalesce(col('formule'), col('formule_ris')))
                df = df.withColumn('formule2', coalesce(col('formule2'), col('formule2_ris')))
                df = df.withColumn('formule3', coalesce(col('formule3'), col('formule3_ris')))
                df = df.withColumn('formule4', coalesce(col('formule4'), col('formule4_ris')))
                df = df.drop('formule_ris', 'formule2_ris', 'formule3_ris', 'formule4_ris')
        except Exception:
            pass
        
        return df

    def _enrich_ca(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrichit avec le chiffre d'affaires (MTCA) agrégé depuis MULPROCU.
        """
        reader = BronzeReader(self.spark, self.config)

        try:
            df_mulprocu = reader.read_file_group('mulprocu_azec', vision)
            if df_mulprocu is not None:
                from pyspark.sql.functions import sum as spark_sum
                df_mulprocu_agg = df_mulprocu.groupBy('police').agg(
                    spark_sum('chiffaff').alias('mtca')
                )

                df = self.coalesce_from_right(
                    df,
                    df_mulprocu_agg,
                    keys=["police"],
                    cols=["mtca"]
                )
        except Exception:
            if 'mtca' not in df.columns:
                df = df.withColumn('mtca', lit(None).cast(DoubleType()))

        return df 

    def coalesce_from_right(self, df, right_df, keys, cols):
        """
        Utilitaire de jointure "Upsert" logique.
        
        Joint deux DataFrames et met à jour les colonnes de gauche avec les valeurs de droite
        si elles existent (Coalesce right -> left).
        Évite les duplications de colonnes et les ambiguïtés d'alias.
        """
        import pyspark.sql.functions as F

        # 1) Renommer les colonnes cibles du RIGHT (préfixe __r_)
        r = right_df
        for c in cols:
            if c in r.columns:
                r = r.withColumnRenamed(c, f"__r_{c}")

        # 2) Jointure Gauche
        out = df.join(r, on=keys, how="left")

        # 3) Coalesce : Valeur droite si existe, sinon gauche
        for c in cols:
            rc = f"__r_{c}"
            # Si __r_c existe dans le résultat de la jointure
            if rc in out.columns:
                out = out.withColumn(c, F.coalesce(F.col(rc), F.col(c)))
                out = out.drop(rc)

        return out

    def _enrich_pe_rd_vi(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrichit avec les capitaux Perte Exploitation et Risque Direct (INCENDCU).
        Calcul également la Valeur Assurée Totale (Value Insured).
        """
        reader = BronzeReader(self.spark, self.config)

        try:
            df_incendcu = reader.read_file_group('incendcu_azec', vision)
            if df_incendcu is not None:
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

                df = self.coalesce_from_right(
                    df,
                    df_pe_rd,
                    keys=["police", "produit"],
                    cols=["perte_exp", "risque_direct", "value_insured"]
                )
        except Exception:
            from utils.processor_helpers import add_null_columns
            df = add_null_columns(df, {
                'perte_exp': DoubleType,
                'risque_direct': DoubleType,
                'value_insured': DoubleType
            })

        return df

    def _enrich_final_constrcu_data(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Enrichissement final complexe (Triple Jointure).
        
        1. CONSTRCU_AZEC : Pour récupérer les segmentations fines (Segment 2, Type Produit 2).
        2. CONSTRCU (Raw) : Pour les données de site (Date ouverture chantier, destination...).
        3. PT_GEST : Pour le regroupement commercial UPPER_MID.
        """
        from pyspark.sql.functions import col
        from src.reader import BronzeReader
        
        reader = BronzeReader(self.spark, self.config)
        
        # --- 1. CONSTRCU_AZEC (Segmentation fine) ---
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
        except Exception as e:
            self.logger.warning(f"Calcul référentiel CONSTRCU_AZEC échoué: {e}")
            df_constrcu_azec = None
        
        # Préparation du DF de jointure (avec colonnes vides si échec)
        if df_constrcu_azec is not None:
            available_cols = df_constrcu_azec.columns
            if "segment" in available_cols and "type_produit" in available_cols:
                df_constrcu_azec_small = df_constrcu_azec.select(
                    col("police"),
                    col("produit").alias("cdprod"),
                    col("segment").alias("segment2"),
                    col("type_produit").alias("type_produit_2")
                ).dropDuplicates(["police", "cdprod"])
            else:
                from pyspark.sql.types import StructType, StructField, StringType
                schema = StructType([
                    StructField("police", StringType(), True),
                    StructField("cdprod", StringType(), True),
                    StructField("segment2", StringType(), True),
                    StructField("type_produit_2", StringType(), True)
                ])
                df_constrcu_azec_small = self.spark.createDataFrame([], schema)
        else:
            from pyspark.sql.types import StructType, StructField, StringType
            schema = StructType([
                StructField("police", StringType(), True),
                StructField("cdprod", StringType(), True),
                StructField("segment2", StringType(), True),
                StructField("type_produit_2", StringType(), True)
            ])
            df_constrcu_azec_small = self.spark.createDataFrame([], schema)
        
        # --- 2. CONSTRCU Raw (Données Site) ---
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
        
        # --- 3. PT_GEST (UPPER_MID) ---
        df_ptgest = reader.read_file_group("table_pt_gest", vision)
        
        df_ptgest = df_ptgest.select(
            col("ptgst").alias("_ptgst_key"),
            col("upper_mid")
        ).dropDuplicates(["_ptgst_key"])
        
        # --- 4. TRIPLE JOINTURE (Optimisée) ---
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
        
        # Sélection finale
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
        
        # Nettoyage des colonnes temporaires ou inutiles
        lob_cols_to_drop = [
            "cdprod", "cprod", "lmarch", "lmarch2", "lseg", "lssseg", "lprod", "activite"
        ]
        cols_to_drop = [c for c in lob_cols_to_drop if c in df.columns]
        if cols_to_drop:
            df = df.drop(*cols_to_drop)
        
        self.logger.info("✓ Enrichissement final terminé")
        
        return df