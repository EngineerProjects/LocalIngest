# -*- coding: utf-8 -*-
"""
===============================================================================
PROCESSEUR AZ (Mouvements de Portefeuille - Agents & Courtiers)
===============================================================================

Ce processeur gère le traitement des mouvements de portefeuille pour le périmètre "AZ",
qui regroupe les canaux Agents et Courtage.

OBJECTIFS DU PROCESSEUR :
------------------------
1. Lire les fichiers de mouvements (PTF16 pour Agents, PTF36 pour Courtage)
2. Appliquer les filtres métier (périmètre marché Construction)
3. Initialiser et calculer les indicateurs clés (Primes, Expositions, Capitaux)
4. Enrichir les données avec la segmentation produit et client
5. Produire une vision consolidée "Silver" des mouvements

SOURCES DE DONNÉES :
-------------------
- PTF16 (Agents) et PTF36 (Courtiers)
- Tables de référence : PRDPFA1, PRDPFA3, CPRODUIT, PRDCAP, TABLE_PT_GEST, IPFM99

FLUX DE TRAITEMENT :
-------------------
Le traitement suit une logique séquentielle pour transformer les données brutes
en données exploitables pour le reporting.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, coalesce, broadcast, to_date, expr, create_map, make_date
)
from pyspark.sql.types import DoubleType, StringType, DateType

# Classe de base
from src.processors.base_processor import BaseProcessor

# Chargeurs de configuration
from utils.loaders import get_default_loader

# Constantes
from config.constants import DIRCOM, POLE

# Utilitaires
from utils.helpers import extract_year_month_int, compute_date_ranges

# Logique métier
from utils.transformations.base.column_operations import apply_column_config
from utils.transformations.operations.business_logic import (
    extract_capitals,
    calculate_az_movements,
    calculate_exposures,
)

# Assistants processeur
from utils.processor_helpers import (
    safe_reference_join,
    safe_multi_reference_join,
    add_null_columns
)

class AZProcessor(BaseProcessor):
    """
    =======================================================================
    PROCESSEUR DE MOUVEMENTS AZ
    =======================================================================
    
    Transforme les données brutes de portefeuille (Bronze) en couche Silver.
    
    WORKFLOW DÉTAILLÉ :
    ------------------
    1. READ : Lecture combinée des fichiers Agents et Courtage avec filtrage préalable
    2. TRANSFORM :
       - Initialisation des colonnes techniques
       - Ajout des métadonnées (vision, dates)
       - Gestion de la Coassurance
       - Jointures référentielles (IPFM99 pour CA spécial)
       - Extraction des capitaux (SMP, LCI, etc.)
       - Calcul des indicateurs de mouvement (Affaires Nouvelles, Résiliations...)
       - Calcul des expositions (Durée de couverture)
       - Calcul des cotisations à 100%
       - Enrichissement complet de la segmentation
       - Déduplication finale
    3. WRITE : Écriture en couche Silver
    """

    def read(self, vision: str) -> DataFrame:
        """
        Lit les fichiers PTF16 (Agents) et PTF36 (Courtage) depuis la couche Bronze.
        
        LOGIQUE DE LECTURE :
        -------------------
        Les filtres métier (ex: cmarch='6' pour Construction) sont appliqués
        DIRECTEMENT à la lecture de chaque fichier, AVANT de les fusionner.
        Cela optimise les performances en réduisant le volume de données dès le départ.
        
        PARAMÈTRES :
        -----------
        vision : str
            Vision (Période) à traiter au format YYYYMM
            
        RETOUR :
        -------
        DataFrame
            Données brutes filtrées et fusionnées (Agents + Courtiers)
        """
        from src.reader import BronzeReader
        reader = BronzeReader(self.spark, self.config)
        
        # Chargement des filtres métier depuis la configuration centralisée
        loader = get_default_loader()
        business_rules = loader.get_business_rules()
        az_filters = business_rules.get('business_filters', {}).get('az', {}).get('filters', [])
        
        # Conversion du format de règles métier vers le format attendu par le Reader
        # Exemple : 'equals' devient '=='
        operator_map = {
            'equals': '==', 'not_equals': '!=', 'in': 'in', 'not_in': 'not_in',
            'greater_than': '>', 'less_than': '<', 'greater_equal': '>=', 'less_equal': '<='
        }
        custom_filters = [
            {
                'operator': operator_map.get(f.get('type'), f.get('type')),
                'column': f.get('column'),
                'value': f.get('value') if 'value' in f else f.get('values')
            }
            for f in az_filters
        ]
        
        self.logger.info(f"Lecture des fichiers IPF (PTF16 + PTF36) avec {len(custom_filters)} filtres appliqués à la source")
        
        # Lecture du groupe 'ipf' qui contient les deux fichiers
        return reader.read_file_group('ipf', vision, custom_filters=custom_filters)


    def transform(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Applique l'ensemble des transformations métier AZ.
        
        C'est le cœur du réacteur : toutes les règles de gestion sont appliquées ici.
        
        PARAMÈTRES :
        -----------
        df : DataFrame
            Données brutes lues par read()
        vision : str
            Vision (Période) à traiter
            
        RETOUR :
        -------
        DataFrame
            Données transformées prêtes pour la couche Silver
        """

        from pyspark.sql.functions import (
            col, when, lit, coalesce, to_date, expr, create_map
        )

        year_int, month_int = extract_year_month_int(vision)
        dates = compute_date_ranges(vision)
        
        loader = get_default_loader()
        az_config = loader.get_az_config()

        # ============================================================
        # ÉTAPE 1 : Initialisation des colonnes techniques
        # ============================================================
        self.logger.step(1, "Initialisation des indicateurs et colonnes techniques")
        
        # On s'assure que toutes les colonnes nécessaires existent, initialisées à 0 ou NULL
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
            # 'ctduree' est préservée si elle existe déjà dans les données
            'ctduree': lit(None).cast('double'),
        }
        
        for name, value in init_columns.items():
            if name not in df.columns:
                df = df.withColumn(name, value)

        # ============================================================
        # ÉTAPE 2 : Métadonnées et Code Pôle
        # ============================================================
        self.logger.step(2, "Ajout des métadonnées (Vision, Direction)")
        
        metadata_cols = {
            'dircom': lit(DIRCOM.AZ),
            'vision': lit(vision),
            'exevue': lit(year_int),
            'moisvue': lit(month_int)
        }
        for name, value in metadata_cols.items():
            df = df.withColumn(name, value)

        # Détermination propre du Code Pôle (CDPOLE)
        # 1 = Agents (via PTF16), 3 = Courtage (via PTF36)
        if '_source_file' in df.columns:
            df = df.withColumn(
                'cdpole',
                when(col('_source_file').contains('ipf16'), lit('1'))
                .when(col('_source_file').contains('ipf36'), lit('3'))
                .otherwise(lit('1')) # Valeur par défaut
            ).drop('_source_file')
        else:
            # Sécurité si la colonne source n'est pas là
            df = df.withColumn('cdpole', lit('1'))

        df = df.withColumn("cdpole", col("cdpole").cast("string"))

        # ============================================================
        # ÉTAPE 3 : Renommage des colonnes (Standardisation)
        # ============================================================
        self.logger.step(3, "Renommage des colonnes selon la configuration")
        
        rename_map = az_config.get("column_selection", {}).get("rename", {})
        self.logger.info(f"Application de {len(rename_map)} renommages")
        
        for old, new in rename_map.items():
            if old in df.columns:
                df = df.withColumnRenamed(old, new)

        # ============================================================
        # ÉTAPE 4 : Gestion de la Coassurance
        # ============================================================
        self.logger.step(4, "Calcul des indicateurs de Coassurance")
        
        # Standardisation du nom de colonne (cdcoas -> cdcoass)
        if 'cdcoass' not in df.columns and 'cdcoas' in df.columns:
            df = df.withColumnRenamed('cdcoas', 'cdcoass')

        # Définition du type de coassurance (Libellé)
        df = df.withColumn(
            "coass",
            when((col("cdpolqpl") == "1") & col("cdcoass").isin("3", "6"), lit("APERITION"))
            .when((col("cdpolqpl") == "1") & col("cdcoass").isin("4", "5"), lit("COASS. ACCEPTEE"))
            .when((col("cdpolqpl") == "1") & (col("cdcoass") == "8"), lit("ACCEPTATION INTERNATIONALE"))
            .when((col("cdpolqpl") == "1"), lit("AUTRES"))
            .otherwise(lit("SANS COASSURANCE"))
        )

        # Calcul de la Part Compagnie (pour normalisation future)
        # Si pas chef de file, part = 100% (1.0)
        # Sinon, on prend le pourcentage réel divisé par 100
        df = df.withColumn(
            "partcie",
            when(col("cdpolqpl") != "1", lit(1.0))      # San Coassurance
            .otherwise(col("prcdcie") / 100.0)          # Avec Coassurance
        )

        # ============================================================
        # ÉTAPE 4bis : Champs calculés génériques (Niveau 1)
        # ============================================================
        self.logger.step(4, "Application des champs calculés génériques (Niveau 1)")
        select_computed = az_config.get("column_selection", {}).get("computed", {})
        df = self._apply_computed_generic(df, select_computed)

        # Gestion spécifique du CRITERE_REVISION avec mapping
        rev_cfg = az_config.get("revision_criteria", {})
        src = rev_cfg.get("source_col", "cdgrev")
        mapping = rev_cfg.get("mapping", {})
        
        if mapping and src in df.columns:
            # Création d'une map Spark pour traduire les codes en libellés
            kv = []
            for k, v in mapping.items():
                kv += [lit(k), lit(v)]
            df = df.withColumn("critere_revision", create_map(kv)[col(src)])

        # ============================================================
        # ÉTAPE 5 : Jointure spécifique IPFM99 (Chiffre d'Affaires Spécial)
        # ============================================================
        self.logger.step(5, "Jointure IPFM99 (Gestion spéciale produit 01099)")
        df = self._join_ipfm99(df, vision)

        # ============================================================
        # ÉTAPE 6 : Extraction des Capitaux (LCI, SMP, RD, PE)
        # ============================================================
        self.logger.step(6, "Extraction des rubriques de Capitaux")
        
        capital_cfg = az_config['capital_extraction']
        
        # L'ordre d'exécution est important (le dernier écrase les précédents si conflit)
        # 1. LCI (Limite Contractuelle d'Indemnité)
        # 2. SMP (Sinistre Maximum Possible)
        # 3. Risque Direct
        # 4. Perte d'Exploitation
        df = extract_capitals(df, {
            'lci_100': capital_cfg['lci_100'],
            'smp_100': capital_cfg['smp_100'],
            'risque_direct': capital_cfg['risque_direct'],
            'perte_exp': capital_cfg['perte_exp']
        })

        # ============================================================
        # ÉTAPE 7 : Champs calculés génériques (Niveau 2)
        # ============================================================
        self.logger.step(7, "Application des champs calculés génériques (Niveau 2)")
        update_computed = az_config.get("computed_fields", {})
        df = self._apply_computed_generic(df, update_computed)

        # ============================================================
        # ÉTAPE 8 : Indicateurs de Mouvement (AFN, RES, RPT...)
        # ============================================================
        self.logger.step(8, "Calcul des indicateurs de mouvement")
        
        # Identifie le type de mouvement (Affaire Nouvelle, Résiliation, etc.)
        # en fonction des dates d'effet par rapport à la vision
        movement_cols = az_config['movements']['column_mapping']
        df = calculate_az_movements(df, dates, year_int, movement_cols)

        # ============================================================
        # ÉTAPE 9 : Calcul des Expositions (Durée de couverture)
        # ============================================================
        self.logger.step(9, "Calcul des expositions (YTD / GLI)")
        
        # Calcule le temps passé en risque sur l'année (YTD) ou sur période glissante (GLI)
        exposure_cols = az_config['exposures']['column_mapping']
        df = calculate_exposures(df, dates, year_int, exposure_cols)

        # ============================================================
        # ÉTAPE 10 : Calcul des Cotisations
        # ============================================================
        self.logger.step(10, "Calcul des cotisations techniques (100% et MTCA)")

        # Règle de sécurité : PRCDCIE = 100 si manquant ou 0
        df = df.withColumn(
            'prcdcie',
            when((col('prcdcie').isNull()) | (col('prcdcie') == 0), lit(100))
            .otherwise(col('prcdcie'))
        )

        # Calcul de la cotisation ramenée à 100% pour la Coassurance Acceptée
        df = df.withColumn(
            'cotis_100',
            when(
                (col('top_coass') == 1) & (col('cdcoass').isin(['4', '5'])),
                (col('mtprprto') * 100) / col('prcdcie')
            ).otherwise(lit(0.0))
        )

        # Consolidation des Montants Chiffre d'Affaires (MTCA)
        df = df.withColumn("mtca", coalesce(col("mtca"), lit(0.0))) \
            .withColumn("mtcaf", coalesce(col("mtcaf"), lit(0.0))) \
            .withColumn("mtca_", col("mtcaf") + col("mtca"))

        # ============================================================
        # ÉTAPE 11 : Indicateur TOP_AOP (Appel d'Offre)
        # ============================================================
        self.logger.step(11, "Calcul du flag TOP_AOP")
        df = df.withColumn(
            'top_aop',
            when(col('opapoffr') == 'O', lit(1)).otherwise(lit(0))
        )

        # ============================================================
        # ÉTAPE 12 : Mouvements Anticipés
        # ============================================================
        self.logger.step(12, "Détection des mouvements anticipés")
        
        finmois = dates['finmois']
        df = df.withColumn("finmois", lit(finmois).cast("date"))

        # AFN Anticipée : Date effet future par rapport à la vision
        df = df.withColumn(
            "nbafn_anticipe",
            when(
                ((col("dteffan") > col("finmois")) | (col("dtcrepol") > col("finmois"))) &
                ~((col("cdtypli1") == "RE") | (col("cdtypli2") == "RE") | (col("cdtypli3") == "RE")),
                lit(1)
            ).otherwise(lit(0))
        )

        # Résiliation Anticipée : Date résiliation future
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
        # ÉTAPE 13 : Nettoyage final des données
        # ============================================================
        self.logger.step(13, "Nettoyage final (Dates d'exposition, Client)")
        
        # Nettoyage des dates d'exposition si l'exposition est nulle
        if 'expo_ytd' in df.columns and 'dt_deb_expo' in df.columns and 'dt_fin_expo' in df.columns:
            df = df.withColumn(
                'dt_deb_expo',
                when(col('expo_ytd') == 0, lit(None)).otherwise(col('dt_deb_expo'))
            ).withColumn(
                'dt_fin_expo',
                when(col('expo_ytd') == 0, lit(None)).otherwise(col('dt_fin_expo'))
            )
        
        # Remplacement du Numéro Client vide par le Numéro Actuaire
        if 'nmclt' in df.columns and 'nmacta' in df.columns:
            df = df.withColumn(
                'nmclt',
                when(
                    (col('nmclt').isNull()) | (col('nmclt') == '') | (col('nmclt') == ' '),
                    col('nmacta')
                ).otherwise(col('nmclt'))
            )

        # ============================================================
        # ÉTAPE 14 : Enrichissement Segmentation
        # ============================================================
        self.logger.step(14, "Enrichissement Segmentation et Typologie")
        # Cette méthode jointe les référentiels PRDPFA1, PRDPFA3, CPRODUIT, etc.
        df = self._enrich_segment_and_product_type(df, vision)

        # ============================================================
        # ÉTAPE 15 : Déduplication
        # ============================================================
        self.logger.step(15, "Déduplication par Numéro de Police")
        
        # On ne garde qu'une seule ligne par police
        # Le tri par 'cdsitp' (Code Situation) permet de prioriser les lignes les plus récentes/pertinentes
        df = df.orderBy("nopol", "cdsitp").dropDuplicates(["nopol"])
        
        self.logger.success("Transformations AZ terminées avec succès")
        return df


    def write(self, df: DataFrame, vision: str) -> None:
        """
        Écrit les données transformées en couche Silver.
        
        FICHIER DE SORTIE :
        ------------------
        mvt_const_ptf_{vision}.parquet
        
        PARAMÈTRES :
        -----------
        df : DataFrame
            Données finales transformées
        vision : str
            Vision traitée
        """
        from utils.helpers import write_to_layer
        write_to_layer(
            df=df,
            config=self.config,
            layer="silver",
            filename=f'mvt_const_ptf_{vision}',
            vision=vision,
            logger=self.logger,
            optimize=True, # Optimisation Z-ORDER activée pour performance de lecture
        )

    def _apply_computed_generic(self, df: DataFrame, computed_config: dict) -> DataFrame:
        """
        Moteur générique de calcul de champs.
        
        Permet d'appliquer des règles de calcul définies dans la configuration JSON
        sans écrire de code Spark spécifique.
        
        TYPES DE CALCULS SUPPORTÉS :
        --------------------------
        - coalesce_default : Prend une colonne, remplace NULL par défaut
        - flag_equality : 1 si égalité, 0 sinon
        - expression : Formule SQL libre
        - conditional : Série de conditions "si... alors..." (CASE WHEN)
        
        PARAMÈTRES :
        -----------
        df : DataFrame
            Données en entrée
        computed_config : dict
            Configuration des champs à calculer
            
        RETOUR :
        -------
        DataFrame
            Données avec les nouvelles colonnes
        """

        from pyspark.sql.functions import col, lit, when, expr

        if not computed_config:
            return df

        # --- Fonction interne : Normalise les valeurs littérales en colonnes Spark ---
        def _normalize(value):
            # Gestion explicite du NULL
            if value is None:
                return lit(None).cast("string")

            # Gestion des chaînes
            if isinstance(value, str):
                cleaned = value.strip()

                # Marqueurs de valeur manquante
                if cleaned in ["", ".", "NULL", "NA"]:
                    return lit(None).cast("string")

                # Détection numérique dans une chaîne
                if cleaned.replace(".", "", 1).isdigit():
                    if "." in cleaned:
                        return lit(float(cleaned))
                    else:
                        return lit(int(cleaned))

                # Sinon, c'est une vraie chaîne
                return lit(cleaned)

            # Gestion des nombres natifs Python
            if isinstance(value, (int, float)):
                return lit(value)

            # Fallback
            return lit(str(value))

        # --- Boucle de traitement des champs ---
        for field_name, field_def in computed_config.items():
            if field_name == "description":
                continue

            field_type = field_def.get("type")

            # TYPE 1 : COALESCE (Valeur par défaut si NULL)
            if field_type == "coalesce_default":
                source = field_def["source_col"]
                default = field_def["default"]
                df = df.withColumn(
                    field_name,
                    when(col(source).isNull(), _normalize(default)).otherwise(col(source))
                )

            # TYPE 2 : FLAG D'ÉGALITÉ (Binaire 0/1)
            elif field_type == "flag_equality":
                source = field_def["source_col"]
                value = field_def["value"]
                df = df.withColumn(
                    field_name,
                    when(col(source) == value, lit(1)).otherwise(lit(0))
                )

            # TYPE 3 : EXPRESSION LIBRE (Formule SQL)
            elif field_type == "expression":
                formula = field_def["formula"]
                df = df.withColumn(field_name, expr(formula))

            # TYPE 4 : CONDITIONNEL (Case When multiple)
            elif field_type == "conditional":
                conditions = field_def["conditions"]
                default_v = field_def["default"]

                # Initialiser avec la valeur par défaut
                result_expr = _normalize(default_v)

                # Appliquer les conditions en ordre inverse (construction par empilement)
                for cond in reversed(conditions):
                    check_expr = expr(cond["check"])  # Condition SQL
                    result_val = _normalize(cond["result"])

                    result_expr = when(check_expr, result_val).otherwise(result_expr)

                df = df.withColumn(field_name, result_expr)

            else:
                self.logger.warning(f"Type de champ calculé inconnu : {field_type}")

        return df
                
    def _join_ipfm99(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Effectue la jointure spécifique avec IPFM99 (Gestion spéciale).
        
        BUT :
        ----
        Pour le produit spécifique '01099', les montants de chiffre d'affaires
        ne sont pas dans le fichier principal mais dans IPFM99.
        
        PARAMÈTRES :
        -----------
        df : DataFrame
            Données principales AZ
        vision : str
            Vision traitée
            
        RETOUR :
        -------
        DataFrame
            Données enrichies avec les montants IPFM99 intégrés
        """
        from src.reader import BronzeReader
        reader = BronzeReader(self.spark, self.config)
        
        # IPFM99 est optionnel
        # Clés de jointure : CDPOLE, CDPROD, NOPOL, NOINT
        # Il est CRUCIAL d'inclure cdpole pour éviter de mélanger Agents(1) et Courtage(3)
        df = safe_reference_join(
            df, reader,
            file_group='ipfm99',
            vision=vision,
            join_keys=['cdpole', 'cdprod', 'nopol', 'noint'],
            select_columns=['mtcaenp', 'mtcasst', 'mtcavnt'],
            null_columns={'mtcaenp': DoubleType, 'mtcasst': DoubleType, 'mtcavnt': DoubleType},
            filter_condition="cdprod == '01099'", # Optimisation : on ne charge que le produit concerné
            use_broadcast=True,
            logger=self.logger,
            required=False
        )
        
        # Consolidation : Si produit 01099, on somme les composantes IPFM99
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
        Enrichit les données avec la Segmentation et le Type de Produit.
        
        MÉCANISME COMPLEXE :
        -------------------
        1. Charge PRDPFA1 (Segmentation Agents)
        2. Charge PRDPFA3 (Segmentation Courtiers)
        3. Fusionne les deux référentiels
        4. Complète avec CPRODUIT (Type Produit)
        5. Complète avec PRDCAP (Libellés Produit)
        6. Joint avec les données principales sur (Code Produit + Code Pôle)
        
        POURQUOI UNE LOGIQUE SI COMPLEXE ?
        ---------------------------------
        Un même code produit peut avoir une segmentation différente chez les Agents
        et chez les Courtiers. Il faut donc absolument distinguer par 'reseau' (Pôle).
        
        PARAMÈTRES :
        -----------
        df : DataFrame
            Données principales (doit contenir cdprod et cdpole)
        vision : str
            Vision traitée
        
        RETOUR :
        -------
        DataFrame
            Données enrichies avec segment2, type_produit_2, upper_mid
        """
        self.logger.info("Enrichissement Segmentation (via PRDPFA1/PRDPFA3)...")
        from src.reader import BronzeReader
        reader = BronzeReader(self.spark, self.config)
        
        # --- ÉTAPE 1 : Chargement PRDPFA1 (Agents) ---
        try:
            df_prdpfa1 = reader.read_file_group('segmprdt_prdpfa1', 'ref')
        except FileNotFoundError as e:
            self.logger.error("Référentiel PRDPFA1 manquant (Bloquant pour AZ)")
            raise RuntimeError("Fichier requis manquant : PRDPFA1") from e
        
        # Filtre Construction (6) et Ajout marqueur Réseau = '1'
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
        
        self.logger.info(f"✓ PRDPFA1 (Agents) chargé : {df_segment1.count()} produits")
        
        # --- ÉTAPE 2 : Chargement PRDPFA3 (Courtiers) ---
        try:
            df_prdpfa3 = reader.read_file_group('segmprdt_prdpfa3', 'ref')
        except FileNotFoundError as e:
            self.logger.error("Référentiel PRDPFA3 manquant (Bloquant pour AZ)")
            raise RuntimeError("Fichier requis manquant : PRDPFA3") from e
        
        # Filtre Construction (6) et Ajout marqueur Réseau = '3'
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
        
        self.logger.info(f"✓ PRDPFA3 (Courtage) chargé : {df_segment3.count()} produits")
        
        # --- ÉTAPE 3 : Fusion des deux référentiels ---
        df_segment = df_segment1.unionByName(df_segment3, allowMissingColumns=True)
        self.logger.info(f"✓ Segmentation combinée : {df_segment.count()} produits au total")
        
        # --- ÉTAPE 4 : Enrichissement via CPRODUIT (Type Produit) ---
        try:
            df_cproduit = reader.read_file_group('cproduit', 'ref')
        except FileNotFoundError:
            self.logger.warning("⚠️ CPRODUIT non trouvé, Type_Produit_2 sera vide")
            df_cproduit = None
        
        if df_cproduit is not None:
            df_cproduit_enrichment = df_cproduit.select(
                col('cprod'),
                col('Type_Produit_2').alias('type_produit_2'),
                col('segment').alias('segment_from_cproduit'),
                col('Segment_3').alias('segment_3')
            ).dropDuplicates(['cprod'])  
            
            df_segment = df_segment.join(
                df_cproduit_enrichment,
                on='cprod',
                how='left'
            )
            self.logger.info("✓ Enrichissement Type Produit via CPRODUIT effectué")
        
        # --- ÉTAPE 5 : Enrichissement Libellés via PRDCAP ---
        try:
            df_prdcap = reader.read_file_group('prdcap', 'ref')
            df_prdcap = df_prdcap.select(
                col('cdprod').alias('cprod'),
                col('lbtprod').alias('lprod_prdcap') # Libellé long
            ).dropDuplicates(['cprod'])
            
            # Mise à jour du libellé si présent dans PRDCAP
            df_segment = df_segment.join(
                df_prdcap,
                on='cprod',
                how='left'
            ).withColumn(
                'lprod',
                when(col('lprod_prdcap').isNotNull(), col('lprod_prdcap')).otherwise(col('lprod'))
            ).drop('lprod_prdcap')
            
            self.logger.info("✓ Mise à jour des libellés via PRDCAP effectuée")
        except FileNotFoundError:
            self.logger.warning("⚠️ PRDCAP non trouvé, conservation des libellés originaux")
        
        # --- ÉTAPE 6 : Préparation de la table de jointure ---
        # Nettoyage des doublons sur la clé (Réseau + Produit)
        df_segment = df_segment.dropDuplicates(['reseau', 'cprod'])
        
        # Nettoyage des espaces (Trim) sur les clés de jointure
        from pyspark.sql.functions import trim
        df = df.withColumn('cdpole', trim(col('cdpole'))) \
               .withColumn('cdprod', trim(col('cdprod')))
        
        # --- ÉTAPE 7 : Jointure Principale ---
        # On joint sur Code Produit ET Code Pôle
        df = df.join(
            broadcast(df_segment.select(
                col('cprod').alias('cdprod'),
                col('reseau').alias('cdpole'),
                # Sélection sécurisée des colonnes optionnelles
                col('segment_from_cproduit').alias('segment2') if 'segment_from_cproduit' in df_segment.columns else lit(None).alias('segment2'),
                col('type_produit_2') if 'type_produit_2' in df_segment.columns else lit(None).alias('type_produit_2')
            )),
            on=['cdprod', 'cdpole'],
            how='left'
        )
        
        self.logger.info("✓ Jointure Segmentation (CDPROD + CDPOLE) terminée")
        
        # --- ÉTAPE 8 : Enrichissement UPPER_MID (via TABLE_PT_GEST) ---
        year_int, month_int = extract_year_month_int(vision)
        
        # Gestion historique : bascule de version de fichier en Janvier 2012
        if year_int < 2011 or (year_int == 2011 and month_int <= 12):
            pt_gest_vision = '201201' # Version figée pour l'historique
            self.logger.info(f"Utilisation TABLE_PT_GEST historique (vision {pt_gest_vision})")
        else:
            pt_gest_vision = vision # Version courante
            self.logger.info(f"Utilisation TABLE_PT_GEST courante (vision {vision})")
        
        # Jointure sécurisée avec fallback
        df = safe_reference_join(
            df, reader,
            file_group='table_pt_gest',
            vision=pt_gest_vision,
            join_keys='ptgst',
            select_columns=['upper_mid'],
            null_columns={'upper_mid': StringType},
            use_broadcast=True,
            logger=self.logger,
            required=False
        )
        
        return df
