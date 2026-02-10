# -*- coding: utf-8 -*-
"""
===============================================================================
PROCESSEUR DES ÉMISSIONS (Calcul des Primes)
===============================================================================

Ce processeur gère le calcul des primes d'assurance (émissions) à partir des
données détaillées de facturation.

OBJECTIFS DU PROCESSEUR :
------------------------
1. Lire les données brutes de facturation (One BI)
2. Filtrer pour ne garder que le marché Construction
3. Calculer les montants de primes et de commissions
4. Enrichir les données avec la segmentation commerciale
5. Produire deux vues consolidées pour le reporting

SOURCES DE DONNÉES :
-------------------
- rf_fr1_prm_dtl_midcorp_m : Données détaillées de primes (Bronze)
- Tables de référence pour la segmentation (Bronze)

SORTIES (GOLD) :
---------------
1. primes_emises_{vision}_pol_garp : Vue détaillée par police et garantie
2. primes_emises_{vision}_pol : Vue agrégée par police (somme des garanties)
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, sum as _sum, coalesce
from config.constants import MARKET_CODE
from src.processors.base_processor import BaseProcessor
from utils.helpers import extract_year_month_int
from utils.loaders.config_loader import ConfigLoader
from utils.processor_helpers import enrich_segmentation


class EmissionsProcessor(BaseProcessor):
    """
    =======================================================================
    PROCESSEUR ÉMISSIONS
    =======================================================================
    
    Implémente la logique de transformation des primes d'arrurance.
    
    FLUX DE TRAITEMENT (WORKFLOW) :
    ------------------------------
    1. READ : Lecture du fichier One BI depuis la couche Bronze
    2. TRANSFORM :
       - Standardisation des colonnes
       - Application des filtres métier (produits, garanties, etc.)
       - Détermination du canal de distribution (CDPOLE)
       - Calcul de l'exercice (courant vs antérieur)
       - Extraction du code garantie (CGARP)
       - Calcul des montants (Primes HT, Commissions)
       - Enrichissement avec la segmentation
       - Agrégation selon deux niveaux de détail
    3. WRITE : Écriture des deux fichiers résultants en couche Gold
    """
    
    def __init__(self, spark, config: ConfigLoader, logger):
        """
        Initialise le processeur des émissions.
        
        PARAMÈTRES :
        -----------
        spark : SparkSession
            Session Spark active
        config : ConfigLoader
            Configuration du pipeline
        logger : PipelineLogger
            Système de journalisation
        """
        super().__init__(spark, config, logger)
        self.logger.info("Initialisation du Processeur Émissions")
    
    def read(self, vision: str) -> DataFrame:
        """
        Lit les données de primes depuis la couche Bronze.
        
        SOURCE :
        -------
        Fichier 'rf_fr1_prm_dtl_midcorp_m' défini dans reading_config.json.
        Ce fichier contient le détail des mouvements financiers (quittances).
        
        PARAMÈTRES :
        -----------
        vision : str
            La période à traiter (YYYYMM)
            
        RETOUR :
        -------
        DataFrame
            Les données brutes de facturation
        """
        self.logger.info(f"Lecture des données One BI pour la vision {vision}")
        
        from src.reader import BronzeReader
        from pathlib import Path
        
        # Récupérer le chemin de la config de lecture
        reading_config_path = self.config.get('config_files.reading_config', 'config/reading_config.json')
        
        # Convertir en chemin absolu si nécessaire
        if not Path(reading_config_path).is_absolute():
            reading_config_path = str(self.get_project_root() / reading_config_path)
        
        # Instancier le lecteur Bronze
        reader = BronzeReader(self.spark, self.config, reading_config_path)
        
        # Lire le groupe de fichiers correspondant aux primes
        df = reader.read_file_group('rf_fr1_prm_dtl_midcorp_m', vision)
        
        self.logger.success(f"Lecture terminée : {df.count():,} enregistrements chargés")
        return df
    
    def transform(self, df: DataFrame, vision: str) -> tuple:
        """
        Applique les règles de gestion pour calculer les primes.
        
        ÉTAPES DE TRANSFORMATION :
        -------------------------
        1. Mise en minuscules des colonnes
        2. Filtrage des données (périmètre métier)
        3. Calcul des attributs dérivés (Pôle, Exercice, Garantie)
        4. Calcul des montants (Primes, Commissions)
        5. Enrichissement (Segmentation)
        6. Agrégation finale
        
        PARAMÈTRES :
        -----------
        df : DataFrame
            Données brutes lues par read()
        vision : str
            Période traitée
            
        RETOUR :
        -------
        tuple (DataFrame, DataFrame)
            - df_pol_garp : Données agrégées par Police + Garantie
            - df_pol : Données agrégées par Police uniquement
        """
        self.logger.info("Démarrage des transformations Émissions")
        
        year_int, month_int = extract_year_month_int(vision)
        
        # --- Étape 1 : Standardisation ---
        self.logger.step(1, "Mise en minuscules des colonnes")
        from utils.transformations import lowercase_all_columns
        df = lowercase_all_columns(df)
        
        # --- Étape 2 : Filtrage métier ---
        self.logger.step(2, "Application des filtres métier")
        import json
        from pathlib import Path
        
        # Charger les règles de filtrage depuis le fichier JSON externe
        emissions_config_path = Path('config/transformations/emissions_config.json')
        
        if not emissions_config_path.is_absolute():
            emissions_config_path = self.get_project_root() / emissions_config_path
        
        with open(emissions_config_path, 'r') as f:
            emissions_config = json.load(f)
        
        from utils.transformations import apply_emissions_filters
        df = apply_emissions_filters(df, emissions_config, vision, self.logger)
        
        # --- Étape 3 : Détermination du pôle de distribution ---
        self.logger.step(3, "Calcul du code Pôle (CDPOLE)")
        from utils.transformations import assign_distribution_channel
        # Le pôle est déduit du canal de distribution (CD_NIV_2_STC)
        # 1 = Agents (DCAG, DCPS, DIGITAL), 3 = Courtage (BROKDIV)
        df = assign_distribution_channel(df)
        
        # --- Étape 4 : Calcul de l'exercice (Courant vs Antérieur) ---
        self.logger.step(4, "Calcul de l'exercice (Courant / Antérieur)")
        from utils.transformations import calculate_exercice_split
        # Compare l'année de rattachement comptable avec l'année de vision
        df = calculate_exercice_split(df, vision)
        
        # --- Étape 5 : Extraction du code garantie ---
        self.logger.step(5, "Extraction du code garantie (CGARP)")
        from utils.transformations import extract_guarantee_code
        # Extrait les caractères 3 à 5 du code garantie prospectif
        df = extract_guarantee_code(df)
        
        # --- Étape 6 : Calcul des montants ---
        self.logger.step(6, "Renommage et calcul des montants")
        
        # Renommage vers le modèle de données cible
        df = df.withColumnRenamed('nu_cnt_prm', 'nopol')     # Numéro de police
        df = df.withColumnRenamed('cd_prd_prm', 'cdprod')    # Code produit
        df = df.withColumnRenamed('cd_int_stc', 'noint')     # Numéro intermédiaire
        df = df.withColumnRenamed('mt_cms_cts', 'mtcom')     # Montant commission
        
        # Ajout de constantes
        df = df.withColumn('dircom', lit('AZ '))  # Direction commerciale (fixe)
        df = df.withColumn('vision', lit(vision))
        
        # Calcul des primes de l'exercice courant (primes_n)
        # Logique : On isole les lignes de l'exercice courant ('cou'), on somme,
        # puis on rejoint avec le dataset principal.
        
        # 1. Isoler les mouvements de l'exercice courant
        df_current = df.filter(col('exercice') == 'cou')
        
        # 2. Agréger les montants HT pour l'exercice courant
        df_current_agg = df_current.groupBy(
            'cdpole', 'cdprod', 'nopol', 'noint', 'cd_gar_princ', 
            'cd_gar_prospctiv', 'cd_cat_min', 'dircom'
        ).agg(
            _sum('mt_ht_cts').alias('primes_n_temp')
        )
        
        # 3. Réintégrer ce montant dans le dataset principal via jointure gauche
        df = df.join(
            df_current_agg,
            on=['cdpole', 'cdprod', 'nopol', 'noint', 'cd_gar_princ', 
                'cd_gar_prospctiv', 'cd_cat_min', 'dircom'],
            how='left'
        )
        
        # 4. Remplacer les NULL par 0 (pour les lignes sans prime exercice courant)
        df = df.withColumn('primes_n', coalesce(col('primes_n_temp'), lit(0.0)))
        df = df.drop('primes_n_temp')
        
        self.logger.info("Primes calculées : primes_x (total) et primes_n (exercice courant)")
        
        # --- Étape 7 : Enrichissement Segmentation ---
        self.logger.step(7, "Enrichissement avec la segmentation")
        from src.reader import BronzeReader
        from pathlib import Path
        
        # Recharger la config de lecture pour accéder aux fichiers référentiels
        reading_config_path = self.config.get('config_files.reading_config', 'config/reading_config.json')
        if not Path(reading_config_path).is_absolute():
            reading_config_path = str(self.get_project_root() / reading_config_path)
        
        reader = BronzeReader(self.spark, self.config, reading_config_path)
        
        # Ajouter les segments commerciaux via jointure
        # Note : Émissions nécessite une jointure sur cdprod ET cdpole spécifique
        df = enrich_segmentation(df, reader, vision, include_cdpole=True, logger=self.logger)
        
        # Filtrer pour ne garder que le marché Construction (Code 6)
        if 'cmarch' in df.columns:
            df = df.filter(col('cmarch') == MARKET_CODE.MARKET)
            self.logger.info(f"Filtre marché Construction appliqué : reste {df.count():,} lignes")
        
        # --- Étape 8 : Agrégations finales ---
        self.logger.step(8, "Création des fichiers de sortie agrégés")
        
        # Sélectionner uniquement les colonnes utiles
        df = df.select(
            'nopol', 'cdprod', 'noint', 'cgarp', 'cmarch', 'cseg', 'cssseg',
            'cdpole', 'vision', 'dircom', 'cd_cat_min', 'mt_ht_cts', 'primes_n', 'mtcom'
        )
        
        # SORTIE 1 : Agrégation par Police + Garantie (POL_GARP)
        from utils.transformations import aggregate_by_policy_guarantee
        
        group_cols_garp = [
            'vision', 'dircom', 'cdpole', 'nopol', 'cdprod', 'noint', 'cgarp',
            'cmarch', 'cseg', 'cssseg', 'cd_cat_min'
        ]
        
        # Somme les montants par clé de regroupement
        df_pol_garp = aggregate_by_policy_guarantee(df, group_cols_garp)
        
        self.logger.info(f"Agrégation POL_GARP terminée : {df_pol_garp.count():,} lignes")
        
        # SORTIE 2 : Agrégation par Police uniquement (POL)
        # On repart de POL_GARP pour éviter de tout recalculer (optimisation)
        group_cols_pol = [
            'vision', 'dircom', 'nopol', 'noint', 'cdpole', 'cdprod',
            'cmarch', 'cseg', 'cssseg'
        ]
        
        df_pol = df_pol_garp.groupBy(*group_cols_pol).agg(
            _sum('primes_x').alias('primes_x'),   # Somme totale
            _sum('primes_n').alias('primes_n'),   # Somme exercice courant
            _sum('mtcom_x').alias('mtcom_x')      # Somme commissions
        )
        
        self.logger.info(f"Agrégation POL terminée : {df_pol.count():,} lignes")
        
        self.logger.success("Transformations Émissions terminées avec succès")
        
        return df_pol_garp, df_pol
    
    def write(self, dfs: tuple, vision: str) -> None:
        """
        Écrit les résultats en couche Gold.
        
        FICHIERS PRODUITS :
        ------------------
        1. primes_emises_{vision}_pol_garp.parquet
        2. primes_emises_{vision}_pol.parquet
        
        PARAMÈTRES :
        -----------
        dfs : tuple
            Paire de DataFrames (df_pol_garp, df_pol) issue de transform()
        vision : str
            Période traitée
        """
        from config.column_definitions import GOLD_COLUMNS_EMISSIONS_POL_GARP, GOLD_COLUMNS_EMISSIONS_POL
        from utils.helpers import write_to_layer
        
        # Déballer le tuple
        df_pol_garp, df_pol = dfs
        
        # --- Écriture Fichier 1 : POL_GARP ---
        self.logger.info(f"Écriture du fichier POL_GARP pour vision {vision}")
        
        # Vérifier et sélectionner les colonnes attendues en Gold
        existing_cols_garp = [c for c in GOLD_COLUMNS_EMISSIONS_POL_GARP if c in df_pol_garp.columns]
        
        # Vérification de sécurité pour les colonnes manquantes
        missing_cols_garp = set(GOLD_COLUMNS_EMISSIONS_POL_GARP) - set(df_pol_garp.columns)
        if missing_cols_garp:
            self.logger.warning(f"POL_GARP : Colonnes manquantes {sorted(missing_cols_garp)}")
        
        df_pol_garp_final = df_pol_garp.select(existing_cols_garp)
        
        output_name_garp = f"primes_emises_{vision}_pol_garp"
        write_to_layer(
            df_pol_garp_final, self.config, 'gold', output_name_garp, vision, self.logger
        )
        self.logger.success(f"Fichier écrit : {output_name_garp}.parquet ({df_pol_garp_final.count():,} lignes)")
        
        # --- Écriture Fichier 2 : POL ---
        self.logger.info(f"Écriture du fichier POL pour vision {vision}")
        
        # Sélection des colonnes
        existing_cols_pol = [c for c in GOLD_COLUMNS_EMISSIONS_POL if c in df_pol.columns]
        
        missing_cols_pol = set(GOLD_COLUMNS_EMISSIONS_POL) - set(df_pol.columns)
        if missing_cols_pol:
            self.logger.warning(f"POL : Colonnes manquantes {sorted(missing_cols_pol)}")
        
        df_pol_final = df_pol.select(existing_cols_pol)
        
        output_name_pol = f"primes_emises_{vision}_pol"
        write_to_layer(
            df_pol_final, self.config, 'gold', output_name_pol, vision, self.logger
        )
        self.logger.success(f"Fichier écrit : {output_name_pol}.parquet ({df_pol_final.count():,} lignes)")
