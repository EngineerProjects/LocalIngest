# -*- coding: utf-8 -*-
"""
===============================================================================
PROCESSEUR CAPITAUX AZ (Canal Agents & Courtiers)
===============================================================================

Ce processeur traite les données de capitaux pour le périmètre "AZ", qui regroupe
les agents généraux et le courtage traditionnel.

OBJECTIFS DU PROCESSEUR :
------------------------
1. Lire les données des fichiers IPF (Inventaire Portefeuille)
2. Extraire les montants de capitaux assurés (SMP, LCI, etc.)
3. Gérer deux versions des capitaux : AVEC et SANS indexation
4. Normaliser les montants à 100% (base technique)
5. Enrichir avec la segmentation commerciale

SOURCES DE DONNÉES :
-------------------
- IPFE16 : Fichiers agents
- IPFE36 : Fichiers courtage
(Ces deux sources sont lues ensemble via le groupe 'ipf')

TRAITEMENTS SPÉCIFIQUES :
------------------------
- Extraction dynamique des capitaux (14 rubriques possibles)
- Application des coefficients d'indexation (PRPRVC)
- Règles de gestion métier (complétude SMP, limites RC)
"""

from pyspark.sql import DataFrame # type: ignore
from src.processors.base_processor import BaseProcessor
from utils.helpers import extract_year_month_int
from utils.loaders.config_loader import ConfigLoader
from utils.processor_helpers import enrich_segmentation
from pathlib import Path


class AZCapitauxProcessor(BaseProcessor):
    """
    =======================================================================
    PROCESSEUR CAPITAUX AZ
    =======================================================================
    
    Transforme les données brutes AZ en données Silver enrichies.
    
    FLUX DE TRAITEMENT (WORKFLOW) :
    ------------------------------
    1. READ : Lecture des fichiers IPF (Agents + Courtiers)
    2. TRANSFORM :
       - Filtrage métier
       - Configuration des colonnes
       - Extraction des capitaux INDEXÉS
       - Extraction des capitaux NON INDEXÉS
       - Normalisation à 100%
       - Application des règles métier
       - Enrichissement (Segmentation)
    3. WRITE : Écriture du fichier Silver resultats
    """
    
    def __init__(self, spark, config: ConfigLoader, logger):
        """
        Initialise le processeur AZ Capitaux.
        
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
        self.logger.info("Initialisation du Processeur Capitaux AZ")
    
    def read(self, vision: str) -> DataFrame:
        """
        Lit les données IPF (Inventaire Portefeuille) depuis la couche Bronze.
        
        FICHIES LUS :
        ------------
        Groupe 'ipf' qui combine :
        - IPFE16 (Agents)
        - IPFE36 (Courtiers)
        
        PARAMÈTRES :
        -----------
        vision : str
            La période à traiter (YYYYMM)
            
        RETOUR :
        -------
        DataFrame
            Les données brutes (Agents + Courtiers)
        """
        self.logger.info(f"Lecture des données capitaux AZ pour vision {vision}")
        
        from src.reader import BronzeReader
        
        # Charger la configuration de lecture
        reading_config_path = self.config.get('config_files.reading_config', 'config/reading_config.json')
        
        if not Path(reading_config_path).is_absolute():
            reading_config_path = str(self.get_project_root() / reading_config_path)
        
        reader = BronzeReader(self.spark, self.config, reading_config_path)

        # Lire les fichiers IPF (combine IPFE16 + IPFE36)
        df = reader.read_file_group('ipf', vision)
        
        self.logger.success(f"Lecture terminée : {df.count():,} enregistrements chargés (AZ)")
        return df
    
    def transform(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Applique les règles de gestion pour extraire et calculer les capitaux.
        
        ÉTAPES DE TRANSFORMATION :
        -------------------------
        1. Application des filtres métier (ex: exclusion des polices annulées)
        2. Renommage et sélection des colonnes
        3. Calcul des capitaux INDEXÉS (avec application des coefficients)
        4. Calcul des capitaux NON INDEXÉS (montants d'origine)
        5. Normalisation des montants (ramener à 100% si coassurance)
        6. Règles métier spécifiques (gestion des plafonds, complétude)
        7. Enrichissement avec la segmentation commerciale
        
        PARAMÈTRES :
        -----------
        df : DataFrame
            Données brutes lues par read()
        vision : str
            Période traitée
            
        RETOUR :
        -------
        DataFrame
            Données transformées prêtes pour la couche Silver
        """
        self.logger.info("Démarrage des transformations Capitaux AZ")
        
        year_int, month_int = extract_year_month_int(vision)
        
        # Chargement des configurations de transformation
        from utils.loaders.transformation_loader import get_default_loader
        loader = get_default_loader()
        
        # Configuration spécifique AZ
        az_config = loader.get_az_config()
        
        # Configuration pour l'extraction des capitaux
        import json
        config_path = 'config/transformations/capitaux_extraction_config.json'
        
        if not Path(config_path).is_absolute():
            config_path = self.get_project_root() / config_path
        
        with open(config_path, 'r') as f:
            capital_config = json.load(f)
        
        # --- Étape 1 : Filtres métier ---
        self.logger.step(1, "Application des filtres métier")
        from utils.transformations import apply_business_filters
        filters = az_config.get('business_filters', {}).get('filters', [])
        df = apply_business_filters(df, {'filters': filters}, self.logger)
        self.logger.info(f"Après filtres : {df.count():,} enregistrements")
        
        # --- Étape 2 : Configuration des colonnes ---
        self.logger.step(2, "Standardisation des colonnes")
        from utils.transformations import lowercase_all_columns, apply_column_config
        df = lowercase_all_columns(df)
        column_config = az_config['column_selection']
        df = apply_column_config(df, column_config, vision, year_int, month_int)
        
        # Ajout de l'indicateur de direction commerciale
        from pyspark.sql.functions import lit
        df = df.withColumn('dircom', lit('az'))
        
        # --- Étape 3 : Capitaux AVEC indexation ---
        self.logger.step(3, "Extraction des capitaux AVEC indexation")
        
        # Appliquer l'indexation sur les colonnes sources (mtcapi1 à 14)
        # STRATÉGIE : Utiliser les coefficients PRPRVC présents dans le fichier
        from utils.transformations.operations.indexation import index_capitals
        df = index_capitals(
            df,
            num_capitals=14,
            date_col='dtechann',
            contract_start_col='dtefsitt',
            capital_prefix='mtcapi',
            nature_prefix='cdprvb',
            index_prefix='prprvc',
            reference_date=None,   # Utilise la date courante
            use_index_table=False, # Utilise les colonnes PRPRVC du fichier (pas de table externe)
            index_table_df=None,
            logger=self.logger
        )
        
        # Extraire les colonnes indexées (suffixe _ind)
        from utils.transformations import extract_capitals_extended
        df = extract_capitals_extended(
            df,
            capital_config,
            num_capitals=14,
            indexed=True  # Mode indexé
        )
        
        self.logger.info("Capitaux indexés extraits : smp_100_ind, lci_100_ind, etc.")
        
        # --- Étape 4 : Capitaux SANS indexation ---
        self.logger.step(4, "Extraction des capitaux SANS indexation")
        
        # Extraire les colonnes brutes
        df = extract_capitals_extended(
            df,
            capital_config,
            num_capitals=14,
            indexed=False  # Mode non indexé
        )
        
        self.logger.info("Capitaux non indexés extraits : smp_100, lci_100, etc.")
        
        # --- Étape 5 : Normalisation à 100% ---
        self.logger.step(5, "Normalisation à 100% (Base Technique)")
        
        from utils.transformations import normalize_capitals_to_100
        
        # Liste de toutes les colonnes capitaux à normaliser
        indexed_cols = [
            'smp_100_ind', 'lci_100_ind', 'perte_exp_100_ind', 'risque_direct_100_ind',
            'limite_rc_100_par_sin_ind', 'limite_rc_100_par_sin_tous_dom_ind', 'limite_rc_100_par_an_ind',
            'smp_pe_100_ind', 'smp_rd_100_ind'
        ]

        non_indexed_cols = [
            'smp_100', 'lci_100', 'perte_exp_100', 'risque_direct_100',
            'limite_rc_100_par_sin', 'limite_rc_100_par_sin_tous_dom', 'limite_rc_100_par_an',
            'smp_pe_100', 'smp_rd_100'
        ]
        
        all_capital_cols = indexed_cols + non_indexed_cols
        
        # Diviser par le pourcentage de part (prcdcie) pour obtenir le 100%
        df = normalize_capitals_to_100(df, all_capital_cols, 'prcdcie')
        
        self.logger.info("Capitaux ramenés à 100% selon la part compagnie (prcdcie)")
        
        # --- Étape 6 : Règles métier ---
        self.logger.step(6, "Application des règles de gestion")
        
        from utils.transformations import apply_capitaux_business_rules
        
        # Appliquer pour les deux jeux de colonnes
        df = apply_capitaux_business_rules(df, indexed=True)
        df = apply_capitaux_business_rules(df, indexed=False)
        
        self.logger.info("Règles appliquées : complétude SMP, limites Responsabilité Civile")
        
        # --- Étape 7 : Enrichissement Segmentation ---
        self.logger.step(7, "Enrichissement avec la segmentation")
        
        from src.reader import BronzeReader
        
        reading_config_path = self.config.get('config_files.reading_config', 'config/reading_config.json')
        if not Path(reading_config_path).is_absolute():
            reading_config_path = str(self.get_project_root() / reading_config_path)
        
        reader = BronzeReader(self.spark, self.config, reading_config_path)

        # Nettoyer les colonnes existantes pour éviter les doublons
        cols_to_drop = ['cmarch', 'cseg', 'cssseg']
        for col in cols_to_drop:
            if col in df.columns:
                df = df.drop(col)
        
        # Ajouter les segments commerciaux
        # AZ nécessite une jointure sur Code Produit + Code Pôle
        df = enrich_segmentation(df, reader, vision, include_cdpole=True, logger=self.logger)
        
        self.logger.success("Transformations Capitaux AZ terminées")
        return df
    
    def write(self, df: DataFrame, vision: str) -> None:
        """
        Écrit les résultats transformés en couche Silver.
        
        FICHIER PRODUIT :
        ----------------
        az_capitaux_{vision}.parquet
        
        PARAMÈTRES :
        -----------
        df : DataFrame
            Données transformées
        vision : str
            Période traitée
        """
        self.logger.info(f"Écriture des données Silver AZ pour vision {vision}")
        
        from utils.helpers import write_to_layer
        
        output_name = f"az_capitaux_{vision}"
        write_to_layer(
            df, self.config, 'silver', output_name, vision, self.logger
        )
        
        self.logger.success(f"Fichier écrit : {output_name}.parquet ({df.count():,} enregistrements)")
