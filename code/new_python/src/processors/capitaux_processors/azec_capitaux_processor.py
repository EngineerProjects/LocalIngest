# -*- coding: utf-8 -*-
"""
===============================================================================
PROCESSEUR CAPITAUX AZEC (Canal Construction)
===============================================================================

Ce processeur traite les données de capitaux pour le périmètre "AZEC" (Construction).

OBJECTIFS DU PROCESSEUR :
------------------------
1. Lire le fichier CAPITXCU pour obtenir les capitaux de base (SMP/LCI)
2. Lire le fichier INCENDCU pour obtenir les Perte d'Exploitation (PE) et Risques Directs (RD)
3. Consolider les informations de capitaux
4. Enrichir avec la segmentation
5. Filtrer sur le marché Construction uniquement

SOURCES DE DONNÉES :
-------------------
- CAPITXCU : Capitaux par branche (Données principales)
- INCENDCU : Données Incendie (Complément PE/RD)

DIFFÉRENCES AVEC AZ :
--------------------
- AZEC génère uniquement des colonnes de capitaux INDEXÉS
- La jointure pour la segmentation se fait seulement sur le Code Produit (CPROD)
- Nécessite une jointure supplémentaire avec INCENDCU pour être complet
"""

from pyspark.sql import DataFrame # type: ignore
from pyspark.sql.functions import col, broadcast, lit, coalesce, when
from pyspark.sql.types import StringType, DoubleType
from config.constants import MARKET_CODE
from src.processors.base_processor import BaseProcessor
from utils.helpers import extract_year_month_int
from utils.loaders.config_loader import ConfigLoader
from utils.processor_helpers import enrich_segmentation
from pathlib import Path


class AZECCapitauxProcessor(BaseProcessor):
    """
    =======================================================================
    PROCESSEUR CAPITAUX AZEC
    =======================================================================
    
    Transforme les données brutes AZEC en données Silver enrichies.
    
    FLUX DE TRAITEMENT (WORKFLOW) :
    ------------------------------
    1. READ : Lecture du fichier CAPITXCU
    2. TRANSFORM :
       - Extraction des SMP/LCI
       - Lecture et agrégation de INCENDCU (PE/RD)
       - Jointure des deux sources
       - Enrichissement (Segmentation via CPROD)
       - Filtrage sur le marché Construction
    3. WRITE : Écriture du fichier Silver résultant
    """
    
    def __init__(self, spark, config: ConfigLoader, logger):
        """
        Initialise le processeur AZEC Capitaux.
        
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
        self.logger.info("Initialisation du Processeur Capitaux AZEC")
    
    def read(self, vision: str) -> DataFrame:
        """
        Lit le fichier principal CAPITXCU depuis la couche Bronze.
        
        PARAMÈTRES :
        -----------
        vision : str
            La période à traiter (YYYYMM)
            
        RETOUR :
        -------
        DataFrame
            Les données brutes CAPITXCU
        """
        self.logger.info(f"Lecture des données capitaux AZEC pour vision {vision}")
        
        from src.reader import BronzeReader
        
        reading_config_path = self.config.get('config_files.reading_config', 'config/reading_config.json')
        
        if not Path(reading_config_path).is_absolute():
            reading_config_path = str(self.get_project_root() / reading_config_path)
            
        reader = BronzeReader(self.spark, self.config, reading_config_path)

        # Lire le fichier principal CAPITXCU
        df_capitxcu = reader.read_file_group('capitxcu_azec', vision)
        
        self.logger.success(f"Lecture terminée : {df_capitxcu.count():,} enregistrements chargés (CAPITXCU)")
        return df_capitxcu
    
    def transform(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Applique les règles de gestion pour consolider les capitaux AZEC.
        
        ÉTAPES DE TRANSFORMATION :
        -------------------------
        1. Extraction des SMP et LCI depuis CAPITXCU
        2. Lecture et agrégation des données INCENDCU (Pour PE/RD)
        3. Jointure entre CAPITXCU et INCENDCU
        4. Enrichissement avec la segmentation (jointure sur CPROD uniquement)
        5. Filtrage strict sur le marché Construction
        
        PARAMÈTRES :
        -----------
        df : DataFrame
            Données brutes CAPITXCU lues par read()
        vision : str
            Période traitée
            
        RETOUR :
        -------
        DataFrame
            Données transformées prêtes pour la couche Silver
        """
        self.logger.info("Démarrage des transformations Capitaux AZEC")
        
        year_int, month_int = extract_year_month_int(vision)
        
        # --- Étape 1 : Traitement CAPITXCU ---
        self.logger.step(1, "Extraction SMP/LCI depuis CAPITXCU")
        
        from utils.transformations import process_azec_capitals
        # Applique la logique d'extraction spécifique à AZEC
        df = process_azec_capitals(df)
        
        # --- Étape 2 : Traitement INCENDCU ---
        self.logger.step(2, "Lecture et agrégation INCENDCU (PE/RD)")
        
        from src.reader import BronzeReader
        
        reading_config_path = self.config.get('config_files.reading_config', 'config/reading_config.json')
        if not Path(reading_config_path).is_absolute():
            reading_config_path = str(self.get_project_root() / reading_config_path)
        
        reader = BronzeReader(self.spark, self.config, reading_config_path)

        try:
            # Lire le fichier complémentaire INCENDCU
            df_incendcu = reader.read_file_group('incendcu_azec', vision)
            
            # Agréger les montants PE (Perte Exploitation) et RD (Risques Directs)
            from utils.transformations import aggregate_azec_pe_rd
            df_pe_rd = aggregate_azec_pe_rd(df_incendcu)
            
            # --- Étape 3 : Jointure ---
            self.logger.step(3, "Jointure CAPITXCU + INCENDCU")
            
            # Joindre les informations complémentaires via une jointure gauche
            df = df.join(df_pe_rd, on=['nopol', 'cdprod'], how='left')
            
        except Exception as e:
            # En cas d'absence du fichier INCENDCU (peut arriver)
            self.logger.warning(f"INCENDCU non disponible : {e}")
            self.logger.info("Initialisation des colonnes PE/RD à 0")
            
            # Initialiser les colonnes manquantes avec 0
            from pyspark.sql.functions import lit
            df = df.withColumn('perte_exp_100_ind', lit(0.0))
            df = df.withColumn('risque_direct_100_ind', lit(0.0))
            df = df.withColumn('value_insured_100_ind', lit(0.0))
        
        # --- Étape 4 : Enrichissement Segmentation ---
        self.logger.step(4, "Enrichissement avec la segmentation")
        
        # Pour AZEC, la jointure se fait UNIQUEMENT sur CPROD (Code Produit)
        # Contrairement à AZ qui utilise CPROD + CDPOLE
        # Le code pôle sera fixé à '3' (Courtage) lors de la consolidation
        df = enrich_segmentation(df, reader, vision, logger=self.logger)

        # --- Étape 5 : Filtre Marché ---
        self.logger.step(5, "Filtre Marché Construction (CMARCH=6)")
        from pyspark.sql.functions import col

        if 'cmarch' in df.columns:
            # Ne conserver que les lignes du marché Construction
            df = df.filter(col('cmarch') == MARKET_CODE.MARKET)
            self.logger.info(f"Après filtre marché : {df.count():,} enregistrements")
        else:
            self.logger.warning("Colonne CMARCH introuvable - Filtre ignoré (Risque de données hors périmètre)")
        
        self.logger.success("Transformations Capitaux AZEC terminées")
        return df
    
    def write(self, df: DataFrame, vision: str) -> None:
        """
        Écrit les résultats transformés en couche Silver.
        
        FICHIER PRODUIT :
        ----------------
        azec_capitaux_{vision}.parquet
        
        PARAMÈTRES :
        -----------
        df : DataFrame
            Données transformées
        vision : str
            Période traitée
        """
        self.logger.info(f"Écriture des données Silver AZEC pour vision {vision}")
        
        from utils.helpers import write_to_layer
        
        output_name = f"azec_capitaux_{vision}"
        write_to_layer(
            df, self.config, 'silver', output_name, vision, self.logger
        )
        
        self.logger.success(f"Fichier écrit : {output_name}.parquet ({df.count():,} enregistrements)")
