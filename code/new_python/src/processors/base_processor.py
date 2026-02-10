# -*- coding: utf-8 -*-
"""
===============================================================================
PROCESSEUR DE BASE (Modèle ETL Read-Transform-Write)
===============================================================================

Ce module définit le contrat que tous les processeurs de données doivent respecter.

QU'EST-CE QU'UN PROCESSEUR ?
---------------------------
Un processeur est une brique logicielle responsable d'une transformation de données
spécifique (ex: calculer les primes, consolider les portefeuilles).

MODÈLE ETL STANDARDISÉ :
-----------------------
Pour garantir la maintenabilité, tous les processeurs suivent strictement
le modèle ETL (Extract - Transform - Load) en 3 étapes :
1. read() : Lire les données brutes (Extract)
2. transform() : Appliquer les règles métier (Transform)
3. write() : Sauvegarder le résultat (Load)

POURQUOI UNE CLASSE ABSTRAITE ?
------------------------------
Cela force tous les développeurs à structurer leur code de la même manière.
Impossible d'oublier une étape ou de mélanger la lecture et l'écriture.
"""

from abc import ABC, abstractmethod
import time
from pyspark.sql import SparkSession, DataFrame # type: ignore
from utils.loaders.config_loader import ConfigLoader
from utils.logger import PipelineLogger


class BaseProcessor(ABC):
    """
    =======================================================================
    CLASSE MÈRE DES PROCESSEURS
    =======================================================================
    
    Classe abstraite qui définit le squelette de tout traitement de données.
    
    CYCLE DE VIE :
    -------------
    1. Instanciation avec Spark/Config/Logger
    2. Appel de la méthode run()
    3. Exécution automatique de read() → transform() → write()
    4. Mesure des temps d'exécution et logs automatiques à chaque étape
    
    RESPONSABILITÉS DU DÉVELOPPEUR :
    -------------------------------
    Le développeur doit implémenter les 3 méthodes abstraites :
    - read(vision) : Retourne un DataFrame
    - transform(df, vision) : Retourne un DataFrame transformé
    - write(df, vision) : Sauvegarde le DataFrame
    """

    def __init__(
        self,
        spark: SparkSession,
        config: ConfigLoader,
        logger: PipelineLogger
    ):
        """
        Initialise le processeur avec les outils nécessaires.
        
        PARAMÈTRES :
        -----------
        spark : SparkSession
            Le moteur de calcul Spark actif
            
        config : ConfigLoader
            Accès à la configuration (chemins, paramètres)
            
        logger : PipelineLogger
            Système de journalisation pour suivre l'exécution
        """
        self.spark = spark
        self.config = config
        self.logger = logger
    
    def get_project_root(self):
        """
        Trouve le répertoire racine du projet de manière dynamique.
        
        COMMENT ÇA MARCHE ?
        ------------------
        On remonte l'arborescence des dossiers depuis l'emplacement de ce fichier
        jusqu'à trouver le dossier 'src'.
        
        POURQUOI ?
        ---------
        Permet de trouver les fichiers de configuration ou de données
        quel que soit l'endroit d'où le script est lancé.
        
        RETOUR :
        -------
        Path
            Chemin absolu vers la racine du projet
        """
        from pathlib import Path
        
        # Partir du dossier où se trouve ce fichier
        current = Path(__file__).parent
        
        # Remonter jusqu'à trouver le dossier parent contenant 'src'
        while current.name != '' and current != current.parent:
            if (current / 'src').exists():
                return current
            current = current.parent
        
        # Si non trouvé (cas rare), utiliser le dossier courant
        return Path.cwd()


    @abstractmethod
    def read(self, vision: str) -> DataFrame:
        """
        [MÉTHODE ABSTRAITE] Doit être implémentée par la classe fille.
        
        BUT :
        ----
        Lire les données d'entrée nécessaires au traitement.
        
        PARAMÈTRES :
        -----------
        vision : str
            La période à traiter (YYYYMM)
            
        RETOUR :
        -------
        DataFrame
            Les données brutes chargées en mémoire
        """
        pass

    @abstractmethod
    def transform(self, df: DataFrame, vision: str) -> DataFrame:
        """
        [MÉTHODE ABSTRAITE] Doit être implémentée par la classe fille.
        
        BUT :
        ----
        Appliquer toute la logique métier aux données :
        - Filtrage
        - Calculs (primes, commissions, etc.)
        - Agrégations
        - Jointures
        
        PARAMÈTRES :
        -----------
        df : DataFrame
            Les données brutes retournées par read()
            
        vision : str
            La période à traiter
            
        RETOUR :
        -------
        DataFrame
            Les données transformées, prêtes à être sauvegardées
        """
        pass

    @abstractmethod
    def write(self, df: DataFrame, vision: str) -> None:
        """
        [MÉTHODE ABSTRAITE] Doit être implémentée par la classe fille.
        
        BUT :
        ----
        Sauvegarder le résultat final (datalake, base de données, fichier).
        
        PARAMÈTRES :
        -----------
        df : DataFrame
            Les données transformées retournées par transform()
            
        vision : str
            La période traitée
        """
        pass

    def run(self, vision: str) -> DataFrame:
        """
        Exécute le pipeline complet ETL pour ce processeur.
        
        C'est le chef d'orchestre interne du processeur.
        Il appelle les étapes dans l'ordre et mesure les performances.
        
        DÉROULEMENT :
        ------------
        1. Affiche le nom du processeur
        2. Lance read() et mesure le temps
        3. Met en cache les données lues (optimisation performance)
        4. Lance transform() et mesure le temps
        5. Lance write() et mesure le temps
        6. Affiche un message de succès
        
        OPTIMISATION PERFORMANCE :
        -------------------------
        On utilise .cache() après la lecture car les données lues sont souvent
        utilisées plusieurs fois (dans transform et write). Sans cache, Spark
        relirait les fichiers à chaque action, ce qui serait très lent.
        
        PARAMÈTRES :
        -----------
        vision : str
            La période à traiter (YYYYMM)
            
        RETOUR :
        -------
        DataFrame
            Le résultat final du traitement (utile si enchaîné avec d'autres étapes)
        """
        processor_name = self.__class__.__name__
        self.logger.section(f"Lancement du processeur : {processor_name}")
        
        # --- ÉTAPE 1 : LECTURE (READ) ---
        self.logger.info(f"Démarrage : {processor_name}.read()")
        start_time = time.time()
        
        # Appel de la méthode implémentée par la classe fille
        df = self.read(vision)
        
        # Mettre en cache immédiatement pour éviter de relire les données plus tard
        # C'est crucial pour la performance Spark
        df = df.cache()
        
        duration = time.time() - start_time
        self.logger.info(f"Terminé : {processor_name}.read() (Durée : {duration:.2f}s)")
        self.logger.info(f"Données lues : {df.count()} lignes")
        
        # --- ÉTAPE 2 : TRANSFORMATION (TRANSFORM) ---
        self.logger.info(f"Démarrage : {processor_name}.transform()")
        start_time = time.time()
        
        # Appel de la méthode métier
        df = self.transform(df, vision)
        
        duration = time.time() - start_time
        self.logger.info(f"Terminé : {processor_name}.transform() (Durée : {duration:.2f}s)")
        
        # Gérer le cas où transform() retourne plusieurs DataFrames (tuple)
        if isinstance(df, tuple):
            for i, result_df in enumerate(df, 1):
                if result_df is not None:
                    self.logger.info(f"Résultat transformation {i}/{len(df)} : {result_df.count()} lignes")
        else:
            self.logger.info(f"Résultat transformation : {df.count()} lignes")
        
        # --- ÉTAPE 3 : ÉCRITURE (WRITE) ---
        self.logger.info(f"Démarrage : {processor_name}.write()")
        start_time = time.time()
        
        # Appel de la méthode de sauvegarde
        self.write(df, vision)
        
        duration = time.time() - start_time
        self.logger.info(f"Terminé : {processor_name}.write() (Durée : {duration:.2f}s)")
        
        # Succès final
        self.logger.success(f"Processeur {processor_name} terminé avec succès")
        
        # Retourner le résultat pour d'éventuels traitements suivants
        return df
