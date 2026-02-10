# -*- coding: utf-8 -*-
"""
===============================================================================
PROCESSEUR DE CONSOLIDATION DES CAPITAUX (AZ + AZEC)
===============================================================================

Ce processeur fusionne les données de capitaux issues des deux canaux de distribution :
1. AZ (Agents & Courtiers traditionnels)
2. AZEC (Marché spécialisé Construction)

OBJECTIF :
---------
Produire un fichier unique "GOLD" contenant l'ensemble des capitaux assurés
pour tout le portefeuille Construction, quel que soit le canal d'origine.

POURQUOI UNE CONSOLIDATION ?
---------------------------
Les deux sources ont des structures légèrement différentes :
- AZ a des capitaux indexés ET non-indexés
- AZEC n'a QUE des capitaux indexés
- AZ a un code pôle variable (1 ou 3)
- AZEC est toujours considéré comme courtage (Code 3)

Le processeur harmonise ces différences avant de fusionner les données.
"""

from pyspark.sql import DataFrame # type: ignore
from src.processors.base_processor import BaseProcessor
from utils.loaders.config_loader import ConfigLoader


class CapitauxConsolidationProcessor(BaseProcessor):
    """
    =======================================================================
    CONSOLIDATION CAPITAUX (SILVER → GOLD)
    =======================================================================
    
    Fusionne les fichiers Silver AZ et AZEC en un fichier Gold unique.
    
    FLUX DE TRAITEMENT (WORKFLOW) :
    ------------------------------
    1. READ : Lecture du fichier Silver AZ
    2. TRANSFORM :
       - Lecture du fichier Silver AZEC
       - Harmonisation du schéma AZ (ajouts colonnes calculées)
       - Harmonisation du schéma AZEC (valeurs par défaut pour colonnes manquantes)
       - UNION des deux jeux de données
       - Déduplication (priorité à la dernière version)
    3. WRITE : Écriture du fichier Gold consolidé
    """
    
    def __init__(self, spark, config: ConfigLoader, logger):
        """
        Initialise le processeur de consolidation.
        
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
        self.logger.info("Initialisation de la Consolidation Capitaux")
    
    def read(self, vision: str) -> DataFrame:
        """
        Lit les données AZ depuis la couche Silver.
        
        NOTE : L'orchestrateur garantit que le fichier AZ existe avant d'arriver ici.
        Le fichier AZEC est lu dans la phase Transform car il est optionnel.
        
        PARAMÈTRES :
        -----------
        vision : str
            La période à traiter (YYYYMM)
            
        RETOUR :
        -------
        DataFrame
            Les données AZ déjà transformées (Silver)
        """
        self.logger.info(f"Lecture des données Silver AZ pour vision {vision}")
        
        from src.reader import SilverReader
        
        reader = SilverReader(self.spark, self.config)
        df_az = reader.read_silver_file(f"az_capitaux_{vision}", vision)
        
        self.logger.success(f"Lecture terminée : {df_az.count():,} enregistrements chargés (AZ)")
        return df_az
    
    def transform(self, df_az: DataFrame, vision: str) -> DataFrame:
        """
        Harmonise et fusionne les données AZ et AZEC.
        
        LOGIQUE DE FUSION :
        ------------------
        1. On part des données AZ (majoritaires)
        2. On essaie de lire les données AZEC
        3. On aligne les colonnes (AZ a plus de colonnes que AZEC)
           -> Les colonnes manquantes dans AZEC sont mises à NULL ou 0
        4. On fusionne les deux DataFrames
        
        PARAMÈTRES :
        -----------
        df_az : DataFrame
            Données AZ lues par read()
        vision : str
            Période traitée
            
        RETOUR :
        -------
        DataFrame
            Données consolidées (AZ + AZEC) prêtes pour Gold
        """
        self.logger.info("Démarrage de la consolidation (AZ + AZEC)")
        
        # --- Étape 1 : Lecture AZEC ---
        from src.reader import SilverReader
        reader = SilverReader(self.spark, self.config)
        
        df_azec = None
        try:
            # Tenter de lire le fichier AZEC
            df_azec = reader.read_silver_file(f"azec_capitaux_{vision}", vision)
            self.logger.success(f"Données AZEC chargées : {df_azec.count():,} enregistrements")
        except Exception as e:
            # Si le fichier n'existe pas (cas toléré par l'orchestrateur)
            self.logger.warning(f"Données AZEC non disponibles : {e}")
            self.logger.info("La consolidation se fera uniquement sur les données AZ")
            df_azec = None
        
        # --- Étape 2 : Harmonisation AZ ---
        self.logger.step(1, "Harmonisation du schéma AZ")
        from pyspark.sql.functions import lit, col, coalesce
        
        # Marquer la provenance
        df_az = df_az.withColumn('dircom', lit('AZ'))
        
        # Calculer le total "Valeur Assurée" (Somme PE + RD)
        # PE = Perte d'Exploitation, RD = Risque Direct
        
        # Version INDEXÉE
        if all(c in df_az.columns for c in ['perte_exp_100_ind', 'risque_direct_100_ind']):
            df_az = df_az.withColumn(
                'value_insured_100_ind',
                coalesce(col('perte_exp_100_ind'), lit(0.0)) + 
                coalesce(col('risque_direct_100_ind'), lit(0.0))
            )
        
        # Version NON INDEXÉE
        if all(c in df_az.columns for c in ['perte_exp_100', 'risque_direct_100']):
            df_az = df_az.withColumn(
                'value_insured_100',
                coalesce(col('perte_exp_100'), lit(0.0)) + 
                coalesce(col('risque_direct_100'), lit(0.0))
            )
        
        # --- Étape 3 : Harmonisation et Fusion AZEC ---
        if df_azec is not None:
            self.logger.step(2, "Harmonisation et Fusion des données AZEC")

            # Marquer la provenance "AZEC"
            df_azec = df_azec.withColumn('dircom', lit('AZEC'))

            # Forcer le Code Pôle à "3" (Courtage) pour tout le périmètre AZEC
            df_azec = df_azec.withColumn('cdpole', lit('3'))

            # Gestion des colonnes manquantes dans AZEC (capital non indexé)
            # AZEC ne fournit que des capitaux indexés. On initialise les autres à NULL.
            non_indexed_columns = [
                'limite_rc_100_par_sin', 'limite_rc_100_par_sin_tous_dom', 'limite_rc_100_par_an',
                'limite_rc_100', 'perte_exp_100', 'risque_direct_100',
                'value_insured_100', 'smp_100', 'lci_100'
            ]

            from utils.processor_helpers import add_null_columns
            from pyspark.sql.types import DoubleType
            
            # Ajouter les colonnes manquantes avec des NULL
            null_cols = {col_name: DoubleType for col_name in non_indexed_columns}
            df_azec = add_null_columns(df_azec, null_cols)

            # Fusionner les deux jeux de données
            # allowMissingColumns gère les différences mineures de schéma
            df_consolidated = df_az.unionByName(df_azec, allowMissingColumns=True)
            
            # Déduplication : conserver une seule ligne par police
            # On trie par police et pôle pour être déterministe
            df_consolidated = df_consolidated.orderBy("nopol", "cdpole").dropDuplicates(["nopol"])
        else:
            # Si pas de données AZEC, la consolidation est égale aux données AZ
            df_consolidated = df_az
        
        self.logger.success(f"Consolidation terminée : {df_consolidated.count():,} enregistrements au total")
        return df_consolidated
    
    def write(self, df: DataFrame, vision: str) -> None:
        """
        Écrit le fichier consolidé en couche Gold.
        
        FICHIER PRODUIT :
        ----------------
        az_azec_capitaux_{vision}.parquet
        
        PARAMÈTRES :
        -----------
        df : DataFrame
            Données consolidées
        vision : str
            Période traitée
        """
        from config.column_definitions import GOLD_COLUMNS_CAPITAUX
        
        self.logger.info(f"Écriture des données Gold consolidées pour vision {vision}")
        
        # Sélectionner exactement les colonnes définies dans le modèle Gold
        existing_cols = [c for c in GOLD_COLUMNS_CAPITAUX if c in df.columns]
        
        # Vérification des colonnes manquantes (alerte qualité)
        missing_cols = set(GOLD_COLUMNS_CAPITAUX) - set(df.columns)
        if missing_cols:
            self.logger.warning(f"Colonnes manquantes dans le schéma cible : {sorted(missing_cols)}")
        
        df_final = df.select(existing_cols)
        
        from utils.helpers import write_to_layer
        
        output_name = f"az_azec_capitaux_{vision}"
        write_to_layer(
            df_final, self.config, 'gold', output_name, vision, self.logger
        )
        
        self.logger.success(f"Fichier écrit : {output_name}.parquet ({df_final.count():,} enregistrements)")
