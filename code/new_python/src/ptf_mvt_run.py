# -*- coding: utf-8 -*-
"""
===============================================================================
ORCHESTRATEUR PIPELINE PTF_MVT (Mouvements de Portefeuille)
===============================================================================

Ce module coordonne l'exécution du pipeline de traitement des mouvements
de portefeuille d'assurance construction.

QUE FAIT CE PIPELINE ?
-----------------------
Traite les données des polices d'assurance pour suivre les mouvements
(nouvelles polices, résiliations, modifications) sur deux canaux :
- AZ : Canal agents et courtiers traditionnels
- AZEC : Canal construction spécialisé

ÉTAPES DU PIPELINE :
-------------------
1. AZ Processor : Traite les données du canal agents (Bronze → Silver)
2. AZEC Processor : Traite les données du canal construction (Bronze → Silver)
3. Consolidation : Fusionne AZ et AZEC en une vue unique (Silver → Gold)
4. IRD Risk Copy : Copie les fichiers de risque IRD (optionnel)

FLEXIBILITÉ :
-------------
Chaque étape peut être activée/désactivée via variables d'environnement.
Utile pour déboguer une seule partie du pipeline ou relancer juste ce qui
a échoué.
"""

import os
from pathlib import Path
from pyspark.sql import SparkSession  # type: ignore

from src.orchestrators import BaseOrchestrator
from src.processors.ptf_mvt_processors.az_processor import AZProcessor
from src.processors.ptf_mvt_processors.azec_processor import AZECProcessor
from src.processors.ptf_mvt_processors.consolidation_processor import ConsolidationProcessor
from src.reader import BronzeReader
from utils.logger import PipelineLogger


def _get_bool_env(name: str, default: str = "0") -> bool:
    """
    Convertit une variable d'environnement en booléen.
    
    UTILITÉ :
    --------
    Permet d'activer/désactiver des étapes du pipeline via des variables
    d'environnement (ex: RUN_AZ=1 pour activer, RUN_AZ=0 pour désactiver).
    
    VALEURS ACCEPTÉES POUR "VRAI" :
    -------------------------------
    - "1", "true", "TRUE", "yes", "YES", "on", "ON"
    
    PARAMÈTRES :
    -----------
    name : str
        Nom de la variable d'environnement à lire
        
    default : str
        Valeur par défaut si la variable n'existe pas (par défaut "0" = faux)
        
    RETOUR :
    -------
    bool
        True si la variable est définie à une valeur "vraie", False sinon
    """
    return str(os.getenv(name, default)).strip() in ("1", "true", "TRUE", "yes", "YES", "on", "ON")


def copy_ird_risk_to_gold(spark, config, vision: str, logger: PipelineLogger):
    """
    Copie les fichiers de risque IRD depuis Bronze directement vers Gold.
    
    QUE SONT LES FICHIERS IRD ?
    ---------------------------
    IRD (Insurance Risk Data) contient les données de risque externe
    utilisées pour l'évaluation des polices. Il existe 3 fichiers :
    - Q45 : Données de risque type 45
    - Q46 : Données de risque type 46
    - QAN : Données de risque anciennes (pour visions < 202210)
    
    POURQUOI COPIER DIRECTEMENT EN GOLD ?
    -------------------------------------
    Ces fichiers ne nécessitent aucune transformation. On les copie tel quel
    depuis Bronze vers Gold pour qu'ils soient disponibles pour l'analyse.
    
    PARAMÈTRES :
    -----------
    spark : SparkSession
        Session Spark active
        
    config : ConfigLoader
        Configuration du pipeline
        
    vision : str
        Période à traiter (ex: '202509')
        
    logger : PipelineLogger
        Système de journalisation
    """
    from utils.helpers import write_to_layer

    # Créer un lecteur pour accéder aux fichiers Bronze
    reader = BronzeReader(spark, config)

    # Liste des 3 fichiers IRD à copier
    ird_files = ['ird_risk_q45', 'ird_risk_q46', 'ird_risk_qan']
    logger.info(f"Début copie fichiers IRD vers Gold : {ird_files}")

    # Compteur pour suivre combien de fichiers ont été copiés
    copied_count = 0
    
    # Traiter chaque fichier individuellement
    for ird_file_group in ird_files:
        try:
            logger.debug(f"Traitement de {ird_file_group}")
            
            # Lire le fichier depuis Bronze
            df_ird = reader.read_file_group(ird_file_group, vision)

            # Vérifier que le fichier existe et contient des données
            if df_ird is None or df_ird.count() == 0:
                logger.warning(f"{ird_file_group} introuvable ou vide, ignoré")
                continue

            # Écrire directement en Gold sans transformation
            write_to_layer(df_ird, config, 'gold', f'{ird_file_group}_{vision}', vision, logger)

            logger.info(f"✓ {ird_file_group} copié vers Gold ({df_ird.count()} lignes)")
            copied_count += 1

        except Exception as e:
            # En cas d'erreur, on continue avec les autres fichiers
            logger.warning(f"Impossible de copier {ird_file_group} vers Gold : {e}")

    # Résumé final de l'opération
    logger.info(f"Copie des fichiers IRD terminée ({copied_count}/3 fichiers copiés)")


class PTFMVTOrchestrator(BaseOrchestrator):
    """
    =======================================================================
    ORCHESTRATEUR DU PIPELINE PTF_MVT
    =======================================================================
    
    Coordonne l'exécution des différentes étapes du pipeline de mouvements
    de portefeuille.
    
    VARIABLES D'ENVIRONNEMENT DISPONIBLES :
    ---------------------------------------
    - RUN_AZ=1/0 : Activer/désactiver le traitement AZ (par défaut : 1)
    - RUN_AZEC=1/0 : Activer/désactiver le traitement AZEC (par défaut : 1)
    - RUN_CONSO=1/0 : Activer/désactiver la consolidation (par défaut : 1)
    - RUN_IRD=1/0 : Activer/désactiver la copie IRD (par défaut : 0)
    
    EXEMPLE D'UTILISATION :
    ----------------------
    # Exécuter seulement le traitement AZ
    export RUN_AZ=1
    export RUN_AZEC=0
    export RUN_CONSO=0
    python main.py --vision 202509 --component ptf_mvt
    """

    def define_stages(self):
        """
        Définit les étapes du pipeline à exécuter selon les variables d'environnement.
        
        RETOUR :
        -------
        list[tuple]
            Liste de paires (nom_étape, classe_processeur)
            Ex: [("AZ Processor", AZProcessor), ("Consolidation", ConsolidationProcessor)]
        """
        stages = []

        # Lire les variables d'environnement (par défaut tout activé)
        run_az = _get_bool_env("RUN_AZ", "1")
        run_azec = _get_bool_env("RUN_AZEC", "1")
        run_conso = _get_bool_env("RUN_CONSO", "1")

        # Construire la liste des étapes activées
        if run_az:
            stages.append(("AZ Processor (Bronze -> Silver)", AZProcessor))
        if run_azec:
            stages.append(("AZEC Processor (Bronze -> Silver)", AZECProcessor))
        if run_conso:
            stages.append(("Consolidation Processor (Silver -> Gold)", ConsolidationProcessor))

        # Sécurité : si rien n'est sélectionné, exécuter au moins AZ
        if not stages:
            self.logger.warning("Aucune étape sélectionnée via variables d'environnement ; AZ par défaut")
            stages.append(("AZ Processor (Bronze -> Silver)", AZProcessor))

        # Afficher quelles étapes vont être exécutées
        self.logger.info(f"Étapes actives : {[name for name, _ in stages]}")
        return stages

    def post_process(self, vision, results):
        """
        Traitement optionnel après l'exécution des étapes principales.
        
        ACTION :
        -------
        Copie les fichiers de risque IRD vers la couche Gold si activé.
        """
        # Vérifier si la copie IRD est activée (par défaut non)
        run_ird = _get_bool_env("RUN_IRD", "0")
        
        if not run_ird:
            self.logger.section("ÉTAPE 4 : Copie des fichiers IRD vers Gold (IGNORÉE)")
            return
            
        # Si activé, exécuter la copie
        self.logger.section("ÉTAPE 4 : Copie des fichiers IRD vers Gold")
        copy_ird_risk_to_gold(self.spark, self.config, vision, self.logger)


def run_ptf_mvt_pipeline(
    vision: str,
    config_path: str = None,
    spark: SparkSession = None,
    logger: PipelineLogger = None
) -> bool:
    """
    Point d'entrée principal pour exécuter le pipeline PTF_MVT.
    
    PARAMÈTRES :
    -----------
    vision : str
        Période à traiter au format YYYYMM (ex: '202509')
        
    config_path : str, optionnel
        Chemin vers le fichier de configuration (par défaut : config/config.yml)
        
    spark : SparkSession
        Session Spark active (OBLIGATOIRE - initialisée dans main.py)
        
    logger : PipelineLogger
        Système de journalisation (OBLIGATOIRE - initialisé dans main.py)
        
    RETOUR :
    -------
    bool
        True si le pipeline s'est terminé avec succès, False sinon
    """
    
    # Vérifier que Spark et le logger sont fournis (prerequis)
    if spark is None:
        raise ValueError("SparkSession obligatoire. Initialiser dans main.py")
    if logger is None:
        raise ValueError("Logger obligatoire. Initialiser dans main.py")

    # Déterminer le chemin de configuration si non fourni
    if config_path is None:
        project_root = Path(__file__).parent.parent
        config_path = project_root / "config" / "config.yml"

    # Créer l'orchestrateur et lancer le pipeline
    orchestrator = PTFMVTOrchestrator(spark, str(config_path), logger)
    return orchestrator.run(vision)


# =============================================================================
# EXÉCUTION STANDALONE (mode développement/test)
# =============================================================================
if __name__ == "__main__":
    import sys
    from utils.logger import get_logger

    # Vérifier que la vision est fournie en argument
    if len(sys.argv) < 2:
        print("Usage: python ptf_mvt_run.py <vision>")
        print("Exemple: python ptf_mvt_run.py 202509")
        sys.exit(1)

    vision = sys.argv[1]

    # Initialiser Spark
    print("Initialisation de la session Spark...")
    spark = SparkSession.builder \
        .appName("Construction_Pipeline_PTF_MVT") \
        .getOrCreate()

    # Initialiser le système de logs
    logger = get_logger('ptf_mvt_standalone', log_file=f'logs/ptf_mvt_{vision}.log')

    try:
        # Exécuter le pipeline
        success = run_ptf_mvt_pipeline(vision, spark=spark, logger=logger)
        
        # Terminer avec le code approprié (0 = succès, 1 = échec)
        sys.exit(0 if success else 1)
    finally:
        # Toujours arrêter Spark proprement
        spark.stop()
