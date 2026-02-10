# -*- coding: utf-8 -*-
"""
===============================================================================
ORCHESTRATEUR PIPELINE CAPITAUX (Extraction des Capitaux Assurés)
===============================================================================

Ce module coordonne l'exécution du pipeline d'extraction des capitaux assurés
pour les polices d'assurance construction.

QUE FAIT CE PIPELINE ?
-----------------------
Extrait et calcule les capitaux assurés (montants garantis) pour chaque police
d'assurance construction. Ces informations sont essentielles pour :
- L'évaluation du risque
- Le calcul des primes
- Le reporting réglementaire

TYPES DE CAPITAUX TRAITÉS :
---------------------------
- SMP (Somme Maximum Probable) : capital maximum garanti par sinistre
- LCI (Limite de Couverture par Incendie) : capital garanti pour incendie
- Capitaux par garantie : montants détaillés par type de garantie

ÉTAPES DU PIPELINE :
-------------------
1. AZ Capitaux : Extrait les capitaux des polices agents (Bronze → Silver)
2. AZEC Capitaux : Extrait les capitaux des polices construction (Bronze → Silver)
3. Consolidation : Fusionne AZ et AZEC en une vue unique (Silver → Gold)

FORMAT DE SORTIE GOLD :
----------------------
Fichier : az_azec_capitaux_{vision}
Contenu : Tous les capitaux consolidés des deux canaux (AZ + AZEC)
"""

from src.orchestrators import BaseOrchestrator


class CapitauxOrchestrator(BaseOrchestrator):
    """
    =======================================================================
    ORCHESTRATEUR DU PIPELINE CAPITAUX
    =======================================================================
    
    Coordonne l'exécution des trois étapes du pipeline d'extraction des
    capitaux assurés.
    
    PARTICULARITÉ : AZEC EST OPTIONNEL
    -----------------------------------
    Le pipeline peut continuer même si le traitement AZEC échoue, car :
    - AZEC ne représente qu'une partie des données (canal construction)
    - AZ contient la majorité des polices (agents + courtiers)
    - Mieux vaut avoir des données partielles que rien
    
    Cette tolérance est configurée dans la méthode allow_stage_failure().
    
    SÉQUENCE D'EXÉCUTION :
    ---------------------
    ÉTAPE 1 : AZ Capitaux Processor
        - Lit les fichiers IPF (polices agents)
        - Extrait les capitaux par garantie
        - Calcule SMP et LCI
        - Écrit en Silver
        
    ÉTAPE 2 : AZEC Capitaux Processor (optionnelle)
        - Lit les fichiers AZEC (polices construction)
        - Extrait les capitaux spécifiques construction
        - Joint avec tables de référence (CAPITXCU, INCENDCU)
        - Écrit en Silver
        
    ÉTAPE 3 : Consolidation
        - Fusionne AZ et AZEC
        - Harmonise les formats
        - Écrit le résultat final en Gold
    """
    
    def define_stages(self):
        """
        Définit les trois étapes du pipeline Capitaux.
        
        ORDRE D'EXÉCUTION :
        ------------------
        1. AZ d'abord (obligatoire - contient la majorité des données)
        2. AZEC ensuite (optionnel - peut échouer sans bloquer)
        3. Consolidation enfin (fusionne les deux)
        
        RETOUR :
        -------
        list[tuple]
            Liste de paires (nom_étape, classe_processeur)
        """
        # Importer les processeurs (import ici pour éviter les dépendances circulaires)
        from src.processors.capitaux_processors import (
            AZCapitauxProcessor,
            AZECCapitauxProcessor,
            CapitauxConsolidationProcessor
        )
        
        return [
            ("AZ Capitaux Processor (Bronze → Silver)", AZCapitauxProcessor),
            ("AZEC Capitaux Processor (Bronze → Silver)", AZECCapitauxProcessor),
            ("Consolidation (Silver → Gold)", CapitauxConsolidationProcessor)
        ]
    
    def allow_stage_failure(self, stage_name: str) -> bool:
        """
        Détermine si le pipeline peut continuer en cas d'échec d'une étape.
        
        LOGIQUE MÉTIER :
        ---------------
        AZEC est optionnel car il ne représente qu'une partie des données.
        Si AZEC échoue, on peut quand même :
        - Avoir les capitaux AZ (majoritaires)
        - Produire un résultat partiel mais utilisable
        - Investiguer le problème AZEC sans bloquer toute la production
        
        PARAMÈTRES :
        -----------
        stage_name : str
            Nom de l'étape qui a échoué
            
        RETOUR :
        -------
        bool
            True si l'échec est toléré (pipeline continue)
            False si l'échec bloque le pipeline
            
        EXEMPLE :
        --------
        >>> orchestrator.allow_stage_failure("AZEC Capitaux Processor")
        True  # AZEC peut échouer
        >>> orchestrator.allow_stage_failure("AZ Capitaux Processor")
        False  # AZ ne peut pas échouer
        """
        return "AZEC" in stage_name


def run_capitaux_pipeline(
    vision: str,
    config_path: str,
    spark,
    logger
) -> bool:
    """
    Point d'entrée principal pour exécuter le pipeline Capitaux.
    
    RÔLE DE CETTE FONCTION :
    ------------------------
    Fonction appelée par main.py pour lancer l'extraction des capitaux assurés.
    Elle initialise l'orchestrateur et démarre le pipeline.
    
    FLUX D'EXÉCUTION :
    -----------------
    1. Crée une instance de CapitauxOrchestrator
    2. Appelle orchestrator.run(vision) qui :
       - Valide la vision
       - Exécute les 3 étapes séquentiellement
       - Gère les erreurs
       - Produit un résumé
    3. Retourne True/False selon le succès
    
    PARAMÈTRES :
    -----------
    vision : str
        Période à traiter au format YYYYMM (ex: '202509')
        
    config_path : str
        Chemin vers le fichier config.yml
        
    spark : SparkSession
        Session Spark active (initialisée dans main.py)
        
    logger : PipelineLogger
        Système de journalisation (initialisé dans main.py)
        
    RETOUR :
    -------
    bool
        True si le pipeline s'est terminé avec succès, False sinon
        
    DÉFINITION DU SUCCÈS :
    ---------------------
    Le pipeline est considéré réussi si :
    - AZ s'est exécuté correctement
    - Consolidation s'est exécutée correctement
    - (AZEC peut avoir échoué, c'est toléré)
    
    EXEMPLE :
    --------
    >>> success = run_capitaux_pipeline('202509', 'config/config.yml', spark, logger)
    >>> if success:
    ...     print("Capitaux extraits et consolidés avec succès")
    """
    # Créer l'orchestrateur avec les paramètres fournis
    orchestrator = CapitauxOrchestrator(spark, config_path, logger)
    
    # Lancer le pipeline complet
    return orchestrator.run(vision)


# =============================================================================
# EXÉCUTION STANDALONE (mode développement/test)
# =============================================================================
# Ce bloc permet d'exécuter le pipeline Capitaux directement depuis la ligne
# de commande sans passer par main.py. Utile pour :
# - Tester le composant isolément
# - Déboguer une étape spécifique
# - Relancer uniquement Capitaux après un échec

if __name__ == "__main__":
    import sys
    from pyspark.sql import SparkSession
    from utils.logger import get_logger
    from pathlib import Path
    
    # Vérifier que la vision est fournie en argument
    if len(sys.argv) < 2:
        print("Usage: python capitaux_run.py <vision>")
        print("Exemple: python capitaux_run.py 202509")
        sys.exit(1)
    
    vision = sys.argv[1]
    
    # Initialiser Spark
    print("Initialisation de la session Spark...")
    spark = SparkSession.builder \
        .appName("Construction_Pipeline_Capitaux") \
        .getOrCreate()
    
    # Initialiser le système de logs
    logger = get_logger('capitaux_standalone', log_file=f'logs/capitaux_{vision}.log')
    
    # Déterminer le chemin de configuration
    project_root = Path(__file__).parent.parent
    config_path = str(project_root / "config" / "config.yml")
    
    try:
        # Exécuter le pipeline
        success = run_capitaux_pipeline(vision, config_path, spark, logger)
        
        # Terminer avec le code approprié (0 = succès, 1 = échec)
        sys.exit(0 if success else 1)
    finally:
        # Toujours arrêter Spark proprement
        spark.stop()
