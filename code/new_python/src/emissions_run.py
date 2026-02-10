# -*- coding: utf-8 -*-
"""
===============================================================================
ORCHESTRATEUR PIPELINE ÉMISSIONS (Primes Émises)
===============================================================================

Ce module coordonne l'exécution du pipeline de traitement des émissions
de primes d'assurance construction.

QUE SONT LES ÉMISSIONS ?
------------------------
Les émissions représentent les primes d'assurance facturées aux clients
pour leurs polices d'assurance construction. Chaque émission contient :
- Le montant de la prime
- Les commissions versées aux intermédiaires
- Les garanties souscrites
- Les dates d'effet

UTILITÉ DES DONNÉES D'ÉMISSIONS :
---------------------------------
Ces données sont essentielles pour :
- Le suivi du chiffre d'affaires
- Le calcul des commissions
- L'analyse de la rentabilité par garantie
- Le reporting financier et réglementaire

SOURCE DES DONNÉES :
-------------------
Les données viennent du système One BI (Business Intelligence) qui centralise
toutes les informations de facturation. Fichier source : rf_fr1_prm_dtl_midcorp_m

PARTICULARITÉ DE CE PIPELINE :
------------------------------
Contrairement aux autres pipelines (PTF_MVT, Capitaux) qui ont plusieurs
processeurs, Emissions n'a qu'UN SEUL processeur qui traite tout le flux
Bronze → Silver → Gold en une seule passe.

FICHIERS DE SORTIE :
-------------------
Le pipeline produit DEUX fichiers Gold :

1. primes_emises_{vision}_pol_garp (détaillé par garantie)
   - Une ligne par police ET par garantie
   - Permet d'analyser les primes par type de garantie
   - Utile pour les analyses de rentabilité

2. primes_emises_{vision}_pol (agrégé par police)
   - Une ligne par police (toutes garanties confondues)
   - Somme totale des primes par police
   - Utile pour les analyses macro et le reporting
"""

from src.orchestrators import BaseOrchestrator


class EmissionsOrchestrator(BaseOrchestrator):
    """
    =======================================================================
    ORCHESTRATEUR DU PIPELINE ÉMISSIONS
    =======================================================================
    
    Coordonne l'exécution du pipeline de traitement des primes émises.
    
    PARTICULARITÉ : PIPELINE SIMPLIFIÉ
    ----------------------------------
    Ce pipeline a une seule étape (EmissionsProcessor) au lieu de trois
    comme les autres pipelines. Pourquoi ?
    
    1. UNE SEULE SOURCE : Toutes les données viennent d'un fichier One BI
    2. PAS DE CONSOLIDATION : Pas besoin de fusionner AZ/AZEC
    3. TRAITEMENT LINÉAIRE : Filtres → Calculs → Agrégations → Écriture
    
    DEUX SORTIES EN UNE PASSE :
    ---------------------------
    Le processeur produit deux DataFrames en une seule exécution :
    - df_pol_garp : Détail par police et garantie
    - df_pol : Agrégation par police uniquement
    
    Ces deux fichiers sont créés simultanément pour optimiser les performances
    (pas besoin de relire les données).
    """
    
    def define_stages(self):
        """
        Définit l'unique étape du pipeline Émissions.
        
        ÉTAPE UNIQUE : EmissionsProcessor
        ---------------------------------
        Ce processeur fait tout le travail :
        1. Lit rf_fr1_prm_dtl_midcorp_m depuis Bronze
        2. Applique les filtres métier (marché Construction uniquement)
        3. Calcule les montants de primes et commissions
        4. Crée deux vues : détaillée (par garantie) et agrégée (par police)
        5. Écrit les deux fichiers en Gold
        
        RETOUR :
        -------
        list[tuple]
            Liste avec une seule paire (nom_étape, classe_processeur)
        """
        # Importer le processeur (import ici pour éviter les dépendances circulaires)
        from src.processors.emissions_processors import EmissionsProcessor
        
        return [
            ("Emissions Processor (Bronze → Silver → Gold)", EmissionsProcessor)
        ]
    
    def print_summary(self, results):
        """
        Affiche un résumé personnalisé pour le pipeline Émissions.
        
        POURQUOI UNE MÉTHODE PERSONNALISÉE ?
        ------------------------------------
        Le processeur Émissions retourne un TUPLE de deux DataFrames au lieu
        d'un seul comme les autres processeurs. On doit donc afficher les
        statistiques des deux fichiers produits.
        
        INFORMATIONS AFFICHÉES :
        -----------------------
        - Nombre de lignes dans POL_GARP (détail par garantie)
        - Nombre de lignes dans POL (agrégé par police)
        
        Le ratio POL_GARP / POL indique le nombre moyen de garanties par police.
        
        PARAMÈTRES :
        -----------
        results : dict
            Dictionnaire {nom_étape: résultat}
            Pour Émissions : résultat = (df_pol_garp, df_pol)
        """
        self.logger.section("PIPELINE ÉMISSIONS TERMINÉ")
        
        # Parcourir les résultats (normalement un seul élément)
        for stage_name, result in results.items():
            # Vérifier que le résultat est bien un tuple de 2 DataFrames
            if result and isinstance(result, tuple):
                df_pol_garp, df_pol = result
                
                # Afficher les statistiques de chaque fichier
                self.logger.success(f"  - POL_GARP : {df_pol_garp.count():,} lignes (détail par garantie)")
                self.logger.success(f"  - POL : {df_pol.count():,} lignes (agrégé par police)")


def run_emissions_pipeline(
    vision: str,
    config_path: str,
    spark,
    logger
) -> bool:
    """
    Point d'entrée principal pour exécuter le pipeline Émissions.
    
    RÔLE DE CETTE FONCTION :
    ------------------------
    Fonction appelée par main.py pour lancer le traitement des primes émises.
    Elle initialise l'orchestrateur et démarre le pipeline.
    
    FLUX D'EXÉCUTION DÉTAILLÉ :
    ---------------------------
    1. Crée une instance de EmissionsOrchestrator
    2. Appelle orchestrator.run(vision) qui :
       a. Valide le format de la vision (YYYYMM)
       b. Initialise le processeur Émissions
       c. Exécute read() → transform() → write()
       d. Affiche le résumé personnalisé (2 fichiers)
    3. Retourne True/False selon le succès
    
    DONNÉES TRAITÉES :
    -----------------
    Entrée : rf_fr1_prm_dtl_midcorp_m (One BI premium data)
        - Toutes les émissions de primes du marché MidCorp
        - Tous les marchés confondus (Construction, Santé, etc.)
        
    Sortie : 2 fichiers Gold
        - primes_emises_{vision}_pol_garp : Détail par garantie
        - primes_emises_{vision}_pol : Agrégé par police
    
    FILTRES APPLIQUÉS :
    ------------------
    - Marché Construction uniquement (cmarch = "6")
    - Période = vision demandée
    - Exclusion des primes nulles ou négatives
    
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
    - Les données One BI ont été lues correctement
    - Les calculs de primes et commissions sont corrects
    - Les deux fichiers Gold ont été écrits
    - Aucune erreur fatale n'est survenue
    
    EXEMPLE :
    --------
    >>> success = run_emissions_pipeline('202509', 'config/config.yml', spark, logger)
    >>> if success:
    ...     print("Primes émises calculées et exportées avec succès")
    ...     print("  - Fichier détaillé (par garantie) : disponible")
    ...     print("  - Fichier agrégé (par police) : disponible")
    """
    # Créer l'orchestrateur avec les paramètres fournis
    orchestrator = EmissionsOrchestrator(spark, config_path, logger)
    
    # Lancer le pipeline complet
    return orchestrator.run(vision)


# =============================================================================
# EXÉCUTION STANDALONE (mode développement/test)
# =============================================================================
# Ce bloc permet d'exécuter le pipeline Émissions directement depuis la ligne
# de commande sans passer par main.py. Utile pour :
# - Tester le composant isolément
# - Déboguer les calculs de primes et commissions
# - Valider les deux fichiers de sortie

if __name__ == "__main__":
    import sys
    from pyspark.sql import SparkSession
    from utils.logger import get_logger
    from pathlib import Path
    
    # Vérifier que la vision est fournie en argument
    if len(sys.argv) < 2:
        print("Usage: python emissions_run.py <vision>")
        print("Exemple: python emissions_run.py 202509")
        sys.exit(1)
    
    vision = sys.argv[1]
    
    # Initialiser Spark
    print("Initialisation de la session Spark...")
    spark = SparkSession.builder \
        .appName("Construction_Pipeline_Emissions") \
        .getOrCreate()
    
    # Initialiser le système de logs
    logger = get_logger('emissions_standalone', log_file=f'logs/emissions_{vision}.log')
    
    # Déterminer le chemin de configuration
    project_root = Path(__file__).parent.parent
    config_path = str(project_root / "config" / "config.yml")
    
    try:
        # Exécuter le pipeline
        success = run_emissions_pipeline(vision, config_path, spark, logger)
        
        # Terminer avec le code approprié (0 = succès, 1 = échec)
        sys.exit(0 if success else 1)
    finally:
        # Toujours arrêter Spark proprement
        spark.stop()
