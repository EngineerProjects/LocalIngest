# -*- coding: utf-8 -*-
"""
===============================================================================
ORCHESTRATEUR DE BASE (Modèle pour l'exécution des pipelines)
===============================================================================

Ce module définit le modèle standard que tous les pipelines doivent suivre.

POURQUOI UNE CLASSE DE BASE ?
----------------------------
Plutôt que de répéter le même code dans chaque pipeline (PTF_MVT, Capitaux, etc.),
on centralise ici la logique commune :
- Chargement de la configuration
- Validation des formats (ex: vision YYYYMM)
- Exécution séquentielle des étapes
- Gestion des erreurs et des logs
- Génération des rapports de fin de traitement

COMMENT L'UTILISER ?
-------------------
Chaque nouveau pipeline doit hériter de cette classe et définir simplement
sa liste d'étapes dans la méthode `define_stages()`. Tout le reste est géré
automatiquement.
"""

from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional, Union
from pyspark.sql import DataFrame


class BaseOrchestrator:
    """
    =======================================================================
    CLASSE MÈRE DES ORCHESTRATEURS
    =======================================================================
    
    Squelette générique pour tous les pipelines de données.
    
    RÔLES DE CETTE CLASSE :
    -----------------------
    1. Initialiser l'environnement (Spark, Config, Logger)
    2. Valider les entrées (format de la vision)
    3. Lancer les étapes de traitement une par une
    4. Attraper les erreurs et arrêter le pipeline proprement si nécessaire
    5. Afficher un résumé clair de ce qui s'est passé
    
    MÉTHODES À SURCHARGER (dans les classes filles) :
    ------------------------------------------------
    - define_stages() : [OBLIGATOIRE] Liste des étapes à exécuter
    - pre_process() : [OPTIONNEL] Actions à faire AVANT les étapes
    - post_process() : [OPTIONNEL] Actions à faire APRÈS les étapes
    - allow_stage_failure() : [OPTIONNEL] Définir quelles étapes peuvent échouer
    
    EXEMPLE D'IMPLÉMENTATION :
    -------------------------
    class MonPipeline(BaseOrchestrator):
        def define_stages(self):
            return [
                ("Étape 1 : Nettoyage", CleanerProcessor),
                ("Étape 2 : Calculs", CalculatorProcessor)
            ]
    """
    
    def __init__(self, spark, config_path: str, logger):
        """
        Initialise l'orchestrateur avec les outils nécessaires.
        
        PARAMÈTRES :
        -----------
        spark : SparkSession
            Le moteur de calcul Spark actif
            
        config_path : str
            Chemin vers le fichier de configuration (config.yml)
            
        logger : PipelineLogger
            Système pour écrire les logs (console et fichier)
        """
        self.spark = spark
        self.logger = logger
        
        # Charger la configuration depuis le fichier YAML
        # (contient les chemins, les paramètres de calcul, etc.)
        from utils.loaders.config_loader import ConfigLoader
        self.config = ConfigLoader(config_path)
    
    def validate_vision(self, vision: str) -> bool:
        """
        Vérifie que la vision (période) est au bon format.
        
        BUT :
        ----
        S'assurer qu'on ne lance pas de calculs avec une date invalide.
        Le format attendu est TOUJOURS 'YYYYMM' (ex: 202509).
        
        PARAMÈTRES :
        -----------
        vision : str
            La chaîne de caractères représentant la période
            
        RETOUR :
        -------
        bool
            True si le format est correct, False sinon
        """
        from utils.helpers import validate_vision
        
        if not validate_vision(vision):
            self.logger.error(f"Format de vision invalide : {vision}. Attendu : YYYYMM (ex: 202509)")
            return False
            
        return True
    
    def run_stage(
        self,
        stage_num: int,
        stage_name: str,
        processor_class,
        vision: str
    ) -> Optional[Union[DataFrame, Tuple[DataFrame, ...]]]:
        """
        Exécute une seule étape du pipeline.
        
        PROCESSUS :
        ----------
        1. Affiche le début de l'étape dans les logs
        2. Instancie le processeur (la classe qui fait le travail)
        3. Lance la méthode .run() du processeur
        4. Vérifie le résultat
        5. Affiche le succès ou l'échec
        
        PARAMÈTRES :
        -----------
        stage_num : int
            Numéro de l'étape (pour l'affichage)
            
        stage_name : str
            Nom lisible de l'étape (ex: "Calcul des primes")
            
        processor_class : class
            La classe Python à instancier (doit hériter de BaseProcessor)
            
        vision : str
            La période à traiter
            
        RETOUR :
        -------
        DataFrame ou Tuple[DataFrame] ou None
            Les données résultantes du traitement, ou None si échec
        """
        self.logger.section(f"ÉTAPE {stage_num} : {stage_name}")
        
        try:
            # Créer une instance du processeur
            processor = processor_class(self.spark, self.config, self.logger)
            
            # Lancer le traitement
            result = processor.run(vision)
            
            # Vérifier si le traitement a retourné quelque chose
            if result is None:
                self.logger.error(f"L'étape {stage_name} a retourné None (pas de résultat)")
                return None
            
            # Gérer le cas où l'étape retourne plusieurs DataFrames (ex: Émissions)
            if isinstance(result, tuple):
                # Calculer le nombre total de lignes générées
                total_rows = sum(df.count() for df in result if df is not None)
                self.logger.success(f"{stage_name} terminée : {total_rows:,} lignes au total")
            else:
                # Cas standard : un seul DataFrame
                self.logger.success(f"{stage_name} terminée : {result.count():,} lignes")
            
            return result
            
        except Exception as e:
            # En cas d'erreur imprévue (bug, problème réseau, etc.)
            self.logger.failure(f"Échec de l'étape {stage_name} : {e}")
            self.logger.error(f"Détails de l'erreur : {e}")
            # Relancer l'exception pour qu'elle soit attrapée par la méthode run()
            raise
    
    def run(self, vision: str) -> bool:
        """
        Lance l'exécution complète du pipeline (toutes les étapes).
        
        FLUX DE TRAVAIL (WORKFLOW) :
        ---------------------------
        1. Valider le format de la date (vision)
        2. Exécuter le pré-traitement (hook optionnel)
        3. Boucler sur chaque étape définie et l'exécuter
           - Si une étape obligatoire échoue → ARRÊT DU PIPELINE
           - Si une étape optionnelle échoue → CONTINUATION
        4. Exécuter le post-traitement (hook optionnel)
        5. Afficher le résumé final
        
        PARAMÈTRES :
        -----------
        vision : str
            La période à traiter (YYYYMM)
            
        RETOUR :
        -------
        bool
            True si tout s'est bien passé, False en cas d'erreur bloquante
        """
        # 1. Validation
        if not self.validate_vision(vision):
            return False
        
        # Récupérer le nom du pipeline pour l'affichage (enlève "Orchestrator")
        pipeline_name = self.__class__.__name__.replace("Orchestrator", "").upper()
        self.logger.section(f"DÉMARRAGE PIPELINE {pipeline_name} - Vision {vision}")
        
        try:
            # 2. Pré-traitement (vide par défaut)
            self.pre_process(vision)
            
            # 3. Exécution des étapes
            stages = self.define_stages()
            results = {} # Stocke les résultats de chaque étape
            
            for i, (stage_name, processor_class) in enumerate(stages, 1):
                # Lancer l'étape courante
                result = self.run_stage(i, stage_name, processor_class, vision)
                
                # Gestion des échecs
                if result is None:
                    # Vérifier si l'échec est toléré pour cette étape
                    if self.allow_stage_failure(stage_name):
                        self.logger.warning(f"L'étape {stage_name} a échoué mais le pipeline continue (échec toléré).")
                    else:
                        # Échec critique : on arrête tout
                        self.logger.failure(f"ARRÊT DU PIPELINE : L'étape obligatoire {stage_name} a échoué.")
                        return False
                
                # Sauvegarder le résultat pour le post-traitement et le résumé
                results[stage_name] = result
            
            # 4. Post-traitement (vide par défaut, ex: copie de fichiers)
            self.post_process(vision, results)
            
            # 5. Résumé final
            self.print_summary(results)
            
            # Succès global
            self.logger.success(f"Pipeline {pipeline_name} terminé avec succès !")
            return True
            
        except Exception as e:
            # Filet de sécurité global : attrape toute erreur non gérée
            self.logger.failure(f"Le pipeline a planté : {e}")
            self.logger.error(f"Exception non gérée : {e}")
            return False
    
    def define_stages(self) -> List[Tuple[str, type]]:
        """
        Définit la liste des étapes du pipeline.
        
        CETTE MÉTHODE DOIT ÊTRE SURCHARGÉE PAR LA CLASSE FILLE.
        Si la classe fille ne la définit pas, une erreur est levée.
        
        RETOUR :
        -------
        list[tuple]
            Liste ordonnée de tuples (Nom de l'étape, Classe du processeur)
        """
        raise NotImplementedError("Les sous-classes doivent définir leurs étapes dans define_stages()")
    
    def pre_process(self, vision: str):
        """
        Point d'extension pour exécuter du code AVANT les étapes principales.
        
        À SURCHARGER SI NÉCESSAIRE.
        Par défaut : ne fait rien.
        """
        pass
    
    def post_process(self, vision: str, results: Dict[str, Any]):
        """
        Point d'extension pour exécuter du code APRÈS les étapes principales.
        
        À SURCHARGER SI NÉCESSAIRE.
        Par défaut : ne fait rien.
        Exemple d'usage : copier des fichiers, nettoyer des temporaires, envoyer un email.
        
        PARAMÈTRES :
        -----------
        vision : str
            La période traitée
        results : dict
            Dictionnaire contenant les résultats de toutes les étapes exécutées
        """
        pass
    
    def allow_stage_failure(self, stage_name: str) -> bool:
        """
        Définit si une étape spécifique a le droit d'échouer.
        
        POURQUOI ?
        ---------
        Parfois, une étape secondaire (ex: calcul statistique optionnel)
        peut échouer sans remettre en cause la validité des données principales.
        
        PARAMÈTRES :
        -----------
        stage_name : str
            Nom de l'étape à vérifier
            
        RETOUR :
        -------
        bool
            True si l'échec est accepté, False sinon (par défaut False)
        """
        return False
    
    def print_summary(self, results: Dict[str, Any]):
        """
        Affiche un tableau récapitulatif des résultats à la fin du pipeline.
        
        BUT :
        ----
        Donner une vue synthétique des volumes de données traités.
        
        PARAMÈTRES :
        -----------
        results : dict
            Les résultats collectés pendant l'exécution
        """
        self.logger.section("RÉSUMÉ DU PIPELINE")
        
        for stage_name, result in results.items():
            if result is None:
                self.logger.info(f"{stage_name} : IGNORÉE OU ÉCHOUÉE")
            
            elif isinstance(result, tuple):
                # Cas spécial : étapes produisant plusieurs fichiers
                for i, df in enumerate(result, 1):
                    if df is not None:
                        self.logger.info(f"{stage_name} (sortie {i}) : {df.count():,} lignes")
            
            else:
                # Cas standard : un seul fichier
                self.logger.info(f"{stage_name} : {result.count():,} lignes")
