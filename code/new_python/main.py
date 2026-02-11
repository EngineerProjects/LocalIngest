"""
Pipeline de Données Construction - Point d'Entrée Principal.

Coordonne l'exécution de tous les composants du pipeline en fonction de la configuration.
"""

import sys
import os
import argparse
import time
from pathlib import Path

# Ajouter la racine du projet au path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from utils.loaders.config_loader import ConfigLoader
from utils.helpers import validate_vision
from src.ptf_mvt_run import run_ptf_mvt_pipeline


def main():
    """
    Point d'entrée principal pour le Pipeline de Données Construction.

    Utilisation CLI :
        # Exécuter un composant spécifique (ignore activé/désactivé dans config.yml)
        python main.py --component ptf_mvt --vision 202509
        python main.py --component capitaux --vision 202509
        
        # Exécuter tous les composants activés dans config.yml
        python main.py --vision 202509
        
        # Utiliser un fichier de config personnalisé
        python main.py --vision 202509 --config chemin/vers/config.yml

    Variables d'Environnement :
        PIPELINE_VISION: Vision par défaut si --vision n'est pas fourni
        DATALAKE_BASE_PATH: Surcharge du chemin de base du datalake depuis la config
    """
    parser = argparse.ArgumentParser(
        description='Pipeline de Données Construction - Domaine PTF_MVT',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  # Exécuter un composant spécifique (ignore config.yml)
  python main.py --component ptf_mvt --vision 202509
  python main.py --component capitaux --vision 202509

  # Exécuter tous les composants activés dans config.yml
  python main.py --vision 202509

  # Utiliser un fichier de config personnalisé
  python main.py --vision 202509 --config ma_config.yml

  # Utiliser une variable d'environnement pour la vision
  export PIPELINE_VISION=202509
  python main.py
        """
    )

    parser.add_argument(
        '--vision',
        type=str,
        help='Vision au format YYYYMM (ex: 202509)'
    )

    parser.add_argument(
        '--config',
        type=str,
        default='config/config.yml',
        help='Chemin vers le fichier config.yml (défaut : config/config.yml)'
    )

    parser.add_argument(
        '--component',
        type=str,
        choices=['ptf_mvt', 'capitaux', 'emissions'],
        default=None,
        help='Composant à exécuter (ignore l\'état activé dans config.yml ; défaut : exécuter tous ceux activés)'
    )

    args = parser.parse_args()

    # =========================================================================
    # Charger la Configuration EN PREMIER
    # =========================================================================
    config_path = Path(args.config) if args.config else project_root / "config" / "config.yml"
    if not config_path.exists():
        print(f"ERREUR : Fichier de config non trouvé : {config_path}")
        sys.exit(1)

    try:
        config = ConfigLoader(str(config_path))
    except Exception as e:
        print(f"ERREUR : Échec du chargement de la configuration : {e}")
        sys.exit(1)

    # =========================================================================
    # Déterminer la Vision (Priorité : Arg CLI > Var Env > Défaut Config)
    # =========================================================================
    vision = args.vision or os.getenv('PIPELINE_VISION')

    if not vision:
        # Essayer d'obtenir le défaut depuis la config
        try:
            vision = config.get('runtime.vision_')
            if vision:
                print(f"Utilisation de la vision par défaut de la config : {vision}")
        except:
            pass

    if not vision:
        print("ERREUR : Vision non fournie !")
        print("Spécifiez via --vision, variable d'env PIPELINE_VISION, ou runtime.vision_ dans la config")
        print("\nExemples :")
        print("  python main.py --vision 202509")
        print("  export PIPELINE_VISION=202509 && python main.py")
        sys.exit(1)

    # =========================================================================
    # Valider le Format de la Vision
    # =========================================================================
    if not validate_vision(vision):
        print(f"ERREUR : Format de vision invalide : {vision}")
        print("Format attendu YYYYMM (ex: 202509)")
        sys.exit(1)

    # Validation supplémentaire utilisant les paramètres de config.yml
    year = int(vision[:4])
    min_year = config.get('vision.validation.min_year', 2000)
    max_year = config.get('vision.validation.max_year', 2100)

    if year < min_year or year > max_year:
        print(f"ERREUR : L'année de vision {year} est hors de la plage valide [{min_year}, {max_year}]")
        print(f"Configurez la plage valide dans config.yml : vision.validation")
        sys.exit(1)

    # =========================================================================
    # Déterminer les Composants à Exécuter
    # =========================================================================
    # Logique :
    # - Si --component spécifié : exécuter SEULEMENT ce composant (ignorer activé/désactivé dans config.yml)
    # - Si --component NON spécifié : exécuter TOUS les composants activés dans config.yml
    # =========================================================================

    components_to_run = []

    if args.component:
        # Composant explicite spécifié - L'EXÉCUTER directement (ignorer l'état config.yml)
        component = args.component
        components_to_run = [component]
        print(f"Exécution d'un seul composant (config.yml ignoré) : {component}")
    else:
        # Aucun composant spécifié - exécuter TOUS les composants activés dans config.yml
        components = config.get('components', {})
        enabled_components = [
            name for name, settings in components.items()
            if settings.get('enabled', False)
        ]

        if not enabled_components:
            print("ERREUR : Aucun composant n'est activé dans config.yml")
            print("Activez au moins un composant : components.<nom>.enabled = true")
            print("Ou utilisez --component pour exécuter un composant spécifique directement")
            sys.exit(1)

        components_to_run = enabled_components
        print(f"Auto-détection de {len(components_to_run)} composant(s) activé(s) : {', '.join(components_to_run)}")

    # =========================================================================
    # Afficher l'En-tête du Pipeline
    # =========================================================================
    print("=" * 80)
    print(f"Pipeline de Données Construction")
    print("=" * 80)
    print(f"Pipeline :  {config.get('pipeline.name')} v{config.get('pipeline.version')}")
    print(f"Composants :{', '.join(components_to_run)}")
    print(f"Vision :    {vision}")
    print(f"Config :    {config_path}")

    # =========================================================================
    # Initialiser le Logging (CENTRALISÉ - Un seul log pour tous les composants)
    # =========================================================================
    log_level = config.get('logging.level', 'INFO')
    log_dir = config.get('logging.local_dir', 'logs')
    log_filename_template = config.get('logging.filename_template', 'pipeline_{vision}.log')
    log_filename = log_filename_template.format(vision=vision)

    # Créer le répertoire de logs
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, log_filename)

    from utils.logger import get_logger
    from utils.helpers import upload_log_to_datalake

    logger = get_logger('main', log_file=log_file, level=log_level)

    logger.section("Pipeline de Données Construction - Démarrage de l'Exécution")
    logger.info(f"Pipeline : {config.get('pipeline.name')} v{config.get('pipeline.version')}")
    logger.info(f"Composants à exécuter : {', '.join(components_to_run)}")
    logger.info(f"Vision : {vision}")
    logger.info(f"Config : {config_path}")
    logger.info(f"Fichier Log : {log_file}")

    # =========================================================================
    # Initialiser la Session Spark (SESSION UNIQUE POUR TOUS LES COMPOSANTS)
    # =========================================================================
    logger.info("Initialisation de la Session Spark...")

    spark = None
    success = False

    try:
        from pyspark.sql import SparkSession # type: ignore
        # # Module interne pour auto-configurer spark et la connexion à Azure
        import azfr_fsspec_utils as fspath 
        import azfr_fsspec_abfs
        # # Configurer SparkSession
        azfr_fsspec_abfs.use()

        app_name = config.get('spark.app_name', 'Construction_Pipeline')
        spark_config = config.get('spark.config', {})

        # Construire SparkSession
        builder = SparkSession.builder.appName(app_name)

        # Appliquer les configurations Spark personnalisées si présentes
        if spark_config:
            for key, value in spark_config.items():
                builder = builder.config(key, value)

        # Créer ou obtenir la session existante
        spark = builder.getOrCreate()

        logger.success(f"Session Spark initialisée : {app_name} v{spark.version}")
        if spark_config:
            logger.info(f"Config Spark personnalisée : {len(spark_config)} paramètres appliqués")

        # =====================================================================
        # Suivi de Performance - Démarrer Chronomètre
        # =====================================================================
        pipeline_start_time = time.time()
        logger.info(f"Exécution pipeline démarrée à : {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(pipeline_start_time))}")

        # =====================================================================
        # Exécuter les Pipelines des Composants (Boucle sur tous les composants activés)
        # =====================================================================
        component_results = {}
        overall_success = True

        for component in components_to_run:
            logger.section(f"Exécution du Composant : {component.upper()}")
            component_desc = config.get(f'components.{component}.description', 'Pas de description')
            logger.info(f"Description : {component_desc}")

            try:
                if component == 'ptf_mvt':
                    component_success = run_ptf_mvt_pipeline(vision, str(config_path), spark, logger)
                elif component == 'capitaux':
                    from src.capitaux_run import run_capitaux_pipeline
                    component_success = run_capitaux_pipeline(vision, str(config_path), spark, logger)
                elif component == 'emissions':
                    from src.emissions_run import run_emissions_pipeline
                    component_success = run_emissions_pipeline(vision, str(config_path), spark, logger)
                else:
                    logger.error(f"Composant inconnu : {component}")
                    component_success = False

                component_results[component] = component_success

                if component_success:
                    logger.success(f"Composant '{component}' terminé avec succès")
                else:
                    logger.failure(f"Composant '{component}' a échoué")
                    overall_success = False

            except Exception as e:
                logger.failure(f"L'exécution du composant '{component}' a échoué : {str(e)}")
                logger.error(f"Détails de l'erreur : {e}", exc_info=True)
                component_results[component] = False
                overall_success = False

            logger.info("─" * 80)  # Séparateur entre composants

        # =====================================================================
        # Suivi de Performance - Arrêter Chronomètre
        # =====================================================================
        pipeline_end_time = time.time()
        total_duration = pipeline_end_time - pipeline_start_time
        
        # Formater la durée en HH:MM:SS
        hours, remainder = divmod(int(total_duration), 3600)
        minutes, seconds = divmod(remainder, 60)
        duration_formatted = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        
        logger.info(f"Exécution pipeline terminée à : {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(pipeline_end_time))}")
        logger.info(f"Temps total d'exécution : {duration_formatted} ({total_duration:.2f} secondes)")

        # Log du résumé de tous les composants
        logger.section("Résumé de l'Exécution des Composants")
        for comp, result in component_results.items():
            status = "✓ SUCCÈS" if result else "✗ ÉCHEC"
            logger.info(f"{comp}: {status}")
        
        logger.info("")
        logger.info(f"⏱️  TEMPS TOTAL D'EXÉCUTION : {duration_formatted}")

        success = overall_success

    except Exception as e:
        logger.failure(f"L'exécution du pipeline a échoué : {str(e)}")
        logger.error(f"Détails de l'erreur : {e}", exc_info=True)
        success = False

    finally:
        # =====================================================================
        # Nettoyage : Upload Log et Arrêt Spark (S'exécute toujours)
        # =====================================================================
        if spark is not None:
            logger.info("Upload du log vers le datalake...")

            try:
                upload_success = upload_log_to_datalake(spark, log_file, config)
                if upload_success:
                    logger.success(f"Log uploadé vers le datalake : bronze/logs/{log_filename}")
                    print(f"√ Log uploadé : bronze/logs/{log_filename}")
                else:
                    logger.warning("Upload du log échoué (datalake peut être indisponible)")
                    print(f"⚠ Log conservé localement : {log_file}")
            except Exception as e:
                logger.warning(f"Upload du log vers le datalake échoué : {e}")
                print(f"⚠ Log conservé localement : {log_file}")

            # Arrêter la session Spark
            logger.info("Arrêt de la session Spark...")
            spark.stop()
            logger.info("Session Spark arrêtée")

        # Statut final
        if success:
            logger.success("=" * 60)
            logger.success("EXÉCUTION DU PIPELINE TERMINÉE AVEC SUCCÈS")
            logger.success("=" * 60)
        else:
            logger.failure("=" * 60)
            logger.failure("ÉCHEC DE L'EXÉCUTION DU PIPELINE")
            logger.failure("=" * 60)

    # =========================================================================
    # Quitter avec le code approprié
    # =========================================================================
    # Calculer les temps finaux pour affichage console
    try:
        hours, remainder = divmod(int(total_duration), 3600)
        minutes, seconds = divmod(remainder, 60)
        duration_display = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    except:
        duration_display = "N/A"
    
    if success:
        print("\n" + "=" * 80)
        print("✓ Pipeline terminé avec succès !")
        print(f"Temps total d'exécution : {duration_display}")
        print("=" * 80)
        sys.exit(0)
    else:
        print("\n" + "=" * 80)
        print("✗ Échec du pipeline !")
        print(f"Temps total d'exécution : {duration_display}")
        print("=" * 80)
        sys.exit(1)


if __name__ == "__main__":
    main()
