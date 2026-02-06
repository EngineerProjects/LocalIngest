"""
Construction Data Pipeline - Main Entry Point.

Coordinates execution of all pipeline components based on configuration.
"""

import sys
import os
import argparse
import time
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from utils.loaders.config_loader import ConfigLoader
from utils.helpers import validate_vision
from src.ptf_mvt_run import run_ptf_mvt_pipeline


def main():
    """
    Main entry point for Construction Data Pipeline.

    CLI Usage:
        python main.py --vision 202509
        python main.py --vision 202509 --config path/to/config.yml
        python main.py --component ptf_mvt --vision 202509

    Environment Variables:
        PIPELINE_VISION: Default vision if --vision not provided
        DATALAKE_BASE_PATH: Override datalake base path from config
    """
    parser = argparse.ArgumentParser(
        description='Construction Data Pipeline - PTF_MVT Domain',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run PTF_MVT for vision 202509
  python main.py --vision 202509

  # Use custom config file
  python main.py --vision 202509 --config my_config.yml

  # Use environment variable for vision
  export PIPELINE_VISION=202509
  python main.py
        """
    )

    parser.add_argument(
        '--vision',
        type=str,
        help='Vision in YYYYMM format (e.g., 202509)'
    )

    parser.add_argument(
        '--config',
        type=str,
        default='config/config.yml',
        help='Path to config.yml file (default: config/config.yml)'
    )

    parser.add_argument(
        '--component',
        type=str,
        choices=['ptf_mvt', 'capitaux', 'emissions'],
        default=None,  # None = auto-detect all enabled components
        help='Component to run (default: all enabled components)'
    )

    args = parser.parse_args()

    # =========================================================================
    # Load Configuration FIRST
    # =========================================================================
    config_path = Path(args.config) if args.config else project_root / "config" / "config.yml"
    if not config_path.exists():
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)

    try:
        config = ConfigLoader(str(config_path))
    except Exception as e:
        print(f"ERROR: Failed to load configuration: {e}")
        sys.exit(1)

    # =========================================================================
    # Determine Vision (Priority: CLI arg > Env var > Config default)
    # =========================================================================
    vision = args.vision or os.getenv('PIPELINE_VISION')

    if not vision:
        # Try to get default from config
        try:
            vision = config.get('runtime.vision_')
            if vision:
                print(f"Using default vision from config: {vision}")
        except:
            pass

    if not vision:
        print("ERROR: Vision not provided!")
        print("Specify via --vision, PIPELINE_VISION env var, or runtime.vision_ in config")
        print("\nExamples:")
        print("  python main.py --vision 202509")
        print("  export PIPELINE_VISION=202509 && python main.py")
        sys.exit(1)

    # =========================================================================
    # Validate Vision Format
    # =========================================================================
    if not validate_vision(vision):
        print(f"ERROR: Invalid vision format: {vision}")
        print("Expected YYYYMM format (e.g., 202509)")
        sys.exit(1)

    # Additional validation using config.yml settings
    year = int(vision[:4])
    min_year = config.get('vision.validation.min_year', 2000)
    max_year = config.get('vision.validation.max_year', 2100)

    if year < min_year or year > max_year:
        print(f"ERROR: Vision year {year} outside valid range [{min_year}, {max_year}]")
        print(f"Configure valid range in config.yml: vision.validation")
        sys.exit(1)

    # =========================================================================
    # Determine Components to Run
    # =========================================================================
    # Logic:
    # - If --component specified: run that component (if enabled)
    # - If --component NOT specified: run ALL enabled components
    # =========================================================================

    components_to_run = []

    if args.component:
        # Explicit component specified
        component = args.component
        enabled = config.get(f'components.{component}.enabled', False)

        if not enabled:
            print(f"ERROR: Component '{component}' is disabled in configuration")
            print(f"Enable it in config.yml: components.{component}.enabled = true")
            sys.exit(1)

        components_to_run = [component]
    else:
        # No component specified - run ALL enabled components
        components = config.get('components', {})
        enabled_components = [
            name for name, settings in components.items()
            if settings.get('enabled', False)
        ]

        if not enabled_components:
            print("ERROR: No components are enabled in config.yml")
            print("Enable at least one component: components.<name>.enabled = true")
            sys.exit(1)

        components_to_run = enabled_components
        print(f"Auto-detected {len(components_to_run)} enabled component(s): {', '.join(components_to_run)}")

    # =========================================================================
    # Print Pipeline Header
    # =========================================================================
    print("=" * 80)
    print(f"Construction Data Pipeline")
    print("=" * 80)
    print(f"Pipeline:   {config.get('pipeline.name')} v{config.get('pipeline.version')}")
    print(f"Components: {', '.join(components_to_run)}")
    print(f"Vision:     {vision}")
    print(f"Config:     {config_path}")

    # =========================================================================
    # Initialize Logging (CENTRALIZED - Single log for all components)
    # =========================================================================
    log_level = config.get('logging.level', 'INFO')
    log_dir = config.get('logging.local_dir', 'logs')
    log_filename_template = config.get('logging.filename_template', 'pipeline_{vision}.log')
    log_filename = log_filename_template.format(vision=vision)

    # Create log directory
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, log_filename)

    from utils.logger import get_logger
    from utils.helpers import upload_log_to_datalake

    logger = get_logger('main', log_file=log_file, level=log_level)

    logger.section("Construction Data Pipeline - Execution Start")
    logger.info(f"Pipeline: {config.get('pipeline.name')} v{config.get('pipeline.version')}")
    logger.info(f"Components to run: {', '.join(components_to_run)}")
    logger.info(f"Vision: {vision}")
    logger.info(f"Config: {config_path}")
    logger.info(f"Log File: {log_file}")

    # =========================================================================
    # Initialize Spark Session (SINGLE SESSION FOR ALL COMPONENTS)
    # =========================================================================
    logger.info("Initializing Spark Session...")

    spark = None
    success = False

    try:
        from pyspark.sql import SparkSession # type: ignore
        # # Internal module to auto configure spark and connection to Azure
        import azfr_fsspec_utils as fspath 
        import azfr_fsspec_abfs
        # # Configure SparkSession
        azfr_fsspec_abfs.use()

        app_name = config.get('spark.app_name', 'Construction_Pipeline')
        spark_config = config.get('spark.config', {})

        # Build SparkSession
        builder = SparkSession.builder.appName(app_name)

        # Apply custom Spark configurations if any
        if spark_config:
            for key, value in spark_config.items():
                builder = builder.config(key, value)

        # Create or get existing session
        spark = builder.getOrCreate()

        logger.success(f"Spark Session initialized: {app_name} v{spark.version}")
        if spark_config:
            logger.info(f"Custom Spark config: {len(spark_config)} settings applied")

        # =====================================================================
        # Performance Tracking - Start Timer
        # =====================================================================
        pipeline_start_time = time.time()
        logger.info(f"Pipeline execution started at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(pipeline_start_time))}")

        # =====================================================================
        # Run Component Pipelines (Loop through all enabled components)
        # =====================================================================
        component_results = {}
        overall_success = True

        for component in components_to_run:
            logger.section(f"Executing Component: {component.upper()}")
            component_desc = config.get(f'components.{component}.description', 'No description')
            logger.info(f"Description: {component_desc}")

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
                    logger.error(f"Unknown component: {component}")
                    component_success = False

                component_results[component] = component_success

                if component_success:
                    logger.success(f"Component '{component}' completed successfully")
                else:
                    logger.failure(f"Component '{component}' failed")
                    overall_success = False

            except Exception as e:
                logger.failure(f"Component '{component}' execution failed: {str(e)}")
                logger.error(f"Error details: {e}", exc_info=True)
                component_results[component] = False
                overall_success = False

            logger.info("─" * 80)  # Separator between components

        # =====================================================================
        # Performance Tracking - End Timer
        # =====================================================================
        pipeline_end_time = time.time()
        total_duration = pipeline_end_time - pipeline_start_time
        
        # Format duration as HH:MM:SS
        hours, remainder = divmod(int(total_duration), 3600)
        minutes, seconds = divmod(remainder, 60)
        duration_formatted = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        
        logger.info(f"Pipeline execution ended at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(pipeline_end_time))}")
        logger.info(f"Total pipeline execution time: {duration_formatted} ({total_duration:.2f} seconds)")

        # Log summary of all components
        logger.section("Component Execution Summary")
        for comp, result in component_results.items():
            status = "✓ SUCCESS" if result else "✗ FAILED"
            logger.info(f"{comp}: {status}")
        
        logger.info("")
        logger.info(f"⏱️  TOTAL EXECUTION TIME: {duration_formatted}")

        success = overall_success

    except Exception as e:
        logger.failure(f"Pipeline execution failed: {str(e)}")
        logger.error(f"Error details: {e}", exc_info=True)
        success = False

    finally:
        # =====================================================================
        # Cleanup: Upload Log and Stop Spark (Always runs)
        # =====================================================================
        if spark is not None:
            logger.info("Uploading log to datalake...")

            try:
                base_path = config.get('datalake.base_path')
                upload_success = upload_log_to_datalake(spark, log_file, base_path)
                if upload_success:
                    logger.success(f"Log uploaded to datalake: bronze/logs/{log_filename}")
                    print(f"√ Log uploaded: bronze/logs/{log_filename}")
                else:
                    logger.warning("Log upload failed (datalake may be unavailable)")
                    print(f"⚠ Log kept locally: {log_file}")
            except Exception as e:
                logger.warning(f"Log upload to datalake failed: {e}")
                print(f"⚠ Log kept locally: {log_file}")

            # Stop Spark session
            logger.info("Stopping Spark session...")
            spark.stop()
            logger.info("Spark session stopped")

        # Final status
        if success:
            logger.success("=" * 60)
            logger.success("PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
            logger.success("=" * 60)
        else:
            logger.failure("=" * 60)
            logger.failure("PIPELINE EXECUTION FAILED")
            logger.failure("=" * 60)

    # =========================================================================
    # Exit with appropriate code
    # =========================================================================
    # Calculate final times for console display
    try:
        hours, remainder = divmod(int(total_duration), 3600)
        minutes, seconds = divmod(remainder, 60)
        duration_display = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    except:
        duration_display = "N/A"
    
    if success:
        print("\n" + "=" * 80)
        print("✓ Pipeline completed successfully!")
        print(f"⏱️  Total execution time: {duration_display}")
        print("=" * 80)
        sys.exit(0)
    else:
        print("\n" + "=" * 80)
        print("✗ Pipeline failed!")
        print(f"⏱️  Total execution time: {duration_display}")
        print("=" * 80)
        sys.exit(1)


if __name__ == "__main__":
    main()
