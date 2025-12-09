"""
Capitaux Pipeline Orchestrator.

Runs the complete capital data processing pipeline:
1. AZ Processor (Bronze → Silver)
2. AZEC Processor (Bronze → Silver)  
3. Consolidation (Silver → Gold)

Based on: CAPITAUX_RUN.sas (211 lines)
"""

from typing import Optional


def run_capitaux_pipeline(
    vision: str,
    config_path: str,
    spark,
    logger
) -> bool:
    """
    Execute the complete Capitaux pipeline.
    
    Workflow:
        STAGE 1: AZ Capitaux (Bronze → Silver)
        STAGE 2: AZEC Capitaux (Bronze → Silver)
        STAGE 3: Consolidation (Silver → Gold)
    
    Args:
        vision: Vision in YYYYMM format (e.g., '202509')
        config_path: Path to config.yml
        spark: SparkSession instance
        logger: Logger instance
    
    Returns:
        bool: True if pipeline completed successfully, False otherwise
    
    Example:
        >>> success = run_capitaux_pipeline('202509', 'config/config.yml', spark, logger)
    """
    try:
        logger.section(f"CAPITAUX PIPELINE - Vision {vision}")
        logger.info("Starting capital data processing")
        
        # Load configuration
        from utils.loaders.config_loader import ConfigLoader
        config = ConfigLoader(config_path)
        
        # Import processors
        from src.processors.capitaux_processors import (
            AZCapitauxProcessor,
            AZECCapitauxProcessor,
            CapitauxConsolidationProcessor
        )
        
        # =====================================================================
        # STAGE 1: AZ Capitaux Processor (Bronze → Silver)
        # =====================================================================
        logger.section("STAGE 1: AZ Capitaux Processor (Bronze → Silver)")
        
        az_processor = AZCapitauxProcessor(spark, config, logger)
        df_az = az_processor.run(vision)
        
        if df_az is None:
            logger.error("AZ Processor failed")
            return False
        
        logger.success(f"AZ Processor completed: {df_az.count():,} rows written to silver")
        
        # =====================================================================
        # STAGE 2: AZEC Capitaux Processor (Bronze → Silver)
        # =====================================================================
        logger.section("STAGE 2: AZEC Capitaux Processor (Bronze → Silver)")
        
        try:
            azec_processor = AZECCapitauxProcessor(spark, config, logger)
            df_azec = azec_processor.run(vision)
            
            if df_azec is not None:
                logger.success(f"AZEC Processor completed: {df_azec.count():,} rows written to silver")
            else:
                logger.warning("AZEC Processor returned None - data may not be available")
        except Exception as e:
            logger.warning(f"AZEC Processor encountered error: {e}")
            logger.info("Continuing with AZ data only")
            df_azec = None
        
        # =====================================================================
        # STAGE 3: Consolidation (Silver → Gold)
        # =====================================================================
        logger.section("STAGE 3: Consolidation (Silver → Gold)")
        
        consolidation_processor = CapitauxConsolidationProcessor(spark, config, logger)
        df_consolidated = consolidation_processor.run(vision)
        
        if df_consolidated is None:
            logger.error("Consolidation failed")
            return False
        
        logger.success(f"Consolidation completed: {df_consolidated.count():,} rows written to gold")
        
        # =====================================================================
        # PIPELINE COMPLETION
        # =====================================================================
        logger.section("CAPITAUX PIPELINE COMPLETED")
        logger.success(f"Capital data processing completed successfully for vision {vision}")
        
        return True
        
    except Exception as e:
        logger.error(f"Capitaux pipeline failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False
