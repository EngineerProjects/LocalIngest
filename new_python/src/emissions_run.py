"""
Emissions Pipeline Orchestrator.

Runs the Emissions data processing pipeline:
1. One BI Emissions Processor (Bronze → Silver → Gold)

Based on: EMISSIONS_RUN.sas (308 lines)
"""

from typing import Optional


def run_emissions_pipeline(
    vision: str,
    config_path: str,
    spark,
    logger
) -> bool:
    """
    Execute the complete Emissions pipeline.
    
    Workflow:
        STAGE 1: Emissions Processor (Bronze → Silver → Gold)
        - Read One BI premium data (rf_fr1_prm_dtl_midcorp_m)
        - Apply business filters
        - Calculate premiums and commissions
        - Create 2 outputs:
          * primes_emises_{vision}_pol_garp (by guarantee)
          * primes_emises_{vision}_pol (aggregated by policy)
    
    Args:
        vision: Vision in YYYYMM format (e.g., '202509')
        config_path: Path to config.yml
        spark: SparkSession instance
        logger: Logger instance
    
    Returns:
        bool: True if pipeline completed successfully, False otherwise
    
    Example:
        >>> success = run_emissions_pipeline('202509', 'config/config.yml', spark, logger)
    """
    try:
        logger.section(f"EMISSIONS PIPELINE - Vision {vision}")
        logger.info("Starting premium emissions data processing")
        
        # Load configuration
        from utils.loaders.config_loader import ConfigLoader
        config = ConfigLoader(config_path)
        
        # Import processor
        from src.processors.emissions_processors import EmissionsProcessor
        
        # =====================================================================
        # STAGE 1: Emissions Processor (Bronze → Silver → Gold)
        # =====================================================================
        logger.section("STAGE 1: Emissions Processor (Bronze → Silver → Gold)")
        
        emissions_processor = EmissionsProcessor(spark, config, logger)
        dfs = emissions_processor.run(vision)
        
        if dfs is None:
            logger.error("Emissions Processor failed")
            return False
        
        df_pol_garp, df_pol = dfs
        
        logger.success(f"Emissions Processor completed:")
        logger.success(f"  - POL_GARP: {df_pol_garp.count():,} rows (by guarantee)")
        logger.success(f"  - POL: {df_pol.count():,} rows (aggregated)")
        
        # =====================================================================
        # PIPELINE COMPLETION
        # =====================================================================
        logger.section("EMISSIONS PIPELINE COMPLETED")
        logger.success(f"Emissions data processing completed successfully for vision {vision}")
        
        return True
        
    except Exception as e:
        logger.error(f"Emissions pipeline failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False
