"""
Emissions Pipeline Orchestrator.

Runs the Emissions data processing pipeline:
1. One BI Emissions Processor (Bronze → Silver → Gold)

Based on: EMISSIONS_RUN.sas (308 lines)
"""

from src.orchestrators import BaseOrchestrator


class EmissionsOrchestrator(BaseOrchestrator):
    """
    Emissions pipeline orchestrator.
    
    Executes:
    1. Emissions Processor (Bronze → Silver → Gold)
       - Outputs 2 files: by guarantee and aggregated by policy
    """
    
    def define_stages(self):
        """Define Emissions pipeline stages."""
        from src.processors.emissions_processors import EmissionsProcessor
        
        return [
            ("Emissions Processor (Bronze → Silver → Gold)", EmissionsProcessor)
        ]
    
    def print_summary(self, results):
        """Custom summary for emissions (2 output files)."""
        self.logger.section("EMISSIONS PIPELINE COMPLETED")
        
        for stage_name, result in results.items():
            if result and isinstance(result, tuple):
                df_pol_garp, df_pol = result
                self.logger.success(f"  - POL_GARP: {df_pol_garp.count():,} rows (by guarantee)")
                self.logger.success(f"  - POL: {df_pol.count():,} rows (aggregated)")


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
    orchestrator = EmissionsOrchestrator(spark, config_path, logger)
    return orchestrator.run(vision)


if __name__ == "__main__":
    import sys
    from pyspark.sql import SparkSession
    from utils.logger import get_logger
    from pathlib import Path
    
    if len(sys.argv) < 2:
        print("Usage: python emissions_run.py <vision>")
        print("Example: python emissions_run.py 202509")
        sys.exit(1)
    
    vision = sys.argv[1]
    
    # Initialize Spark and Logger
    print("Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("Construction_Pipeline_Emissions") \
        .getOrCreate()
    
    logger = get_logger('emissions_standalone', log_file=f'logs/emissions_{vision}.log')
    
    # Config path
    project_root = Path(__file__).parent.parent
    config_path = str(project_root / "config" / "config.yml")
    
    try:
        success = run_emissions_pipeline(vision, config_path, spark, logger)
        sys.exit(0 if success else 1)
    finally:
        spark.stop()
