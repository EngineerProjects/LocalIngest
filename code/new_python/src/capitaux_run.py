"""
Capitaux Pipeline Orchestrator.

Runs the complete capital data processing pipeline:
1. AZ Processor (Bronze → Silver)
2. AZEC Processor (Bronze → Silver)  
3. Consolidation (Silver → Gold)

Based on: CAPITAUX_RUN.sas (211 lines)
"""

from src.orchestrators import BaseOrchestrator


class CapitauxOrchestrator(BaseOrchestrator):
    """
    Capitaux pipeline orchestrator.
    
    Executes:
    1. AZ Capitaux Processor (Bronze → Silver)
    2. AZEC Capitaux Processor (Bronze → Silver)
    3. Consolidation (Silver → Gold)
    """
    
    def define_stages(self):
        """Define Capitaux pipeline stages."""
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
        """AZEC is optional - pipeline can continue if it fails."""
        return "AZEC" in stage_name


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
        STAGE 2: AZEC Capitaux (Bronze → Silver) - optional
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
    orchestrator = CapitauxOrchestrator(spark, config_path, logger)
    return orchestrator.run(vision)


if __name__ == "__main__":
    import sys
    from pyspark.sql import SparkSession
    from utils.logger import get_logger
    from pathlib import Path
    
    if len(sys.argv) < 2:
        print("Usage: python capitaux_run.py <vision>")
        print("Example: python capitaux_run.py 202509")
        sys.exit(1)
    
    vision = sys.argv[1]
    
    # Initialize Spark and Logger
    print("Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("Construction_Pipeline_Capitaux") \
        .getOrCreate()
    
    logger = get_logger('capitaux_standalone', log_file=f'logs/capitaux_{vision}.log')
    
    # Config path
    project_root = Path(__file__).parent.parent
    config_path = str(project_root / "config" / "config.yml")
    
    try:
        success = run_capitaux_pipeline(vision, config_path, spark, logger)
        sys.exit(0 if success else 1)
    finally:
        spark.stop()
