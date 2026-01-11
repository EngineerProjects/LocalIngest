"""
PTF_MVT Pipeline Orchestrator.

Coordinates execution of AZ, AZEC, and Consolidation processors
for the Portfolio Movements domain.
"""

import os
from pathlib import Path
from pyspark.sql import SparkSession # type: ignore

from src.orchestrators import BaseOrchestrator
from src.processors.ptf_mvt_processors.az_processor import AZProcessor
from src.processors.ptf_mvt_processors.azec_processor import AZECProcessor
from src.processors.ptf_mvt_processors.consolidation_processor import ConsolidationProcessor
from src.reader import BronzeReader


def copy_ird_risk_to_gold(spark, config, vision: str, logger):
    """
    Copy IRD risk files from bronze to gold layer.

    Based on SAS CUBE library outputs (PTF_MVTS_CONSOLIDATION_MACRO.sas L158-258):
    - CUBE.ird_risk_q45_{vision}
    - CUBE.ird_risk_q46_{vision}
    - CUBE.ird_risk_qan_{vision}

    IMPORTANT: SAS always creates ALL THREE IRD files in CUBE regardless of vision.
    The consolidation macro reads and joins all three files (lines 158-258).
    Python must match this behavior exactly.

    Args:
        spark: SparkSession
        config: ConfigLoader instance
        vision: Vision in YYYYMM format
        logger: Logger instance
    """
    from utils.helpers import write_to_layer

    reader = BronzeReader(spark, config)
    
    # ALL THREE IRD files must be copied to gold (matching SAS CUBE output)
    # SAS reads all three files regardless of vision (L158, L194, L230)
    ird_files = ['ird_risk_q45', 'ird_risk_q46', 'ird_risk_qan']
    
    logger.info(f"Copying ALL IRD risk files to gold: {ird_files}")

    copied_count = 0
    for ird_file_group in ird_files:
        try:
            logger.debug(f"Processing {ird_file_group}")

            # Read from bronze
            df_ird = reader.read_file_group(ird_file_group, vision)

            if df_ird is None or df_ird.count() == 0:
                logger.warning(f"{ird_file_group} not found or empty, skipping")
                continue

            # Write to gold layer
            write_to_layer(df_ird, config, 'gold', f'{ird_file_group}_{vision}', vision, logger)

            logger.success(f"{ird_file_group} copied to gold ({df_ird.count()} rows)")
            copied_count += 1

        except Exception as e:
            logger.warning(f"Could not copy {ird_file_group} to gold: {e}")

    logger.success(f"IRD risk files copy completed ({copied_count}/3 files copied)")


class PTFMVTOrchestrator(BaseOrchestrator):
    """
    PTF_MVT pipeline orchestrator.
    
    Executes:
    1. AZ Processor (Bronze → Silver)
    2. AZEC Processor (Bronze → Silver)  
    3. Consolidation Processor (Silver → Gold)
    4. IRD Risk Files copy to Gold
    """
    
    def define_stages(self):
        """Define PTF_MVT pipeline stages."""
        return [
            ("AZ Processor (Bronze → Silver)", AZProcessor),
            ("AZEC Processor (Bronze → Silver)", AZECProcessor),
            ("Consolidation Processor (Silver → Gold)", ConsolidationProcessor)
        ]
    
    def post_process(self, vision, results):
        """Copy IRD risk files to gold layer after main processing."""
        self.logger.section("STAGE 4: Copy IRD Risk Files to Gold")
        copy_ird_risk_to_gold(self.spark, self.config, vision, self.logger)


def run_ptf_mvt_pipeline(
    vision: str,
    config_path: str = None,
    spark: SparkSession = None,
    logger = None
) -> bool:
    """
    Execute PTF_MVT pipeline: Bronze → Silver → Gold.
    
    Pipeline stages:
    1. AZ Processor (bronze → silver)
    2. AZEC Processor (bronze → silver)
    3. Consolidation Processor (silver → gold)
    4. IRD Risk Files copy to gold

    Args:
        vision: Vision in YYYYMM format (e.g., '202509')
        config_path: Optional path to config.yml
        spark: SparkSession (REQUIRED - initialized in main.py)
        logger: Logger instance (REQUIRED - initialized in main.py)

    Returns:
        True if successful, False otherwise

    Example:
        >>> from pyspark.sql import SparkSession
        >>> from utils.logger import get_logger
        >>> spark = SparkSession.builder.appName("Pipeline").getOrCreate()
        >>> logger = get_logger('main')
        >>> success = run_ptf_mvt_pipeline('202509', spark=spark, logger=logger)
    """
    # Validate required parameters
    if spark is None:
        raise ValueError("SparkSession is required. Initialize in main.py")
    
    if logger is None:
        raise ValueError("Logger is required. Initialize in main.py")
    
    # Default config path
    if config_path is None:
        project_root = Path(__file__).parent.parent
        config_path = project_root / "config" / "config.yml"
    
    # Create orchestrator and run
    orchestrator = PTFMVTOrchestrator(spark, str(config_path), logger)
    return orchestrator.run(vision)


if __name__ == "__main__":
    import sys
    from pyspark.sql import SparkSession # type: ignore
    from utils.logger import get_logger
    
    # Simple CLI: python ptf_mvt_run.py YYYYMM
    if len(sys.argv) < 2:
        print("Usage: python ptf_mvt_run.py <vision>")
        print("Example: python ptf_mvt_run.py 202509")
        sys.exit(1)
    
    vision = sys.argv[1]
    
    # Initialize Spark and Logger (when running standalone)
    print("Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("Construction_Pipeline_PTF_MVT") \
        .getOrCreate()
    
    logger = get_logger('ptf_mvt_standalone', log_file=f'logs/ptf_mvt_{vision}.log')
    
    try:
        success = run_ptf_mvt_pipeline(vision, spark=spark, logger=logger)
        sys.exit(0 if success else 1)
    finally:
        spark.stop()
