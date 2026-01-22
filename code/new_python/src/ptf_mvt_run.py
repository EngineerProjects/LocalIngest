
# -*- coding: utf-8 -*-
"""
PTF_MVT Pipeline Orchestrator.

Coordinates execution of AZ, AZEC, and Consolidation processors
for the Portfolio Movements domain.
"""

import os
from pathlib import Path
from pyspark.sql import SparkSession  # type: ignore

from src.orchestrators import BaseOrchestrator
from src.processors.ptf_mvt_processors.az_processor import AZProcessor
from src.processors.ptf_mvt_processors.azec_processor import AZECProcessor
from src.processors.ptf_mvt_processors.consolidation_processor import ConsolidationProcessor
from src.reader import BronzeReader


def _get_bool_env(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip() in ("1", "true", "TRUE", "yes", "YES", "on", "ON")


def copy_ird_risk_to_gold(spark, config, vision: str, logger):
    """
    Copy IRD risk files from bronze to gold layer.
    Based on SAS CUBE behavior (always present Q45/Q46/QAN).
    """
    from utils.helpers import write_to_layer

    reader = BronzeReader(spark, config)

    ird_files = ['ird_risk_q45', 'ird_risk_q46', 'ird_risk_qan']
    logger.info(f"Copying ALL IRD risk files to gold: {ird_files}")

    copied_count = 0
    for ird_file_group in ird_files:
        try:
            logger.debug(f"Processing {ird_file_group}")
            df_ird = reader.read_file_group(ird_file_group, vision)

            if df_ird is None or df_ird.count() == 0:
                logger.warning(f"{ird_file_group} not found or empty, skipping")
                continue

            write_to_layer(df_ird, config, 'gold', f'{ird_file_group}_{vision}', vision, logger)

            # Remplacer success par info (compat logging std)
            logger.info(f"{ird_file_group} copied to gold ({df_ird.count()} rows)")
            copied_count += 1

        except Exception as e:
            logger.warning(f"Could not copy {ird_file_group} to gold: {e}")

    logger.info(f"IRD risk files copy completed ({copied_count}/3 files copied)")


class PTFMVTOrchestrator(BaseOrchestrator):
    """
    PTF_MVT pipeline orchestrator with runtime toggles.

    Env toggles (default AZ only):
      RUN_AZ=1, RUN_AZEC=0, RUN_CONSO=0, RUN_IRD=0
    """

    def define_stages(self):
        """Define PTF_MVT pipeline stages based on toggles."""
        stages = []

        run_az = _get_bool_env("RUN_AZ", "0")        
        run_azec = _get_bool_env("RUN_AZEC", "0")    
        run_conso = _get_bool_env("RUN_CONSO", "1")  

        if run_az:
            stages.append(("AZ Processor (Bronze → Silver)", AZProcessor))
        if run_azec:
            stages.append(("AZEC Processor (Bronze → Silver)", AZECProcessor))
        if run_conso:
            stages.append(("Consolidation Processor (Silver → Gold)", ConsolidationProcessor))

        # Safety: if nothing selected, at least run AZ
        if not stages:
            self.logger.warning("No stages selected via env; defaulting to AZ only.")
            stages.append(("AZ Processor (Bronze → Silver)", AZProcessor))

        # Log what will run
        self.logger.info(f"Selected stages: {[name for name, _ in stages]}")
        return stages

    def post_process(self, vision, results):
        """Optionally copy IRD risk files to gold layer."""
        run_ird = _get_bool_env("RUN_IRD", "0")  # default OFF while debugging AZ
        if not run_ird:
            self.logger.section("STAGE 4: Copy IRD Risk Files to Gold (SKIPPED)")
            return
        self.logger.section("STAGE 4: Copy IRD Risk Files to Gold")
        copy_ird_risk_to_gold(self.spark, self.config, vision, self.logger)


def run_ptf_mvt_pipeline(
    vision: str,
    config_path: str = None,
    spark: SparkSession = None,
    logger = None
) -> bool:
    """
    Execute PTF_MVT pipeline with toggles.
    """
    if spark is None:
        raise ValueError("SparkSession is required. Initialize in main.py")
    if logger is None:
        raise ValueError("Logger is required. Initialize in main.py")

    if config_path is None:
        project_root = Path(__file__).parent.parent
        config_path = project_root / "config" / "config.yml"

    orchestrator = PTFMVTOrchestrator(spark, str(config_path), logger)
    return orchestrator.run(vision)


if __name__ == "__main__":
    import sys
    from pyspark.sql import SparkSession  # type: ignore
    from utils.logger import get_logger

    # Simple CLI: python ptf_mvt_run.py YYYYMM
    if len(sys.argv) < 2:
        print("Usage: python ptf_mvt_run.py <vision>")
        print("Example: python ptf_mvt_run.py 202509")
        sys.exit(1)

    vision = sys.argv[1]

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
