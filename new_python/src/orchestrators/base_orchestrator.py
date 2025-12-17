"""
Base Orchestrator for Pipeline Execution.

Provides common patterns for pipeline orchestration:
- Config loading and validation
- Processor initialization
- Sequential stage execution with logging
- Error handling and reporting
- Summary generation

Eliminates ~200 lines of duplicate code across pipeline orchestrators.
"""

from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional, Union
from pyspark.sql import DataFrame


class BaseOrchestrator:
    """
    Base class for pipeline orchestrators.
    
    Subclasses should override:
    - define_stages() - List of (stage_name, processor_class) tuples
    - pre_process() - Optional pre-processing hook
    - post_process() - Optional post-processing hook
    
    Example:
        class MyOrchestrator(BaseOrchestrator):
            def define_stages(self):
                return [
                    ("Stage 1", MyProcessor1),
                    ("Stage 2", MyProcessor2)
                ]
    """
    
    def __init__(self, spark, config_path: str, logger):
        """
        Initialize orchestrator.
        
        Args:
            spark: SparkSession instance
            config_path: Path to config.yml
            logger: Logger instance
        """
        self.spark = spark
        self.logger = logger
        
        # Load configuration
        from utils.loaders.config_loader import ConfigLoader
        self.config = ConfigLoader(config_path)
    
    def validate_vision(self, vision: str) -> bool:
        """
        Validate vision format (YYYYMM).
        
        Args:
            vision: Vision string to validate
        
        Returns:
            True if valid, False otherwise
        """
        from utils.helpers import validate_vision
        if not validate_vision(vision):
            self.logger.error(f"Invalid vision format: {vision}. Expected YYYYMM")
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
        Execute a single processor stage.
        
        Args:
            stage_num: Stage number for logging (e.g., 1, 2, 3)
            stage_name: Human-readable stage name
            processor_class: Processor class to instantiate
            vision: Vision in YYYYMM format
        
        Returns:
            DataFrame result, tuple of DataFrames, or None if failed
        """
        self.logger.section(f"STAGE {stage_num}: {stage_name}")
        
        try:
            processor = processor_class(self.spark, self.config, self.logger)
            result = processor.run(vision)
            
            if result is None:
                self.logger.error(f"{stage_name} returned None")
                return None
            
            # Handle tuple results (e.g., emissions returns 2 DFs)
            if isinstance(result, tuple):
                total_rows = sum(df.count() for df in result if df is not None)
                self.logger.success(f"{stage_name} completed: {total_rows:,} total rows")
            else:
                self.logger.success(f"{stage_name} completed: {result.count():,} rows")
            
            return result
            
        except Exception as e:
            self.logger.failure(f"{stage_name} failed: {e}")
            self.logger.error(f"Error details: {e}")
            raise
    
    def run(self, vision: str) -> bool:
        """
        Execute complete pipeline.
        
        Workflow:
        1. Validate vision
        2. Pre-process (optional)
        3. Execute stages sequentially
        4. Post-process (optional)
        5. Print summary
        
        Args:
            vision: Vision in YYYYMM format
        
        Returns:
            True if successful, False otherwise
        """
        # Validate vision format
        if not self.validate_vision(vision):
            return False
        
        # Log pipeline start
        pipeline_name = self.__class__.__name__.replace("Orchestrator", "").upper()
        self.logger.section(f"{pipeline_name} PIPELINE - Vision {vision}")
        
        try:
            # Pre-processing hook
            self.pre_process(vision)
            
            # Execute stages
            stages = self.define_stages()
            results = {}
            
            for i, (stage_name, processor_class) in enumerate(stages, 1):
                result = self.run_stage(i, stage_name, processor_class, vision)
                
                # Allow stage failure if explicitly configured
                if result is None and not self.allow_stage_failure(stage_name):
                    self.logger.failure(f"Pipeline stopped due to {stage_name} failure")
                    return False
                
                results[stage_name] = result
            
            # Post-processing hook
            self.post_process(vision, results)
            
            # Print summary
            self.print_summary(results)
            
            # Success
            self.logger.success(f"{pipeline_name} Pipeline completed successfully!")
            return True
            
        except Exception as e:
            self.logger.failure(f"Pipeline failed: {e}")
            self.logger.error(f"Exception: {e}")
            return False
    
    def define_stages(self) -> List[Tuple[str, type]]:
        """
        Define pipeline stages.
        
        Override in subclass to specify processor sequence.
        
        Returns:
            List of (stage_name, processor_class) tuples
        
        Example:
            return [
                ("AZ Processor (Bronze → Silver)", AZProcessor),
                ("AZEC Processor (Bronze → Silver)", AZECProcessor)
            ]
        """
        raise NotImplementedError("Subclasses must define stages")
    
    def pre_process(self, vision: str):
        """
        Pre-processing hook (optional).
        
        Override in subclass if needed.
        
        Args:
            vision: Vision in YYYYMM format
        """
        pass
    
    def post_process(self, vision: str, results: Dict[str, Any]):
        """
        Post-processing hook (optional).
        
        Override in subclass for custom post-processing (e.g., copy files, cleanup).
        
        Args:
            vision: Vision in YYYYMM format
            results: Dict mapping stage_name → DataFrame result
        """
        pass
    
    def allow_stage_failure(self, stage_name: str) -> bool:
        """
        Check if a stage is allowed to fail without stopping pipeline.
        
        Override in subclass to allow specific optional stages.
        
        Args:
            stage_name: Name of the stage
        
        Returns:
            True if failure is acceptable, False otherwise
        """
        return False
    
    def print_summary(self, results: Dict[str, Any]):
        """
        Print pipeline execution summary.
        
        Args:
            results: Dict mapping stage_name → DataFrame result
        """
        self.logger.section("Pipeline Summary")
        
        for stage_name, result in results.items():
            if result is None:
                self.logger.info(f"{stage_name}: SKIPPED or FAILED")
            elif isinstance(result, tuple):
                # Handle multiple outputs (e.g., emissions)
                for i, df in enumerate(result, 1):
                    if df is not None:
                        self.logger.info(f"{stage_name} (output {i}): {df.count():,} rows")
            else:
                self.logger.info(f"{stage_name}: {result.count():,} rows")
