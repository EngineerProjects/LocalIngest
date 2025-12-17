"""
Base processor abstract class for all data processors.
Defines the contract for read→transform→write ETL pattern.
"""

from abc import ABC, abstractmethod
import time
from pyspark.sql import SparkSession, DataFrame # type: ignore
from utils.loaders.config_loader import ConfigLoader
from utils.logger import PipelineLogger


class BaseProcessor(ABC):
    """
    Abstract base class for all data processors.
    
    Enforces a consistent ETL pattern: read() → transform() → write()
    """

    def __init__(
        self,
        spark: SparkSession,
        config: ConfigLoader,
        logger: PipelineLogger
    ):
        """
        Initialize base processor.

        Args:
            spark: SparkSession instance
            config: ConfigLoader instance
            logger: PipelineLogger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
    
    def get_project_root(self):
        """
        Get the project root directory by finding the 'src' directory.
        
        This method walks up the directory tree from this file's location
        to find the directory containing the 'src' folder, which is the
        project root.
        
        Returns:
            Path: Absolute path to project root
        """
        from pathlib import Path
        
        # Start from this file's directory
        current = Path(__file__).parent
        
        # Walk up until we find the directory containing 'src'
        while current.name != '' and current != current.parent:
            if (current / 'src').exists():
                return current
            current = current.parent
        
        # Fallback: use CWD
        return Path.cwd()


    @abstractmethod
    def read(self, vision: str) -> DataFrame:
        """
        Read input data.

        Args:
            vision: Vision in YYYYMM format

        Returns:
            Input DataFrame
        """
        pass

    @abstractmethod
    def transform(self, df: DataFrame, vision: str) -> DataFrame:
        """
        Apply business transformations.

        Args:
            df: Input DataFrame
            vision: Vision in YYYYMM format

        Returns:
            Transformed DataFrame
        """
        pass

    @abstractmethod
    def write(self, df: DataFrame, vision: str) -> None:
        """
        Write output to datalake.

        Args:
            df: Output DataFrame
            vision: Vision in YYYYMM format
        """
        pass

    def run(self, vision: str) -> DataFrame:
        """
        Execute full ETL pipeline: read → transform → write.

        Args:
            vision: Vision in YYYYMM format

        Returns:
            Final transformed DataFrame
        """
        processor_name = self.__class__.__name__
        self.logger.section(f"Running {processor_name}")
        
        # Read data
        self.logger.info(f"Starting: {processor_name}.read()")
        start_time = time.time()
        df = self.read(vision)
        
        # Cache immediately after read to optimize subsequent operations
        # This prevents recomputation when count() or transforms are called
        df = df.cache()
        
        duration = time.time() - start_time
        self.logger.info(f"Completed: {processor_name}.read() (Duration: {duration:.2f}s)")
        self.logger.info(f"Read {df.count()} rows")
        
        # Transform
        self.logger.info(f"Starting: {processor_name}.transform()")
        start_time = time.time()
        df = self.transform(df, vision)
        duration = time.time() - start_time
        self.logger.info(f"Completed: {processor_name}.transform() (Duration: {duration:.2f}s)")
        self.logger.info(f"Transformed to {df.count()} rows")
        
        # Write
        self.logger.info(f"Starting: {processor_name}.write()")
        start_time = time.time()
        self.write(df, vision)
        duration = time.time() - start_time
        self.logger.info(f"Completed: {processor_name}.write() (Duration: {duration:.2f}s)")
        
        self.logger.success(f"{processor_name} completed successfully")
        
        return df
