"""
Base processor abstract class for all data processors.
Defines the contract for read→transform→write ETL pattern.
"""

from abc import ABC, abstractmethod
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
        
        # Read
        start_time = self.logger.timer_start(f"{processor_name}.read()")
        df = self.read(vision)
        self.logger.timer_end(f"{processor_name}.read()", start_time)
        self.logger.info(f"Read {df.count()} rows")
        
        # Transform
        start_time = self.logger.timer_start(f"{processor_name}.transform()")
        df = self.transform(df, vision)
        self.logger.timer_end(f"{processor_name}.transform()", start_time)
        self.logger.info(f"Transformed to {df.count()} rows")
        
        # Write
        start_time = self.logger.timer_start(f"{processor_name}.write()")
        self.write(df, vision)
        self.logger.timer_end(f"{processor_name}.write()", start_time)
        
        self.logger.success(f"{processor_name} completed successfully")
        
        return df
