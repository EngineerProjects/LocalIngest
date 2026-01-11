"""
Logging utilities for Construction Data Pipeline.
Provides simple, clean logging to file and console with section markers.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional


class PipelineLogger:
    """Logger for pipeline operations with enhanced formatting."""

    def __init__(
        self,
        name: str,
        log_file: Optional[str] = None,
        level: str = "INFO"
    ):
        """
        Initialize pipeline logger.

        Args:
            name: Logger name (usually module name)
            log_file: Path to log file (optional)
            level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))

        # Avoid duplicate handlers
        if self.logger.handlers:
            return

        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # File handler (if log_file provided)
        if log_file:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)

            file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def debug(self, message: str) -> None:
        """Log debug message."""
        self.logger.debug(message)

    def info(self, message: str) -> None:
        """Log info message."""
        self.logger.info(message)

    def warning(self, message: str) -> None:
        """Log warning message."""
        self.logger.warning(message)

    def error(self, message: str) -> None:
        """Log error message."""
        self.logger.error(message)

    def critical(self, message: str) -> None:
        """Log critical message."""
        self.logger.critical(message)

    def section(self, title: str) -> None:
        """
        Log a section separator for better readability.

        Args:
            title: Section title
        """
        separator = "=" * 80
        self.logger.info(separator)
        self.logger.info(f"  {title}")
        self.logger.info(separator)

    def step(self, step_number: int, description: str) -> None:
        """
        Log a numbered step in the pipeline.

        Args:
            step_number: Step number
            description: Step description
        """
        self.logger.info(f"STEP {step_number}: {description}")

    def success(self, message: str) -> None:
        """Log success message with special prefix."""
        self.logger.info(f"✓ SUCCESS: {message}")

    def failure(self, message: str) -> None:
        """Log failure message with special prefix."""
        self.logger.error(f"✗ FAILURE: {message}")

    def timer_start(self, operation: str) -> datetime:
        """
        Start timing an operation.

        Args:
            operation: Operation name

        Returns:
            Start timestamp
        """
        start_time = datetime.now()
        self.logger.info(f"Starting: {operation}")
        return start_time

    def timer_end(self, operation: str, start_time: datetime) -> None:
        """
        End timing an operation and log duration.

        Args:
            operation: Operation name
            start_time: Start timestamp from timer_start
        """
        duration = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"Completed: {operation} (Duration: {duration:.2f}s)")


def get_logger(
    name: str,
    log_file: Optional[str] = None,
    level: str = "INFO"
) -> PipelineLogger:
    """
    Get a logger instance.

    Args:
        name: Logger name
        log_file: Path to log file (optional)
        level: Logging level

    Returns:
        PipelineLogger instance

    Example:
        >>> logger = get_logger(__name__, 'logs/pipeline_202509.log')
        >>> logger.info('Pipeline started')
        >>> logger.success('Data loaded successfully')
    """
    return PipelineLogger(name, log_file, level)

