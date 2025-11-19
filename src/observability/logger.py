"""
Structured JSON logging for dirty-spark-pipeline

This module provides consistent structured logging across the application
using python-json-logger for easy parsing and analysis.
"""
import logging
import os
import sys

from pythonjsonlogger import jsonlogger

# Log level mapping
LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """
    Custom JSON formatter that adds additional context fields

    Adds: timestamp, level, logger_name, and custom fields
    """

    def add_fields(self, log_record: dict, record: logging.LogRecord, message_dict: dict) -> None:
        """
        Add custom fields to log record

        Args:
            log_record: Log record dictionary
            record: LogRecord object
            message_dict: Message dictionary
        """
        super().add_fields(log_record, record, message_dict)

        # Add timestamp
        if not log_record.get("timestamp"):
            log_record["timestamp"] = self.formatTime(record, self.datefmt)

        # Add level name
        if log_record.get("level"):
            log_record["level"] = log_record["level"].upper()
        else:
            log_record["level"] = record.levelname

        # Add logger name
        log_record["logger"] = record.name

        # Add module and function for debugging
        log_record["module"] = record.module
        log_record["function"] = record.funcName

        # Add process and thread info
        log_record["process_id"] = record.process
        log_record["thread_id"] = record.thread


def setup_logger(
    name: str = "dirty-spark-pipeline",
    level: str | None = None,
    format_type: str = "json",
) -> logging.Logger:
    """
    Setup and configure a logger

    Args:
        name: Logger name
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_type: "json" or "text"

    Returns:
        Configured logger instance
    """
    # Get log level from environment or parameter
    log_level_str = level or os.getenv("LOG_LEVEL", "INFO")
    log_level = LOG_LEVELS.get(log_level_str.upper(), logging.INFO)

    # Get or create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()

    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)

    # Set formatter based on format type
    if format_type == "json":
        formatter = CustomJsonFormatter(
            fmt="%(timestamp)s %(level)s %(logger)s %(module)s %(function)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    else:
        # Text format for local development
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(funcName)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Prevent propagation to root logger
    logger.propagate = False

    return logger


def get_logger(name: str = "dirty-spark-pipeline") -> logging.Logger:
    """
    Get a logger instance

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)

    # If logger has no handlers, set it up
    if not logger.handlers:
        return setup_logger(name)

    return logger


# Global logger instance
_default_logger: logging.Logger | None = None


def get_default_logger() -> logging.Logger:
    """
    Get the default application logger

    Returns:
        Default logger instance
    """
    global _default_logger
    if _default_logger is None:
        _default_logger = setup_logger("dirty-spark-pipeline")
    return _default_logger


# Convenience functions for common log levels
def debug(message: str, **kwargs) -> None:
    """Log debug message with extra fields"""
    get_default_logger().debug(message, extra=kwargs)


def info(message: str, **kwargs) -> None:
    """Log info message with extra fields"""
    get_default_logger().info(message, extra=kwargs)


def warning(message: str, **kwargs) -> None:
    """Log warning message with extra fields"""
    get_default_logger().warning(message, extra=kwargs)


def error(message: str, **kwargs) -> None:
    """Log error message with extra fields"""
    get_default_logger().error(message, extra=kwargs)


def critical(message: str, **kwargs) -> None:
    """Log critical message with extra fields"""
    get_default_logger().critical(message, extra=kwargs)


# Context manager for logging operation duration
class log_operation:
    """
    Context manager for logging operation duration

    Usage:
        with log_operation("Processing batch", logger=logger, batch_id="123"):
            # do work
            pass
    """

    def __init__(self, operation_name: str, logger: logging.Logger | None = None, **extra_fields):
        """
        Initialize operation logger

        Args:
            operation_name: Name of the operation
            logger: Logger instance (uses default if None)
            **extra_fields: Additional fields to include in logs
        """
        self.operation_name = operation_name
        self.logger = logger or get_default_logger()
        self.extra_fields = extra_fields
        self.start_time = None

    def __enter__(self):
        """Start operation"""
        import time
        self.start_time = time.time()
        self.logger.info(
            f"Starting: {self.operation_name}",
            extra={"operation": self.operation_name, **self.extra_fields}
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """End operation"""
        import time
        duration = time.time() - self.start_time

        if exc_type is None:
            self.logger.info(
                f"Completed: {self.operation_name}",
                extra={
                    "operation": self.operation_name,
                    "duration_seconds": round(duration, 3),
                    "status": "success",
                    **self.extra_fields
                }
            )
        else:
            self.logger.error(
                f"Failed: {self.operation_name}",
                extra={
                    "operation": self.operation_name,
                    "duration_seconds": round(duration, 3),
                    "status": "error",
                    "error_type": exc_type.__name__,
                    "error_message": str(exc_val),
                    **self.extra_fields
                },
                exc_info=True
            )
        return False  # Don't suppress exceptions
