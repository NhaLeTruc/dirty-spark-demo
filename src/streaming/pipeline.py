"""
Streaming pipeline orchestration for real-time data validation.

Coordinates the flow: source → validation → routing → sinks (warehouse/quarantine)
"""

import logging
from typing import Optional, Dict, Any
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

from src.core.models.data_source import DataSource
from src.core.rules.rule_engine import RuleEngine
from src.streaming.sources.file_stream_source import FileStreamSource
from src.streaming.sources.kafka_source import KafkaSource
from src.observability.logger import get_logger
from src.observability.metrics import MetricsCollector

logger = get_logger(__name__)


class StreamingPipeline:
    """
    Main streaming pipeline orchestrator.

    Handles the complete flow:
    1. Read from stream source (file or Kafka)
    2. Validate records against rules
    3. Route valid/invalid records
    4. Write to warehouse or quarantine
    5. Track lineage and metrics
    """

    def __init__(
        self,
        spark: SparkSession,
        data_source: DataSource,
        validation_engine: RuleEngine,
        checkpoint_location: str,
        schema: Optional[StructType] = None,
        trigger_interval: str = "10 seconds",
    ):
        """
        Initialize streaming pipeline.

        Args:
            spark: Active Spark session
            data_source: DataSource configuration
            validation_engine: RuleEngine for data quality rules
            checkpoint_location: Path for Spark checkpoints (for exactly-once)
            schema: Optional schema for source data
            trigger_interval: Micro-batch trigger interval (e.g., "10 seconds")
        """
        self.spark = spark
        self.data_source = data_source
        self.validation_engine = validation_engine
        self.checkpoint_location = checkpoint_location
        self.schema = schema
        self.trigger_interval = trigger_interval

        # Initialize metrics collector
        self.metrics = MetricsCollector()

        # Streaming query handle (set when started)
        self.query: Optional[StreamingQuery] = None

        # Ensure checkpoint directory exists
        checkpoint_path = Path(checkpoint_location)
        checkpoint_path.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Initialized StreamingPipeline for {data_source.source_id} "
            f"(checkpoint: {checkpoint_location}, trigger: {trigger_interval})"
        )

    def _create_source(self) -> DataFrame:
        """
        Create streaming source based on data source type.

        Returns:
            Streaming DataFrame from source

        Raises:
            ValueError: If source type is not supported
        """
        source_type = self.data_source.source_type

        if source_type == "json_stream":
            # File stream source
            source = FileStreamSource(
                self.spark,
                self.data_source,
                self.schema
            )
            return source.read_stream()

        elif source_type == "kafka_topic":
            # Kafka source
            source = KafkaSource(
                self.spark,
                self.data_source,
                self.schema
            )
            return source.read_stream()

        else:
            raise ValueError(
                f"Unsupported streaming source type: {source_type}. "
                f"Supported: json_stream, kafka_topic"
            )

    def _validate_stream(self, stream_df: DataFrame) -> DataFrame:
        """
        Apply validation rules to streaming DataFrame.

        Args:
            stream_df: Input streaming DataFrame

        Returns:
            DataFrame with validation results
        """
        # Lazy import: Avoid loading validation module until pipeline starts
        # Reduces initial import overhead and allows pipeline to be imported without validation dependencies
        from src.streaming.validation import apply_validation_to_stream

        logger.info("Applying validation to streaming DataFrame")

        # T079: Check for schema evolution
        if self.schema:
            current_schema = stream_df.schema
            if current_schema != self.schema:
                logger.warning(
                    "Schema evolution detected - schema differs from baseline. "
                    "This will be handled in schema evolution module."
                )
                # In production, would call schema evolution manager here

        # Apply validation using the validation engine
        validated_df = apply_validation_to_stream(stream_df, self.validation_engine)

        logger.info("Validation successfully integrated into stream")

        return validated_df

    def _route_records(self, validated_df: DataFrame) -> Dict[str, DataFrame]:
        """
        Route records to warehouse or quarantine based on validation status.

        Args:
            validated_df: DataFrame with validation_status column

        Returns:
            Dictionary with 'valid' and 'invalid' DataFrames
        """
        # Split stream based on validation status
        valid_df = validated_df.filter("validation_status = 'valid'")
        invalid_df = validated_df.filter("validation_status IN ('invalid', 'manual_review')")

        logger.info("Stream split into valid and invalid branches")

        return {
            "valid": valid_df,
            "invalid": invalid_df,
        }

    def start(self) -> StreamingQuery:
        """
        Start the streaming pipeline.

        Returns:
            Active StreamingQuery handle

        Raises:
            RuntimeError: If pipeline is already running
        """
        if self.query and self.query.isActive:
            raise RuntimeError(
                f"Pipeline for {self.data_source.source_id} is already running"
            )

        logger.info(f"Starting streaming pipeline for {self.data_source.source_id}")

        try:
            # 1. Create source stream
            source_df = self._create_source()
            logger.info("Source stream created")

            # 2. Validate records
            validated_df = self._validate_stream(source_df)
            logger.info("Validation applied to stream")

            # 3. Route records
            routed = self._route_records(validated_df)
            valid_df = routed["valid"]
            invalid_df = routed["invalid"]

            # 4. Write to sinks using foreachBatch for transactional writes
            # Lazy import: Only load sink modules when actually creating streams
            # Allows pipeline to be imported without all sink dependencies
            from src.streaming.sinks.warehouse_sink import create_warehouse_sink_writer
            from src.streaming.sinks.quarantine_sink import create_quarantine_sink_writer

            # Create sink writers
            warehouse_writer = create_warehouse_sink_writer(self.metrics)
            quarantine_writer = create_quarantine_sink_writer(self.metrics)

            # Start both sink queries
            # Write valid records to warehouse
            warehouse_query = (
                valid_df.writeStream
                .foreachBatch(warehouse_writer)
                .option("checkpointLocation", f"{self.checkpoint_location}/warehouse")
                .trigger(processingTime=self.trigger_interval)
                .queryName(f"{self.data_source.source_id}_warehouse")
                .start()
            )

            # Write invalid records to quarantine
            quarantine_query = (
                invalid_df.writeStream
                .foreachBatch(quarantine_writer)
                .option("checkpointLocation", f"{self.checkpoint_location}/quarantine")
                .trigger(processingTime=self.trigger_interval)
                .queryName(f"{self.data_source.source_id}_quarantine")
                .start()
            )

            # Store warehouse query as primary
            query = warehouse_query
            self._quarantine_query = quarantine_query

            self.query = query

            logger.info(
                f"Streaming pipeline started successfully. "
                f"Query ID: {query.id}, Name: {query.name}"
            )

            return query

        except Exception as e:
            logger.error(f"Failed to start streaming pipeline: {e}", exc_info=True)
            raise

    def stop(self, timeout_seconds: Optional[int] = None) -> None:
        """
        Stop the streaming pipeline gracefully.

        Args:
            timeout_seconds: Maximum time to wait for graceful shutdown

        Raises:
            RuntimeError: If pipeline is not running
        """
        if not self.query:
            raise RuntimeError("Pipeline is not running")

        logger.info(f"Stopping streaming pipeline for {self.data_source.source_id}")

        try:
            # Stop both queries
            if hasattr(self, '_quarantine_query') and self._quarantine_query:
                self._quarantine_query.stop()
                logger.info("Quarantine query stopped")

            if timeout_seconds:
                self.query.awaitTermination(timeout_seconds)
            self.query.stop()

            logger.info("Streaming pipeline stopped successfully")

        except Exception as e:
            logger.error(f"Error stopping streaming pipeline: {e}", exc_info=True)
            raise

    def get_status(self) -> Dict[str, Any]:
        """
        Get current status of the streaming pipeline.

        Returns:
            Dictionary with pipeline status information
        """
        if not self.query:
            return {
                "status": "not_started",
                "source_id": self.data_source.source_id,
            }

        try:
            status = {
                "status": "active" if self.query.isActive else "stopped",
                "source_id": self.data_source.source_id,
                "query_id": str(self.query.id),
                "query_name": self.query.name,
                "checkpoint_location": self.checkpoint_location,
            }

            # Add recent progress if available
            if self.query.recentProgress:
                latest = self.query.recentProgress[-1]
                status["latest_progress"] = {
                    "batch_id": latest.get("batchId"),
                    "num_input_rows": latest.get("numInputRows"),
                    "input_rows_per_second": latest.get("inputRowsPerSecond"),
                    "processing_time_ms": latest.get("durationMs", {}).get("triggerExecution"),
                }

            return status

        except Exception as e:
            logger.error(f"Error getting pipeline status: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "source_id": self.data_source.source_id,
            }

    def await_termination(self, timeout_seconds: Optional[int] = None) -> None:
        """
        Wait for streaming query to terminate.

        Args:
            timeout_seconds: Maximum time to wait (None = wait forever)

        Raises:
            RuntimeError: If pipeline is not running
        """
        if not self.query:
            raise RuntimeError("Pipeline is not running")

        logger.info(
            f"Waiting for pipeline termination "
            f"(timeout: {timeout_seconds or 'infinite'} seconds)"
        )

        if timeout_seconds:
            self.query.awaitTermination(timeout_seconds)
        else:
            self.query.awaitTermination()


def create_streaming_pipeline(
    spark: SparkSession,
    data_source: DataSource,
    validation_rules_path: str,
    checkpoint_location: str,
    schema: Optional[StructType] = None,
    trigger_interval: str = "10 seconds",
) -> StreamingPipeline:
    """
    Factory function to create StreamingPipeline with configuration.

    Args:
        spark: Active Spark session
        data_source: DataSource configuration
        validation_rules_path: Path to validation rules YAML file
        checkpoint_location: Path for Spark checkpoints
        schema: Optional schema for source data
        trigger_interval: Micro-batch trigger interval

    Returns:
        Configured StreamingPipeline instance

    Example:
        >>> spark = SparkSession.builder.getOrCreate()
        >>> data_source = DataSource(
        ...     source_id="live_events",
        ...     source_type="json_stream",
        ...     connection_info={"stream_path": "/tmp/events"},
        ...     enabled=True
        ... )
        >>> pipeline = create_streaming_pipeline(
        ...     spark,
        ...     data_source,
        ...     validation_rules_path="config/validation_rules.yaml",
        ...     checkpoint_location="/tmp/checkpoints"
        ... )
        >>> query = pipeline.start()
    """
    # Lazy import: Configuration loading happens at runtime, not import time
    # Allows using this module without having config files present
    from src.core.rules.rule_config import load_validation_rules

    # Load validation rules
    rules = load_validation_rules(validation_rules_path)
    validation_engine = RuleEngine(rules)

    return StreamingPipeline(
        spark,
        data_source,
        validation_engine,
        checkpoint_location,
        schema,
        trigger_interval,
    )
