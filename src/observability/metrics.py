"""
Prometheus metrics collection for dirty-spark-pipeline

This module provides metrics instrumentation for monitoring
pipeline performance, data quality, and system health.
"""
import os
from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    CollectorRegistry,
    generate_latest,
    CONTENT_TYPE_LATEST,
)
from typing import Optional


# Global registry for metrics
REGISTRY = CollectorRegistry()


# =======================
# PIPELINE METRICS
# =======================

# Records processed counter
records_processed_total = Counter(
    name="pipeline_records_processed_total",
    documentation="Total number of records processed by the pipeline",
    labelnames=["source_id", "status"],  # status: valid, invalid, error
    registry=REGISTRY,
)

# Processing duration histogram
processing_duration_seconds = Histogram(
    name="pipeline_processing_duration_seconds",
    documentation="Time spent processing records in seconds",
    labelnames=["source_id", "mode"],  # mode: batch, stream
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0],
    registry=REGISTRY,
)

# Record processing latency (individual records)
record_processing_latency_seconds = Histogram(
    name="pipeline_record_processing_latency_seconds",
    documentation="Latency for processing individual records",
    labelnames=["source_id"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0],
    registry=REGISTRY,
)

# Throughput (records per second)
throughput_records_per_second = Gauge(
    name="pipeline_throughput_records_per_second",
    documentation="Current throughput in records per second",
    labelnames=["source_id", "mode"],
    registry=REGISTRY,
)

# =======================
# DATA QUALITY METRICS
# =======================

# Validation failures counter
validation_failures_total = Counter(
    name="pipeline_validation_failures_total",
    documentation="Total number of validation failures",
    labelnames=["source_id", "rule_type", "field_name"],
    registry=REGISTRY,
)

# Validation warnings counter
validation_warnings_total = Counter(
    name="pipeline_validation_warnings_total",
    documentation="Total number of validation warnings (non-blocking issues)",
    labelnames=["source_id", "rule_name"],
    registry=REGISTRY,
)

# Quarantine size gauge
quarantine_size = Gauge(
    name="pipeline_quarantine_size",
    documentation="Current number of records in quarantine",
    labelnames=["source_id"],
    registry=REGISTRY,
)

# =======================
# STREAMING METRICS (T086)
# =======================

# Streaming latency histogram
streaming_latency_seconds = Histogram(
    name="pipeline_streaming_latency_seconds",
    documentation="End-to-end latency for streaming records (source to sink)",
    labelnames=["source_id"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
    registry=REGISTRY,
)

# Streaming batch size
streaming_batch_size = Histogram(
    name="pipeline_streaming_batch_size",
    documentation="Number of records per streaming micro-batch",
    labelnames=["source_id"],
    buckets=[1, 10, 50, 100, 500, 1000, 5000, 10000],
    registry=REGISTRY,
)

# Streaming backpressure indicator
streaming_backpressure = Gauge(
    name="pipeline_streaming_backpressure",
    documentation="Backpressure detected (1) or not (0)",
    labelnames=["source_id"],
    registry=REGISTRY,
)

# Input rate (records/sec from source)
streaming_input_rate = Gauge(
    name="pipeline_streaming_input_rate",
    documentation="Input rate in records per second",
    labelnames=["source_id"],
    registry=REGISTRY,
)

# Processing rate (records/sec processed)
streaming_processing_rate = Gauge(
    name="pipeline_streaming_processing_rate",
    documentation="Processing rate in records per second",
    labelnames=["source_id"],
    registry=REGISTRY,
)

# Schema evolution events
schema_evolution_total = Counter(
    name="pipeline_schema_evolution_total",
    documentation="Total number of schema evolution events",
    labelnames=["source_id", "change_type"],  # change_type: new_field, type_change, etc.
    registry=REGISTRY,
)

# Type coercion successes and failures
type_coercion_total = Counter(
    name="pipeline_type_coercion_total",
    documentation="Total number of type coercion attempts",
    labelnames=["source_id", "from_type", "to_type", "status"],  # status: success, failure
    registry=REGISTRY,
)

# =======================
# BATCH PROCESSING METRICS
# =======================

# Batch size
batch_size = Histogram(
    name="pipeline_batch_size_records",
    documentation="Number of records in each batch",
    labelnames=["source_id"],
    buckets=[100, 500, 1000, 5000, 10000, 50000, 100000, 500000],
    registry=REGISTRY,
)

# Batches processed counter
batches_processed_total = Counter(
    name="pipeline_batches_processed_total",
    documentation="Total number of batches processed",
    labelnames=["source_id", "status"],  # status: success, failure
    registry=REGISTRY,
)

# =======================
# STREAMING METRICS
# =======================

# Stream processing lag (milliseconds)
stream_processing_lag_milliseconds = Gauge(
    name="pipeline_stream_processing_lag_milliseconds",
    documentation="Processing lag for streaming pipeline in milliseconds",
    labelnames=["source_id", "topic"],
    registry=REGISTRY,
)

# Stream backpressure indicator
stream_backpressure = Gauge(
    name="pipeline_stream_backpressure",
    documentation="Backpressure indicator (1 if backpressured, 0 otherwise)",
    labelnames=["source_id"],
    registry=REGISTRY,
)

# Streaming query status
stream_query_running = Gauge(
    name="pipeline_stream_query_running",
    documentation="Whether streaming query is running (1 for running, 0 for stopped)",
    labelnames=["query_id"],
    registry=REGISTRY,
)

# =======================
# WAREHOUSE METRICS
# =======================

# Warehouse writes counter
warehouse_writes_total = Counter(
    name="pipeline_warehouse_writes_total",
    documentation="Total number of records written to warehouse",
    labelnames=["source_id", "operation"],  # operation: insert, upsert
    registry=REGISTRY,
)

# Warehouse write duration
warehouse_write_duration_seconds = Histogram(
    name="pipeline_warehouse_write_duration_seconds",
    documentation="Time spent writing to warehouse in seconds",
    labelnames=["source_id", "operation"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0],
    registry=REGISTRY,
)

# Upsert conflicts (records updated vs inserted)
upsert_conflicts_total = Counter(
    name="pipeline_upsert_conflicts_total",
    documentation="Total number of upsert conflicts (updates)",
    labelnames=["source_id"],
    registry=REGISTRY,
)

# =======================
# SYSTEM METRICS
# =======================

# Database connection pool usage
db_connection_pool_size = Gauge(
    name="pipeline_db_connection_pool_size",
    documentation="Current size of database connection pool",
    labelnames=["pool_name"],
    registry=REGISTRY,
)

# Database connection pool waiting
db_connection_pool_waiting = Gauge(
    name="pipeline_db_connection_pool_waiting",
    documentation="Number of threads waiting for database connection",
    labelnames=["pool_name"],
    registry=REGISTRY,
)

# Memory usage (if needed)
memory_usage_bytes = Gauge(
    name="pipeline_memory_usage_bytes",
    documentation="Memory usage in bytes",
    labelnames=["component"],
    registry=REGISTRY,
)

# =======================
# ERROR METRICS
# =======================

# Errors counter
errors_total = Counter(
    name="pipeline_errors_total",
    documentation="Total number of errors",
    labelnames=["source_id", "error_type", "component"],
    registry=REGISTRY,
)

# Retries counter
retries_total = Counter(
    name="pipeline_retries_total",
    documentation="Total number of retry attempts",
    labelnames=["source_id", "operation", "status"],  # status: success, failure
    registry=REGISTRY,
)


# =======================
# HELPER FUNCTIONS
# =======================

def generate_metrics() -> bytes:
    """
    Generate Prometheus metrics in text format

    Returns:
        Metrics in Prometheus text format
    """
    return generate_latest(REGISTRY)


def get_content_type() -> str:
    """
    Get content type for Prometheus metrics

    Returns:
        Content type string
    """
    return CONTENT_TYPE_LATEST


def start_metrics_server(port: Optional[int] = None) -> None:
    """
    Start HTTP server for Prometheus metrics

    Args:
        port: Port to listen on (defaults to env var METRICS_PORT or 8000)
    """
    # Lazy import: Prometheus HTTP server only needed when metrics endpoint is enabled
    # Avoids port binding on import and allows using metrics without HTTP server
    from prometheus_client import start_http_server

    metrics_port = port or int(os.getenv("METRICS_PORT", "8000"))
    start_http_server(metrics_port, registry=REGISTRY)


# =======================
# CONTEXT MANAGERS
# =======================

class track_duration:
    """
    Context manager for tracking operation duration

    Usage:
        with track_duration(processing_duration_seconds, source_id="csv1", mode="batch"):
            # do work
            pass
    """

    def __init__(self, histogram: Histogram, **labels):
        """
        Initialize duration tracker

        Args:
            histogram: Prometheus Histogram metric
            **labels: Label values for the metric
        """
        self.histogram = histogram
        self.labels = labels
        self.timer = None

    def __enter__(self):
        """Start timer"""
        self.timer = self.histogram.labels(**self.labels).time()
        self.timer.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop timer"""
        self.timer.__exit__(exc_type, exc_val, exc_tb)
        return False


def increment_counter(counter: Counter, value: float = 1.0, **labels) -> None:
    """
    Increment a counter metric

    Args:
        counter: Prometheus Counter metric
        value: Amount to increment (default: 1.0)
        **labels: Label values for the metric
    """
    counter.labels(**labels).inc(value)


def set_gauge(gauge: Gauge, value: float, **labels) -> None:
    """
    Set a gauge metric value

    Args:
        gauge: Prometheus Gauge metric
        value: Value to set
        **labels: Label values for the metric
    """
    gauge.labels(**labels).set(value)


def observe_histogram(histogram: Histogram, value: float, **labels) -> None:
    """
    Observe a value in a histogram metric

    Args:
        histogram: Prometheus Histogram metric
        value: Value to observe
        **labels: Label values for the metric
    """
    histogram.labels(**labels).observe(value)


# =======================
# BATCH-SPECIFIC HELPERS
# =======================

def record_batch_processing(
    source_id: str,
    total_records: int,
    valid_records: int,
    invalid_records: int,
    duplicate_records: int,
    duration_seconds: float
) -> None:
    """
    Record batch processing metrics.

    Args:
        source_id: Data source ID
        total_records: Total records processed
        valid_records: Valid records written to warehouse
        invalid_records: Invalid records quarantined
        duplicate_records: Duplicate records removed
        duration_seconds: Processing duration in seconds
    """
    # Record counts
    increment_counter(records_processed_total, valid_records, source_id=source_id, status="valid")
    increment_counter(records_processed_total, invalid_records, source_id=source_id, status="invalid")

    # Record batch size
    observe_histogram(batch_size, total_records, source_id=source_id)

    # Record duration
    observe_histogram(processing_duration_seconds, duration_seconds, source_id=source_id, mode="batch")

    # Calculate and record throughput
    if duration_seconds > 0:
        throughput = total_records / duration_seconds
        set_gauge(throughput_records_per_second, throughput, source_id=source_id, mode="batch")

    # Record duplicates
    if duplicate_records > 0:
        increment_counter(batches_processed_total, 1, source_id=source_id, status="has_duplicates")

    # Mark batch as completed
    increment_counter(batches_processed_total, 1, source_id=source_id, status="success")


def record_validation_failure(source_id: str, rule_type: str, field_name: str) -> None:
    """
    Record a validation failure.

    Args:
        source_id: Data source ID
        rule_type: Type of validation rule that failed
        field_name: Name of field that failed validation
    """
    increment_counter(validation_failures_total, 1, source_id=source_id, rule_type=rule_type, field_name=field_name)


# =======================
# METRICS COLLECTOR CLASS
# =======================

class MetricsCollector:
    """
    Metrics collector for streaming pipeline components.

    This class provides a unified interface for collecting metrics
    from various pipeline components.
    """

    def __init__(self):
        """Initialize metrics collector."""
        pass

    def record_batch_processed(
        self,
        source_id: str,
        record_count: int,
        success: bool = True,
        duration_seconds: float = 0.0
    ) -> None:
        """
        Record a batch processing event.

        Args:
            source_id: Data source ID
            record_count: Number of records in the batch
            success: Whether the batch was processed successfully
            duration_seconds: Time taken to process the batch
        """
        status = "success" if success else "failure"
        increment_counter(batches_processed_total, 1, source_id=source_id, status=status)
        if record_count > 0:
            observe_histogram(batch_size, record_count, source_id=source_id)
        if duration_seconds > 0:
            observe_histogram(processing_duration_seconds, duration_seconds, source_id=source_id, mode="stream")

    def record_quarantine_batch(
        self,
        source_id: str,
        record_count: int,
        duration_seconds: float = 0.0
    ) -> None:
        """
        Record a quarantine batch write event.

        Args:
            source_id: Data source ID
            record_count: Number of records quarantined
            duration_seconds: Time taken to write the batch
        """
        if record_count > 0:
            increment_counter(records_processed_total, record_count, source_id=source_id, status="invalid")
            set_gauge(quarantine_size, record_count, source_id=source_id)
        if duration_seconds > 0:
            observe_histogram(processing_duration_seconds, duration_seconds, source_id=source_id, mode="quarantine")
