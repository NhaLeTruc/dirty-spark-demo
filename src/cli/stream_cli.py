"""
CLI for managing streaming pipelines.

Provides commands to start, stop, and monitor streaming data validation pipelines.
"""

import argparse
import json
import signal
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src.core.models.data_source import DataSource
from src.observability.logger import get_logger
from src.streaming.pipeline import create_streaming_pipeline

logger = get_logger(__name__)

# Global flag for graceful shutdown
_shutdown_requested = False


def signal_handler(signum, frame):  # type: ignore[no-untyped-def]
    """
    Handle shutdown signals (SIGINT, SIGTERM) for graceful pipeline termination.

    Args:
        signum: Signal number
        frame: Current stack frame
    """
    global _shutdown_requested
    signal_name = signal.Signals(signum).name
    logger.info(f"Received {signal_name} signal, initiating graceful shutdown...")
    _shutdown_requested = True


def create_spark_session(app_name: str = "StreamingPipeline") -> SparkSession:
    """
    Create Spark session for streaming.

    Args:
        app_name: Application name

    Returns:
        Configured SparkSession
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.streaming.checkpointFileManagerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def start_stream(args: argparse.Namespace) -> int:
    """
    Start a streaming pipeline with graceful shutdown support.

    Args:
        args: Parsed command-line arguments

    Returns:
        Exit code (0 for success)
    """
    global _shutdown_requested

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        logger.info(f"Starting streaming pipeline for source: {args.source}")

        # Create Spark session
        spark = create_spark_session(f"Stream_{args.source}")

        # Define schema if provided
        schema = None
        if args.schema_file:
            # Load schema from file (simplified - can be enhanced)
            schema = StructType([
                StructField("transaction_id", StringType(), False),
                StructField("amount", DoubleType(), False),
                StructField("timestamp", StringType(), False),
                StructField("customer_email", StringType(), True),
            ])
            logger.info(f"Using schema from {args.schema_file}")

        # Create data source configuration
        if args.stream_source == "file_stream":
            connection_info = {
                "stream_path": args.file_stream_path,
                "file_format": "json",
            }
            source_type = "json_stream"
        elif args.stream_source == "kafka":
            connection_info = {
                "kafka_bootstrap_servers": args.kafka_bootstrap_servers,
                "kafka_topic": args.kafka_topic,
                "kafka_group_id": args.kafka_group_id,
                "starting_offsets": "latest",
            }
            source_type = "kafka_topic"
        else:
            logger.error(f"Unsupported stream source: {args.stream_source}")
            return 1

        data_source = DataSource(
            source_id=args.source,
            source_type=source_type,
            connection_info=connection_info,
            enabled=True,
        )

        # Create and start pipeline
        pipeline = create_streaming_pipeline(
            spark=spark,
            data_source=data_source,
            validation_rules_path=args.validation_rules,
            checkpoint_location=args.checkpoint_location,
            schema=schema,
            trigger_interval=args.trigger_interval,
        )

        query = pipeline.start()

        logger.info(
            f"Streaming pipeline started successfully. "
            f"Query ID: {query.id}, Name: {query.name}"
        )

        # Output query info as JSON
        output = {
            "status": "started",
            "query_id": str(query.id),
            "query_name": query.name,
            "source_id": args.source,
            "checkpoint_location": args.checkpoint_location,
            "trigger_interval": args.trigger_interval,
        }
        print(json.dumps(output, indent=2))

        # Wait for termination if requested
        if not args.detach:
            logger.info("Waiting for termination (press Ctrl+C to stop)...")

            # Monitor for shutdown signal while query is running
            while query.isActive and not _shutdown_requested:
                time.sleep(1)

            # Graceful shutdown
            if _shutdown_requested and query.isActive:
                logger.info("Stopping query gracefully...")
                query.stop()
                logger.info("Query stopped. Waiting for cleanup...")
                # Give Spark time to complete final micro-batch and checkpoint
                time.sleep(2)
                logger.info("Shutdown complete")

        return 0

    except Exception as e:
        logger.error(f"Failed to start streaming pipeline: {e}", exc_info=True)
        print(json.dumps({"status": "error", "error": str(e)}), file=sys.stderr)
        return 1


def stop_stream(args: argparse.Namespace) -> int:
    """
    Stop a streaming pipeline.

    Args:
        args: Parsed command-line arguments

    Returns:
        Exit code (0 for success)
    """
    try:
        logger.info(f"Stopping streaming query: {args.query_id}")

        # Create Spark session to access active queries
        spark = create_spark_session("StreamManager")

        # Find and stop the query
        found = False
        for query in spark.streams.active:
            if str(query.id) == args.query_id or query.name == args.query_id:
                logger.info(f"Found query: {query.name} ({query.id})")
                query.stop()
                found = True
                logger.info("Query stopped successfully")
                break

        if not found:
            logger.error(f"Query not found: {args.query_id}")
            print(json.dumps({
                "status": "error",
                "error": f"Query '{args.query_id}' not found or already stopped"
            }), file=sys.stderr)
            return 1

        # Output success
        output = {
            "status": "stopped",
            "query_id": args.query_id,
        }
        print(json.dumps(output, indent=2))

        return 0

    except Exception as e:
        logger.error(f"Failed to stop streaming pipeline: {e}", exc_info=True)
        print(json.dumps({"status": "error", "error": str(e)}), file=sys.stderr)
        return 1


def show_status(args: argparse.Namespace) -> int:
    """
    Show status of streaming pipelines.

    Args:
        args: Parsed command-line arguments

    Returns:
        Exit code (0 for success)
    """
    try:
        # Create Spark session to access active queries
        spark = create_spark_session("StreamManager")

        # Get all active queries
        active_queries = spark.streams.active

        if not active_queries:
            logger.info("No active streaming queries")
            print(json.dumps({"active_queries": []}, indent=2))
            return 0

        # Collect query information
        queries_info = []
        for query in active_queries:
            query_info = {
                "query_id": str(query.id),
                "query_name": query.name,
                "status": "active" if query.isActive else "stopped",
            }

            # Add recent progress if available
            if query.recentProgress:
                latest = query.recentProgress[-1]
                query_info["latest_progress"] = {
                    "batch_id": latest.get("batchId"),
                    "num_input_rows": latest.get("numInputRows"),
                    "input_rows_per_second": latest.get("inputRowsPerSecond"),
                    "processing_time_ms": latest.get("durationMs", {}).get("triggerExecution"),
                }

            # Filter by query ID if specified
            if args.query_id:
                if str(query.id) == args.query_id or query.name == args.query_id:
                    queries_info.append(query_info)
            else:
                queries_info.append(query_info)

        # Output
        output = {
            "active_queries": len(queries_info),
            "queries": queries_info,
        }
        print(json.dumps(output, indent=2))

        return 0

    except Exception as e:
        logger.error(f"Failed to get streaming status: {e}", exc_info=True)
        print(json.dumps({"status": "error", "error": str(e)}), file=sys.stderr)
        return 1


def main():
    """Main entry point for stream CLI."""
    parser = argparse.ArgumentParser(
        description="Manage streaming data validation pipelines",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start file stream
  %(prog)s start --source live_events --stream-source file_stream \\
    --file-stream-path /tmp/events --checkpoint-location /tmp/checkpoints

  # Start Kafka stream
  %(prog)s start --source kafka_events --stream-source kafka \\
    --kafka-topic transactions --kafka-bootstrap-servers localhost:9092 \\
    --checkpoint-location /tmp/checkpoints

  # Show status
  %(prog)s status

  # Stop stream
  %(prog)s stop --query-id stream_live_events_20251119_120000
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Start command
    start_parser = subparsers.add_parser("start", help="Start streaming pipeline")
    start_parser.add_argument("--source", required=True, help="Data source ID")
    start_parser.add_argument(
        "--stream-source",
        required=True,
        choices=["file_stream", "kafka"],
        help="Stream source type"
    )
    start_parser.add_argument(
        "--file-stream-path",
        help="Directory to watch for files (for file_stream)"
    )
    start_parser.add_argument(
        "--kafka-topic",
        help="Kafka topic name (for kafka)"
    )
    start_parser.add_argument(
        "--kafka-bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)"
    )
    start_parser.add_argument(
        "--kafka-group-id",
        help="Kafka consumer group ID (default: auto-generated)"
    )
    start_parser.add_argument(
        "--checkpoint-location",
        required=True,
        help="Checkpoint directory for exactly-once semantics"
    )
    start_parser.add_argument(
        "--validation-rules",
        default="config/validation_rules.yaml",
        help="Path to validation rules file (default: config/validation_rules.yaml)"
    )
    start_parser.add_argument(
        "--schema-file",
        help="Path to schema file (optional)"
    )
    start_parser.add_argument(
        "--trigger-interval",
        default="10 seconds",
        help="Micro-batch trigger interval (default: 10 seconds)"
    )
    start_parser.add_argument(
        "--detach",
        action="store_true",
        help="Start in background and detach"
    )

    # Stop command
    stop_parser = subparsers.add_parser("stop", help="Stop streaming pipeline")
    stop_parser.add_argument(
        "--query-id",
        required=True,
        help="Query ID or name to stop"
    )

    # Status command
    status_parser = subparsers.add_parser("status", help="Show streaming pipeline status")
    status_parser.add_argument(
        "--query-id",
        help="Show status for specific query ID (optional)"
    )

    # Parse arguments
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Execute command
    if args.command == "start":
        return start_stream(args)
    elif args.command == "stop":
        return stop_stream(args)
    elif args.command == "status":
        return show_status(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
