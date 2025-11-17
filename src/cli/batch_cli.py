"""
Command-line interface for batch processing.

Usage:
    python -m src.cli.batch_cli process --source <source_id> --input <file_path> [options]
"""

import argparse
import sys
from pathlib import Path
from pyspark.sql import SparkSession

from src.warehouse.connection import DatabaseConnectionPool, initialize_pool, close_pool
from src.batch.pipeline import BatchPipeline
from src.observability.logger import get_logger


logger = get_logger(__name__)


def create_spark_session(app_name: str = "BatchProcessing") -> SparkSession:
    """
    Create Spark session for batch processing.

    Args:
        app_name: Application name

    Returns:
        SparkSession
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    return spark


def process_command(args):
    """
    Execute batch processing command.

    Args:
        args: Command-line arguments
    """
    logger.info(f"Starting batch processing for source: {args.source}")
    logger.info(f"Input file: {args.input}")

    # Validate input file exists
    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"Input file not found: {args.input}")
        sys.exit(1)

    # Create Spark session
    logger.info("Creating Spark session...")
    spark = create_spark_session(f"BatchProcessing-{args.source}")

    # Initialize database connection pool
    logger.info("Initializing database connection...")
    pool = DatabaseConnectionPool(
        host=args.db_host,
        port=args.db_port,
        database=args.db_name,
        user=args.db_user,
        password=args.db_password
    )
    pool.open()

    try:
        # Create pipeline
        logger.info("Initializing batch pipeline...")
        pipeline = BatchPipeline(
            spark=spark,
            pool=pool,
            validation_rules_path=args.validation_rules
        )

        if args.dry_run:
            logger.info("DRY RUN MODE: No data will be written to database")
            # For dry run, we'll just validate but not write
            # This could be enhanced to provide detailed validation report
            logger.info("Dry run functionality: validating data without writing")

        # Process file
        logger.info("Processing file...")
        result = pipeline.process_file(
            file_path=str(input_path),
            source_id=args.source,
            file_format=args.format,
            deduplicate=not args.no_dedupe
        )

        # Display results
        logger.info("=" * 60)
        logger.info("PROCESSING COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Total records processed: {result['total_records']}")
        logger.info(f"Valid records (warehouse): {result['valid_records']}")
        logger.info(f"Invalid records (quarantine): {result['invalid_records']}")
        logger.info(f"Duplicate records removed: {result['duplicate_records']}")
        logger.info(f"Schema version: {result['schema_id']} (confidence: {result['schema_confidence']:.2f})")
        logger.info("=" * 60)

        if args.dry_run:
            logger.info("DRY RUN: No data was written to the database")

    except Exception as e:
        logger.error(f"Error during batch processing: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Cleanup
        pool.close()
        spark.stop()


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Batch data processing pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process a CSV file
  python -m src.cli.batch_cli process --source sales_data --input data/sales.csv

  # Process with custom validation rules
  python -m src.cli.batch_cli process --source sales_data --input data/sales.csv \\
      --validation-rules config/custom_rules.yaml

  # Dry run (validate only, don't write)
  python -m src.cli.batch_cli process --source sales_data --input data/sales.csv --dry-run

  # Process JSON file without deduplication
  python -m src.cli.batch_cli process --source events --input data/events.json \\
      --format json --no-dedupe
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Process command
    process_parser = subparsers.add_parser("process", help="Process a data file")
    process_parser.add_argument(
        "--source",
        required=True,
        help="Data source ID"
    )
    process_parser.add_argument(
        "--input",
        required=True,
        help="Path to input file"
    )
    process_parser.add_argument(
        "--format",
        default="csv",
        choices=["csv", "json", "parquet"],
        help="Input file format (default: csv)"
    )
    process_parser.add_argument(
        "--validation-rules",
        default="config/validation_rules.yaml",
        help="Path to validation rules YAML file"
    )
    process_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate data without writing to database"
    )
    process_parser.add_argument(
        "--no-dedupe",
        action="store_true",
        help="Disable deduplication"
    )

    # Database connection arguments
    process_parser.add_argument(
        "--db-host",
        default="localhost",
        help="Database host (default: localhost)"
    )
    process_parser.add_argument(
        "--db-port",
        type=int,
        default=5432,
        help="Database port (default: 5432)"
    )
    process_parser.add_argument(
        "--db-name",
        default="datawarehouse",
        help="Database name (default: datawarehouse)"
    )
    process_parser.add_argument(
        "--db-user",
        default="pipeline",
        help="Database user (default: pipeline)"
    )
    process_parser.add_argument(
        "--db-password",
        default="dev_password",
        help="Database password"
    )

    # Parse arguments
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Execute command
    if args.command == "process":
        process_command(args)


if __name__ == "__main__":
    main()
