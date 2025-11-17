"""
Batch processing pipeline orchestration.

Coordinates the flow: read → validate → deduplicate → route → write
"""

from typing import Tuple, Dict, Any, Optional
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, struct, array, lit, concat_ws
from pyspark.sql.types import ArrayType, StringType

from src.warehouse.connection import DatabaseConnectionPool
from src.core.schema import SchemaInferrer, SchemaRegistry
from src.core.rules import RuleEngine, RuleConfigLoader
from src.core.models import DataRecord, ValidationResult
from src.batch.readers import FileReader
from src.batch.writers import BatchWarehouseWriter, BatchQuarantineWriter
from src.observability.logger import get_logger


logger = get_logger(__name__)


class BatchPipeline:
    """
    Orchestrates batch data processing pipeline.

    Flow:
    1. Read data from file (CSV/JSON/Parquet)
    2. Infer or load schema
    3. Apply validation rules
    4. Deduplicate records
    5. Route valid records to warehouse
    6. Route invalid records to quarantine
    """

    def __init__(
        self,
        spark: SparkSession,
        pool: DatabaseConnectionPool,
        validation_rules_path: Optional[str] = None
    ):
        """
        Initialize batch pipeline.

        Args:
            spark: Active Spark session
            pool: Database connection pool
            validation_rules_path: Path to validation rules YAML file
        """
        self.spark = spark
        self.pool = pool

        # Initialize components
        self.file_reader = FileReader(spark)
        self.schema_inferrer = SchemaInferrer(spark)
        self.schema_registry = SchemaRegistry(pool, self.schema_inferrer)
        self.warehouse_writer = BatchWarehouseWriter(pool)
        self.quarantine_writer = BatchQuarantineWriter(pool)

        # Load validation rules
        self.validation_rules_path = validation_rules_path or "config/validation_rules.yaml"
        if Path(self.validation_rules_path).exists():
            rule_loader = RuleConfigLoader(self.validation_rules_path)
            rules = rule_loader.load_rules()
            self.rule_engine = RuleEngine(rules)
        else:
            logger.warning(f"Validation rules file not found: {self.validation_rules_path}")
            self.rule_engine = RuleEngine([])

    def process_file(
        self,
        file_path: str,
        source_id: str,
        file_format: str = "csv",
        deduplicate: bool = True,
        **read_options
    ) -> Dict[str, Any]:
        """
        Process a file through the complete pipeline.

        Args:
            file_path: Path to input file
            source_id: Data source ID
            file_format: File format (csv, json, parquet)
            deduplicate: Whether to deduplicate records
            **read_options: Additional read options

        Returns:
            Dictionary with processing results:
            - total_records: Total records read
            - valid_records: Records written to warehouse
            - invalid_records: Records quarantined
            - duplicate_records: Duplicates removed
            - schema_id: Schema version ID
        """
        logger.info(f"Starting batch processing for file: {file_path}")

        # Step 1: Read file
        logger.info(f"Reading {file_format} file...")
        df = self.file_reader.read(file_path, file_format=file_format, **read_options)
        total_records = df.count()
        logger.info(f"Read {total_records} records")

        # Step 2: Get or infer schema
        logger.info("Getting schema...")
        schema, schema_id, confidence = self.schema_registry.get_or_infer_schema(
            source_id=source_id,
            sample_df=df
        )
        logger.info(f"Using schema version {schema_id} (confidence: {confidence})")

        # Step 3: Deduplicate if enabled
        duplicate_count = 0
        if deduplicate:
            logger.info("Deduplicating records...")
            df, duplicate_count = self._deduplicate(df)
            logger.info(f"Removed {duplicate_count} duplicate records")

        # Step 4: Validate records
        logger.info("Validating records...")
        valid_df, invalid_df = self._validate_dataframe(df, source_id)
        valid_count = valid_df.count() if valid_df else 0
        invalid_count = invalid_df.count() if invalid_df else 0
        logger.info(f"Validation complete: {valid_count} valid, {invalid_count} invalid")

        # Step 5: Write valid records to warehouse
        warehouse_count = 0
        if valid_df and valid_count > 0:
            logger.info("Writing valid records to warehouse...")
            warehouse_count = self.warehouse_writer.write_dataframe(
                valid_df,
                source_id=source_id,
                schema_version_id=schema_id
            )
            logger.info(f"Wrote {warehouse_count} records to warehouse")

        # Step 6: Write invalid records to quarantine
        quarantine_count = 0
        if invalid_df and invalid_count > 0:
            logger.info("Writing invalid records to quarantine...")
            quarantine_count = self.quarantine_writer.write_dataframe(
                invalid_df,
                source_id=source_id
            )
            logger.info(f"Wrote {quarantine_count} records to quarantine")

        logger.info("Batch processing complete")

        return {
            "total_records": total_records,
            "valid_records": warehouse_count,
            "invalid_records": quarantine_count,
            "duplicate_records": duplicate_count,
            "schema_id": schema_id,
            "schema_confidence": confidence
        }

    def _deduplicate(self, df: DataFrame) -> Tuple[DataFrame, int]:
        """
        Remove duplicate records based on record_id or transaction_id.

        Args:
            df: Input DataFrame

        Returns:
            Tuple of (deduplicated_df, duplicate_count)
        """
        original_count = df.count()

        # Determine ID column
        id_column = None
        for col_name in ["transaction_id", "record_id", "id"]:
            if col_name in df.columns:
                id_column = col_name
                break

        if not id_column:
            # No ID column found, return as-is
            return df, 0

        # Remove duplicates, keeping first occurrence
        deduped_df = df.dropDuplicates([id_column])
        final_count = deduped_df.count()
        duplicate_count = original_count - final_count

        return deduped_df, duplicate_count

    def _validate_dataframe(
        self,
        df: DataFrame,
        source_id: str
    ) -> Tuple[Optional[DataFrame], Optional[DataFrame]]:
        """
        Validate DataFrame records using rule engine.

        Args:
            df: Input DataFrame
            source_id: Data source ID

        Returns:
            Tuple of (valid_df, invalid_df)
        """
        if not self.rule_engine or not self.rule_engine.validators:
            # No validation rules, treat all as valid
            return df, None

        # Convert DataFrame to list of DataRecord models for validation
        # Note: This is a simplified approach. For very large datasets,
        # consider implementing validation as Spark UDFs

        rows = df.collect()
        valid_records = []
        invalid_records = []

        for row in rows:
            record_dict = row.asDict()

            # Create DataRecord
            record_id = record_dict.get("transaction_id") or record_dict.get("record_id") or "unknown"
            data_record = DataRecord(
                record_id=str(record_id),
                source_id=source_id,
                raw_payload=record_dict
            )

            # Validate
            validation_result = self.rule_engine.validate_record(data_record)

            if validation_result.passed:
                valid_records.append(record_dict)
            else:
                # Add error information to record
                invalid_record = record_dict.copy()
                invalid_record["_record_id"] = record_id
                invalid_record["_failed_rules"] = validation_result.failed_rules
                invalid_record["_error_messages"] = [
                    f"Rule '{rule}' failed" for rule in validation_result.failed_rules
                ]
                invalid_records.append(invalid_record)

        # Convert back to DataFrames
        valid_df = None
        if valid_records:
            valid_df = self.spark.createDataFrame(valid_records)

        invalid_df = None
        if invalid_records:
            invalid_df = self.spark.createDataFrame(invalid_records)

        return valid_df, invalid_df
