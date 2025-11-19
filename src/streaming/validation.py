"""
Validation integration for Spark Structured Streaming.

Adapts the core RuleEngine to work with streaming DataFrames.
"""

import json
import logging
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, struct, to_json, udf
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from src.core.models.data_record import DataRecord
from src.core.rules.rule_engine import RuleEngine

logger = logging.getLogger(__name__)


def create_validation_udf(rule_engine: RuleEngine):
    """
    Create a Spark UDF that applies validation rules to records.

    Args:
        rule_engine: Configured RuleEngine

    Returns:
        Spark UDF that validates records and returns validation status
    """

    def validate_row(raw_payload_json: str, record_id: str, source_id: str) -> dict[str, Any]:
        """
        Validate a single row.

        Args:
            raw_payload_json: JSON string of record data
            record_id: Record identifier
            source_id: Source identifier

        Returns:
            Dictionary with validation results
        """
        try:
            # Parse JSON payload
            raw_payload = json.loads(raw_payload_json) if raw_payload_json else {}

            # Create DataRecord
            data_record = DataRecord(
                record_id=record_id or "unknown",
                source_id=source_id,
                raw_payload=raw_payload,
                validation_status="pending",
            )

            # Validate
            result = rule_engine.validate_record(data_record)

            # Return validation results
            return {
                "validation_status": "valid" if result.passed else "invalid",
                "passed_rules": result.passed_rules,
                "failed_rules": result.failed_rules,
                "transformations_applied": result.transformations_applied,
            }

        except Exception as e:
            logger.error(f"Validation error for record {record_id}: {e}")
            return {
                "validation_status": "invalid",
                "passed_rules": [],
                "failed_rules": ["validation_exception"],
                "transformations_applied": [],
            }

    # Define return schema
    validation_schema = StructType([
        StructField("validation_status", StringType(), False),
        StructField("passed_rules", ArrayType(StringType()), False),
        StructField("failed_rules", ArrayType(StringType()), False),
        StructField("transformations_applied", ArrayType(StringType()), False),
    ])

    # Create UDF
    return udf(validate_row, validation_schema)


def apply_validation_to_stream(
    stream_df: DataFrame,
    rule_engine: RuleEngine,
) -> DataFrame:
    """
    Apply validation rules to a streaming DataFrame.

    Args:
        stream_df: Input streaming DataFrame
        rule_engine: Configured RuleEngine

    Returns:
        DataFrame with validation results added
    """
    logger.info("Applying validation to streaming DataFrame")

    # Create validation UDF
    validation_udf = create_validation_udf(rule_engine)

    # Convert all columns to JSON string for validation
    # This assumes the DataFrame has been properly parsed from source
    stream_df = stream_df.withColumn(
        "_raw_json",
        to_json(struct(*[col(c) for c in stream_df.columns if c not in ["source_id", "record_id"]]))
    )

    # Apply validation UDF
    validated_df = stream_df.withColumn(
        "_validation_result",
        validation_udf(
            col("_raw_json"),
            col("record_id") if "record_id" in stream_df.columns else col("transaction_id"),  # Fallback
            col("source_id")
        )
    )

    # Expand validation result struct
    validated_df = validated_df.withColumn(
        "validation_status",
        col("_validation_result.validation_status")
    ).withColumn(
        "passed_rules",
        col("_validation_result.passed_rules")
    ).withColumn(
        "failed_rules",
        col("_validation_result.failed_rules")
    ).withColumn(
        "transformations_applied",
        col("_validation_result.transformations_applied")
    )

    # Drop temporary columns
    validated_df = validated_df.drop("_raw_json", "_validation_result")

    logger.info("Validation applied successfully")

    return validated_df


def apply_validation_foreach_batch(
    rule_engine: RuleEngine,
):
    """
    Create a foreachBatch function that applies validation to micro-batches.

    This is an alternative to UDF-based validation for better performance
    and more control over batch processing.

    Args:
        rule_engine: Configured RuleEngine

    Returns:
        Function compatible with foreachBatch
    """

    def validate_batch(batch_df: DataFrame, batch_id: int):
        """
        Validate a micro-batch.

        Args:
            batch_df: Batch DataFrame
            batch_id: Batch identifier
        """
        logger.info(f"Validating batch {batch_id} with {batch_df.count()} records")

        # Convert DataFrame rows to DataRecords and validate
        # This provides more flexibility than UDF approach
        validated_rows = []

        for row in batch_df.collect():
            try:
                # Extract data
                raw_payload = row.asDict()
                record_id = raw_payload.get("record_id") or raw_payload.get("transaction_id", "unknown")
                source_id = raw_payload.get("source_id", "unknown")

                # Create DataRecord
                data_record = DataRecord(
                    record_id=record_id,
                    source_id=source_id,
                    raw_payload=raw_payload,
                    validation_status="pending",
                )

                # Validate
                result = rule_engine.validate_record(data_record)

                # Store result
                validated_row = raw_payload.copy()
                validated_row["validation_status"] = "valid" if result.passed else "invalid"
                validated_row["passed_rules"] = result.passed_rules
                validated_row["failed_rules"] = result.failed_rules
                validated_row["transformations_applied"] = result.transformations_applied

                validated_rows.append(validated_row)

            except Exception as e:
                logger.error(f"Error validating record in batch {batch_id}: {e}")
                # Add record with error status
                validated_row = raw_payload.copy() if 'raw_payload' in locals() else {}
                validated_row["validation_status"] = "invalid"
                validated_row["failed_rules"] = ["validation_exception"]
                validated_rows.append(validated_row)

        logger.info(
            f"Batch {batch_id} validation complete: "
            f"{sum(1 for r in validated_rows if r.get('validation_status') == 'valid')} valid, "
            f"{sum(1 for r in validated_rows if r.get('validation_status') != 'valid')} invalid"
        )

        return validated_rows

    return validate_batch


class StreamValidationHandler:
    """
    Handler for streaming validation with metrics and error handling.
    """

    def __init__(self, rule_engine: RuleEngine):
        """
        Initialize validation handler.

        Args:
            rule_engine: Configured RuleEngine
        """
        self.rule_engine = rule_engine
        self.validation_count = 0
        self.validation_errors = 0

    def validate_stream_batch(self, batch_df: DataFrame, batch_id: int) -> DataFrame:
        """
        Validate a streaming batch with error handling and metrics.

        Args:
            batch_df: Batch DataFrame
            batch_id: Batch identifier

        Returns:
            Validated DataFrame
        """
        try:
            logger.info(f"Processing batch {batch_id}")

            # Apply validation
            validated_df = apply_validation_to_stream(batch_df, self.rule_engine)

            # Update metrics
            self.validation_count += 1

            return validated_df

        except Exception as e:
            logger.error(f"Error in batch {batch_id}: {e}", exc_info=True)
            self.validation_errors += 1
            raise

    def get_metrics(self) -> dict[str, int]:
        """
        Get validation metrics.

        Returns:
            Dictionary of metrics
        """
        return {
            "batches_validated": self.validation_count,
            "validation_errors": self.validation_errors,
        }
