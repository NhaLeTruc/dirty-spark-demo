"""
Batch quarantine writer for invalid records.

Writes invalid records to quarantine_record table with detailed error messages.
"""

from typing import List, Dict, Any
from pyspark.sql import DataFrame
from src.warehouse.connection import DatabaseConnectionPool
from src.warehouse.upsert import QuarantineWriter as UpsertQuarantineWriter
from src.core.models import QuarantineRecord


class BatchQuarantineWriter:
    """
    Writes invalid records from Spark DataFrame to quarantine in bulk.
    """

    def __init__(self, pool: DatabaseConnectionPool):
        """
        Initialize batch quarantine writer.

        Args:
            pool: Database connection pool
        """
        self.pool = pool
        self.quarantine_writer = UpsertQuarantineWriter(pool)

    def write_dataframe(
        self,
        df: DataFrame,
        source_id: str
    ) -> int:
        """
        Write Spark DataFrame with errors to quarantine.

        Expected DataFrame schema should include:
        - Original data fields
        - _failed_rules: Array of rule names that failed
        - _error_messages: Array of corresponding error messages
        - _record_id: Optional record ID

        Args:
            df: DataFrame with invalid records and error details
            source_id: Data source ID

        Returns:
            Number of records quarantined
        """
        # Collect DataFrame to list of rows
        rows = df.collect()

        if not rows:
            return 0

        # Convert rows to QuarantineRecord models
        quarantine_records = []
        for row in rows:
            # Convert row to dictionary
            record_dict = row.asDict()

            # Extract error information
            failed_rules = record_dict.pop("_failed_rules", [])
            error_messages = record_dict.pop("_error_messages", [])
            record_id = record_dict.pop("_record_id", None)

            # Ensure failed_rules and error_messages are lists
            if not isinstance(failed_rules, list):
                failed_rules = [str(failed_rules)]
            if not isinstance(error_messages, list):
                error_messages = [str(error_messages)]

            # If no failures recorded, add generic error
            if not failed_rules:
                failed_rules = ["unknown_validation_failure"]
                error_messages = ["Record failed validation but no specific rule was recorded"]

            # Create QuarantineRecord model
            quarantine_record = QuarantineRecord(
                record_id=str(record_id) if record_id else None,
                source_id=source_id,
                raw_payload=record_dict,
                failed_rules=failed_rules,
                error_messages=error_messages
            )
            quarantine_records.append(quarantine_record)

        # Use quarantine writer for bulk insert
        count = self.quarantine_writer.quarantine_batch(quarantine_records)

        return count

    def write_with_errors(
        self,
        records: List[Dict[str, Any]],
        source_id: str
    ) -> int:
        """
        Write records with validation errors to quarantine.

        This is a direct API for writing quarantine records without Spark DataFrame.

        Args:
            records: List of dictionaries with keys:
                     - raw_data: Original record data
                     - failed_rules: List of failed rule names
                     - error_messages: List of error messages
                     - record_id: Optional record ID
            source_id: Data source ID

        Returns:
            Number of records quarantined
        """
        quarantine_records = []

        for record in records:
            raw_data = record.get("raw_data", {})
            failed_rules = record.get("failed_rules", ["unknown_error"])
            error_messages = record.get("error_messages", ["Unknown validation error"])
            record_id = record.get("record_id")

            quarantine_record = QuarantineRecord(
                record_id=str(record_id) if record_id else None,
                source_id=source_id,
                raw_payload=raw_data,
                failed_rules=failed_rules,
                error_messages=error_messages
            )
            quarantine_records.append(quarantine_record)

        count = self.quarantine_writer.quarantine_batch(quarantine_records)
        return count
