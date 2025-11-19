"""
Quarantine sink for streaming invalid records.

Writes invalid records to PostgreSQL quarantine table using foreachBatch.
"""

import logging
from typing import List, Dict, Any
import json

from pyspark.sql import DataFrame
from psycopg.rows import dict_row

from src.warehouse.connection import get_connection
from src.observability.metrics import MetricsCollector

logger = logging.getLogger(__name__)


class QuarantineSink:
    """
    Streaming sink for writing invalid records to quarantine.

    Uses foreachBatch to write micro-batches transactionally.
    """

    def __init__(self, metrics: MetricsCollector):
        """
        Initialize quarantine sink.

        Args:
            metrics: Metrics collector for tracking writes
        """
        self.metrics = metrics
        self.total_records = 0
        self.total_batches = 0

    def write_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """
        Write a micro-batch to quarantine.

        Args:
            batch_df: Batch DataFrame with invalid records
            batch_id: Batch identifier

        Raises:
            Exception: If batch write fails
        """
        try:
            logger.info(f"Writing batch {batch_id} to quarantine")

            # Convert DataFrame to list of dicts
            records = batch_df.collect()

            if not records:
                logger.info(f"Batch {batch_id} is empty, skipping")
                return

            # Prepare quarantine records
            quarantine_records = []
            for row in records:
                row_dict = row.asDict()

                # Extract required fields
                record_id = row_dict.get("record_id") or row_dict.get("transaction_id")
                source_id = row_dict.get("source_id", "unknown")
                failed_rules = row_dict.get("failed_rules", [])

                # Generate error messages from failed rules
                error_messages = []
                for rule in failed_rules:
                    error_messages.append(f"Validation failed: {rule}")

                # Raw payload (entire record)
                raw_payload = {
                    k: v for k, v in row_dict.items()
                    if k not in ["validation_status", "passed_rules",
                                 "failed_rules", "transformations_applied"]
                }

                quarantine_records.append({
                    "record_id": record_id,
                    "source_id": source_id,
                    "raw_payload": raw_payload,
                    "failed_rules": failed_rules if failed_rules else ["unknown_error"],
                    "error_messages": error_messages if error_messages else ["Validation failed"],
                })

            # Write to database
            conn = get_connection()
            try:
                cursor = conn.cursor()

                for record in quarantine_records:
                    cursor.execute(
                        """
                        INSERT INTO quarantine_record (
                            record_id,
                            source_id,
                            raw_payload,
                            failed_rules,
                            error_messages,
                            quarantined_at,
                            reviewed,
                            reprocess_requested
                        ) VALUES (
                            %s, %s, %s, %s, %s, NOW(), FALSE, FALSE
                        )
                        """,
                        (
                            record["record_id"],
                            record["source_id"],
                            json.dumps(record["raw_payload"]),
                            record["failed_rules"],
                            record["error_messages"],
                        )
                    )

                conn.commit()

                # Update metrics
                self.total_records += len(quarantine_records)
                self.total_batches += 1

                logger.info(
                    f"Batch {batch_id} quarantined successfully: "
                    f"{len(quarantine_records)} records"
                )

                # Update Prometheus metrics
                self.metrics.record_quarantine_batch(
                    source_id=quarantine_records[0]["source_id"],
                    record_count=len(quarantine_records),
                )

            except Exception as e:
                conn.rollback()
                logger.error(f"Database error in batch {batch_id}: {e}", exc_info=True)
                raise
            finally:
                cursor.close()
                conn.close()

        except Exception as e:
            logger.error(f"Failed to write batch {batch_id} to quarantine: {e}", exc_info=True)
            raise

    def get_stats(self) -> Dict[str, int]:
        """
        Get sink statistics.

        Returns:
            Dictionary with stats
        """
        return {
            "total_batches": self.total_batches,
            "total_records": self.total_records,
        }


def create_quarantine_sink_writer(metrics: MetricsCollector):
    """
    Create a foreachBatch writer function for quarantine.

    Args:
        metrics: Metrics collector

    Returns:
        Function compatible with writeStream.foreachBatch()

    Example:
        >>> metrics = MetricsCollector()
        >>> writer = create_quarantine_sink_writer(metrics)
        >>> query = invalid_stream_df.writeStream \\
        ...     .foreachBatch(writer) \\
        ...     .start()
    """
    sink = QuarantineSink(metrics)

    def write_batch_wrapper(batch_df: DataFrame, batch_id: int):
        """Wrapper for foreachBatch."""
        sink.write_batch(batch_df, batch_id)

    return write_batch_wrapper
