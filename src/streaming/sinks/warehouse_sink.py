"""
Warehouse sink for streaming valid records.

Writes validated records to PostgreSQL warehouse using foreachBatch.
"""

import logging
from typing import List, Dict, Any
import json

from pyspark.sql import DataFrame
from psycopg.rows import dict_row

from src.warehouse.connection import get_connection
from src.warehouse.upsert import upsert_warehouse_data
from src.core.models.warehouse_data import WarehouseData
from src.observability.metrics import MetricsCollector
from src.observability.lineage import log_transformation

logger = logging.getLogger(__name__)


class WarehouseSink:
    """
    Streaming sink for writing valid records to warehouse.

    Uses foreachBatch to write micro-batches transactionally.
    """

    def __init__(self, metrics: MetricsCollector):
        """
        Initialize warehouse sink.

        Args:
            metrics: Metrics collector for tracking writes
        """
        self.metrics = metrics
        self.total_records = 0
        self.total_batches = 0

    def write_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """
        Write a micro-batch to the warehouse.

        Args:
            batch_df: Batch DataFrame with valid records
            batch_id: Batch identifier

        Raises:
            Exception: If batch write fails
        """
        try:
            logger.info(f"Writing batch {batch_id} to warehouse")

            # Convert DataFrame to list of dicts
            records = batch_df.collect()

            if not records:
                logger.info(f"Batch {batch_id} is empty, skipping")
                return

            # Prepare warehouse records
            warehouse_records = []
            for row in records:
                row_dict = row.asDict()

                # Extract required fields
                record_id = row_dict.get("record_id") or row_dict.get("transaction_id", f"stream_{batch_id}")
                source_id = row_dict.get("source_id", "unknown")

                # Remove metadata fields from data payload
                data_payload = {
                    k: v for k, v in row_dict.items()
                    if k not in ["source_id", "validation_status", "passed_rules",
                                 "failed_rules", "transformations_applied", "_corrupt_record"]
                }

                warehouse_records.append({
                    "record_id": record_id,
                    "source_id": source_id,
                    "data": data_payload,
                })

            # Write to database
            conn = None
            cursor = None
            try:
                conn = get_connection()
                cursor = conn.cursor()

                for record in warehouse_records:
                    upsert_warehouse_data(
                        cursor,
                        record_id=record["record_id"],
                        source_id=record["source_id"],
                        data=record["data"],
                    )

                conn.commit()

                # Update metrics
                self.total_records += len(warehouse_records)
                self.total_batches += 1

                logger.info(
                    f"Batch {batch_id} written successfully: "
                    f"{len(warehouse_records)} records"
                )

                # Update Prometheus metrics
                self.metrics.record_batch_processed(
                    source_id=warehouse_records[0]["source_id"],
                    record_count=len(warehouse_records),
                    status="success"
                )

            except Exception as e:
                if conn:
                    try:
                        conn.rollback()
                    except Exception as rollback_error:
                        logger.warning(f"Rollback failed: {rollback_error}")
                logger.error(f"Database error in batch {batch_id}: {e}", exc_info=True)
                raise
            finally:
                # Safe cleanup - check for None before closing
                if cursor:
                    try:
                        cursor.close()
                    except Exception as e:
                        logger.warning(f"Error closing cursor: {e}")
                if conn:
                    try:
                        conn.close()
                    except Exception as e:
                        logger.warning(f"Error closing connection: {e}")

        except Exception as e:
            logger.error(f"Failed to write batch {batch_id}: {e}", exc_info=True)

            # Update error metrics
            self.metrics.record_batch_processed(
                source_id="unknown",
                record_count=0,
                status="error"
            )
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


def create_warehouse_sink_writer(metrics: MetricsCollector):
    """
    Create a foreachBatch writer function for warehouse.

    Args:
        metrics: Metrics collector

    Returns:
        Function compatible with writeStream.foreachBatch()

    Example:
        >>> metrics = MetricsCollector()
        >>> writer = create_warehouse_sink_writer(metrics)
        >>> query = valid_stream_df.writeStream \\
        ...     .foreachBatch(writer) \\
        ...     .start()
    """
    sink = WarehouseSink(metrics)

    def write_batch_wrapper(batch_df: DataFrame, batch_id: int):
        """Wrapper for foreachBatch."""
        sink.write_batch(batch_df, batch_id)

    return write_batch_wrapper
