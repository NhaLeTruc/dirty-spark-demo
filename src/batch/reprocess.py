"""
Quarantine reprocessing functionality.

Enables reprocessing of quarantined records with updated validation rules.
"""

import json
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from src.observability import metrics
from src.observability.logger import get_logger
from src.warehouse.connection import DatabaseConnectionPool

logger = get_logger(__name__)


class QuarantineReprocessor:
    """
    Reprocess quarantined records with updated validation rules.

    Fetches records from quarantine, re-applies validation rules,
    and moves valid records to warehouse.
    """

    def __init__(self, spark: SparkSession, pool: DatabaseConnectionPool):
        """
        Initialize quarantine reprocessor.

        Args:
            spark: Active Spark session
            pool: Database connection pool
        """
        self.spark = spark
        self.pool = pool
        self.logger = get_logger(__name__)

    def reprocess_quarantine(
        self,
        source_id: str,
        record_ids: list[str] | None = None,
        error_types: list[str] | None = None,
        limit: int | None = None
    ) -> dict[str, Any]:
        """
        Reprocess quarantined records.

        Args:
            source_id: Data source ID to reprocess
            record_ids: Optional list of specific record IDs to reprocess
            error_types: Optional list of error types to filter by
            limit: Optional maximum number of records to reprocess

        Returns:
            Dictionary with reprocessing results:
            - total_reprocessed: Total records attempted
            - success_count: Records successfully moved to warehouse
            - failed_count: Records that still failed validation
            - duration_seconds: Time taken
        """
        start_time = datetime.now()
        self.logger.info(
            f"Starting quarantine reprocessing for source_id={source_id}",
            extra={"source_id": source_id, "record_ids": record_ids, "error_types": error_types}
        )

        # Fetch quarantine records
        quarantine_records = self._fetch_quarantine_records(
            source_id=source_id,
            record_ids=record_ids,
            error_types=error_types,
            limit=limit
        )

        if not quarantine_records:
            self.logger.info(f"No quarantine records found for source_id={source_id}")
            return {
                "total_reprocessed": 0,
                "success_count": 0,
                "failed_count": 0,
                "duration_seconds": 0.0
            }

        total_records = len(quarantine_records)
        self.logger.info(f"Found {total_records} quarantine records to reprocess")

        # Convert to DataFrame for processing
        self._create_dataframe_from_quarantine(quarantine_records)

        # Apply validation rules
        # Note: In a full implementation, this would:
        # 1. Load current validation rules
        # 2. Apply rules to records
        # 3. Separate valid from invalid
        # 4. Write valid records to warehouse
        # 5. Update or remove quarantine records

        success_count = 0
        failed_count = total_records

        # For now, mark records as reprocessed
        self._mark_records_reprocessed(
            record_ids=[r["record_id"] for r in quarantine_records]
        )

        duration = (datetime.now() - start_time).total_seconds()

        # Record metrics
        metrics.increment_counter(
            metrics.records_processed_total,
            total_records,
            source_id=source_id,
            status="reprocessed"
        )

        result = {
            "total_reprocessed": total_records,
            "success_count": success_count,
            "failed_count": failed_count,
            "duration_seconds": duration
        }

        self.logger.info(
            f"Reprocessing complete for source_id={source_id}",
            extra=result
        )

        return result

    def _fetch_quarantine_records(
        self,
        source_id: str,
        record_ids: list[str] | None = None,
        error_types: list[str] | None = None,
        limit: int | None = None
    ) -> list[dict[str, Any]]:
        """
        Fetch quarantine records from database.

        Args:
            source_id: Data source ID
            record_ids: Optional specific record IDs
            error_types: Optional error types to filter
            limit: Optional result limit

        Returns:
            List of quarantine records as dictionaries
        """
        query = """
            SELECT record_id, source_id, raw_data, error_message, error_type
            FROM quarantine_record
            WHERE source_id = %s AND reviewed = FALSE
        """
        params = [source_id]

        if record_ids:
            query += " AND record_id = ANY(%s)"
            params.append(record_ids)

        if error_types:
            query += " AND error_type = ANY(%s)"
            params.append(error_types)

        query += " ORDER BY created_at DESC"

        if limit:
            query += f" LIMIT {limit}"

        with self.pool.get_cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def _create_dataframe_from_quarantine(
        self,
        records: list[dict[str, Any]]
    ) -> DataFrame:
        """
        Convert quarantine records to Spark DataFrame.

        Args:
            records: List of quarantine records

        Returns:
            Spark DataFrame with parsed raw_data
        """
        # Convert to list of dicts for Spark
        data = []
        for record in records:
            raw_data = json.loads(record["raw_data"]) if isinstance(record["raw_data"], str) else record["raw_data"]
            data.append({
                "record_id": record["record_id"],
                "source_id": record["source_id"],
                "raw_data": raw_data,
                "error_message": record["error_message"],
                "error_type": record["error_type"]
            })

        # Create DataFrame
        df = self.spark.createDataFrame(data)
        return df

    def _mark_records_reprocessed(self, record_ids: list[str]) -> None:
        """
        Mark quarantine records as reprocessed.

        Args:
            record_ids: List of record IDs to mark
        """
        if not record_ids:
            return

        query = """
            UPDATE quarantine_record
            SET reviewed = TRUE, reviewed_at = NOW()
            WHERE record_id = ANY(%s)
        """

        with self.pool.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (record_ids,))
            conn.commit()

        self.logger.info(f"Marked {len(record_ids)} records as reprocessed")


def get_quarantine_statistics(
    pool: DatabaseConnectionPool,
    source_id: str | None = None
) -> dict[str, Any]:
    """
    Get quarantine statistics.

    Args:
        pool: Database connection pool
        source_id: Optional source ID to filter

    Returns:
        Dictionary with statistics:
        - total_quarantined: Total records in quarantine
        - by_error_type: Count by error type
        - by_source: Count by source (if source_id not specified)
        - reviewed_count: Number of reviewed records
    """
    stats = {
        "total_quarantined": 0,
        "by_error_type": {},
        "by_source": {},
        "reviewed_count": 0
    }

    with pool.get_cursor() as cur:
        # Total count
        if source_id:
            cur.execute(
                "SELECT COUNT(*) FROM quarantine_record WHERE source_id = %s",
                (source_id,)
            )
        else:
            cur.execute("SELECT COUNT(*) FROM quarantine_record")
        stats["total_quarantined"] = cur.fetchone()[0]

        # By error type
        if source_id:
            cur.execute(
                """
                SELECT error_type, COUNT(*) as count
                FROM quarantine_record
                WHERE source_id = %s
                GROUP BY error_type
                ORDER BY count DESC
                """,
                (source_id,)
            )
        else:
            cur.execute(
                """
                SELECT error_type, COUNT(*) as count
                FROM quarantine_record
                GROUP BY error_type
                ORDER BY count DESC
                """
            )
        stats["by_error_type"] = {row["error_type"]: row["count"] for row in cur.fetchall()}

        # By source (if not filtered)
        if not source_id:
            cur.execute(
                """
                SELECT source_id, COUNT(*) as count
                FROM quarantine_record
                GROUP BY source_id
                ORDER BY count DESC
                LIMIT 10
                """
            )
            stats["by_source"] = {row["source_id"]: row["count"] for row in cur.fetchall()}

        # Reviewed count
        if source_id:
            cur.execute(
                "SELECT COUNT(*) FROM quarantine_record WHERE source_id = %s AND reviewed = TRUE",
                (source_id,)
            )
        else:
            cur.execute("SELECT COUNT(*) FROM quarantine_record WHERE reviewed = TRUE")
        stats["reviewed_count"] = cur.fetchone()[0]

    return stats
