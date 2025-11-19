"""
Idempotent upsert operations for warehouse data.

Implements INSERT ... ON CONFLICT UPDATE for reliable, idempotent writes.
"""

import hashlib
import json
from typing import Any

from src.core.models import QuarantineRecord, WarehouseData

from .connection import DatabaseConnectionPool


class WarehouseWriter:
    """
    Handles idempotent upsert operations to the warehouse.

    All writes use PostgreSQL's INSERT ... ON CONFLICT UPDATE
    to ensure idempotency and enable reprocessing.
    """

    def __init__(self, pool: DatabaseConnectionPool):
        """
        Initialize warehouse writer.

        Args:
            pool: Database connection pool
        """
        self.pool = pool

    def upsert_record(self, record: WarehouseData) -> None:
        """
        Upsert a single warehouse record.

        Args:
            record: WarehouseData instance to insert/update

        Implementation uses INSERT ... ON CONFLICT to ensure idempotency.
        """
        # Calculate checksum if not provided
        checksum = record.checksum
        if not checksum:
            checksum = self._calculate_checksum(record.data)

        query = """
            INSERT INTO warehouse_data (
                record_id, source_id, data, schema_version_id, processed_at, checksum
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (record_id) DO UPDATE SET
                data = EXCLUDED.data,
                schema_version_id = EXCLUDED.schema_version_id,
                processed_at = EXCLUDED.processed_at,
                checksum = EXCLUDED.checksum
        """

        self.pool.execute_command(
            query,
            (
                record.record_id,
                record.source_id,
                json.dumps(record.data),
                record.schema_version_id,
                record.processed_at,
                checksum
            )
        )

    def upsert_batch(self, records: list[WarehouseData]) -> int:
        """
        Upsert a batch of warehouse records.

        Args:
            records: List of WarehouseData instances

        Returns:
            Number of records upserted
        """
        if not records:
            return 0

        # Use a transaction for batch insert
        with self.pool.get_connection() as conn:
            with conn.cursor() as cur:
                # Prepare batch insert
                query = """
                    INSERT INTO warehouse_data (
                        record_id, source_id, data, schema_version_id, processed_at, checksum
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (record_id) DO UPDATE SET
                        data = EXCLUDED.data,
                        schema_version_id = EXCLUDED.schema_version_id,
                        processed_at = EXCLUDED.processed_at,
                        checksum = EXCLUDED.checksum
                """

                # Prepare data tuples
                data_tuples = []
                for record in records:
                    checksum = record.checksum or self._calculate_checksum(record.data)
                    data_tuples.append((
                        record.record_id,
                        record.source_id,
                        json.dumps(record.data),
                        record.schema_version_id,
                        record.processed_at,
                        checksum
                    ))

                # Execute batch insert
                cur.executemany(query, data_tuples)
                conn.commit()

                return len(records)

    def _calculate_checksum(self, data: dict[str, Any]) -> str:
        """
        Calculate MD5 checksum of data payload.

        Args:
            data: Data dictionary

        Returns:
            Hexadecimal checksum string
        """
        data_str = json.dumps(data, sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()


class QuarantineWriter:
    """
    Handles writing invalid records to quarantine table.
    """

    def __init__(self, pool: DatabaseConnectionPool):
        """
        Initialize quarantine writer.

        Args:
            pool: Database connection pool
        """
        self.pool = pool

    def quarantine_record(self, record: QuarantineRecord) -> int:
        """
        Insert a record into quarantine.

        Args:
            record: QuarantineRecord instance

        Returns:
            quarantine_id of the inserted record
        """
        query = """
            INSERT INTO quarantine_record (
                record_id, source_id, raw_payload, failed_rules, error_messages,
                quarantined_at, reviewed, reprocess_requested
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING quarantine_id
        """

        result = self.pool.execute_query(
            query,
            (
                record.record_id,
                record.source_id,
                json.dumps(record.raw_payload),
                record.failed_rules,
                record.error_messages,
                record.quarantined_at,
                record.reviewed,
                record.reprocess_requested
            )
        )

        return result[0]["quarantine_id"]

    def quarantine_batch(self, records: list[QuarantineRecord]) -> int:
        """
        Insert a batch of records into quarantine.

        Args:
            records: List of QuarantineRecord instances

        Returns:
            Number of records quarantined
        """
        if not records:
            return 0

        with self.pool.get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO quarantine_record (
                        record_id, source_id, raw_payload, failed_rules, error_messages,
                        quarantined_at, reviewed, reprocess_requested
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """

                data_tuples = [
                    (
                        r.record_id,
                        r.source_id,
                        json.dumps(r.raw_payload),
                        r.failed_rules,
                        r.error_messages,
                        r.quarantined_at,
                        r.reviewed,
                        r.reprocess_requested
                    )
                    for r in records
                ]

                cur.executemany(query, data_tuples)
                conn.commit()

                return len(records)

    def get_quarantine_stats(self, source_id: str | None = None) -> dict[str, Any]:
        """
        Get quarantine statistics.

        Args:
            source_id: Optional source ID to filter by

        Returns:
            Dictionary with quarantine statistics
        """
        if source_id:
            query = """
                SELECT
                    COUNT(*) as total_quarantined,
                    COUNT(*) FILTER (WHERE reviewed = FALSE) as unreviewed,
                    COUNT(*) FILTER (WHERE reprocess_requested = TRUE) as reprocess_pending
                FROM quarantine_record
                WHERE source_id = %s
            """
            params = (source_id,)
        else:
            query = """
                SELECT
                    COUNT(*) as total_quarantined,
                    COUNT(*) FILTER (WHERE reviewed = FALSE) as unreviewed,
                    COUNT(*) FILTER (WHERE reprocess_requested = TRUE) as reprocess_pending
                FROM quarantine_record
            """
            params = ()

        result = self.pool.execute_query(query, params)
        return result[0] if result else {}

    def mark_reviewed(self, quarantine_id: int) -> None:
        """
        Mark a quarantine record as reviewed.

        Args:
            quarantine_id: The quarantine record ID
        """
        query = """
            UPDATE quarantine_record
            SET reviewed = TRUE
            WHERE quarantine_id = %s
        """

        self.pool.execute_command(query, (quarantine_id,))

    def request_reprocess(self, quarantine_id: int) -> None:
        """
        Mark a quarantine record for reprocessing.

        Args:
            quarantine_id: The quarantine record ID
        """
        query = """
            UPDATE quarantine_record
            SET reprocess_requested = TRUE
            WHERE quarantine_id = %s
        """

        self.pool.execute_command(query, (quarantine_id,))
