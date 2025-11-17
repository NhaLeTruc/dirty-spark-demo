"""
Data lineage tracking for audit trail.

This module provides a LineageTracker class to track data transformations
and create audit log entries for compliance and debugging purposes.
"""

from datetime import datetime
from typing import Any

from src.core.models.audit_log import AuditLog
from src.observability.logger import get_logger
from src.warehouse.audit import insert_audit_log, insert_audit_logs_batch
from src.warehouse.connection import DatabaseConnectionPool

logger = get_logger(__name__)


class LineageTracker:
    """
    Tracks data transformations and creates audit log entries.

    Usage:
        tracker = LineageTracker(pool)
        tracker.track_transformation(
            record_id="TXN001",
            source_id="csv1",
            transformation_type="type_coercion",
            field_name="amount",
            old_value="99.99",
            new_value=99.99
        )
        tracker.flush()  # Write pending entries to database
    """

    def __init__(
        self,
        pool: DatabaseConnectionPool | None = None,
        batch_size: int = 100
    ):
        """
        Initialize lineage tracker.

        Args:
            pool: Database connection pool (optional for testing)
            batch_size: Number of entries to buffer before auto-flush
        """
        self.pool = pool
        self.batch_size = batch_size
        self._pending_logs: list[AuditLog] = []

    def track_transformation(
        self,
        record_id: str,
        source_id: str,
        transformation_type: str,
        field_name: str | None = None,
        old_value: Any = None,
        new_value: Any = None,
        rule_applied: str | None = None
    ) -> AuditLog:
        """
        Track a data transformation.

        Args:
            record_id: ID of the record being transformed
            source_id: Source of the record
            transformation_type: Type of transformation (e.g., "type_coercion")
            field_name: Field that was transformed
            old_value: Original value before transformation
            new_value: New value after transformation
            rule_applied: Rule or validator that applied the transformation

        Returns:
            AuditLog model instance
        """
        # Convert values to strings for storage
        old_value_str = str(old_value) if old_value is not None else None
        new_value_str = str(new_value) if new_value is not None else None

        audit_log = AuditLog(
            record_id=record_id,
            source_id=source_id,
            transformation_type=transformation_type,
            field_name=field_name,
            old_value=old_value_str,
            new_value=new_value_str,
            rule_applied=rule_applied,
            created_at=datetime.utcnow()
        )

        self._pending_logs.append(audit_log)

        logger.debug(
            f"Tracked transformation: {transformation_type} on {field_name} "
            f"for record {record_id}"
        )

        # Auto-flush if batch size reached
        if len(self._pending_logs) >= self.batch_size:
            self.flush()

        return audit_log

    def track_type_coercion(
        self,
        record_id: str,
        source_id: str,
        field_name: str,
        old_value: Any,
        new_value: Any,
        target_type: str
    ) -> AuditLog:
        """
        Track a type coercion transformation.

        Args:
            record_id: ID of the record
            source_id: Source of the record
            field_name: Field that was coerced
            old_value: Original value (e.g., "99.99" string)
            new_value: Coerced value (e.g., 99.99 float)
            target_type: Target type (e.g., "float")

        Returns:
            AuditLog model instance
        """
        return self.track_transformation(
            record_id=record_id,
            source_id=source_id,
            transformation_type="type_coercion",
            field_name=field_name,
            old_value=old_value,
            new_value=new_value,
            rule_applied=f"TypeValidator(target={target_type})"
        )

    def track_warehouse_write(
        self,
        record_id: str,
        source_id: str,
        schema_version_id: int | None = None
    ) -> AuditLog:
        """
        Track a successful warehouse write event.

        Args:
            record_id: ID of the record written
            source_id: Source of the record
            schema_version_id: Schema version used

        Returns:
            AuditLog model instance
        """
        return self.track_transformation(
            record_id=record_id,
            source_id=source_id,
            transformation_type="warehouse_write",
            field_name=None,
            old_value=None,
            new_value=f"schema_version={schema_version_id}" if schema_version_id else None,
            rule_applied=None
        )

    def track_quarantine(
        self,
        record_id: str,
        source_id: str,
        failed_rule: str,
        field_name: str | None = None,
        error_message: str | None = None
    ) -> AuditLog:
        """
        Track a quarantine event.

        Args:
            record_id: ID of the quarantined record
            source_id: Source of the record
            failed_rule: Rule that caused quarantine
            field_name: Field that failed validation
            error_message: Error message describing the failure

        Returns:
            AuditLog model instance
        """
        return self.track_transformation(
            record_id=record_id,
            source_id=source_id,
            transformation_type="quarantine",
            field_name=field_name,
            old_value=None,
            new_value=error_message,
            rule_applied=failed_rule
        )

    def track_deduplication(
        self,
        record_id: str,
        source_id: str,
        duplicate_count: int
    ) -> AuditLog:
        """
        Track a deduplication event.

        Args:
            record_id: ID of the deduplicated record
            source_id: Source of the record
            duplicate_count: Number of duplicates removed

        Returns:
            AuditLog model instance
        """
        return self.track_transformation(
            record_id=record_id,
            source_id=source_id,
            transformation_type="deduplication",
            field_name=None,
            old_value=None,
            new_value=f"duplicates_removed={duplicate_count}",
            rule_applied=None
        )

    def track_date_normalization(
        self,
        record_id: str,
        source_id: str,
        field_name: str,
        old_value: Any,
        new_value: Any,
        date_format: str
    ) -> AuditLog:
        """
        Track a date normalization transformation.

        Args:
            record_id: ID of the record
            source_id: Source of the record
            field_name: Field that was normalized
            old_value: Original date string
            new_value: Normalized date
            date_format: Target date format

        Returns:
            AuditLog model instance
        """
        return self.track_transformation(
            record_id=record_id,
            source_id=source_id,
            transformation_type="date_normalization",
            field_name=field_name,
            old_value=old_value,
            new_value=new_value,
            rule_applied=f"DateNormalizer(format={date_format})"
        )

    def track_field_trimming(
        self,
        record_id: str,
        source_id: str,
        field_name: str,
        old_value: str,
        new_value: str
    ) -> AuditLog:
        """
        Track a field trimming transformation.

        Args:
            record_id: ID of the record
            source_id: Source of the record
            field_name: Field that was trimmed
            old_value: Original value with whitespace
            new_value: Trimmed value

        Returns:
            AuditLog model instance
        """
        return self.track_transformation(
            record_id=record_id,
            source_id=source_id,
            transformation_type="field_trimming",
            field_name=field_name,
            old_value=old_value,
            new_value=new_value,
            rule_applied="StringTrimmer"
        )

    def flush(self) -> int:
        """
        Write all pending audit logs to database.

        Returns:
            Number of audit log entries written

        Raises:
            RuntimeError: If pool not configured
        """
        if not self.pool:
            logger.warning(
                "No database pool configured, cannot flush audit logs. "
                "Clearing pending logs."
            )
            count = len(self._pending_logs)
            self._pending_logs.clear()
            return count

        if not self._pending_logs:
            return 0

        try:
            count = insert_audit_logs_batch(self.pool, self._pending_logs)
            logger.info(f"Flushed {count} audit log entries to database")
            self._pending_logs.clear()
            return count

        except Exception as e:
            logger.error(f"Failed to flush audit logs: {e}")
            raise

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - auto-flush pending logs."""
        if self._pending_logs:
            try:
                self.flush()
            except Exception as e:
                logger.error(f"Error flushing audit logs on exit: {e}")
        return False
