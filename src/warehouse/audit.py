"""
Audit log operations for data lineage tracking.

This module provides functions to insert and query audit log entries
for compliance and lineage tracking purposes.
"""

from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from src.core.models.audit_log import AuditLog
from src.observability.logger import get_logger
from src.warehouse.connection import DatabaseConnectionPool

logger = get_logger(__name__)


def insert_audit_log(
    pool: DatabaseConnectionPool,
    audit_log: AuditLog
) -> int:
    """
    Insert a single audit log entry into the database.

    Args:
        pool: Database connection pool
        audit_log: AuditLog model instance

    Returns:
        log_id: Generated log ID

    Raises:
        psycopg.DatabaseError: If insert fails
    """
    insert_sql = """
        INSERT INTO audit_log (
            record_id,
            source_id,
            transformation_type,
            field_name,
            old_value,
            new_value,
            rule_applied,
            created_at
        ) VALUES (
            %(record_id)s,
            %(source_id)s,
            %(transformation_type)s,
            %(field_name)s,
            %(old_value)s,
            %(new_value)s,
            %(rule_applied)s,
            %(created_at)s
        ) RETURNING log_id;
    """

    try:
        with pool.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    insert_sql,
                    {
                        "record_id": audit_log.record_id,
                        "source_id": audit_log.source_id,
                        "transformation_type": audit_log.transformation_type,
                        "field_name": audit_log.field_name,
                        "old_value": audit_log.old_value,
                        "new_value": audit_log.new_value,
                        "rule_applied": audit_log.rule_applied,
                        "created_at": audit_log.created_at,
                    },
                )
                result = cur.fetchone()
                log_id = result[0] if result else None
                conn.commit()

                logger.debug(
                    f"Inserted audit log entry: log_id={log_id}, "
                    f"record_id={audit_log.record_id}, "
                    f"type={audit_log.transformation_type}"
                )

                return log_id

    except psycopg.DatabaseError as e:
        logger.error(f"Failed to insert audit log: {e}")
        raise


def insert_audit_logs_batch(
    pool: DatabaseConnectionPool,
    audit_logs: list[AuditLog]
) -> int:
    """
    Insert multiple audit log entries in batch.

    Args:
        pool: Database connection pool
        audit_logs: List of AuditLog model instances

    Returns:
        count: Number of audit log entries inserted

    Raises:
        psycopg.DatabaseError: If batch insert fails
    """
    if not audit_logs:
        return 0

    insert_sql = """
        INSERT INTO audit_log (
            record_id,
            source_id,
            transformation_type,
            field_name,
            old_value,
            new_value,
            rule_applied,
            created_at
        ) VALUES (
            %(record_id)s,
            %(source_id)s,
            %(transformation_type)s,
            %(field_name)s,
            %(old_value)s,
            %(new_value)s,
            %(rule_applied)s,
            %(created_at)s
        );
    """

    try:
        with pool.get_connection() as conn:
            with conn.cursor() as cur:
                # Use executemany for batch insert
                params = [
                    {
                        "record_id": log.record_id,
                        "source_id": log.source_id,
                        "transformation_type": log.transformation_type,
                        "field_name": log.field_name,
                        "old_value": log.old_value,
                        "new_value": log.new_value,
                        "rule_applied": log.rule_applied,
                        "created_at": log.created_at,
                    }
                    for log in audit_logs
                ]

                cur.executemany(insert_sql, params)
                conn.commit()

                count = len(audit_logs)
                logger.info(f"Inserted {count} audit log entries in batch")

                return count

    except psycopg.DatabaseError as e:
        logger.error(f"Failed to insert audit logs batch: {e}")
        raise


def query_audit_logs_by_record(
    pool: DatabaseConnectionPool,
    record_id: str,
    limit: int = 100
) -> list[dict[str, Any]]:
    """
    Query audit log entries for a specific record.

    Args:
        pool: Database connection pool
        record_id: Record ID to query
        limit: Maximum number of entries to return

    Returns:
        List of audit log entries as dictionaries

    Raises:
        psycopg.DatabaseError: If query fails
    """
    query_sql = """
        SELECT
            log_id,
            record_id,
            source_id,
            transformation_type,
            field_name,
            old_value,
            new_value,
            rule_applied,
            created_at
        FROM audit_log
        WHERE record_id = %(record_id)s
        ORDER BY created_at DESC
        LIMIT %(limit)s;
    """

    try:
        with pool.get_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    query_sql,
                    {"record_id": record_id, "limit": limit}
                )
                results = cur.fetchall()

                logger.debug(
                    f"Found {len(results)} audit log entries for record_id={record_id}"
                )

                return results

    except psycopg.DatabaseError as e:
        logger.error(f"Failed to query audit logs by record: {e}")
        raise


def query_audit_logs_by_source(
    pool: DatabaseConnectionPool,
    source_id: str,
    limit: int = 1000
) -> list[dict[str, Any]]:
    """
    Query audit log entries for a specific source.

    Args:
        pool: Database connection pool
        source_id: Source ID to query
        limit: Maximum number of entries to return

    Returns:
        List of audit log entries as dictionaries

    Raises:
        psycopg.DatabaseError: If query fails
    """
    query_sql = """
        SELECT
            log_id,
            record_id,
            source_id,
            transformation_type,
            field_name,
            old_value,
            new_value,
            rule_applied,
            created_at
        FROM audit_log
        WHERE source_id = %(source_id)s
        ORDER BY created_at DESC
        LIMIT %(limit)s;
    """

    try:
        with pool.get_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    query_sql,
                    {"source_id": source_id, "limit": limit}
                )
                results = cur.fetchall()

                logger.debug(
                    f"Found {len(results)} audit log entries for source_id={source_id}"
                )

                return results

    except psycopg.DatabaseError as e:
        logger.error(f"Failed to query audit logs by source: {e}")
        raise


def query_audit_logs_by_transformation_type(
    pool: DatabaseConnectionPool,
    transformation_type: str,
    limit: int = 1000
) -> list[dict[str, Any]]:
    """
    Query audit log entries by transformation type.

    Args:
        pool: Database connection pool
        transformation_type: Type of transformation to filter
        limit: Maximum number of entries to return

    Returns:
        List of audit log entries as dictionaries

    Raises:
        psycopg.DatabaseError: If query fails
    """
    query_sql = """
        SELECT
            log_id,
            record_id,
            source_id,
            transformation_type,
            field_name,
            old_value,
            new_value,
            rule_applied,
            created_at
        FROM audit_log
        WHERE transformation_type = %(transformation_type)s
        ORDER BY created_at DESC
        LIMIT %(limit)s;
    """

    try:
        with pool.get_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    query_sql,
                    {"transformation_type": transformation_type, "limit": limit}
                )
                results = cur.fetchall()

                logger.debug(
                    f"Found {len(results)} audit log entries "
                    f"for transformation_type={transformation_type}"
                )

                return results

    except psycopg.DatabaseError as e:
        logger.error(f"Failed to query audit logs by transformation type: {e}")
        raise


def get_audit_summary(
    pool: DatabaseConnectionPool,
    source_id: str | None = None
) -> dict[str, Any]:
    """
    Get summary statistics from audit log.

    Args:
        pool: Database connection pool
        source_id: Optional source ID to filter

    Returns:
        Dictionary with summary statistics:
        - total_transformations
        - transformations_by_type
        - unique_records_processed

    Raises:
        psycopg.DatabaseError: If query fails
    """
    base_query = """
        SELECT
            COUNT(*) as total_transformations,
            COUNT(DISTINCT record_id) as unique_records,
            transformation_type,
            COUNT(*) as type_count
        FROM audit_log
    """

    if source_id:
        base_query += " WHERE source_id = %(source_id)s"

    base_query += " GROUP BY transformation_type"

    try:
        with pool.get_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                params = {"source_id": source_id} if source_id else {}
                cur.execute(base_query, params)
                results = cur.fetchall()

                total = sum(r["type_count"] for r in results)
                unique_records = results[0]["unique_records"] if results else 0
                by_type = {r["transformation_type"]: r["type_count"] for r in results}

                summary = {
                    "total_transformations": total,
                    "unique_records_processed": unique_records,
                    "transformations_by_type": by_type,
                }

                logger.info(f"Audit summary: {summary}")

                return summary

    except psycopg.DatabaseError as e:
        logger.error(f"Failed to get audit summary: {e}")
        raise
