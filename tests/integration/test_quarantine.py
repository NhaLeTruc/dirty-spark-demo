"""
Integration tests for quarantine queries and operations.

Tests quarantine filtering, statistics, and review functionality.
"""

import json
import pytest
from datetime import datetime, timedelta


@pytest.mark.integration
def test_quarantine_query_by_source(db_connection):
    """Test querying quarantine records by source_id."""
    # Insert test records
    with db_connection.cursor() as cur:
        for i in range(3):
            cur.execute(
                """
                INSERT INTO quarantine_record
                (record_id, source_id, raw_payload, failed_rules, error_messages, quarantined_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                """,
                (
                    f"test_source_query_{i}",
                    "test_source_A",
                    json.dumps({"id": i}),
                    [f"rule_{i}"],
                    [f"Error {i}"]
                )
            )
        db_connection.commit()

    # Query records
    with db_connection.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM quarantine_record WHERE source_id = %s",
            ("test_source_A",)
        )
        count = cur.fetchone()[0]

    assert count == 3


@pytest.mark.integration
def test_quarantine_query_by_failed_rule(db_connection):
    """Test querying quarantine records by failed rule."""
    # Insert records with different failed rules
    with db_connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO quarantine_record
            (record_id, source_id, raw_payload, failed_rules, error_messages, quarantined_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            """,
            (
                "test_type_1",
                "test_source",
                json.dumps({"id": 1}),
                ["type_check"],
                ["Type error"]
            )
        )
        cur.execute(
            """
            INSERT INTO quarantine_record
            (record_id, source_id, raw_payload, failed_rules, error_messages, quarantined_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            """,
            (
                "test_type_2",
                "test_source",
                json.dumps({"id": 2}),
                ["required_field"],
                ["Validation error"]
            )
        )
        db_connection.commit()

    # Query by failed rule (using array contains operator)
    with db_connection.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM quarantine_record WHERE %s = ANY(failed_rules)",
            ("type_check",)
        )
        count = cur.fetchone()[0]

    assert count >= 1


@pytest.mark.integration
def test_quarantine_statistics(db_connection):
    """Test querying quarantine statistics grouped by source."""
    # Insert sample records
    with db_connection.cursor() as cur:
        sources = ["source_A", "source_A", "source_B", "source_C"]
        for i, source in enumerate(sources):
            cur.execute(
                """
                INSERT INTO quarantine_record
                (record_id, source_id, raw_payload, failed_rules, error_messages, quarantined_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                """,
                (
                    f"test_stats_{i}",
                    source,
                    json.dumps({"id": i}),
                    [f"rule_{i}"],
                    [f"Error {i}"]
                )
            )
        db_connection.commit()

    # Get statistics
    with db_connection.cursor() as cur:
        cur.execute(
            """
            SELECT source_id, COUNT(*) as count
            FROM quarantine_record
            WHERE record_id LIKE 'test_stats_%'
            GROUP BY source_id
            ORDER BY count DESC
            """
        )
        stats = cur.fetchall()

    # Verify statistics structure
    assert len(stats) >= 1
    assert stats[0]["count"] >= 1


@pytest.mark.integration
def test_mark_record_as_reviewed(db_connection):
    """Test marking quarantine records as reviewed."""
    # Insert test record
    with db_connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO quarantine_record
            (record_id, source_id, raw_payload, failed_rules, error_messages, reviewed, quarantined_at)
            VALUES (%s, %s, %s, %s, %s, FALSE, NOW())
            """,
            (
                "test_reviewed",
                "test_source",
                json.dumps({"id": 1}),
                ["test_rule"],
                ["Test error"]
            )
        )
        db_connection.commit()

    # Mark as reviewed
    with db_connection.cursor() as cur:
        cur.execute(
            """
            UPDATE quarantine_record
            SET reviewed = TRUE
            WHERE record_id = %s
            """,
            ("test_reviewed",)
        )
        db_connection.commit()

    # Verify updated
    with db_connection.cursor() as cur:
        cur.execute(
            "SELECT reviewed FROM quarantine_record WHERE record_id = %s",
            ("test_reviewed",)
        )
        result = cur.fetchone()

    assert result["reviewed"] is True


@pytest.mark.integration
def test_quarantine_date_range_query(db_connection):
    """Test querying quarantine records by date range."""
    # Insert records with specific timestamps
    with db_connection.cursor() as cur:
        # Old record (2 days ago)
        cur.execute(
            """
            INSERT INTO quarantine_record
            (record_id, source_id, raw_payload, failed_rules, error_messages, quarantined_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                "test_old",
                "test_date_source",
                json.dumps({"id": 1}),
                ["old_rule"],
                ["Old error"],
                datetime.now() - timedelta(days=2)
            )
        )
        # Recent record (now)
        cur.execute(
            """
            INSERT INTO quarantine_record
            (record_id, source_id, raw_payload, failed_rules, error_messages, quarantined_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            """,
            (
                "test_recent",
                "test_date_source",
                json.dumps({"id": 2}),
                ["recent_rule"],
                ["Recent error"]
            )
        )
        db_connection.commit()

    # Query last 24 hours
    with db_connection.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM quarantine_record
            WHERE source_id = %s AND quarantined_at >= NOW() - INTERVAL '24 hours'
            """,
            ("test_date_source",)
        )
        recent_count = cur.fetchone()[0]

    assert recent_count >= 1  # At least the recent record