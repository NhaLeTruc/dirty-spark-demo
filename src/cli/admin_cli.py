"""
Admin CLI for managing the data validation pipeline.

Usage:
    python -m src.cli.admin_cli trace-record --record-id <record_id> [options]
    python -m src.cli.admin_cli audit-report --source <source_id> [options]
    python -m src.cli.admin_cli quarantine-review [options]
    python -m src.cli.admin_cli quarantine-stats [--source <source_id>]
    python -m src.cli.admin_cli add-rule --rule-file <path> [options]
    python -m src.cli.admin_cli update-rule --rule-id <id> [options]
    python -m src.cli.admin_cli disable-rule --rule-id <id>
    python -m src.cli.admin_cli reprocess --source <source_id> [options]
    python -m src.cli.admin_cli mark-reviewed --record-ids <ids>
"""

import argparse
import json
import sys
from datetime import datetime
from typing import Any, List
import yaml

from pyspark.sql import SparkSession

from src.observability.logger import get_logger
from src.warehouse.audit import (
    get_audit_summary,
    query_audit_logs_by_record,
    query_audit_logs_by_source,
    query_audit_logs_by_transformation_type,
)
from src.warehouse.connection import DatabaseConnectionPool
from src.batch.reprocess import QuarantineReprocessor, get_quarantine_statistics

logger = get_logger(__name__)


def format_timestamp(ts: datetime | str) -> str:
    """Format timestamp for display."""
    if isinstance(ts, str):
        return ts
    return ts.strftime("%Y-%m-%d %H:%M:%S") if ts else "N/A"


def trace_record_command(args):
    """
    Trace the complete lineage of a record from source to destination.

    Args:
        args: Command line arguments
    """
    logger.info(f"Tracing lineage for record: {args.record_id}")

    # Create database connection pool
    pool = DatabaseConnectionPool(
        host=args.db_host,
        port=args.db_port,
        database=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )

    try:
        pool.open()

        # Query audit logs for this record
        audit_logs = query_audit_logs_by_record(
            pool, args.record_id, limit=args.limit
        )

        if not audit_logs:
            print(f"\nNo audit trail found for record ID: {args.record_id}")
            print("This record may not have been processed yet.")
            return

        # Display audit trail
        print(f"\n{'=' * 80}")
        print(f"AUDIT TRAIL FOR RECORD: {args.record_id}")
        print(f"{'=' * 80}\n")

        print(f"Total events: {len(audit_logs)}\n")

        # Group by transformation type
        transformations_by_type: dict[str, list[dict[str, Any]]] = {}
        for log in audit_logs:
            t_type = log["transformation_type"]
            if t_type not in transformations_by_type:
                transformations_by_type[t_type] = []
            transformations_by_type[t_type].append(log)

        # Display summary
        print("Event Summary:")
        for t_type, logs in transformations_by_type.items():
            print(f"  - {t_type}: {len(logs)} event(s)")
        print()

        # Display detailed events in chronological order
        print(f"{'Timestamp':<20} {'Type':<20} {'Field':<15} {'Details'}")
        print(f"{'-' * 80}")

        for log in reversed(audit_logs):  # Show oldest first
            timestamp = format_timestamp(log["created_at"])
            t_type = log["transformation_type"]
            field = log["field_name"] or "-"

            # Build details string
            details = []
            if log["old_value"]:
                details.append(f"'{log['old_value']}'")
            if log["new_value"]:
                if log["old_value"]:
                    details.append("→")
                details.append(f"'{log['new_value']}'")
            if log["rule_applied"]:
                details.append(f"({log['rule_applied']})")

            details_str = " ".join(details) if details else "-"

            print(f"{timestamp:<20} {t_type:<20} {field:<15} {details_str}")

        print(f"\n{'=' * 80}\n")

    except Exception as e:
        logger.error(f"Error tracing record: {e}", exc_info=True)
        print(f"\nError: {e}")
        sys.exit(1)

    finally:
        pool.close()


def audit_report_command(args):
    """
    Generate an audit report with statistics.

    Args:
        args: Command line arguments
    """
    logger.info(f"Generating audit report{f' for source: {args.source}' if args.source else ''}")

    # Create database connection pool
    pool = DatabaseConnectionPool(
        host=args.db_host,
        port=args.db_port,
        database=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )

    try:
        pool.open()

        # Get audit summary
        summary = get_audit_summary(pool, source_id=args.source)

        # Display report
        print(f"\n{'=' * 60}")
        print("AUDIT REPORT")
        if args.source:
            print(f"Source: {args.source}")
        print(f"{'=' * 60}\n")

        print("Overall Statistics:")
        print(f"  Total transformations: {summary['total_transformations']}")
        print(f"  Unique records processed: {summary['unique_records_processed']}\n")

        print("Transformations by Type:")
        for t_type, count in sorted(
            summary["transformations_by_type"].items(),
            key=lambda x: x[1],
            reverse=True
        ):
            print(f"  {t_type:<30} {count:>8}")

        print(f"\n{'=' * 60}\n")

        # Optionally show detailed logs
        if args.detailed:
            print("Most Recent Events:")
            print(f"{'-' * 60}\n")

            if args.source:
                logs = query_audit_logs_by_source(pool, args.source, limit=args.limit)
            else:
                # Get all recent logs (no specific filter)
                logs = query_audit_logs_by_transformation_type(
                    pool, "warehouse_write", limit=args.limit
                )

            for log in logs[:10]:  # Show first 10
                print(f"Record: {log['record_id']}")
                print(f"  Type: {log['transformation_type']}")
                print(f"  Time: {format_timestamp(log['created_at'])}")
                if log["field_name"]:
                    print(f"  Field: {log['field_name']}")
                print()

    except Exception as e:
        logger.error(f"Error generating audit report: {e}", exc_info=True)
        print(f"\nError: {e}")
        sys.exit(1)

    finally:
        pool.close()


def quarantine_review_command(args):
    """
    Display quarantined records with filtering options.

    Args:
        args: Command line arguments
    """
    logger.info(f"Reviewing quarantine records{f' for source: {args.source}' if args.source else ''}")

    pool = DatabaseConnectionPool(
        host=args.db_host,
        port=args.db_port,
        database=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )

    try:
        pool.open()

        # Build query
        query = """
            SELECT record_id, source_id, error_message, error_type,
                   created_at, reviewed
            FROM quarantine_record
            WHERE 1=1
        """
        params = []

        if args.source:
            query += " AND source_id = %s"
            params.append(args.source)

        if args.error_type:
            query += " AND error_type = %s"
            params.append(args.error_type)

        if args.unreviewed_only:
            query += " AND reviewed = FALSE"

        query += f" ORDER BY created_at DESC LIMIT {args.limit}"

        # Execute query
        with pool.get_cursor() as cur:
            cur.execute(query, params)
            records = cur.fetchall()

        if not records:
            print("\nNo quarantine records found matching the criteria.")
            return

        # Display records
        print(f"\n{'=' * 100}")
        print(f"QUARANTINE REVIEW{f' - Source: {args.source}' if args.source else ''}")
        print(f"{'=' * 100}\n")
        print(f"Total records: {len(records)}\n")

        print(f"{'Record ID':<25} {'Source':<15} {'Error Type':<20} {'Created':<20} {'Reviewed'}")
        print(f"{'-' * 100}")

        for record in records:
            reviewed_mark = "✓" if record["reviewed"] else " "
            print(
                f"{record['record_id']:<25} {record['source_id']:<15} "
                f"{record['error_type']:<20} {format_timestamp(record['created_at']):<20} {reviewed_mark}"
            )

        print(f"\n{'=' * 100}\n")

        # Show sample error messages
        if args.show_errors and len(records) > 0:
            print("Sample Error Messages:")
            print(f"{'-' * 100}\n")
            for record in records[:5]:
                print(f"Record: {record['record_id']}")
                print(f"  Error: {record['error_message']}")
                print()

    except Exception as e:
        logger.error(f"Error reviewing quarantine: {e}", exc_info=True)
        print(f"\nError: {e}")
        sys.exit(1)

    finally:
        pool.close()


def quarantine_stats_command(args):
    """
    Display quarantine statistics.

    Args:
        args: Command line arguments
    """
    logger.info(f"Getting quarantine statistics{f' for source: {args.source}' if args.source else ''}")

    pool = DatabaseConnectionPool(
        host=args.db_host,
        port=args.db_port,
        database=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )

    try:
        pool.open()

        # Get statistics
        stats = get_quarantine_statistics(pool, source_id=args.source)

        # Display statistics
        print(f"\n{'=' * 60}")
        print("QUARANTINE STATISTICS")
        if args.source:
            print(f"Source: {args.source}")
        print(f"{'=' * 60}\n")

        print("Overall:")
        print(f"  Total quarantined: {stats['total_quarantined']}")
        print(f"  Reviewed: {stats['reviewed_count']}")
        print(f"  Unreviewed: {stats['total_quarantined'] - stats['reviewed_count']}\n")

        print("By Error Type:")
        for error_type, count in sorted(
            stats["by_error_type"].items(),
            key=lambda x: x[1],
            reverse=True
        ):
            print(f"  {error_type:<30} {count:>8}")

        if stats["by_source"] and not args.source:
            print("\nTop Sources with Quarantine Records:")
            for source, count in list(stats["by_source"].items())[:10]:
                print(f"  {source:<30} {count:>8}")

        print(f"\n{'=' * 60}\n")

    except Exception as e:
        logger.error(f"Error getting quarantine statistics: {e}", exc_info=True)
        print(f"\nError: {e}")
        sys.exit(1)

    finally:
        pool.close()


def reprocess_command(args):
    """
    Reprocess quarantined records with updated validation rules.

    Args:
        args: Command line arguments
    """
    logger.info(f"Reprocessing quarantine for source: {args.source}")

    pool = DatabaseConnectionPool(
        host=args.db_host,
        port=args.db_port,
        database=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )

    try:
        pool.open()

        # Create Spark session
        spark = SparkSession.builder \
            .appName("QuarantineReprocessing") \
            .master("local[*]") \
            .getOrCreate()

        # Create reprocessor
        reprocessor = QuarantineReprocessor(spark=spark, pool=pool)

        # Parse record IDs if provided
        record_ids = None
        if args.record_ids:
            record_ids = [rid.strip() for rid in args.record_ids.split(",")]

        # Reprocess
        print(f"\nReprocessing quarantine records for source: {args.source}")
        if record_ids:
            print(f"  Specific records: {len(record_ids)} records")
        if args.error_type:
            print(f"  Error type filter: {args.error_type}")

        result = reprocessor.reprocess_quarantine(
            source_id=args.source,
            record_ids=record_ids,
            error_types=[args.error_type] if args.error_type else None,
            limit=args.limit
        )

        # Display results
        print(f"\n{'=' * 60}")
        print("REPROCESSING RESULTS")
        print(f"{'=' * 60}\n")
        print(f"  Total reprocessed: {result['total_reprocessed']}")
        print(f"  Successfully moved to warehouse: {result['success_count']}")
        print(f"  Still failed validation: {result['failed_count']}")
        print(f"  Duration: {result['duration_seconds']:.2f} seconds")
        print(f"\n{'=' * 60}\n")

        spark.stop()

    except Exception as e:
        logger.error(f"Error reprocessing quarantine: {e}", exc_info=True)
        print(f"\nError: {e}")
        sys.exit(1)

    finally:
        pool.close()


def mark_reviewed_command(args):
    """
    Mark quarantine records as reviewed.

    Args:
        args: Command line arguments
    """
    logger.info(f"Marking records as reviewed: {args.record_ids}")

    pool = DatabaseConnectionPool(
        host=args.db_host,
        port=args.db_port,
        database=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )

    try:
        pool.open()

        # Parse record IDs
        record_ids = [rid.strip() for rid in args.record_ids.split(",")]

        # Update records
        query = """
            UPDATE quarantine_record
            SET reviewed = TRUE, reviewed_at = NOW()
            WHERE record_id = ANY(%s)
        """

        with pool.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (record_ids,))
                updated_count = cur.rowcount
            conn.commit()

        print(f"\nMarked {updated_count} record(s) as reviewed.")

    except Exception as e:
        logger.error(f"Error marking records as reviewed: {e}", exc_info=True)
        print(f"\nError: {e}")
        sys.exit(1)

    finally:
        pool.close()


def add_rule_command(args):
    """
    Add a new validation rule to the database.

    Args:
        args: Command line arguments
    """
    logger.info(f"Adding validation rule from file: {args.rule_file}")

    pool = DatabaseConnectionPool(
        host=args.db_host,
        port=args.db_port,
        database=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )

    try:
        pool.open()

        # Load rule from file
        with open(args.rule_file, 'r') as f:
            if args.rule_file.endswith('.yaml') or args.rule_file.endswith('.yml'):
                rule_data = yaml.safe_load(f)
            else:
                rule_data = json.load(f)

        # Insert rule
        query = """
            INSERT INTO validation_rule
            (rule_id, source_id, rule_type, field_name, rule_config, enabled)
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        with pool.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query,
                    (
                        rule_data["rule_id"],
                        rule_data["source_id"],
                        rule_data["rule_type"],
                        rule_data.get("field_name"),
                        json.dumps(rule_data.get("rule_config", {})),
                        rule_data.get("enabled", True)
                    )
                )
            conn.commit()

        print(f"\nRule added successfully: {rule_data['rule_id']}")

    except Exception as e:
        logger.error(f"Error adding rule: {e}", exc_info=True)
        print(f"\nError: {e}")
        sys.exit(1)

    finally:
        pool.close()


def update_rule_command(args):
    """
    Update an existing validation rule.

    Args:
        args: Command line arguments
    """
    logger.info(f"Updating validation rule: {args.rule_id}")

    pool = DatabaseConnectionPool(
        host=args.db_host,
        port=args.db_port,
        database=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )

    try:
        pool.open()

        # Build update query
        updates = []
        params = []

        if args.rule_config:
            with open(args.rule_config, 'r') as f:
                config = json.load(f)
            updates.append("rule_config = %s")
            params.append(json.dumps(config))

        if args.enabled is not None:
            updates.append("enabled = %s")
            params.append(args.enabled)

        if not updates:
            print("\nNo updates specified. Use --rule-config or --enabled.")
            return

        params.append(args.rule_id)
        query = f"UPDATE validation_rule SET {', '.join(updates)} WHERE rule_id = %s"

        with pool.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                updated_count = cur.rowcount
            conn.commit()

        if updated_count > 0:
            print(f"\nRule updated successfully: {args.rule_id}")
        else:
            print(f"\nNo rule found with ID: {args.rule_id}")

    except Exception as e:
        logger.error(f"Error updating rule: {e}", exc_info=True)
        print(f"\nError: {e}")
        sys.exit(1)

    finally:
        pool.close()


def disable_rule_command(args):
    """
    Disable a validation rule.

    Args:
        args: Command line arguments
    """
    logger.info(f"Disabling validation rule: {args.rule_id}")

    pool = DatabaseConnectionPool(
        host=args.db_host,
        port=args.db_port,
        database=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )

    try:
        pool.open()

        query = "UPDATE validation_rule SET enabled = FALSE WHERE rule_id = %s"

        with pool.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (args.rule_id,))
                updated_count = cur.rowcount
            conn.commit()

        if updated_count > 0:
            print(f"\nRule disabled successfully: {args.rule_id}")
        else:
            print(f"\nNo rule found with ID: {args.rule_id}")

    except Exception as e:
        logger.error(f"Error disabling rule: {e}", exc_info=True)
        print(f"\nError: {e}")
        sys.exit(1)

    finally:
        pool.close()


def main():
    """Main entry point for admin CLI."""
    parser = argparse.ArgumentParser(
        description="Admin CLI for data validation pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Global database connection options
    parser.add_argument(
        "--db-host",
        default="localhost",
        help="Database host (default: localhost)"
    )
    parser.add_argument(
        "--db-port",
        type=int,
        default=5432,
        help="Database port (default: 5432)"
    )
    parser.add_argument(
        "--db-name",
        default="datawarehouse",
        help="Database name (default: datawarehouse)"
    )
    parser.add_argument(
        "--db-user",
        default="pipeline",
        help="Database user (default: pipeline)"
    )
    parser.add_argument(
        "--db-password",
        default="dev_password",
        help="Database password"
    )

    # Subcommands
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # trace-record command
    trace_parser = subparsers.add_parser(
        "trace-record",
        help="Trace complete lineage of a record"
    )
    trace_parser.add_argument(
        "--record-id",
        required=True,
        help="Record ID to trace"
    )
    trace_parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of events to display (default: 100)"
    )

    # audit-report command
    report_parser = subparsers.add_parser(
        "audit-report",
        help="Generate audit report with statistics"
    )
    report_parser.add_argument(
        "--source",
        help="Filter by source ID (optional)"
    )
    report_parser.add_argument(
        "--detailed",
        action="store_true",
        help="Show detailed event logs"
    )
    report_parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of events to show in detailed view (default: 100)"
    )

    # quarantine-review command
    quarantine_parser = subparsers.add_parser(
        "quarantine-review",
        help="Review quarantined records"
    )
    quarantine_parser.add_argument(
        "--source",
        help="Filter by source ID (optional)"
    )
    quarantine_parser.add_argument(
        "--error-type",
        help="Filter by error type (optional)"
    )
    quarantine_parser.add_argument(
        "--unreviewed-only",
        action="store_true",
        help="Show only unreviewed records"
    )
    quarantine_parser.add_argument(
        "--show-errors",
        action="store_true",
        help="Display sample error messages"
    )
    quarantine_parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum number of records to display (default: 50)"
    )

    # quarantine-stats command
    stats_parser = subparsers.add_parser(
        "quarantine-stats",
        help="Display quarantine statistics"
    )
    stats_parser.add_argument(
        "--source",
        help="Filter by source ID (optional)"
    )

    # reprocess command
    reprocess_parser = subparsers.add_parser(
        "reprocess",
        help="Reprocess quarantined records"
    )
    reprocess_parser.add_argument(
        "--source",
        required=True,
        help="Source ID to reprocess"
    )
    reprocess_parser.add_argument(
        "--record-ids",
        help="Comma-separated list of specific record IDs to reprocess (optional)"
    )
    reprocess_parser.add_argument(
        "--error-type",
        help="Filter by error type (optional)"
    )
    reprocess_parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of records to reprocess (optional)"
    )

    # mark-reviewed command
    reviewed_parser = subparsers.add_parser(
        "mark-reviewed",
        help="Mark quarantine records as reviewed"
    )
    reviewed_parser.add_argument(
        "--record-ids",
        required=True,
        help="Comma-separated list of record IDs to mark as reviewed"
    )

    # add-rule command
    add_rule_parser = subparsers.add_parser(
        "add-rule",
        help="Add a new validation rule"
    )
    add_rule_parser.add_argument(
        "--rule-file",
        required=True,
        help="Path to YAML or JSON file containing rule definition"
    )

    # update-rule command
    update_rule_parser = subparsers.add_parser(
        "update-rule",
        help="Update an existing validation rule"
    )
    update_rule_parser.add_argument(
        "--rule-id",
        required=True,
        help="Rule ID to update"
    )
    update_rule_parser.add_argument(
        "--rule-config",
        help="Path to JSON file with new rule configuration (optional)"
    )
    update_rule_parser.add_argument(
        "--enabled",
        type=bool,
        help="Set rule enabled status (optional)"
    )

    # disable-rule command
    disable_rule_parser = subparsers.add_parser(
        "disable-rule",
        help="Disable a validation rule"
    )
    disable_rule_parser.add_argument(
        "--rule-id",
        required=True,
        help="Rule ID to disable"
    )

    # Parse arguments
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Route to command handler
    try:
        if args.command == "trace-record":
            trace_record_command(args)
        elif args.command == "audit-report":
            audit_report_command(args)
        elif args.command == "quarantine-review":
            quarantine_review_command(args)
        elif args.command == "quarantine-stats":
            quarantine_stats_command(args)
        elif args.command == "reprocess":
            reprocess_command(args)
        elif args.command == "mark-reviewed":
            mark_reviewed_command(args)
        elif args.command == "add-rule":
            add_rule_command(args)
        elif args.command == "update-rule":
            update_rule_command(args)
        elif args.command == "disable-rule":
            disable_rule_command(args)
        else:
            parser.print_help()
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(130)


if __name__ == "__main__":
    main()
