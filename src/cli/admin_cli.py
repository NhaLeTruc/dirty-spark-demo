"""
Admin CLI for managing the data validation pipeline.

Usage:
    python -m src.cli.admin_cli trace-record --record-id <record_id> [options]
    python -m src.cli.admin_cli audit-report --source <source_id> [options]
    python -m src.cli.admin_cli quarantine-review [options]
"""

import argparse
import sys
from datetime import datetime
from typing import Any

from src.observability.logger import get_logger
from src.warehouse.audit import (
    get_audit_summary,
    query_audit_logs_by_record,
    query_audit_logs_by_source,
    query_audit_logs_by_transformation_type,
)
from src.warehouse.connection import DatabaseConnectionPool

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

    # quarantine-review command (placeholder for future implementation)
    quarantine_parser = subparsers.add_parser(
        "quarantine-review",
        help="Review quarantined records (to be implemented in Phase 6)"
    )
    quarantine_parser.add_argument(
        "--source",
        help="Filter by source ID (optional)"
    )
    quarantine_parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum number of records to display (default: 50)"
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
            print("Quarantine review functionality will be implemented in Phase 6 (User Story 3)")
            sys.exit(0)
        else:
            parser.print_help()
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(130)


if __name__ == "__main__":
    main()
