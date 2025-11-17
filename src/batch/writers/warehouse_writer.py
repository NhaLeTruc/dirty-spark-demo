"""
Batch warehouse writer for valid records.

Writes validated records to warehouse_data table using bulk operations.
"""

from pyspark.sql import DataFrame

from src.core.models import WarehouseData
from src.observability.lineage import LineageTracker
from src.warehouse.connection import DatabaseConnectionPool
from src.warehouse.upsert import WarehouseWriter as UpsertWriter


class BatchWarehouseWriter:
    """
    Writes valid records from Spark DataFrame to warehouse in bulk.
    """

    def __init__(
        self,
        pool: DatabaseConnectionPool,
        lineage_tracker: LineageTracker | None = None
    ):
        """
        Initialize batch warehouse writer.

        Args:
            pool: Database connection pool
            lineage_tracker: Optional lineage tracker for audit trail
        """
        self.pool = pool
        self.upsert_writer = UpsertWriter(pool)
        self.lineage_tracker = lineage_tracker

    def write_dataframe(
        self,
        df: DataFrame,
        source_id: str,
        schema_version_id: int | None = None
    ) -> int:
        """
        Write Spark DataFrame to warehouse.

        Args:
            df: DataFrame with valid records
            source_id: Data source ID
            schema_version_id: Schema version ID

        Returns:
            Number of records written
        """
        # Collect DataFrame to list of rows
        rows = df.collect()

        if not rows:
            return 0

        # Convert rows to WarehouseData models
        warehouse_records = []
        for row in rows:
            # Convert row to dictionary
            record_dict = row.asDict()

            # Extract record_id
            record_id = record_dict.get("transaction_id") or record_dict.get("record_id")
            if not record_id:
                # Skip records without ID
                continue

            # Create WarehouseData model
            warehouse_record = WarehouseData(
                record_id=str(record_id),
                source_id=source_id,
                data=record_dict,
                schema_version_id=schema_version_id
            )
            warehouse_records.append(warehouse_record)

        # Use upsert writer for bulk insert
        count = self.upsert_writer.upsert_batch(warehouse_records)

        # Track warehouse writes in lineage
        if self.lineage_tracker:
            for record in warehouse_records:
                self.lineage_tracker.track_warehouse_write(
                    record_id=record.record_id,
                    source_id=source_id,
                    schema_version_id=schema_version_id
                )

        return count

    def write_to_table_direct(
        self,
        df: DataFrame,
        source_id: str,
        schema_version_id: int | None = None
    ) -> int:
        """
        Write DataFrame directly to PostgreSQL using JDBC (alternative approach).

        This method uses Spark's built-in JDBC writer for potentially better performance
        on very large datasets.

        Args:
            df: DataFrame with valid records
            source_id: Data source ID
            schema_version_id: Schema version ID

        Returns:
            Number of records written
        """
        # Get PostgreSQL connection details from pool
        # This is a placeholder for JDBC-based bulk insert
        # In practice, you'd configure JDBC properties and use:
        # df.write.jdbc(url, table, mode="append", properties=jdbc_props)

        # For now, fall back to the standard approach
        return self.write_dataframe(df, source_id, schema_version_id)
