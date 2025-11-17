"""
Unit tests for warehouse operations (schema management and upsert).
"""

import pytest
from datetime import datetime

from src.core.models import WarehouseData, QuarantineRecord
from src.warehouse.schema_mgmt import SchemaManager
from src.warehouse.upsert import WarehouseWriter, QuarantineWriter


@pytest.mark.integration
class TestSchemaManager:
    """Integration tests for SchemaManager"""

    def test_create_schema_version(self, clean_db, postgres_container):
        """Test creating a new schema version"""
        from src.warehouse.connection import DatabaseConnectionPool

        pool = DatabaseConnectionPool(
            host=postgres_container.get_container_host_ip(),
            port=int(postgres_container.get_exposed_port(5432)),
            database="test_datawarehouse",
            user="test_pipeline",
            password="test_password",
        )
        pool.open()

        try:
            # Create a data source first
            pool.execute_command(
                "INSERT INTO data_source (source_id, source_type, connection_info) VALUES (%s, %s, %s)",
                ("test_source", "csv_file", '{"path": "/test"}')
            )

            manager = SchemaManager(pool)

            schema_def = {
                "fields": [
                    {"name": "id", "type": "string", "nullable": False},
                    {"name": "amount", "type": "double", "nullable": False}
                ]
            }

            schema_id = manager.create_schema_version(
                source_id="test_source",
                schema_definition=schema_def,
                inferred=True,
                confidence=0.95
            )

            assert schema_id is not None
            assert isinstance(schema_id, int)

        finally:
            pool.close()

    def test_get_active_schema(self, clean_db, postgres_container):
        """Test retrieving active schema"""
        from src.warehouse.connection import DatabaseConnectionPool

        pool = DatabaseConnectionPool(
            host=postgres_container.get_container_host_ip(),
            port=int(postgres_container.get_exposed_port(5432)),
            database="test_datawarehouse",
            user="test_pipeline",
            password="test_password",
        )
        pool.open()

        try:
            # Create data source
            pool.execute_command(
                "INSERT INTO data_source (source_id, source_type, connection_info) VALUES (%s, %s, %s)",
                ("test_source", "csv_file", '{"path": "/test"}')
            )

            manager = SchemaManager(pool)

            # Create schema version
            schema_def = {"fields": [{"name": "id", "type": "string"}]}
            manager.create_schema_version("test_source", schema_def)

            # Retrieve active schema
            active = manager.get_active_schema("test_source")

            assert active is not None
            assert active["source_id"] == "test_source"
            assert active["version"] == 1

        finally:
            pool.close()

    def test_deprecate_schema(self, clean_db, postgres_container):
        """Test deprecating a schema version"""
        from src.warehouse.connection import DatabaseConnectionPool

        pool = DatabaseConnectionPool(
            host=postgres_container.get_container_host_ip(),
            port=int(postgres_container.get_exposed_port(5432)),
            database="test_datawarehouse",
            user="test_pipeline",
            password="test_password",
        )
        pool.open()

        try:
            # Create data source
            pool.execute_command(
                "INSERT INTO data_source (source_id, source_type, connection_info) VALUES (%s, %s, %s)",
                ("test_source", "csv_file", '{"path": "/test"}')
            )

            manager = SchemaManager(pool)

            # Create schema version
            schema_def = {"fields": [{"name": "id", "type": "string"}]}
            manager.create_schema_version("test_source", schema_def)

            # Deprecate it
            manager.deprecate_schema("test_source", 1)

            # Verify deprecated
            schema = manager.get_schema_by_version("test_source", 1)
            assert schema["deprecated_at"] is not None

        finally:
            pool.close()

    def test_compare_schemas_compatible(self):
        """Test schema comparison for compatible changes"""
        manager = SchemaManager(None)  # No pool needed for comparison

        schema1 = {
            "fields": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "amount", "type": "double", "nullable": False}
            ]
        }

        schema2 = {
            "fields": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "amount", "type": "double", "nullable": False},
                {"name": "timestamp", "type": "timestamp", "nullable": True}  # Added nullable field
            ]
        }

        comparison = manager.compare_schemas(schema1, schema2)

        assert comparison["compatible"] is True
        assert "timestamp" in comparison["added_fields"]
        assert len(comparison["removed_fields"]) == 0
        assert len(comparison["type_changes"]) == 0

    def test_compare_schemas_incompatible(self):
        """Test schema comparison for incompatible changes"""
        manager = SchemaManager(None)

        schema1 = {
            "fields": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "amount", "type": "double", "nullable": False}
            ]
        }

        schema2 = {
            "fields": [
                {"name": "id", "type": "integer", "nullable": False},  # Type changed
            ]
        }

        comparison = manager.compare_schemas(schema1, schema2)

        assert comparison["compatible"] is False
        assert "amount" in comparison["removed_fields"]
        assert len(comparison["type_changes"]) > 0


@pytest.mark.integration
class TestWarehouseWriter:
    """Integration tests for WarehouseWriter"""

    def test_upsert_single_record(self, clean_db, postgres_container):
        """Test upserting a single warehouse record"""
        from src.warehouse.connection import DatabaseConnectionPool

        pool = DatabaseConnectionPool(
            host=postgres_container.get_container_host_ip(),
            port=int(postgres_container.get_exposed_port(5432)),
            database="test_datawarehouse",
            user="test_pipeline",
            password="test_password",
        )
        pool.open()

        try:
            # Create data source
            pool.execute_command(
                "INSERT INTO data_source (source_id, source_type, connection_info) VALUES (%s, %s, %s)",
                ("test_source", "csv_file", '{"path": "/test"}')
            )

            writer = WarehouseWriter(pool)

            record = WarehouseData(
                record_id="TXN001",
                source_id="test_source",
                data={"transaction_id": "TXN001", "amount": 99.99}
            )

            writer.upsert_record(record)

            # Verify inserted
            result = pool.execute_query(
                "SELECT * FROM warehouse_data WHERE record_id = %s",
                ("TXN001",)
            )

            assert len(result) == 1
            assert result[0]["record_id"] == "TXN001"

        finally:
            pool.close()

    def test_upsert_idempotency(self, clean_db, postgres_container):
        """Test that upserting same record twice is idempotent"""
        from src.warehouse.connection import DatabaseConnectionPool

        pool = DatabaseConnectionPool(
            host=postgres_container.get_container_host_ip(),
            port=int(postgres_container.get_exposed_port(5432)),
            database="test_datawarehouse",
            user="test_pipeline",
            password="test_password",
        )
        pool.open()

        try:
            # Create data source
            pool.execute_command(
                "INSERT INTO data_source (source_id, source_type, connection_info) VALUES (%s, %s, %s)",
                ("test_source", "csv_file", '{"path": "/test"}')
            )

            writer = WarehouseWriter(pool)

            record = WarehouseData(
                record_id="TXN001",
                source_id="test_source",
                data={"transaction_id": "TXN001", "amount": 99.99}
            )

            # Insert twice
            writer.upsert_record(record)
            writer.upsert_record(record)

            # Should still only have one record
            result = pool.execute_query(
                "SELECT COUNT(*) as count FROM warehouse_data WHERE record_id = %s",
                ("TXN001",)
            )

            assert result[0]["count"] == 1

        finally:
            pool.close()

    def test_upsert_batch(self, clean_db, postgres_container):
        """Test batch upserting multiple records"""
        from src.warehouse.connection import DatabaseConnectionPool

        pool = DatabaseConnectionPool(
            host=postgres_container.get_container_host_ip(),
            port=int(postgres_container.get_exposed_port(5432)),
            database="test_datawarehouse",
            user="test_pipeline",
            password="test_password",
        )
        pool.open()

        try:
            # Create data source
            pool.execute_command(
                "INSERT INTO data_source (source_id, source_type, connection_info) VALUES (%s, %s, %s)",
                ("test_source", "csv_file", '{"path": "/test"}')
            )

            writer = WarehouseWriter(pool)

            records = [
                WarehouseData(
                    record_id=f"TXN{i:03d}",
                    source_id="test_source",
                    data={"transaction_id": f"TXN{i:03d}", "amount": i * 10.0}
                )
                for i in range(1, 6)
            ]

            count = writer.upsert_batch(records)

            assert count == 5

            # Verify all inserted
            result = pool.execute_query("SELECT COUNT(*) as count FROM warehouse_data")
            assert result[0]["count"] == 5

        finally:
            pool.close()


@pytest.mark.integration
class TestQuarantineWriter:
    """Integration tests for QuarantineWriter"""

    def test_quarantine_single_record(self, clean_db, postgres_container):
        """Test quarantining a single invalid record"""
        from src.warehouse.connection import DatabaseConnectionPool

        pool = DatabaseConnectionPool(
            host=postgres_container.get_container_host_ip(),
            port=int(postgres_container.get_exposed_port(5432)),
            database="test_datawarehouse",
            user="test_pipeline",
            password="test_password",
        )
        pool.open()

        try:
            # Create data source
            pool.execute_command(
                "INSERT INTO data_source (source_id, source_type, connection_info) VALUES (%s, %s, %s)",
                ("test_source", "csv_file", '{"path": "/test"}')
            )

            writer = QuarantineWriter(pool)

            record = QuarantineRecord(
                record_id="TXN001",
                source_id="test_source",
                raw_payload={"invalid": "data"},
                failed_rules=["rule1", "rule2"],
                error_messages=["error1", "error2"]
            )

            quarantine_id = writer.quarantine_record(record)

            assert quarantine_id is not None
            assert isinstance(quarantine_id, int)

        finally:
            pool.close()

    def test_quarantine_batch(self, clean_db, postgres_container):
        """Test batch quarantining multiple records"""
        from src.warehouse.connection import DatabaseConnectionPool

        pool = DatabaseConnectionPool(
            host=postgres_container.get_container_host_ip(),
            port=int(postgres_container.get_exposed_port(5432)),
            database="test_datawarehouse",
            user="test_pipeline",
            password="test_password",
        )
        pool.open()

        try:
            # Create data source
            pool.execute_command(
                "INSERT INTO data_source (source_id, source_type, connection_info) VALUES (%s, %s, %s)",
                ("test_source", "csv_file", '{"path": "/test"}')
            )

            writer = QuarantineWriter(pool)

            records = [
                QuarantineRecord(
                    record_id=f"TXN{i:03d}",
                    source_id="test_source",
                    raw_payload={"invalid": f"data{i}"},
                    failed_rules=["rule1"],
                    error_messages=["error1"]
                )
                for i in range(1, 4)
            ]

            count = writer.quarantine_batch(records)

            assert count == 3

        finally:
            pool.close()

    def test_get_quarantine_stats(self, clean_db, postgres_container):
        """Test getting quarantine statistics"""
        from src.warehouse.connection import DatabaseConnectionPool

        pool = DatabaseConnectionPool(
            host=postgres_container.get_container_host_ip(),
            port=int(postgres_container.get_exposed_port(5432)),
            database="test_datawarehouse",
            user="test_pipeline",
            password="test_password",
        )
        pool.open()

        try:
            # Create data source
            pool.execute_command(
                "INSERT INTO data_source (source_id, source_type, connection_info) VALUES (%s, %s, %s)",
                ("test_source", "csv_file", '{"path": "/test"}')
            )

            writer = QuarantineWriter(pool)

            # Quarantine some records
            record = QuarantineRecord(
                source_id="test_source",
                raw_payload={"invalid": "data"},
                failed_rules=["rule1"],
                error_messages=["error1"]
            )

            writer.quarantine_record(record)

            # Get stats
            stats = writer.get_quarantine_stats("test_source")

            assert stats["total_quarantined"] == 1
            assert stats["unreviewed"] == 1
            assert stats["reprocess_pending"] == 0

        finally:
            pool.close()

    def test_mark_reviewed(self, clean_db, postgres_container):
        """Test marking a quarantine record as reviewed"""
        from src.warehouse.connection import DatabaseConnectionPool

        pool = DatabaseConnectionPool(
            host=postgres_container.get_container_host_ip(),
            port=int(postgres_container.get_exposed_port(5432)),
            database="test_datawarehouse",
            user="test_pipeline",
            password="test_password",
        )
        pool.open()

        try:
            # Create data source
            pool.execute_command(
                "INSERT INTO data_source (source_id, source_type, connection_info) VALUES (%s, %s, %s)",
                ("test_source", "csv_file", '{"path": "/test"}')
            )

            writer = QuarantineWriter(pool)

            record = QuarantineRecord(
                source_id="test_source",
                raw_payload={"invalid": "data"},
                failed_rules=["rule1"],
                error_messages=["error1"]
            )

            quarantine_id = writer.quarantine_record(record)

            # Mark as reviewed
            writer.mark_reviewed(quarantine_id)

            # Verify
            result = pool.execute_query(
                "SELECT reviewed FROM quarantine_record WHERE quarantine_id = %s",
                (quarantine_id,)
            )

            assert result[0]["reviewed"] is True

        finally:
            pool.close()
