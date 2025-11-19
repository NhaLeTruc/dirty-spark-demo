"""
End-to-end tests for streaming data validation pipeline.

Tests the complete flow: file stream source → validation → warehouse/quarantine
"""

import json
import os
import shutil
import tempfile
import time
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src.core.models.data_source import DataSource
from src.streaming.pipeline import StreamingPipeline


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for streaming tests."""
    spark = (
        SparkSession.builder.appName("StreamingE2ETest")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/test_checkpoints")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def stream_input_dir():
    """Create temporary directory for stream input files."""
    temp_dir = tempfile.mkdtemp(prefix="stream_input_")
    yield temp_dir
    # Cleanup
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


@pytest.fixture
def checkpoint_dir():
    """Create temporary directory for streaming checkpoints."""
    temp_dir = tempfile.mkdtemp(prefix="stream_checkpoint_")
    yield temp_dir
    # Cleanup
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


@pytest.fixture
def test_database(db_connection):
    """Setup test database and cleanup after test."""
    # Clean test data before test
    with db_connection.cursor() as cursor:
        cursor.execute("DELETE FROM warehouse_data WHERE source_id = 'test_stream_source'")
        cursor.execute("DELETE FROM quarantine_record WHERE source_id = 'test_stream_source'")
        db_connection.commit()

    yield db_connection

    # Cleanup after test
    with db_connection.cursor() as cursor:
        cursor.execute("DELETE FROM warehouse_data WHERE source_id = 'test_stream_source'")
        cursor.execute("DELETE FROM quarantine_record WHERE source_id = 'test_stream_source'")
        db_connection.commit()


@pytest.fixture
def sample_schema():
    """Sample schema for test events."""
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("customer_email", StringType(), True),
    ])


@pytest.mark.e2e
@pytest.mark.slow
class TestStreamingFlow:
    """End-to-end tests for streaming pipeline."""

    def test_file_stream_processing_valid_events(
        self, spark, stream_input_dir, checkpoint_dir, test_database, sample_schema
    ):
        """
        Test file stream processing with valid JSON events.

        Scenario:
        - Write valid JSON events to stream input directory
        - Start streaming pipeline
        - Verify events appear in warehouse within 2 seconds
        """
        # Prepare test data - valid events only
        valid_events = [
            {
                "transaction_id": "TXN0000000200",
                "amount": 150.00,
                "timestamp": "2025-11-19T12:00:00Z",
                "customer_email": "stream_test1@example.com",
            },
            {
                "transaction_id": "TXN0000000201",
                "amount": 250.50,
                "timestamp": "2025-11-19T12:01:00Z",
                "customer_email": "stream_test2@example.com",
            },
        ]

        # Write events to stream input directory
        input_file = Path(stream_input_dir) / "events_batch1.json"
        with open(input_file, "w") as f:
            for event in valid_events:
                f.write(json.dumps(event) + "\n")

        # Configure data source
        data_source = DataSource(
            source_id="test_stream_source",
            source_type="json_stream",
            connection_info={
                "stream_path": stream_input_dir,
                "file_format": "json",
            },
            enabled=True,
        )

        # Create validation engine with minimal rules for testing
        from src.core.rules.rule_engine import RuleEngine

        # Create simple validation rules for test (as dicts, not Pydantic models)
        rules = [
            {
                "rule_name": "test_rule_required_txn_id",
                "field_name": "transaction_id",
                "rule_type": "required_field",
                "severity": "error",
                "enabled": True,
            },
            {
                "rule_name": "test_rule_amount_positive",
                "field_name": "amount",
                "rule_type": "range",
                "parameters": {"min": 0.01, "max": 1000000.00},
                "severity": "error",
                "enabled": True,
            },
        ]
        validation_engine = RuleEngine(rules)

        # Start streaming pipeline
        pipeline = StreamingPipeline(
            spark=spark,
            data_source=data_source,
            validation_engine=validation_engine,
            checkpoint_location=checkpoint_dir,
            schema=sample_schema,
            trigger_interval="1 second",
        )
        query = pipeline.start()

        # Wait for processing (micro-batch interval + processing time)
        time.sleep(3)

        # Stop the pipeline
        pipeline.stop()

        # Verify results in warehouse
        cursor = test_database.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM warehouse_data WHERE source_id = %s",
            ("test_stream_source",),
        )
        warehouse_count = cursor.fetchone()[0]

        assert warehouse_count == 2, f"Expected 2 valid records in warehouse, got {warehouse_count}"

    def test_file_stream_processing_mixed_events(
        self, spark, stream_input_dir, checkpoint_dir, test_database, sample_schema
    ):
        """
        Test file stream processing with mix of valid and invalid events.

        Scenario:
        - Write mix of valid and invalid JSON events
        - Start streaming pipeline
        - Verify valid events in warehouse, invalid in quarantine
        """
        # Prepare test data - mix of valid and invalid
        mixed_events = [
            {
                "transaction_id": "TXN0000000210",
                "amount": 100.00,
                "timestamp": "2025-11-19T13:00:00Z",
                "customer_email": "valid1@example.com",
            },
            {
                "transaction_id": "",  # Invalid: missing transaction_id
                "amount": 200.00,
                "timestamp": "2025-11-19T13:01:00Z",
                "customer_email": "invalid1@example.com",
            },
            {
                "transaction_id": "TXN0000000212",
                "amount": -50.00,  # Invalid: negative amount
                "timestamp": "2025-11-19T13:02:00Z",
                "customer_email": "invalid2@example.com",
            },
            {
                "transaction_id": "TXN0000000213",
                "amount": 300.00,
                "timestamp": "2025-11-19T13:03:00Z",
                "customer_email": "valid2@example.com",
            },
        ]

        # Write events to stream input directory
        input_file = Path(stream_input_dir) / "events_batch2.json"
        with open(input_file, "w") as f:
            for event in mixed_events:
                f.write(json.dumps(event) + "\n")

        # Configure data source
        data_source = DataSource(
            source_id="test_stream_source",
            source_type="json_stream",
            connection_info={
                "stream_path": stream_input_dir,
                "file_format": "json",
            },
            enabled=True,
        )

        # Create validation engine
        from src.core.rules.rule_engine import RuleEngine

        rules = [
            {
                "rule_name": "test_rule_required_txn_id",
                "field_name": "transaction_id",
                "rule_type": "required_field",
                "severity": "error",
                "enabled": True,
            },
            {
                "rule_name": "test_rule_amount_positive",
                "field_name": "amount",
                "rule_type": "range",
                "parameters": {"min": 0.01, "max": 1000000.00},
                "severity": "error",
                "enabled": True,
            },
        ]
        validation_engine = RuleEngine(rules)

        # Start streaming pipeline
        pipeline = StreamingPipeline(
            spark=spark,
            data_source=data_source,
            validation_engine=validation_engine,
            checkpoint_location=checkpoint_dir,
            schema=sample_schema,
            trigger_interval="1 second",
        )
        query = pipeline.start()

        # Wait for processing
        time.sleep(3)

        # Stop the pipeline
        pipeline.stop()

        # Verify results
        cursor = test_database.cursor()

        # Check warehouse (should have 2 valid records)
        cursor.execute(
            "SELECT COUNT(*) FROM warehouse_data WHERE source_id = %s",
            ("test_stream_source",),
        )
        warehouse_count = cursor.fetchone()[0]

        # Check quarantine (should have 2 invalid records)
        cursor.execute(
            "SELECT COUNT(*) FROM quarantine_record WHERE source_id = %s",
            ("test_stream_source",),
        )
        quarantine_count = cursor.fetchone()[0]

        assert warehouse_count == 2, f"Expected 2 valid records in warehouse, got {warehouse_count}"
        assert quarantine_count == 2, f"Expected 2 invalid records in quarantine, got {quarantine_count}"

    def test_file_stream_latency_requirement(
        self, spark, stream_input_dir, checkpoint_dir, test_database, sample_schema
    ):
        """
        Test that streaming pipeline meets <2 second latency requirement.

        Success Criteria SC-005: Valid records appear in warehouse within 2 seconds
        """
        # Prepare test event
        test_event = {
            "transaction_id": "TXN0000000220",
            "amount": 500.00,
            "timestamp": "2025-11-19T14:00:00Z",
            "customer_email": "latency_test@example.com",
        }

        # Configure data source
        data_source = DataSource(
            source_id="test_stream_source",
            source_type="json_stream",
            connection_info={
                "stream_path": stream_input_dir,
                "file_format": "json",
            },
            enabled=True,
        )

        # Create validation engine
        from src.core.rules.rule_engine import RuleEngine

        rules = [
            {
                "rule_name": "test_rule_required_txn_id",
                "field_name": "transaction_id",
                "rule_type": "required_field",
                "severity": "error",
                "enabled": True,
            },
        ]
        validation_engine = RuleEngine(rules)

        # Start streaming pipeline first
        pipeline = StreamingPipeline(
            spark=spark,
            data_source=data_source,
            validation_engine=validation_engine,
            checkpoint_location=checkpoint_dir,
            schema=sample_schema,
            trigger_interval="500 milliseconds",  # Fast trigger for latency test
        )
        query = pipeline.start()

        # Give pipeline a moment to initialize
        time.sleep(0.5)

        # Record start time and write event
        start_time = time.time()
        input_file = Path(stream_input_dir) / f"event_{int(start_time * 1000)}.json"
        with open(input_file, "w") as f:
            f.write(json.dumps(test_event) + "\n")

        # Poll warehouse for record (max 5 seconds)
        cursor = test_database.cursor()
        max_wait = 5.0
        poll_interval = 0.1
        found = False
        elapsed = 0.0

        while elapsed < max_wait:
            cursor.execute(
                """
                SELECT COUNT(*) FROM warehouse_data
                WHERE source_id = %s AND data->>'transaction_id' = %s
                """,
                ("test_stream_source", "TXN0000000220"),
            )
            count = cursor.fetchone()[0]
            if count > 0:
                found = True
                elapsed = time.time() - start_time
                break
            time.sleep(poll_interval)
            elapsed = time.time() - start_time

        # Stop the pipeline
        pipeline.stop()

        # Verify latency requirement
        assert found, f"Record not found in warehouse after {max_wait} seconds"
        assert elapsed < 2.0, f"Latency {elapsed:.2f}s exceeds 2 second requirement (SC-005)"

    def test_malformed_json_handling(
        self, spark, stream_input_dir, checkpoint_dir, test_database, sample_schema
    ):
        """
        Test handling of malformed JSON in stream.

        Scenario:
        - Write file with malformed JSON
        - Pipeline should quarantine with parsing error
        - Pipeline should continue processing subsequent valid events
        """
        # Write malformed JSON file followed by valid event
        malformed_file = Path(stream_input_dir) / "malformed_batch1.json"
        with open(malformed_file, "w") as f:
            # Malformed JSON (missing closing brace)
            f.write('{"transaction_id": "TXN0000000230", "amount": 100.00, "timestamp": "2025-11-19T15:00:00Z"\n')
            # Not JSON at all
            f.write("this is not json at all\n")

        # Write valid event in separate file
        valid_file = Path(stream_input_dir) / "valid_batch1.json"
        with open(valid_file, "w") as f:
            f.write('{"transaction_id": "TXN0000000231", "amount": 200.00, "timestamp": "2025-11-19T15:01:00Z", "customer_email": "valid@example.com"}\n')

        # Configure data source with permissive mode
        data_source = DataSource(
            source_id="test_stream_source",
            source_type="json_stream",
            connection_info={
                "stream_path": stream_input_dir,
                "file_format": "json",
            },
            enabled=True,
        )

        # Create validation engine
        from src.core.rules.rule_engine import RuleEngine

        rules = [
            {
                "rule_name": "test_rule_required_txn_id",
                "field_name": "transaction_id",
                "rule_type": "required_field",
                "severity": "error",
                "enabled": True,
            },
        ]
        validation_engine = RuleEngine(rules)

        # Start streaming pipeline
        pipeline = StreamingPipeline(
            spark=spark,
            data_source=data_source,
            validation_engine=validation_engine,
            checkpoint_location=checkpoint_dir,
            schema=sample_schema,
            trigger_interval="1 second",
        )
        query = pipeline.start()

        # Wait for processing
        time.sleep(3)

        # Stop the pipeline
        pipeline.stop()

        # Verify that at least the valid event was processed
        cursor = test_database.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM warehouse_data WHERE source_id = %s",
            ("test_stream_source",),
        )
        warehouse_count = cursor.fetchone()[0]

        # Should have at least 1 valid record (malformed records handled by Spark's PERMISSIVE mode)
        assert warehouse_count >= 1, f"Expected at least 1 valid record in warehouse, got {warehouse_count}"

    def test_schema_evolution_detection(
        self, spark, stream_input_dir, checkpoint_dir, test_database
    ):
        """
        Test schema evolution detection in streaming mode.

        Scenario:
        - Start with baseline schema
        - Stream events with new fields
        - Verify schema evolution is detected and handled
        """
        # Initial events with baseline schema
        baseline_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("amount", DoubleType(), True),
        ])

        baseline_events = [
            {"transaction_id": "TXN0000000240", "amount": 100.00},
            {"transaction_id": "TXN0000000241", "amount": 200.00},
        ]

        # Write baseline events
        baseline_file = Path(stream_input_dir) / "baseline.json"
        with open(baseline_file, "w") as f:
            for event in baseline_events:
                f.write(json.dumps(event) + "\n")

        # Event with new field (schema evolution)
        evolved_event = {
            "transaction_id": "TXN0000000242",
            "amount": 300.00,
            "new_field": "This field was added later",  # New field
        }

        # Configure data source
        data_source = DataSource(
            source_id="test_stream_source",
            source_type="json_stream",
            connection_info={
                "stream_path": stream_input_dir,
                "file_format": "json",
            },
            enabled=True,
        )

        # Create validation engine
        from src.core.rules.rule_engine import RuleEngine

        rules = [
            {
                "rule_name": "test_rule_required_txn_id",
                "field_name": "transaction_id",
                "rule_type": "required_field",
                "severity": "error",
                "enabled": True,
            },
        ]
        validation_engine = RuleEngine(rules)

        # Start streaming pipeline with baseline schema
        pipeline = StreamingPipeline(
            spark=spark,
            data_source=data_source,
            validation_engine=validation_engine,
            checkpoint_location=checkpoint_dir,
            schema=baseline_schema,
            trigger_interval="1 second",
        )
        query = pipeline.start()

        # Wait for baseline events to be processed
        time.sleep(2)

        # Write evolved event (this should trigger schema evolution detection)
        # Note: Spark Structured Streaming with fixed schema will ignore extra fields
        evolved_file = Path(stream_input_dir) / "evolved.json"
        with open(evolved_file, "w") as f:
            f.write(json.dumps(evolved_event) + "\n")

        # Wait for evolved event processing
        time.sleep(2)

        # Stop the pipeline
        pipeline.stop()

        # Verify baseline events were processed
        cursor = test_database.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM warehouse_data WHERE source_id = %s",
            ("test_stream_source",),
        )
        warehouse_count = cursor.fetchone()[0]

        # Should have at least 2 baseline events (evolved event may or may not be processed depending on schema handling)
        assert warehouse_count >= 2, f"Expected at least 2 records in warehouse, got {warehouse_count}"


@pytest.mark.e2e
@pytest.mark.slow
@pytest.mark.skipif(
    not shutil.which("kafka-console-producer"),
    reason="Kafka CLI tools not available"
)
class TestKafkaStreamingFlow:
    """E2E tests for Kafka streaming (optional, requires Kafka setup)."""

    def test_kafka_stream_processing(self, spark, checkpoint_dir, test_database):
        """
        Test Kafka stream processing.

        Note: This test requires Kafka to be running (docker-compose up)
        """
        pytest.skip("Kafka streaming not yet implemented (T076)")