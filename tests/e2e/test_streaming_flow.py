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
from typing import Dict, List

import pytest
from pyspark.sql import SparkSession

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


@pytest.mark.e2e
@pytest.mark.slow
class TestStreamingFlow:
    """End-to-end tests for streaming pipeline."""

    def test_file_stream_processing_valid_events(
        self, spark, stream_input_dir, checkpoint_dir, test_database
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

        # Start streaming pipeline (this will be implemented in T077)
        # For now, this test will FAIL as expected (TDD approach)
        # pipeline = StreamingPipeline(
        #     spark=spark,
        #     data_source=data_source,
        #     checkpoint_location=checkpoint_dir,
        # )
        # query = pipeline.start()

        # Wait for processing (micro-batch interval + processing time)
        # time.sleep(3)

        # query.stop()

        # Verify results in warehouse
        cursor = test_database.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM warehouse_data WHERE source_id = %s",
            ("test_stream_source",)
        )
        warehouse_count = cursor.fetchone()[0]

        # This assertion will FAIL until streaming pipeline is implemented
        # assert warehouse_count == 2, f"Expected 2 valid records in warehouse, got {warehouse_count}"

        # For now, mark as expected failure
        pytest.skip("Streaming pipeline not yet implemented (T077)")

    def test_file_stream_processing_mixed_events(
        self, spark, stream_input_dir, checkpoint_dir, test_database
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

        # This test will FAIL until implementation is complete
        pytest.skip("Streaming pipeline not yet implemented (T077-T084)")

    def test_file_stream_latency_requirement(
        self, spark, stream_input_dir, checkpoint_dir, test_database
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

        # Record start time
        start_time = time.time()

        # Write event to stream input
        input_file = Path(stream_input_dir) / f"event_{int(start_time)}.json"
        with open(input_file, "w") as f:
            f.write(json.dumps(test_event) + "\n")

        # Poll warehouse for record (max 5 seconds)
        cursor = test_database.cursor()
        max_wait = 5.0
        poll_interval = 0.1
        elapsed = 0.0
        found = False

        while elapsed < max_wait:
            cursor.execute(
                """
                SELECT COUNT(*) FROM warehouse_data
                WHERE source_id = %s AND data->>'transaction_id' = %s
                """,
                ("test_stream_source", "TXN0000000220")
            )
            count = cursor.fetchone()[0]
            if count > 0:
                found = True
                break
            time.sleep(poll_interval)
            elapsed = time.time() - start_time

        # This test will FAIL until streaming is implemented
        # assert found, f"Record not found in warehouse after {max_wait} seconds"
        # assert elapsed < 2.0, f"Latency {elapsed:.2f}s exceeds 2 second requirement"

        pytest.skip("Streaming pipeline not yet implemented (T077-T084)")

    def test_malformed_json_handling(
        self, spark, stream_input_dir, checkpoint_dir, test_database
    ):
        """
        Test handling of malformed JSON in stream.

        Scenario:
        - Write file with malformed JSON
        - Pipeline should quarantine with parsing error
        - Pipeline should continue processing subsequent valid events
        """
        # Write malformed JSON file
        input_file = Path(stream_input_dir) / "malformed.json"
        with open(input_file, "w") as f:
            f.write('{"transaction_id": "TXN0000000230", "amount": 100.00, "timestamp": "2025-11-19T15:00:00Z"\n')  # Missing closing brace
            f.write('this is not json at all\n')
            f.write('{"transaction_id": "TXN0000000231", "amount": 200.00, "timestamp": "2025-11-19T15:01:00Z", "customer_email": "valid@example.com"}\n')  # Valid

        # This test will FAIL until error handling is implemented
        pytest.skip("Streaming pipeline error handling not yet implemented (T077-T084)")

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
        baseline_event = {
            "transaction_id": "TXN0000000240",
            "amount": 100.00,
            "timestamp": "2025-11-19T16:00:00Z",
            "customer_email": "baseline@example.com",
        }

        # Event with new field
        evolved_event = {
            "transaction_id": "TXN0000000241",
            "amount": 150.00,
            "timestamp": "2025-11-19T16:01:00Z",
            "customer_email": "evolved@example.com",
            "payment_method": "credit_card",  # New field
            "merchant_category": "retail",  # Another new field
        }

        # This test will FAIL until schema evolution is implemented
        pytest.skip("Schema evolution not yet implemented (T079-T082)")


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
