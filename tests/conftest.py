"""
Pytest configuration and fixtures for dirty-spark-pipeline tests

This module provides shared fixtures for unit, integration, and E2E tests.
"""
import os
import pytest
from typing import Generator
from testcontainers.postgres import PostgresContainer
from testcontainers.kafka import KafkaContainer
from pyspark.sql import SparkSession
import psycopg


# =======================
# PYTEST CONFIGURATION
# =======================

def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "unit: Unit tests that don't require external services"
    )
    config.addinivalue_line(
        "markers", "integration: Integration tests that require Docker containers"
    )
    config.addinivalue_line(
        "markers", "e2e: End-to-end tests that test the full pipeline"
    )
    config.addinivalue_line(
        "markers", "slow: Tests that take more than 5 seconds to run"
    )


# =======================
# SPARK FIXTURES
# =======================

@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:
    """
    Create a Spark session for testing with local mode

    Yields:
        SparkSession configured for local testing
    """
    spark = (
        SparkSession.builder
        .appName("dirty-spark-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.ui.enabled", "false")  # Disable UI for tests
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .getOrCreate()
    )

    # Set log level to WARN to reduce test output noise
    spark.sparkContext.setLogLevel("WARN")

    yield spark

    # Cleanup
    spark.stop()


@pytest.fixture(scope="function")
def spark_test_session(spark_session) -> SparkSession:
    """
    Function-scoped Spark session that clears catalog between tests

    Args:
        spark_session: Session-scoped Spark session

    Returns:
        SparkSession for individual test
    """
    # Clear any cached tables
    spark_session.catalog.clearCache()

    return spark_session


# =======================
# DATABASE FIXTURES (Testcontainers)
# =======================

@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """
    Start PostgreSQL container for integration tests

    Yields:
        PostgresContainer instance with initialized database
    """
    with PostgresContainer(
        image="postgres:16.2-alpine",
        username="test_pipeline",
        password="test_password",
        dbname="test_datawarehouse"
    ) as postgres:
        # Wait for container to be ready
        postgres.get_connection_url()

        # Run init script
        init_sql_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "docker",
            "init-db.sql"
        )

        if os.path.exists(init_sql_path):
            with open(init_sql_path, 'r') as f:
                init_sql = f.read()

            # Execute init script
            conn_url = postgres.get_connection_url()
            with psycopg.connect(conn_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(init_sql)
                conn.commit()

        yield postgres


@pytest.fixture(scope="function")
def db_connection(postgres_container) -> Generator[psycopg.Connection, None, None]:
    """
    Provide a database connection for a single test

    Args:
        postgres_container: PostgreSQL container fixture

    Yields:
        psycopg Connection object
    """
    conn_url = postgres_container.get_connection_url()
    with psycopg.connect(conn_url) as conn:
        yield conn
        # Rollback any uncommitted changes after test
        conn.rollback()


@pytest.fixture(scope="function")
def clean_db(db_connection) -> Generator[psycopg.Connection, None, None]:
    """
    Provide a clean database by truncating all tables before each test

    Args:
        db_connection: Database connection fixture

    Yields:
        psycopg Connection object with clean database
    """
    with db_connection.cursor() as cur:
        # Truncate all tables (order matters due to foreign keys)
        cur.execute("TRUNCATE TABLE audit_log CASCADE")
        cur.execute("TRUNCATE TABLE quarantine_record CASCADE")
        cur.execute("TRUNCATE TABLE warehouse_data CASCADE")
        cur.execute("TRUNCATE TABLE validation_rule CASCADE")
        cur.execute("TRUNCATE TABLE schema_version CASCADE")
        cur.execute("TRUNCATE TABLE data_source CASCADE")

        db_connection.commit()

    yield db_connection


# =======================
# KAFKA FIXTURES (Testcontainers)
# =======================

@pytest.fixture(scope="session")
def kafka_container() -> Generator[KafkaContainer, None, None]:
    """
    Start Kafka container for streaming integration tests

    Yields:
        KafkaContainer instance
    """
    with KafkaContainer(image="confluentinc/cp-kafka:7.6.0") as kafka:
        # Wait for Kafka to be ready
        kafka.get_bootstrap_server()
        yield kafka


@pytest.fixture(scope="function")
def kafka_bootstrap_servers(kafka_container) -> str:
    """
    Get Kafka bootstrap servers for a test

    Args:
        kafka_container: Kafka container fixture

    Returns:
        Bootstrap servers connection string
    """
    return kafka_container.get_bootstrap_server()


# =======================
# FILE FIXTURES
# =======================

@pytest.fixture(scope="session")
def test_data_dir() -> str:
    """
    Get path to test data fixtures directory

    Returns:
        Path to tests/fixtures directory
    """
    return os.path.join(os.path.dirname(__file__), "fixtures")


@pytest.fixture(scope="function")
def temp_checkpoint_dir(tmp_path) -> str:
    """
    Provide temporary directory for Spark checkpoints

    Args:
        tmp_path: pytest tmp_path fixture

    Returns:
        Path to temporary checkpoint directory
    """
    checkpoint_dir = tmp_path / "checkpoints"
    checkpoint_dir.mkdir()
    return str(checkpoint_dir)


# =======================
# CONFIGURATION FIXTURES
# =======================

@pytest.fixture(scope="session")
def test_env_vars():
    """
    Set test environment variables

    This fixture loads test.env and sets environment variables
    """
    from dotenv import load_dotenv

    env_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "config",
        "test.env"
    )

    if os.path.exists(env_path):
        load_dotenv(env_path, override=True)


# =======================
# CLEANUP FIXTURES
# =======================

@pytest.fixture(autouse=True, scope="function")
def cleanup_temp_files(tmp_path):
    """
    Automatically clean up temporary files after each test

    Args:
        tmp_path: pytest tmp_path fixture
    """
    yield
    # Cleanup happens automatically with tmp_path
