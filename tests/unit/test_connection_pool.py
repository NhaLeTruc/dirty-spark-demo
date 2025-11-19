"""
Unit tests for database connection pool

Tests the PostgreSQL connection pool functionality using testcontainers.
"""
import pytest

from src.warehouse.connection import (
    DatabaseConnectionPool,
    close_pool,
    get_pool,
    initialize_pool,
)


@pytest.mark.integration
def test_connection_pool_initialization(postgres_container):
    """Test that connection pool initializes correctly"""
    pool = DatabaseConnectionPool(
        host=postgres_container.get_container_host_ip(),
        port=int(postgres_container.get_exposed_port(5432)),
        database="test_datawarehouse",
        user="test_pipeline",
        password="test_password",
        min_size=2,
        max_size=5,
    )

    pool.open()

    assert pool._pool is not None
    assert pool._pool.min_size == 2
    assert pool._pool.max_size == 5

    pool.close()


@pytest.mark.integration
def test_get_connection(postgres_container):
    """Test getting a connection from the pool"""
    pool = DatabaseConnectionPool(
        host=postgres_container.get_container_host_ip(),
        port=int(postgres_container.get_exposed_port(5432)),
        database="test_datawarehouse",
        user="test_pipeline",
        password="test_password",
    )

    pool.open()

    with pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 as test")
            result = cur.fetchone()
            assert result["test"] == 1

    pool.close()


@pytest.mark.integration
def test_execute_query(postgres_container):
    """Test executing a query using the pool"""
    pool = DatabaseConnectionPool(
        host=postgres_container.get_container_host_ip(),
        port=int(postgres_container.get_exposed_port(5432)),
        database="test_datawarehouse",
        user="test_pipeline",
        password="test_password",
    )

    pool.open()

    result = pool.execute_query("SELECT 42 as answer")
    assert len(result) == 1
    assert result[0]["answer"] == 42

    pool.close()


@pytest.mark.integration
def test_execute_command(clean_db, postgres_container):
    """Test executing INSERT/UPDATE commands"""
    pool = DatabaseConnectionPool(
        host=postgres_container.get_container_host_ip(),
        port=int(postgres_container.get_exposed_port(5432)),
        database="test_datawarehouse",
        user="test_pipeline",
        password="test_password",
    )

    pool.open()

    # Insert a data source
    rowcount = pool.execute_command(
        """
        INSERT INTO data_source (source_id, source_type, connection_info)
        VALUES (%s, %s, %s)
        """,
        ("test_source", "csv_file", '{"path": "/test"}'),
    )

    assert rowcount == 1

    # Verify insert
    result = pool.execute_query(
        "SELECT source_id FROM data_source WHERE source_id = %s",
        ("test_source",)
    )
    assert len(result) == 1
    assert result[0]["source_id"] == "test_source"

    pool.close()


@pytest.mark.integration
def test_context_manager(postgres_container):
    """Test using pool as context manager"""
    with DatabaseConnectionPool(
        host=postgres_container.get_container_host_ip(),
        port=int(postgres_container.get_exposed_port(5432)),
        database="test_datawarehouse",
        user="test_pipeline",
        password="test_password",
    ) as pool:
        result = pool.execute_query("SELECT 1 as test")
        assert result[0]["test"] == 1

    # Pool should be closed after context
    with pytest.raises(RuntimeError):
        pool.execute_query("SELECT 1")


@pytest.mark.integration
def test_global_pool_initialization(postgres_container):
    """Test global pool initialization and retrieval"""
    # Initialize global pool
    pool = initialize_pool(
        host=postgres_container.get_container_host_ip(),
        port=int(postgres_container.get_exposed_port(5432)),
        database="test_datawarehouse",
        user="test_pipeline",
        password="test_password",
    )

    assert pool is not None

    # Get global pool
    retrieved_pool = get_pool()
    assert retrieved_pool is pool

    # Close global pool
    close_pool()

    # Should raise error after closing
    with pytest.raises(RuntimeError):
        get_pool()
