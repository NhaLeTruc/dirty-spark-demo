"""
PostgreSQL connection pool management using psycopg3

This module provides a connection pool for efficient database access
with automatic connection lifecycle management.
"""
import os
from typing import Optional
from contextlib import contextmanager
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


class DatabaseConnectionPool:
    """
    PostgreSQL connection pool manager using psycopg3

    Provides efficient connection pooling with automatic reconnection
    and connection lifecycle management.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        min_size: int = 2,
        max_size: int = 10,
        timeout: float = 30.0,
    ) -> None:
        """
        Initialize database connection pool

        Args:
            host: Database host (defaults to env var DB_HOST)
            port: Database port (defaults to env var DB_PORT)
            database: Database name (defaults to env var DB_NAME)
            user: Database user (defaults to env var DB_USER)
            password: Database password (defaults to env var DB_PASSWORD)
            min_size: Minimum pool size
            max_size: Maximum pool size
            timeout: Connection timeout in seconds
        """
        self.host = host or os.getenv("DB_HOST", "localhost")
        self.port = port or int(os.getenv("DB_PORT", "5432"))
        self.database = database or os.getenv("DB_NAME", "datawarehouse")
        self.user = user or os.getenv("DB_USER", "pipeline")
        self.password = password or os.getenv("DB_PASSWORD")

        # Security: Require password to be explicitly set
        if not self.password:
            raise ValueError(
                "Database password must be provided. "
                "Set DB_PASSWORD environment variable or pass to constructor."
            )

        self.min_size = min_size
        self.max_size = max_size
        self.timeout = timeout

        # Build connection string
        self.conninfo = (
            f"host={self.host} "
            f"port={self.port} "
            f"dbname={self.database} "
            f"user={self.user} "
            f"password={self.password} "
            f"connect_timeout={int(self.timeout)}"
        )

        # Initialize connection pool
        self._pool: Optional[ConnectionPool] = None

    def open(self) -> None:
        """Open the connection pool"""
        if self._pool is None:
            self._pool = ConnectionPool(
                conninfo=self.conninfo,
                min_size=self.min_size,
                max_size=self.max_size,
                timeout=self.timeout,
                kwargs={"row_factory": dict_row},  # Return rows as dictionaries
            )
            # Open the pool (wait for min_size connections)
            self._pool.open()

    def close(self) -> None:
        """Close the connection pool"""
        if self._pool is not None:
            self._pool.close()
            self._pool = None

    @contextmanager
    def get_connection(self):
        """
        Get a connection from the pool

        Yields:
            psycopg.Connection: Database connection

        Raises:
            RuntimeError: If pool is not open
        """
        if self._pool is None:
            raise RuntimeError("Connection pool is not open. Call open() first.")

        with self._pool.connection() as conn:
            yield conn

    @contextmanager
    def get_cursor(self):
        """
        Get a cursor from a pooled connection

        Yields:
            psycopg.Cursor: Database cursor

        Raises:
            RuntimeError: If pool is not open
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                yield cur

    def execute_query(self, query: str, params: Optional[tuple] = None) -> list[dict]:
        """
        Execute a SELECT query and return results

        Args:
            query: SQL SELECT query
            params: Query parameters (optional)

        Returns:
            List of dictionaries (one per row)
        """
        with self.get_cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def execute_command(
        self, command: str, params: Optional[tuple] = None
    ) -> int:
        """
        Execute an INSERT/UPDATE/DELETE command

        Args:
            command: SQL command
            params: Command parameters (optional)

        Returns:
            Number of rows affected
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(command, params)
                rowcount = cur.rowcount
            conn.commit()
            return rowcount

    def execute_batch(
        self, command: str, params_list: list[tuple]
    ) -> None:
        """
        Execute a command in batch mode for multiple parameter sets

        Args:
            command: SQL command
            params_list: List of parameter tuples
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(command, params_list)
            conn.commit()

    def __enter__(self):
        """Context manager entry"""
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
        return False


# Singleton instance for application-wide use
_global_pool: Optional[DatabaseConnectionPool] = None


def get_pool() -> DatabaseConnectionPool:
    """
    Get the global database connection pool

    Returns:
        DatabaseConnectionPool instance

    Raises:
        RuntimeError: If pool has not been initialized
    """
    global _global_pool
    if _global_pool is None:
        raise RuntimeError(
            "Database pool not initialized. Call initialize_pool() first."
        )
    return _global_pool


def initialize_pool(**kwargs) -> DatabaseConnectionPool:
    """
    Initialize the global database connection pool

    Args:
        **kwargs: Arguments passed to DatabaseConnectionPool constructor

    Returns:
        Initialized DatabaseConnectionPool instance
    """
    global _global_pool
    if _global_pool is not None:
        _global_pool.close()

    _global_pool = DatabaseConnectionPool(**kwargs)
    _global_pool.open()
    return _global_pool


def close_pool() -> None:
    """Close the global database connection pool"""
    global _global_pool
    if _global_pool is not None:
        _global_pool.close()
        _global_pool = None
