"""
Input validation utilities for the data validation pipeline.

Provides reusable validation functions for common input types like
record IDs, source IDs, and quarantine IDs to ensure data integrity
and prevent injection attacks.
"""

import re
from typing import Optional


class ValidationError(ValueError):
    """Raised when input validation fails."""
    pass


def validate_record_id(record_id: str, field_name: str = "record_id") -> str:
    """
    Validate a record ID.

    Record IDs must be non-empty strings containing only alphanumeric
    characters, hyphens, and underscores.

    Args:
        record_id: The record ID to validate
        field_name: Name of the field (for error messages)

    Returns:
        The validated record ID (stripped of whitespace)

    Raises:
        ValidationError: If validation fails

    Examples:
        >>> validate_record_id("record-123")
        'record-123'
        >>> validate_record_id("user_456")
        'user_456'
        >>> validate_record_id("invalid id!")  # doctest: +SKIP
        ValidationError: record_id contains invalid characters
    """
    if not record_id or not isinstance(record_id, str):
        raise ValidationError(f"{field_name} must be a non-empty string")

    # Strip whitespace
    record_id = record_id.strip()

    if not record_id:
        raise ValidationError(f"{field_name} cannot be empty or whitespace-only")

    # Allow alphanumeric, hyphens, underscores, and dots
    if not re.match(r'^[a-zA-Z0-9_\-\.]+$', record_id):
        raise ValidationError(
            f"{field_name} contains invalid characters. "
            "Only alphanumeric, hyphens, underscores, and dots are allowed."
        )

    # Prevent excessively long IDs (DOS protection)
    if len(record_id) > 255:
        raise ValidationError(f"{field_name} exceeds maximum length of 255 characters")

    return record_id


def validate_source_id(source_id: str, field_name: str = "source_id") -> str:
    """
    Validate a source ID.

    Source IDs must be non-empty strings containing only alphanumeric
    characters, hyphens, and underscores.

    Args:
        source_id: The source ID to validate
        field_name: Name of the field (for error messages)

    Returns:
        The validated source ID (stripped of whitespace)

    Raises:
        ValidationError: If validation fails

    Examples:
        >>> validate_source_id("kafka-prod")
        'kafka-prod'
        >>> validate_source_id("csv_input_1")
        'csv_input_1'
    """
    # Source IDs have the same validation rules as record IDs
    return validate_record_id(source_id, field_name)


def validate_quarantine_ids(quarantine_ids: list[int], field_name: str = "quarantine_ids") -> list[int]:
    """
    Validate a list of quarantine IDs.

    Quarantine IDs must be positive integers.

    Args:
        quarantine_ids: List of quarantine IDs to validate
        field_name: Name of the field (for error messages)

    Returns:
        The validated list of quarantine IDs

    Raises:
        ValidationError: If validation fails

    Examples:
        >>> validate_quarantine_ids([1, 2, 3])
        [1, 2, 3]
        >>> validate_quarantine_ids([])
        []
        >>> validate_quarantine_ids([1, -5])  # doctest: +SKIP
        ValidationError: quarantine_ids must contain only positive integers
    """
    if not isinstance(quarantine_ids, list):
        raise ValidationError(f"{field_name} must be a list")

    # Empty list is valid (no records to reprocess)
    if not quarantine_ids:
        return quarantine_ids

    # Check all elements are positive integers
    for i, qid in enumerate(quarantine_ids):
        if not isinstance(qid, int):
            raise ValidationError(
                f"{field_name}[{i}] must be an integer, got {type(qid).__name__}"
            )
        if qid <= 0:
            raise ValidationError(
                f"{field_name}[{i}] must be a positive integer, got {qid}"
            )

    # Prevent excessively large lists (DOS protection)
    if len(quarantine_ids) > 10000:
        raise ValidationError(
            f"{field_name} exceeds maximum length of 10000 items. "
            "For bulk operations, use batch processing."
        )

    return quarantine_ids


def validate_limit(limit: int, field_name: str = "limit", max_limit: int = 10000) -> int:
    """
    Validate a limit parameter for queries.

    Args:
        limit: The limit value to validate
        field_name: Name of the field (for error messages)
        max_limit: Maximum allowed limit value

    Returns:
        The validated limit value

    Raises:
        ValidationError: If validation fails

    Examples:
        >>> validate_limit(100)
        100
        >>> validate_limit(0)  # doctest: +SKIP
        ValidationError: limit must be a positive integer
        >>> validate_limit(20000)  # doctest: +SKIP
        ValidationError: limit exceeds maximum of 10000
    """
    if not isinstance(limit, int):
        raise ValidationError(f"{field_name} must be an integer, got {type(limit).__name__}")

    if limit <= 0:
        raise ValidationError(f"{field_name} must be a positive integer, got {limit}")

    if limit > max_limit:
        raise ValidationError(f"{field_name} exceeds maximum of {max_limit}")

    return limit


def validate_offset(offset: int, field_name: str = "offset") -> int:
    """
    Validate an offset parameter for queries.

    Args:
        offset: The offset value to validate
        field_name: Name of the field (for error messages)

    Returns:
        The validated offset value

    Raises:
        ValidationError: If validation fails

    Examples:
        >>> validate_offset(0)
        0
        >>> validate_offset(100)
        100
        >>> validate_offset(-5)  # doctest: +SKIP
        ValidationError: offset must be a non-negative integer
    """
    if not isinstance(offset, int):
        raise ValidationError(f"{field_name} must be an integer, got {type(offset).__name__}")

    if offset < 0:
        raise ValidationError(f"{field_name} must be a non-negative integer, got {offset}")

    return offset


def sanitize_sql_identifier(identifier: str, field_name: str = "identifier") -> str:
    """
    Sanitize an SQL identifier (table name, column name, etc.).

    This is a strict validation that only allows safe SQL identifiers.
    Use this for dynamic table/column names to prevent SQL injection.

    Args:
        identifier: The identifier to sanitize
        field_name: Name of the field (for error messages)

    Returns:
        The validated identifier

    Raises:
        ValidationError: If validation fails

    Examples:
        >>> sanitize_sql_identifier("my_table")
        'my_table'
        >>> sanitize_sql_identifier("users_2024")
        'users_2024'
        >>> sanitize_sql_identifier("table; DROP TABLE users;")  # doctest: +SKIP
        ValidationError: identifier contains invalid characters
    """
    if not identifier or not isinstance(identifier, str):
        raise ValidationError(f"{field_name} must be a non-empty string")

    identifier = identifier.strip()

    # SQL identifiers: alphanumeric and underscores only, must start with letter or underscore
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
        raise ValidationError(
            f"{field_name} contains invalid characters. "
            "SQL identifiers must start with a letter or underscore and contain only "
            "alphanumeric characters and underscores."
        )

    # Prevent excessively long identifiers
    if len(identifier) > 63:  # PostgreSQL limit
        raise ValidationError(f"{field_name} exceeds PostgreSQL maximum length of 63 characters")

    # Prevent SQL reserved keywords (basic check)
    reserved_keywords = {
        "select", "insert", "update", "delete", "drop", "create", "alter",
        "table", "database", "index", "view", "user", "grant", "revoke"
    }
    if identifier.lower() in reserved_keywords:
        raise ValidationError(
            f"{field_name} '{identifier}' is a reserved SQL keyword. "
            "Please use a different name."
        )

    return identifier


def validate_file_path(file_path: str, field_name: str = "file_path", allow_wildcards: bool = False) -> str:
    """
    Validate a file path for security.

    Prevents path traversal attacks and ensures the path is reasonable.

    Args:
        file_path: The file path to validate
        field_name: Name of the field (for error messages)
        allow_wildcards: Whether to allow wildcards (* and ?) in the path

    Returns:
        The validated file path (stripped of whitespace)

    Raises:
        ValidationError: If validation fails

    Examples:
        >>> validate_file_path("/data/input.csv")
        '/data/input.csv'
        >>> validate_file_path("/data/*.csv", allow_wildcards=True)
        '/data/*.csv'
        >>> validate_file_path("../../../etc/passwd")  # doctest: +SKIP
        ValidationError: file_path contains path traversal characters
    """
    if not file_path or not isinstance(file_path, str):
        raise ValidationError(f"{field_name} must be a non-empty string")

    file_path = file_path.strip()

    if not file_path:
        raise ValidationError(f"{field_name} cannot be empty or whitespace-only")

    # Prevent path traversal
    if ".." in file_path:
        raise ValidationError(f"{field_name} contains path traversal characters (..)")

    # Check for null bytes (security)
    if "\x00" in file_path:
        raise ValidationError(f"{field_name} contains null bytes")

    # Check wildcards
    if not allow_wildcards and ("*" in file_path or "?" in file_path):
        raise ValidationError(
            f"{field_name} contains wildcards (* or ?). "
            "If this is intentional, set allow_wildcards=True."
        )

    # Prevent excessively long paths
    if len(file_path) > 4096:  # Linux PATH_MAX
        raise ValidationError(f"{field_name} exceeds maximum length of 4096 characters")

    return file_path
