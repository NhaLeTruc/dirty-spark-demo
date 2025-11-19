"""
Unit tests to verify the code quality improvement implementations.

This module tests:
- Input validation utilities
- ValidationResult warnings field
- Database connection context managers
- Validation warnings metrics
- Exception handling improvements
"""

import pytest
from src.utils.validation import (
    validate_record_id,
    validate_source_id,
    validate_quarantine_ids,
    validate_limit,
    validate_offset,
    sanitize_sql_identifier,
    validate_file_path,
    ValidationError,
)
from src.core.models import ValidationResult
from src.warehouse.connection import (
    get_connection_context,
    get_cursor_context,
)
from src.observability.metrics import validation_warnings_total
from src.core.schema.inference import SchemaInferrer


# =======================
# VALIDATION UTILITIES TESTS
# =======================

class TestValidationUtilities:
    """Test the input validation utilities."""

    def test_validate_record_id_valid(self):
        """Test valid record IDs."""
        assert validate_record_id("record-123") == "record-123"
        assert validate_record_id("user_456") == "user_456"
        assert validate_record_id("TXN.2024.001") == "TXN.2024.001"
        assert validate_record_id("  spaces  ") == "spaces"  # Strips whitespace

    def test_validate_record_id_invalid(self):
        """Test invalid record IDs."""
        with pytest.raises(ValidationError, match="must be a non-empty string"):
            validate_record_id("")

        with pytest.raises(ValidationError, match="cannot be empty"):
            validate_record_id("   ")

        with pytest.raises(ValidationError, match="invalid characters"):
            validate_record_id("invalid id!")

        with pytest.raises(ValidationError, match="invalid characters"):
            validate_record_id("sql'; DROP TABLE--")

    def test_validate_source_id(self):
        """Test source ID validation."""
        assert validate_source_id("kafka_prod") == "kafka_prod"
        assert validate_source_id("csv-input-1") == "csv-input-1"

    def test_validate_quarantine_ids_valid(self):
        """Test valid quarantine ID lists."""
        assert validate_quarantine_ids([1, 2, 3]) == [1, 2, 3]
        assert validate_quarantine_ids([]) == []
        assert validate_quarantine_ids([100]) == [100]

    def test_validate_quarantine_ids_invalid(self):
        """Test invalid quarantine ID lists."""
        with pytest.raises(ValidationError, match="must be a list"):
            validate_quarantine_ids("not a list")

        with pytest.raises(ValidationError, match="must be an integer"):
            validate_quarantine_ids([1, "2", 3])

        with pytest.raises(ValidationError, match="must be a positive integer"):
            validate_quarantine_ids([1, -5, 3])

        with pytest.raises(ValidationError, match="must be a positive integer"):
            validate_quarantine_ids([0])

    def test_validate_limit(self):
        """Test limit validation."""
        assert validate_limit(100) == 100
        assert validate_limit(1) == 1
        assert validate_limit(10000) == 10000

        with pytest.raises(ValidationError, match="must be a positive integer"):
            validate_limit(0)

        with pytest.raises(ValidationError, match="must be a positive integer"):
            validate_limit(-5)

        with pytest.raises(ValidationError, match="exceeds maximum"):
            validate_limit(20000)

    def test_validate_offset(self):
        """Test offset validation."""
        assert validate_offset(0) == 0
        assert validate_offset(100) == 100

        with pytest.raises(ValidationError, match="must be a non-negative integer"):
            validate_offset(-5)

    def test_sanitize_sql_identifier_valid(self):
        """Test valid SQL identifiers."""
        assert sanitize_sql_identifier("my_table") == "my_table"
        assert sanitize_sql_identifier("users_2024") == "users_2024"
        assert sanitize_sql_identifier("_private") == "_private"

    def test_sanitize_sql_identifier_invalid(self):
        """Test invalid SQL identifiers."""
        with pytest.raises(ValidationError, match="invalid characters"):
            sanitize_sql_identifier("table; DROP TABLE users;")

        with pytest.raises(ValidationError, match="invalid characters"):
            sanitize_sql_identifier("123_table")  # Can't start with number

        with pytest.raises(ValidationError, match="invalid characters"):
            sanitize_sql_identifier("table-name")  # Hyphen not allowed

        with pytest.raises(ValidationError, match="reserved SQL keyword"):
            sanitize_sql_identifier("select")

    def test_validate_file_path_valid(self):
        """Test valid file paths."""
        assert validate_file_path("/data/input.csv") == "/data/input.csv"
        assert validate_file_path("/var/log/app.log") == "/var/log/app.log"
        assert validate_file_path("/data/*.csv", allow_wildcards=True) == "/data/*.csv"

    def test_validate_file_path_invalid(self):
        """Test invalid file paths."""
        with pytest.raises(ValidationError, match="path traversal"):
            validate_file_path("../../../etc/passwd")

        with pytest.raises(ValidationError, match="contains wildcards"):
            validate_file_path("/data/*.csv")  # No wildcards by default

        with pytest.raises(ValidationError, match="null bytes"):
            validate_file_path("/data/file\x00.csv")


# =======================
# VALIDATION RESULT TESTS
# =======================

class TestValidationResultWarnings:
    """Test ValidationResult warnings field."""

    def test_validation_result_with_warnings(self):
        """Test creating ValidationResult with warnings."""
        result = ValidationResult(
            record_id="test-123",
            passed=True,
            passed_rules=["rule1", "rule2"],
            failed_rules=[],
            warnings=["warning1", "warning2"],
            transformations_applied=["transform1"],
        )

        assert result.record_id == "test-123"
        assert result.passed is True
        assert result.warnings == ["warning1", "warning2"]
        assert len(result.warnings) == 2

    def test_validation_result_without_warnings(self):
        """Test ValidationResult with empty warnings list."""
        result = ValidationResult(
            record_id="test-456",
            passed=True,
            passed_rules=["rule1"],
            failed_rules=[],
            warnings=[],  # Empty warnings
            transformations_applied=[],
        )

        assert result.warnings == []
        assert len(result.warnings) == 0

    def test_validation_result_default_warnings(self):
        """Test ValidationResult warnings defaults to empty list."""
        result = ValidationResult(
            record_id="test-789",
            passed=True,
            passed_rules=["rule1"],
            failed_rules=[],
            transformations_applied=[],
        )

        # Should default to empty list
        assert result.warnings == []


# =======================
# CONNECTION CONTEXT MANAGER TESTS
# =======================

class TestConnectionContextManagers:
    """Test database connection context managers."""

    def test_connection_context_exists(self):
        """Test that get_connection_context exists and is callable."""
        assert callable(get_connection_context)

    def test_cursor_context_exists(self):
        """Test that get_cursor_context exists and is callable."""
        assert callable(get_cursor_context)


# =======================
# METRICS TESTS
# =======================

class TestValidationWarningsMetric:
    """Test validation warnings Prometheus metric."""

    def test_metric_exists(self):
        """Test that validation_warnings_total metric exists."""
        assert validation_warnings_total is not None

    def test_metric_name(self):
        """Test metric has correct name."""
        assert validation_warnings_total._name == "pipeline_validation_warnings_total"

    def test_metric_labels(self):
        """Test metric has correct labels."""
        # Prometheus counters store label names in _labelnames
        expected_labels = {"source_id", "rule_name"}
        actual_labels = set(validation_warnings_total._labelnames)
        assert actual_labels == expected_labels


# =======================
# EXCEPTION HANDLING TESTS
# =======================

class TestInferenceExceptionHandling:
    """Test inference.py exception handling improvements."""

    def test_schema_inferrer_imports(self):
        """Test that SchemaInferrer imports without errors."""
        assert SchemaInferrer is not None

    def test_schema_inferrer_is_class(self):
        """Test that SchemaInferrer is a class."""
        assert isinstance(SchemaInferrer, type)
