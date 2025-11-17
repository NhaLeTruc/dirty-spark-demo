"""
Unit tests for Pydantic data models.

Tests all core models for validation, type coercion, and constraint enforcement.
"""

import pytest
from datetime import datetime
from pydantic import ValidationError

from src.core.models import (
    DataSource,
    ValidationRule,
    DataRecord,
    WarehouseData,
    QuarantineRecord,
    ValidationResult,
    SchemaVersion,
    AuditLog,
)


class TestDataSource:
    """Tests for DataSource model"""

    def test_valid_data_source(self):
        """Test creating a valid DataSource"""
        source = DataSource(
            source_id="test_source",
            source_type="csv_file",
            connection_info={"path": "/data/test.csv"},
        )
        assert source.source_id == "test_source"
        assert source.source_type == "csv_file"
        assert source.enabled is True
        assert isinstance(source.created_at, datetime)

    def test_invalid_source_type(self):
        """Test that invalid source_type raises ValidationError"""
        with pytest.raises(ValidationError) as exc_info:
            DataSource(
                source_id="test_source",
                source_type="invalid_type",
                connection_info={"path": "/data/test.csv"},
            )
        assert "source_type" in str(exc_info.value)

    def test_empty_source_id(self):
        """Test that empty source_id raises ValidationError"""
        with pytest.raises(ValidationError) as exc_info:
            DataSource(
                source_id="",
                source_type="csv_file",
                connection_info={"path": "/data/test.csv"},
            )
        assert "source_id" in str(exc_info.value)


class TestValidationRule:
    """Tests for ValidationRule model"""

    def test_valid_validation_rule(self):
        """Test creating a valid ValidationRule"""
        rule = ValidationRule(
            rule_name="require_transaction_id",
            rule_type="required_field",
            field_name="transaction_id",
        )
        assert rule.rule_name == "require_transaction_id"
        assert rule.rule_type == "required_field"
        assert rule.enabled is True
        assert rule.severity == "error"

    def test_rule_with_parameters(self):
        """Test ValidationRule with parameters"""
        rule = ValidationRule(
            rule_name="amount_range_check",
            rule_type="range",
            field_name="amount",
            parameters={"min": 0.01, "max": 10000.0},
        )
        assert rule.parameters["min"] == 0.01
        assert rule.parameters["max"] == 10000.0

    def test_invalid_rule_type(self):
        """Test that invalid rule_type raises ValidationError"""
        with pytest.raises(ValidationError) as exc_info:
            ValidationRule(
                rule_name="test_rule",
                rule_type="invalid_type",
                field_name="test_field",
            )
        assert "rule_type" in str(exc_info.value)

    def test_invalid_severity(self):
        """Test that invalid severity raises ValidationError"""
        with pytest.raises(ValidationError) as exc_info:
            ValidationRule(
                rule_name="test_rule",
                rule_type="required_field",
                field_name="test_field",
                severity="critical",
            )
        assert "severity" in str(exc_info.value)


class TestDataRecord:
    """Tests for DataRecord model"""

    def test_valid_data_record(self):
        """Test creating a valid DataRecord"""
        record = DataRecord(
            record_id="TXN001",
            source_id="test_source",
            raw_payload={"transaction_id": "TXN001", "amount": "99.99"},
        )
        assert record.record_id == "TXN001"
        assert record.validation_status == "pending"
        assert isinstance(record.processing_timestamp, datetime)

    def test_data_record_with_processed_payload(self):
        """Test DataRecord with processed payload"""
        record = DataRecord(
            record_id="TXN001",
            source_id="test_source",
            raw_payload={"amount": "99.99"},
            processed_payload={"amount": 99.99},
            validation_status="valid",
        )
        assert record.processed_payload["amount"] == 99.99
        assert record.validation_status == "valid"

    def test_invalid_validation_status(self):
        """Test that invalid validation_status raises ValidationError"""
        with pytest.raises(ValidationError) as exc_info:
            DataRecord(
                record_id="TXN001",
                source_id="test_source",
                raw_payload={"test": "data"},
                validation_status="unknown_status",
            )
        assert "validation_status" in str(exc_info.value)


class TestWarehouseData:
    """Tests for WarehouseData model"""

    def test_valid_warehouse_data(self):
        """Test creating valid WarehouseData"""
        warehouse = WarehouseData(
            record_id="TXN001",
            source_id="test_source",
            data={"transaction_id": "TXN001", "amount": 99.99},
        )
        assert warehouse.record_id == "TXN001"
        assert warehouse.data["amount"] == 99.99
        assert isinstance(warehouse.processed_at, datetime)

    def test_warehouse_data_with_checksum(self):
        """Test WarehouseData with checksum"""
        warehouse = WarehouseData(
            record_id="TXN001",
            source_id="test_source",
            data={"test": "data"},
            checksum="abc123",
        )
        assert warehouse.checksum == "abc123"


class TestQuarantineRecord:
    """Tests for QuarantineRecord model"""

    def test_valid_quarantine_record(self):
        """Test creating a valid QuarantineRecord"""
        quarantine = QuarantineRecord(
            source_id="test_source",
            raw_payload={"invalid": "data"},
            failed_rules=["rule1", "rule2"],
            error_messages=["error1", "error2"],
        )
        assert len(quarantine.failed_rules) == 2
        assert len(quarantine.error_messages) == 2
        assert quarantine.reviewed is False
        assert quarantine.reprocess_requested is False

    def test_quarantine_mismatched_arrays(self):
        """Test that mismatched failed_rules and error_messages raises ValidationError"""
        with pytest.raises(ValidationError) as exc_info:
            QuarantineRecord(
                source_id="test_source",
                raw_payload={"invalid": "data"},
                failed_rules=["rule1", "rule2"],
                error_messages=["error1"],  # Length mismatch
            )
        assert "length" in str(exc_info.value).lower()

    def test_quarantine_empty_rules(self):
        """Test that empty failed_rules raises ValidationError"""
        with pytest.raises(ValidationError) as exc_info:
            QuarantineRecord(
                source_id="test_source",
                raw_payload={"invalid": "data"},
                failed_rules=[],
                error_messages=[],
            )
        assert "failed_rules" in str(exc_info.value)


class TestValidationResult:
    """Tests for ValidationResult model"""

    def test_valid_validation_result_passed(self):
        """Test creating a ValidationResult for passed validation"""
        result = ValidationResult(
            record_id="TXN001",
            passed=True,
            passed_rules=["rule1", "rule2"],
            failed_rules=[],
        )
        assert result.passed is True
        assert len(result.passed_rules) == 2
        assert len(result.failed_rules) == 0

    def test_valid_validation_result_failed(self):
        """Test creating a ValidationResult for failed validation"""
        result = ValidationResult(
            record_id="TXN001",
            passed=False,
            passed_rules=["rule1"],
            failed_rules=["rule2", "rule3"],
        )
        assert result.passed is False
        assert len(result.failed_rules) == 2

    def test_validation_result_with_transformations(self):
        """Test ValidationResult with transformations"""
        result = ValidationResult(
            record_id="TXN001",
            passed=True,
            passed_rules=["rule1"],
            failed_rules=[],
            transformations_applied=["date_normalization", "type_coercion"],
            confidence_score=0.95,
        )
        assert len(result.transformations_applied) == 2
        assert result.confidence_score == 0.95

    def test_validation_result_inconsistent_state(self):
        """Test that passed=True with failed_rules raises ValidationError"""
        with pytest.raises(ValidationError) as exc_info:
            ValidationResult(
                record_id="TXN001",
                passed=True,
                passed_rules=["rule1"],
                failed_rules=["rule2"],  # Inconsistent: passed=True but has failed rules
            )
        assert "passed" in str(exc_info.value).lower()

    def test_validation_result_invalid_confidence(self):
        """Test that confidence_score outside 0-1 range raises ValidationError"""
        with pytest.raises(ValidationError) as exc_info:
            ValidationResult(
                record_id="TXN001",
                passed=True,
                confidence_score=1.5,  # Invalid: > 1.0
            )
        assert "confidence_score" in str(exc_info.value)


class TestSchemaVersion:
    """Tests for SchemaVersion model"""

    def test_valid_schema_version(self):
        """Test creating a valid SchemaVersion"""
        schema = SchemaVersion(
            source_id="test_source",
            version=1,
            schema_definition={
                "fields": [
                    {"name": "id", "type": "string", "nullable": False},
                    {"name": "amount", "type": "double", "nullable": True},
                ]
            },
        )
        assert schema.version == 1
        assert schema.inferred is True
        assert len(schema.schema_definition["fields"]) == 2

    def test_schema_version_with_confidence(self):
        """Test SchemaVersion with confidence score"""
        schema = SchemaVersion(
            source_id="test_source",
            version=2,
            schema_definition={"fields": []},
            inferred=True,
            confidence=0.87,
        )
        assert schema.confidence == 0.87

    def test_schema_version_invalid_version(self):
        """Test that version <= 0 raises ValidationError"""
        with pytest.raises(ValidationError) as exc_info:
            SchemaVersion(
                source_id="test_source",
                version=0,  # Invalid: must be > 0
                schema_definition={"fields": []},
            )
        assert "version" in str(exc_info.value)

    def test_schema_version_invalid_confidence(self):
        """Test that confidence outside 0-1 range raises ValidationError"""
        with pytest.raises(ValidationError) as exc_info:
            SchemaVersion(
                source_id="test_source",
                version=1,
                schema_definition={"fields": []},
                confidence=-0.1,  # Invalid: < 0
            )
        assert "confidence" in str(exc_info.value)


class TestAuditLog:
    """Tests for AuditLog model"""

    def test_valid_audit_log(self):
        """Test creating a valid AuditLog"""
        log = AuditLog(
            record_id="TXN001",
            source_id="test_source",
            transformation_type="type_coercion",
            field_name="amount",
            old_value="99.99",
            new_value="99.99",
            rule_applied="amount_type_check",
        )
        assert log.record_id == "TXN001"
        assert log.transformation_type == "type_coercion"
        assert isinstance(log.created_at, datetime)

    def test_audit_log_minimal(self):
        """Test AuditLog with minimal required fields"""
        log = AuditLog(
            record_id="TXN001",
            source_id="test_source",
            transformation_type="normalization",
        )
        assert log.field_name is None
        assert log.old_value is None
        assert log.new_value is None
        assert log.rule_applied is None
