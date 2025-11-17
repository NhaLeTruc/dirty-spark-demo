"""
Unit tests for rule engine and rule configuration.
"""

import pytest
import tempfile
from pathlib import Path

from src.core.models import DataRecord
from src.core.rules import RuleEngine, RuleConfigLoader, RuleConfigBuilder


class TestRuleEngine:
    """Tests for RuleEngine"""

    def test_validate_record_all_pass(self):
        """Test validation passes when all rules pass"""
        rules = RuleConfigBuilder() \
            .add_required_field("transaction_id") \
            .add_type_check("amount", "float") \
            .add_range("amount", min_value=0.01, max_value=10000.0) \
            .build()

        engine = RuleEngine(rules)

        record = DataRecord(
            record_id="TXN001",
            source_id="test_source",
            raw_payload={
                "transaction_id": "TXN001",
                "amount": 99.99
            }
        )

        result = engine.validate_record(record)

        assert result.passed is True
        assert len(result.failed_rules) == 0
        assert len(result.passed_rules) > 0

    def test_validate_record_with_failures(self):
        """Test validation fails when rules fail"""
        rules = RuleConfigBuilder() \
            .add_required_field("transaction_id") \
            .add_required_field("amount") \
            .build()

        engine = RuleEngine(rules)

        record = DataRecord(
            record_id="TXN001",
            source_id="test_source",
            raw_payload={
                "transaction_id": "TXN001"
                # amount is missing
            }
        )

        result = engine.validate_record(record)

        assert result.passed is False
        assert len(result.failed_rules) > 0
        assert "amount_required" in result.failed_rules

    def test_validate_record_with_type_coercion(self):
        """Test validation with type coercion"""
        rules = RuleConfigBuilder() \
            .add_type_check("amount", "float", coerce=True) \
            .build()

        engine = RuleEngine(rules)

        record = DataRecord(
            record_id="TXN001",
            source_id="test_source",
            raw_payload={
                "amount": "99.99"  # String that can be coerced
            }
        )

        result = engine.validate_record(record)

        assert result.passed is True

    def test_validate_record_range_check(self):
        """Test validation with range check"""
        rules = RuleConfigBuilder() \
            .add_range("age", min_value=0, max_value=120) \
            .build()

        engine = RuleEngine(rules)

        # Valid age
        record_valid = DataRecord(
            record_id="REC001",
            source_id="test_source",
            raw_payload={"age": 25}
        )
        result = engine.validate_record(record_valid)
        assert result.passed is True

        # Invalid age (too high)
        record_invalid = DataRecord(
            record_id="REC002",
            source_id="test_source",
            raw_payload={"age": 150}
        )
        result = engine.validate_record(record_invalid)
        assert result.passed is False

    def test_validate_record_regex_pattern(self):
        """Test validation with regex pattern"""
        rules = RuleConfigBuilder() \
            .add_regex("email", r"^[a-z]+@[a-z]+\.[a-z]+$") \
            .build()

        engine = RuleEngine(rules)

        # Valid email
        record_valid = DataRecord(
            record_id="REC001",
            source_id="test_source",
            raw_payload={"email": "user@example.com"}
        )
        result = engine.validate_record(record_valid)
        assert result.passed is True

        # Invalid email
        record_invalid = DataRecord(
            record_id="REC002",
            source_id="test_source",
            raw_payload={"email": "invalid_email"}
        )
        result = engine.validate_record(record_invalid)
        assert result.passed is False

    def test_validate_batch(self):
        """Test batch validation"""
        rules = RuleConfigBuilder() \
            .add_required_field("id") \
            .build()

        engine = RuleEngine(rules)

        records = [
            DataRecord(
                record_id="REC001",
                source_id="test_source",
                raw_payload={"id": "001"}
            ),
            DataRecord(
                record_id="REC002",
                source_id="test_source",
                raw_payload={"id": "002"}
            ),
            DataRecord(
                record_id="REC003",
                source_id="test_source",
                raw_payload={}  # Missing id
            ),
        ]

        results = engine.validate_batch(records)

        assert len(results) == 3
        assert results[0].passed is True
        assert results[1].passed is True
        assert results[2].passed is False

    def test_disabled_rules_skipped(self):
        """Test that disabled rules are skipped"""
        rules = [
            {
                "rule_name": "id_required",
                "rule_type": "required_field",
                "field_name": "id",
                "parameters": {},
                "severity": "error",
                "enabled": False  # Disabled
            }
        ]

        engine = RuleEngine(rules)

        # Record missing 'id' should pass because rule is disabled
        record = DataRecord(
            record_id="REC001",
            source_id="test_source",
            raw_payload={}
        )

        result = engine.validate_record(record)
        assert result.passed is True

    def test_warning_severity_does_not_fail_record(self):
        """Test that warning severity doesn't fail the record"""
        rules = [
            {
                "rule_name": "id_required",
                "rule_type": "required_field",
                "field_name": "id",
                "parameters": {},
                "severity": "warning",  # Warning, not error
                "enabled": True
            }
        ]

        engine = RuleEngine(rules)

        # Record missing 'id' should still pass (warning only)
        record = DataRecord(
            record_id="REC001",
            source_id="test_source",
            raw_payload={}
        )

        result = engine.validate_record(record)
        assert result.passed is True  # No errors, only warnings

    def test_get_rule_summary(self):
        """Test rule summary statistics"""
        rules = RuleConfigBuilder() \
            .add_required_field("id") \
            .add_type_check("amount", "float") \
            .add_range("amount", min_value=0) \
            .build()

        engine = RuleEngine(rules)
        summary = engine.get_rule_summary()

        assert summary["total_rules"] == 3
        assert "rules_by_type" in summary
        assert "rules_by_severity" in summary

    def test_invalid_rule_type_raises_error(self):
        """Test that invalid rule type raises ValueError"""
        rules = [
            {
                "rule_name": "invalid_rule",
                "rule_type": "unknown_type",
                "field_name": "field",
                "parameters": {},
                "severity": "error",
                "enabled": True
            }
        ]

        with pytest.raises(ValueError) as exc_info:
            RuleEngine(rules)

        assert "unknown rule type" in str(exc_info.value).lower()


class TestRuleConfigLoader:
    """Tests for RuleConfigLoader"""

    def test_load_rules_from_yaml(self):
        """Test loading rules from YAML file"""
        yaml_content = """
rules:
  transaction_id:
    - type: required_field
    - type: regex
      params:
        pattern: "^TXN[0-9]{10}$"

  amount:
    - type: required_field
    - type: type_check
      params:
        expected_type: float
    - type: range
      params:
        min: 0.01
        max: 10000.0
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            temp_path = Path(f.name)

        try:
            loader = RuleConfigLoader(temp_path)
            rules = loader.load_rules()

            assert len(rules) == 5  # 2 for transaction_id, 3 for amount
            assert any(r["field_name"] == "transaction_id" for r in rules)
            assert any(r["field_name"] == "amount" for r in rules)
            assert any(r["rule_type"] == "required_field" for r in rules)
            assert any(r["rule_type"] == "type_check" for r in rules)
            assert any(r["rule_type"] == "range" for r in rules)
            assert any(r["rule_type"] == "regex" for r in rules)

        finally:
            temp_path.unlink()

    def test_load_rules_with_severity(self):
        """Test loading rules with different severities"""
        yaml_content = """
rules:
  field1:
    - type: required_field
      severity: error
  field2:
    - type: required_field
      severity: warning
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            temp_path = Path(f.name)

        try:
            loader = RuleConfigLoader(temp_path)
            rules = loader.load_rules()

            assert len(rules) == 2
            assert rules[0]["severity"] == "error"
            assert rules[1]["severity"] == "warning"

        finally:
            temp_path.unlink()

    def test_load_rules_file_not_found(self):
        """Test that missing file raises FileNotFoundError"""
        with pytest.raises(FileNotFoundError):
            RuleConfigLoader("/nonexistent/path/rules.yaml")

    def test_load_rules_invalid_yaml(self):
        """Test that invalid YAML raises ValueError"""
        yaml_content = """
invalid:
  - this is not a valid rules section
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            temp_path = Path(f.name)

        try:
            loader = RuleConfigLoader(temp_path)
            with pytest.raises(ValueError) as exc_info:
                loader.load_rules()

            assert "rules" in str(exc_info.value).lower()

        finally:
            temp_path.unlink()

    def test_load_rules_missing_type(self):
        """Test that rule missing 'type' raises ValueError"""
        yaml_content = """
rules:
  field1:
    - params:
        min: 0
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            temp_path = Path(f.name)

        try:
            loader = RuleConfigLoader(temp_path)
            with pytest.raises(ValueError) as exc_info:
                loader.load_rules()

            assert "type" in str(exc_info.value).lower()

        finally:
            temp_path.unlink()


class TestRuleConfigBuilder:
    """Tests for RuleConfigBuilder"""

    def test_builder_fluent_interface(self):
        """Test fluent interface for building rules"""
        rules = RuleConfigBuilder() \
            .add_required_field("id") \
            .add_type_check("amount", "float") \
            .add_range("amount", min_value=0, max_value=10000) \
            .add_regex("email", r"^[a-z]+@[a-z]+\.[a-z]+$") \
            .build()

        assert len(rules) == 4
        assert rules[0]["rule_type"] == "required_field"
        assert rules[1]["rule_type"] == "type_check"
        assert rules[2]["rule_type"] == "range"
        assert rules[3]["rule_type"] == "regex"

    def test_builder_add_required_field(self):
        """Test adding required field rule"""
        rules = RuleConfigBuilder() \
            .add_required_field("name", allow_empty_string=True) \
            .build()

        assert len(rules) == 1
        assert rules[0]["field_name"] == "name"
        assert rules[0]["parameters"]["allow_empty_string"] is True

    def test_builder_add_type_check(self):
        """Test adding type check rule"""
        rules = RuleConfigBuilder() \
            .add_type_check("age", "int", coerce=False) \
            .build()

        assert len(rules) == 1
        assert rules[0]["parameters"]["expected_type"] == "int"
        assert rules[0]["parameters"]["coerce"] is False

    def test_builder_add_range(self):
        """Test adding range rule"""
        rules = RuleConfigBuilder() \
            .add_range("score", min_value=0, max_value=100) \
            .build()

        assert len(rules) == 1
        assert rules[0]["parameters"]["min"] == 0
        assert rules[0]["parameters"]["max"] == 100

    def test_builder_add_regex(self):
        """Test adding regex rule"""
        rules = RuleConfigBuilder() \
            .add_regex("phone", r"^\d{3}-\d{3}-\d{4}$") \
            .build()

        assert len(rules) == 1
        assert rules[0]["parameters"]["pattern"] == r"^\d{3}-\d{3}-\d{4}$"
