"""
Unit tests for validation rules.

Includes property-based testing with hypothesis for validators.
"""

import re

import pytest
from hypothesis import given
from hypothesis import strategies as st

from src.core.validators import (
    CustomValidator,
    RangeValidator,
    RegexValidator,
    RequiredFieldValidator,
    TypeValidator,
    ValidationError,
)


class TestRequiredFieldValidator:
    """Tests for RequiredFieldValidator"""

    def test_valid_required_field(self):
        """Test validation passes for present field"""
        validator = RequiredFieldValidator("name")
        record = {"name": "John Doe"}
        validator.validate(record["name"], record)  # Should not raise

    def test_missing_field_raises_error(self):
        """Test validation fails for missing field"""
        validator = RequiredFieldValidator("name")
        record = {"age": 30}  # name is missing

        with pytest.raises(ValidationError) as exc_info:
            validator.validate(None, record)

        assert "missing" in str(exc_info.value).lower()
        assert exc_info.value.field_name == "name"

    def test_null_field_raises_error(self):
        """Test validation fails for null field"""
        validator = RequiredFieldValidator("name")
        record = {"name": None}

        with pytest.raises(ValidationError) as exc_info:
            validator.validate(record["name"], record)

        assert "null" in str(exc_info.value).lower()

    def test_empty_string_raises_error(self):
        """Test validation fails for empty string by default"""
        validator = RequiredFieldValidator("name")
        record = {"name": "   "}  # Only whitespace

        with pytest.raises(ValidationError) as exc_info:
            validator.validate(record["name"], record)

        assert "empty" in str(exc_info.value).lower()

    def test_empty_string_allowed_when_configured(self):
        """Test empty string passes when allow_empty_string=True"""
        validator = RequiredFieldValidator("name", {"allow_empty_string": True})
        record = {"name": ""}
        validator.validate(record["name"], record)  # Should not raise

    @given(st.text(min_size=1).filter(lambda s: s.strip() != ""))
    def test_property_any_nonempty_string_passes(self, value):
        """Property test: any non-empty, non-whitespace string should pass"""
        validator = RequiredFieldValidator("field")
        record = {"field": value}
        validator.validate(record["field"], record)  # Should not raise


class TestTypeValidator:
    """Tests for TypeValidator"""

    def test_valid_integer_type(self):
        """Test integer type validation"""
        validator = TypeValidator("age", {"expected_type": "int"})
        record = {"age": 25}
        validator.validate(record["age"], record)  # Should not raise

    def test_valid_float_type(self):
        """Test float type validation"""
        validator = TypeValidator("amount", {"expected_type": "float"})
        record = {"amount": 99.99}
        validator.validate(record["amount"], record)  # Should not raise

    def test_string_to_int_coercion(self):
        """Test string is coerced to int"""
        validator = TypeValidator("age", {"expected_type": "int", "coerce": True})
        record = {"age": "25"}
        validator.validate(record["age"], record)  # Should coerce and pass

    def test_string_to_float_coercion(self):
        """Test string is coerced to float"""
        validator = TypeValidator("amount", {"expected_type": "float", "coerce": True})
        record = {"amount": "99.99"}
        validator.validate(record["amount"], record)  # Should coerce and pass

    def test_boolean_coercion_from_string(self):
        """Test boolean coercion from various string values"""
        validator = TypeValidator("active", {"expected_type": "bool", "coerce": True})

        for true_val in ["true", "True", "TRUE", "1", "yes"]:
            record = {"active": true_val}
            validator.validate(record["active"], record)  # Should coerce and pass

        for false_val in ["false", "False", "FALSE", "0", "no"]:
            record = {"active": false_val}
            validator.validate(record["active"], record)  # Should coerce and pass

    def test_invalid_coercion_raises_error(self):
        """Test invalid coercion raises ValidationError"""
        validator = TypeValidator("age", {"expected_type": "int", "coerce": True})
        record = {"age": "not_a_number"}

        with pytest.raises(ValidationError) as exc_info:
            validator.validate(record["age"], record)

        assert "coerce" in str(exc_info.value).lower()

    def test_strict_type_check_without_coercion(self):
        """Test strict type check fails without coercion"""
        validator = TypeValidator("age", {"expected_type": "int", "coerce": False})
        record = {"age": "25"}  # String, not int

        with pytest.raises(ValidationError) as exc_info:
            validator.validate(record["age"], record)

        assert "expected" in str(exc_info.value).lower()

    def test_none_value_skipped(self):
        """Test None values are skipped (handled by required_field)"""
        validator = TypeValidator("age", {"expected_type": "int"})
        record = {"age": None}
        validator.validate(record["age"], record)  # Should not raise

    @given(st.integers())
    def test_property_all_integers_valid(self, value):
        """Property test: all integers should pass integer validation"""
        validator = TypeValidator("field", {"expected_type": "int"})
        record = {"field": value}
        validator.validate(record["field"], record)

    @given(st.floats(allow_nan=False, allow_infinity=False))
    def test_property_all_floats_valid(self, value):
        """Property test: all floats should pass float validation"""
        validator = TypeValidator("field", {"expected_type": "float"})
        record = {"field": value}
        validator.validate(record["field"], record)


class TestRangeValidator:
    """Tests for RangeValidator"""

    def test_value_within_range(self):
        """Test value within range passes"""
        validator = RangeValidator("age", {"min": 0, "max": 120})
        record = {"age": 25}
        validator.validate(record["age"], record)  # Should not raise

    def test_value_at_min_boundary(self):
        """Test value at minimum boundary passes"""
        validator = RangeValidator("age", {"min": 0, "max": 120})
        record = {"age": 0}
        validator.validate(record["age"], record)  # Should not raise

    def test_value_at_max_boundary(self):
        """Test value at maximum boundary passes"""
        validator = RangeValidator("age", {"min": 0, "max": 120})
        record = {"age": 120}
        validator.validate(record["age"], record)  # Should not raise

    def test_value_below_min_raises_error(self):
        """Test value below minimum raises ValidationError"""
        validator = RangeValidator("age", {"min": 0, "max": 120})
        record = {"age": -1}

        with pytest.raises(ValidationError) as exc_info:
            validator.validate(record["age"], record)

        assert "less than" in str(exc_info.value).lower() or "minimum" in str(exc_info.value).lower()

    def test_value_above_max_raises_error(self):
        """Test value above maximum raises ValidationError"""
        validator = RangeValidator("age", {"min": 0, "max": 120})
        record = {"age": 121}

        with pytest.raises(ValidationError) as exc_info:
            validator.validate(record["age"], record)

        assert "exceed" in str(exc_info.value).lower() or "maximum" in str(exc_info.value).lower()

    def test_exclusive_min_boundary(self):
        """Test exclusive minimum boundary"""
        validator = RangeValidator("amount", {"min_exclusive": 0})

        # Value exactly at boundary should fail
        with pytest.raises(ValidationError):
            validator.validate(0, {"amount": 0})

        # Value above boundary should pass
        validator.validate(0.01, {"amount": 0.01})

    def test_exclusive_max_boundary(self):
        """Test exclusive maximum boundary"""
        validator = RangeValidator("amount", {"max_exclusive": 100})

        # Value exactly at boundary should fail
        with pytest.raises(ValidationError):
            validator.validate(100, {"amount": 100})

        # Value below boundary should pass
        validator.validate(99.99, {"amount": 99.99})

    def test_non_numeric_value_raises_error(self):
        """Test non-numeric value raises ValidationError"""
        validator = RangeValidator("age", {"min": 0, "max": 120})
        record = {"age": "not_a_number"}

        with pytest.raises(ValidationError) as exc_info:
            validator.validate(record["age"], record)

        assert "numeric" in str(exc_info.value).lower()

    @given(st.floats(min_value=0, max_value=100, allow_nan=False, allow_infinity=False))
    def test_property_values_in_range_pass(self, value):
        """Property test: all values in range should pass"""
        validator = RangeValidator("field", {"min": 0, "max": 100})
        record = {"field": value}
        validator.validate(record["field"], record)

    @given(st.floats(max_value=-0.01, allow_nan=False, allow_infinity=False))
    def test_property_values_below_range_fail(self, value):
        """Property test: all values below range should fail"""
        validator = RangeValidator("field", {"min": 0})
        record = {"field": value}

        with pytest.raises(ValidationError):
            validator.validate(record["field"], record)


class TestRegexValidator:
    """Tests for RegexValidator"""

    def test_valid_pattern_match(self):
        """Test value matching pattern passes"""
        validator = RegexValidator("email", {"pattern": r"^[a-z]+@[a-z]+\.[a-z]+$"})
        record = {"email": "user@example.com"}
        validator.validate(record["email"], record)  # Should not raise

    def test_invalid_pattern_match_raises_error(self):
        """Test value not matching pattern raises ValidationError"""
        validator = RegexValidator("email", {"pattern": r"^[a-z]+@[a-z]+\.[a-z]+$"})
        record = {"email": "invalid_email"}

        with pytest.raises(ValidationError) as exc_info:
            validator.validate(record["email"], record)

        assert "does not match" in str(exc_info.value).lower()

    def test_transaction_id_pattern(self):
        """Test transaction ID pattern validation"""
        validator = RegexValidator("txn_id", {"pattern": r"^TXN[0-9]{10}$"})

        # Valid transaction ID
        validator.validate("TXN0001234567", {"txn_id": "TXN0001234567"})

        # Invalid transaction IDs
        with pytest.raises(ValidationError):
            validator.validate("TXN123", {"txn_id": "TXN123"})  # Too short

        with pytest.raises(ValidationError):
            validator.validate("INVALID", {"txn_id": "INVALID"})  # Wrong format

    def test_case_insensitive_pattern(self):
        """Test case-insensitive regex matching"""
        validator = RegexValidator("status", {"pattern": r"^(active|inactive)$", "flags": re.IGNORECASE})

        # Should match regardless of case
        validator.validate("ACTIVE", {"status": "ACTIVE"})
        validator.validate("inactive", {"status": "inactive"})
        validator.validate("Active", {"status": "Active"})

    def test_non_string_value_converted(self):
        """Test non-string values are converted to string"""
        validator = RegexValidator("id", {"pattern": r"^\d+$"})
        record = {"id": 12345}  # Integer, not string
        validator.validate(record["id"], record)  # Should convert to "12345" and pass

    @given(st.from_regex(r"^TXN[0-9]{10}$", fullmatch=True))
    def test_property_generated_patterns_match(self, value):
        """Property test: values generated from pattern should match"""
        validator = RegexValidator("field", {"pattern": r"^TXN[0-9]{10}$"})
        record = {"field": value}
        validator.validate(record["field"], record)


class TestCustomValidator:
    """Tests for CustomValidator"""

    def test_custom_validator_passes(self):
        """Test custom validator that passes"""
        def is_even(value, record):
            if value % 2 != 0:
                raise ValueError("Value must be even")

        validator = CustomValidator("number", {"validator_func": is_even})
        record = {"number": 42}
        validator.validate(record["number"], record)  # Should not raise

    def test_custom_validator_fails(self):
        """Test custom validator that fails"""
        def is_even(value, record):
            if value % 2 != 0:
                raise ValueError("Value must be even")

        validator = CustomValidator("number", {"validator_func": is_even})
        record = {"number": 43}

        with pytest.raises(ValidationError) as exc_info:
            validator.validate(record["number"], record)

        assert "must be even" in str(exc_info.value).lower()

    def test_custom_validator_with_record_context(self):
        """Test custom validator that uses record context"""
        def amount_less_than_balance(value, record):
            balance = record.get("balance", 0)
            if value > balance:
                raise ValueError(f"Amount {value} exceeds balance {balance}")

        validator = CustomValidator(
            "amount",
            {"validator_func": amount_less_than_balance}
        )

        # Should pass when amount <= balance
        record = {"amount": 50, "balance": 100}
        validator.validate(record["amount"], record)

        # Should fail when amount > balance
        record = {"amount": 150, "balance": 100}
        with pytest.raises(ValidationError):
            validator.validate(record["amount"], record)

    def test_custom_validator_custom_error_message(self):
        """Test custom validator with custom error message"""
        def always_fails(value, record):
            raise ValueError("Specific failure")

        validator = CustomValidator(
            "field",
            {
                "validator_func": always_fails,
                "error_message": "Custom error occurred"
            }
        )

        with pytest.raises(ValidationError) as exc_info:
            validator.validate("test", {"field": "test"})

        assert "custom error occurred" in str(exc_info.value).lower()

    def test_custom_validator_not_callable_raises_error(self):
        """Test that non-callable validator_func raises ValueError"""
        with pytest.raises(ValueError) as exc_info:
            CustomValidator("field", {"validator_func": "not_callable"})

        assert "callable" in str(exc_info.value).lower()
