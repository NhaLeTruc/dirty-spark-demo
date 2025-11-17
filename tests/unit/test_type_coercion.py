"""
Unit tests for type coercion functionality.

Tests the TypeValidator's ability to coerce types and handle failures.
"""

import pytest

from src.core.validators import TypeValidator, ValidationError


class TestTypeCoercion:
    """Tests for type coercion in TypeValidator"""

    def test_string_to_int_coercion(self):
        """Test successful coercion from string to int"""
        validator = TypeValidator("age", {"expected_type": "int", "coerce": True})

        # Should not raise error - coercion succeeds
        validator.validate("25", {"age": "25"})
        validator.validate("100", {"age": "100"})

    def test_string_to_float_coercion(self):
        """Test successful coercion from string to float"""
        validator = TypeValidator("amount", {"expected_type": "float", "coerce": True})

        # Should not raise error - coercion succeeds
        validator.validate("99.99", {"amount": "99.99"})
        validator.validate("150.50", {"amount": "150.50"})

    def test_string_to_bool_coercion_true_values(self):
        """Test coercion of various string values to True"""
        validator = TypeValidator("active", {"expected_type": "bool", "coerce": True})

        # All these should coerce to True without raising error
        for true_val in ["true", "True", "TRUE", "1", "yes", "Yes", "YES"]:
            validator.validate(true_val, {"active": true_val})

    def test_string_to_bool_coercion_false_values(self):
        """Test coercion of various string values to False"""
        validator = TypeValidator("active", {"expected_type": "bool", "coerce": True})

        # All these should coerce to False without raising error
        for false_val in ["false", "False", "FALSE", "0", "no", "No", "NO"]:
            validator.validate(false_val, {"active": false_val})

    def test_invalid_int_coercion_raises_error(self):
        """Test that invalid int coercion raises ValidationError"""
        validator = TypeValidator("age", {"expected_type": "int", "coerce": True})

        with pytest.raises(ValidationError) as exc_info:
            validator.validate("not_a_number", {"age": "not_a_number"})

        assert "coerce" in str(exc_info.value).lower()
        assert "age" in str(exc_info.value)

    def test_invalid_float_coercion_raises_error(self):
        """Test that invalid float coercion raises ValidationError"""
        validator = TypeValidator("amount", {"expected_type": "float", "coerce": True})

        with pytest.raises(ValidationError) as exc_info:
            validator.validate("abc", {"amount": "abc"})

        assert "coerce" in str(exc_info.value).lower()

    def test_invalid_bool_coercion_raises_error(self):
        """Test that invalid bool coercion raises ValidationError"""
        validator = TypeValidator("active", {"expected_type": "bool", "coerce": True})

        with pytest.raises(ValidationError) as exc_info:
            validator.validate("maybe", {"active": "maybe"})

        assert "bool" in str(exc_info.value).lower() or "parse" in str(exc_info.value).lower()

    def test_coercion_disabled_strict_type_check(self):
        """Test that disabling coercion enforces strict type checking"""
        validator = TypeValidator("age", {"expected_type": "int", "coerce": False})

        # String should fail even if it could be coerced
        with pytest.raises(ValidationError) as exc_info:
            validator.validate("25", {"age": "25"})

        assert "expected" in str(exc_info.value).lower()

    def test_already_correct_type_no_coercion_needed(self):
        """Test that values with correct type pass without coercion"""
        validator = TypeValidator("age", {"expected_type": "int", "coerce": True})

        # Already an int, should pass without coercion
        validator.validate(25, {"age": 25})

    def test_none_value_skipped_by_type_validator(self):
        """Test that None values are not coerced (handled by required_field)"""
        validator = TypeValidator("age", {"expected_type": "int", "coerce": True})

        # None should be skipped by type validator
        validator.validate(None, {"age": None})

    def test_float_to_int_coercion_truncates(self):
        """Test that float to int coercion truncates decimal part"""
        validator = TypeValidator("count", {"expected_type": "int", "coerce": True})

        # Float should coerce to int (truncating decimal part)
        validator.validate(99.9, {"count": 99.9})

    def test_empty_string_coercion_raises_error(self):
        """Test that empty strings cannot be coerced to numbers"""
        validator = TypeValidator("amount", {"expected_type": "float", "coerce": True})

        with pytest.raises(ValidationError):
            validator.validate("", {"amount": ""})

    def test_whitespace_string_coercion_raises_error(self):
        """Test that whitespace-only strings cannot be coerced to numbers"""
        validator = TypeValidator("amount", {"expected_type": "float", "coerce": True})

        with pytest.raises(ValidationError):
            validator.validate("   ", {"amount": "   "})

    def test_scientific_notation_float_coercion(self):
        """Test that scientific notation strings can be coerced to float"""
        validator = TypeValidator("amount", {"expected_type": "float", "coerce": True})

        # Scientific notation should coerce successfully
        validator.validate("1.5e3", {"amount": "1.5e3"})  # 1500.0
        validator.validate("2.5E-2", {"amount": "2.5E-2"})  # 0.025


class TestTypeCoercionWithRealData:
    """Integration-style tests with realistic data scenarios"""

    def test_csv_numeric_string_coercion(self):
        """Test coercing numeric strings from CSV files"""
        amount_validator = TypeValidator("amount", {"expected_type": "float", "coerce": True})
        age_validator = TypeValidator("age", {"expected_type": "int", "coerce": True})

        # Simulate CSV data (all fields are strings)
        csv_record = {
            "transaction_id": "TXN001",
            "amount": "99.99",
            "age": "25"
        }

        # Both should coerce successfully
        amount_validator.validate(csv_record["amount"], csv_record)
        age_validator.validate(csv_record["age"], csv_record)

    def test_mixed_valid_invalid_coercions(self):
        """Test batch of records with both valid and invalid coercions"""
        validator = TypeValidator("amount", {"expected_type": "float", "coerce": True})

        test_cases = [
            ("99.99", True),  # Valid
            ("150.50", True),  # Valid
            ("not_a_number", False),  # Invalid
            ("", False),  # Invalid
            ("200", True),  # Valid
            ("abc123", False),  # Invalid
        ]

        for value, should_pass in test_cases:
            record = {"amount": value}
            if should_pass:
                validator.validate(value, record)
            else:
                with pytest.raises(ValidationError):
                    validator.validate(value, record)

    def test_negative_number_coercion(self):
        """Test that negative numbers can be coerced"""
        validator = TypeValidator("temperature", {"expected_type": "float", "coerce": True})

        # Negative numbers should coerce successfully
        validator.validate("-10.5", {"temperature": "-10.5"})
        validator.validate("-273.15", {"temperature": "-273.15"})

    def test_zero_coercion(self):
        """Test that zero values can be coerced"""
        int_validator = TypeValidator("count", {"expected_type": "int", "coerce": True})
        float_validator = TypeValidator("amount", {"expected_type": "float", "coerce": True})

        # Zero should coerce successfully
        int_validator.validate("0", {"count": "0"})
        float_validator.validate("0.0", {"amount": "0.0"})
        float_validator.validate("0.00", {"amount": "0.00"})
