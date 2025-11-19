"""
RangeValidator - validates numeric values are within a specified range.
"""

from typing import Any

from .base_validator import BaseValidator, ValidationError


class RangeValidator(BaseValidator):
    """
    Validates that a numeric field is within a specified range.

    Parameters:
    - min: Minimum value (inclusive)
    - max: Maximum value (inclusive)
    - min_exclusive: Minimum value (exclusive)
    - max_exclusive: Maximum value (exclusive)
    """

    def __init__(self, field_name: str, parameters: dict[str, Any] | None = None):
        super().__init__(field_name, parameters)

        # Extract range boundaries
        self.min_value = self.parameters.get("min")
        self.max_value = self.parameters.get("max")
        self.min_exclusive = self.parameters.get("min_exclusive")
        self.max_exclusive = self.parameters.get("max_exclusive")

        # Validate that at least one boundary is specified
        if all(v is None for v in [self.min_value, self.max_value, self.min_exclusive, self.max_exclusive]):
            raise ValueError("RangeValidator requires at least one of: min, max, min_exclusive, max_exclusive")

    def validate(self, value: Any, record: dict[str, Any]) -> None:
        """
        Validate that the value is within the specified range.

        Args:
            value: The field value to validate
            record: The entire record

        Raises:
            ValidationError: If value is outside the range
        """
        # Skip validation for None (handled by required_field validator)
        if value is None:
            return

        # Ensure value is numeric
        if not isinstance(value, int | float):
            raise ValidationError(
                rule_name="range",
                field_name=self.field_name,
                message=f"Value must be numeric, got {type(value).__name__}"
            )

        # Check minimum (inclusive)
        if self.min_value is not None and value < self.min_value:
            raise ValidationError(
                rule_name="range",
                field_name=self.field_name,
                message=f"Value {value} is less than minimum {self.min_value}"
            )

        # Check minimum (exclusive)
        if self.min_exclusive is not None and value <= self.min_exclusive:
            raise ValidationError(
                rule_name="range",
                field_name=self.field_name,
                message=f"Value {value} must be greater than {self.min_exclusive}"
            )

        # Check maximum (inclusive)
        if self.max_value is not None and value > self.max_value:
            raise ValidationError(
                rule_name="range",
                field_name=self.field_name,
                message=f"Value {value} exceeds maximum {self.max_value}"
            )

        # Check maximum (exclusive)
        if self.max_exclusive is not None and value >= self.max_exclusive:
            raise ValidationError(
                rule_name="range",
                field_name=self.field_name,
                message=f"Value {value} must be less than {self.max_exclusive}"
            )

    @property
    def rule_type(self) -> str:
        return "range"
