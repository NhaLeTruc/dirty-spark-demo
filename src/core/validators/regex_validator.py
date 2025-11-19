"""
RegexValidator - validates field values against a regular expression pattern.
"""

import re
from re import Pattern
from typing import Any

from .base_validator import BaseValidator, ValidationError


class RegexValidator(BaseValidator):
    """
    Validates that a field value matches a regular expression pattern.

    Parameters:
    - pattern: Regular expression pattern (string or compiled Pattern)
    - flags: Optional regex flags (e.g., re.IGNORECASE)
    """

    def __init__(self, field_name: str, parameters: dict[str, Any] | None = None):
        super().__init__(field_name, parameters)

        # Get pattern from parameters
        pattern = self.parameters.get("pattern")
        if not pattern:
            raise ValueError("RegexValidator requires 'pattern' parameter")

        # Get optional flags
        flags = self.parameters.get("flags", 0)

        # Compile pattern
        try:
            if isinstance(pattern, str):
                self.pattern: Pattern = re.compile(pattern, flags)
            elif isinstance(pattern, Pattern):
                self.pattern = pattern
            else:
                raise ValueError(f"Pattern must be string or compiled Pattern, got {type(pattern)}")
        except re.error as e:
            raise ValueError(f"Invalid regex pattern: {e}")

    def validate(self, value: Any, record: dict[str, Any]) -> None:
        """
        Validate that the value matches the regex pattern.

        Args:
            value: The field value to validate
            record: The entire record

        Raises:
            ValidationError: If value doesn't match the pattern
        """
        # Skip validation for None (handled by required_field validator)
        if value is None:
            return

        # Convert to string if needed
        if not isinstance(value, str):
            value_str = str(value)
        else:
            value_str = value

        # Check pattern match
        if not self.pattern.match(value_str):
            raise ValidationError(
                rule_name="regex",
                field_name=self.field_name,
                message=f"Value '{value_str}' does not match pattern '{self.pattern.pattern}'"
            )

    @property
    def rule_type(self) -> str:
        return "regex"
