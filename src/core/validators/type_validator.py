"""
TypeValidator - validates and optionally coerces field types.
"""

from typing import Any

from .base_validator import BaseValidator, ValidationError


class TypeValidator(BaseValidator):
    """
    Validates that a field matches the expected type.

    Supports optional type coercion (e.g., "99.99" -> 99.99 for float).

    Supported types:
    - int, float, str, bool
    - Custom type names: "integer", "decimal", "string", "boolean"
    """

    TYPE_MAPPING = {
        "integer": int,
        "int": int,
        "decimal": float,
        "float": float,
        "double": float,
        "string": str,
        "str": str,
        "boolean": bool,
        "bool": bool,
    }

    def __init__(self, field_name: str, parameters: dict[str, Any] | None = None):
        super().__init__(field_name, parameters)

        # Get expected type from parameters
        expected_type = self.parameters.get("expected_type")
        if not expected_type:
            raise ValueError("TypeValidator requires 'expected_type' parameter")

        # Map type name to Python type
        if isinstance(expected_type, str):
            self.expected_type = self.TYPE_MAPPING.get(expected_type.lower())
            if not self.expected_type:
                raise ValueError(f"Unsupported type: {expected_type}")
        else:
            self.expected_type = expected_type

        # Whether to attempt type coercion
        self.coerce = self.parameters.get("coerce", True)

    def validate(self, value: Any, record: dict[str, Any]) -> None:
        """
        Validate that the value matches the expected type.

        Args:
            value: The field value to validate
            record: The entire record

        Raises:
            ValidationError: If type validation fails
        """
        # Skip validation for None (handled by required_field validator)
        if value is None:
            return

        # Check if already correct type
        if isinstance(value, self.expected_type):
            return

        # Attempt coercion if enabled
        if self.coerce:
            try:
                self._coerce_type(value)
                return  # Coercion successful
            except (ValueError, TypeError) as e:
                raise ValidationError(
                    rule_name="type_check",
                    field_name=self.field_name,
                    message=f"Cannot coerce {type(value).__name__} to {self.expected_type.__name__}: {e}"
                )
        else:
            # No coercion - strict type check
            raise ValidationError(
                rule_name="type_check",
                field_name=self.field_name,
                message=f"Expected {self.expected_type.__name__}, got {type(value).__name__}"
            )

    def _coerce_type(self, value: Any) -> Any:
        """
        Attempt to coerce value to the expected type.

        Args:
            value: The value to coerce

        Returns:
            The coerced value

        Raises:
            ValueError: If coercion fails
        """
        # Special handling for bool (avoid "False" -> True)
        if self.expected_type is bool:
            if isinstance(value, str):
                if value.lower() in ("true", "1", "yes"):
                    return True
                elif value.lower() in ("false", "0", "no"):
                    return False
                else:
                    raise ValueError(f"Cannot parse '{value}' as boolean")
            return bool(value)

        # Standard type coercion
        return self.expected_type(value)

    @property
    def rule_type(self) -> str:
        return "type_check"
