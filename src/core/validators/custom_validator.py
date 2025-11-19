"""
CustomValidator - validates using a custom Python function.
"""

from typing import Any

from .base_validator import BaseValidator, ValidationError


class CustomValidator(BaseValidator):
    """
    Validates using a custom validation function.

    Parameters:
    - validator_func: A callable that takes (value, record) and returns None on success
                      or raises an exception on failure
    - error_message: Optional custom error message template

    The validator function signature should be:
        def my_validator(value: Any, record: Dict[str, Any]) -> None:
            if not valid:
                raise ValueError("Validation failed")
    """

    def __init__(self, field_name: str, parameters: dict[str, Any] | None = None):
        super().__init__(field_name, parameters)

        # Get validator function
        self.validator_func = self.parameters.get("validator_func")
        if not self.validator_func:
            raise ValueError("CustomValidator requires 'validator_func' parameter")

        if not callable(self.validator_func):
            raise ValueError("validator_func must be callable")

        # Optional custom error message
        self.error_message = self.parameters.get("error_message", "Custom validation failed")

    def validate(self, value: Any, record: dict[str, Any]) -> None:
        """
        Validate using the custom function.

        Args:
            value: The field value to validate
            record: The entire record

        Raises:
            ValidationError: If custom validation fails
        """
        try:
            self.validator_func(value, record)
        except Exception as e:
            raise ValidationError(
                rule_name="custom",
                field_name=self.field_name,
                message=f"{self.error_message}: {str(e)}"
            )

    @property
    def rule_type(self) -> str:
        return "custom"
