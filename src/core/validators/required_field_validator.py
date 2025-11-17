"""
RequiredFieldValidator - ensures a field is present and not null/empty.
"""

from typing import Any, Dict
from .base_validator import BaseValidator, ValidationError


class RequiredFieldValidator(BaseValidator):
    """
    Validates that a required field is present and not null/empty.

    Fails if:
    - Field is missing from the record
    - Field value is None
    - Field value is an empty string (configurable)
    """

    def __init__(self, field_name: str, parameters: Dict[str, Any] | None = None):
        super().__init__(field_name, parameters)
        self.allow_empty_string = self.parameters.get("allow_empty_string", False)

    def validate(self, value: Any, record: Dict[str, Any]) -> None:
        """
        Validate that the field is present and not null/empty.

        Args:
            value: The field value to validate
            record: The entire record

        Raises:
            ValidationError: If field is missing, None, or empty string
        """
        # Check if field is missing from record
        if self.field_name not in record:
            raise ValidationError(
                rule_name="required_field",
                field_name=self.field_name,
                message="Field is missing from record"
            )

        # Check if value is None
        if value is None:
            raise ValidationError(
                rule_name="required_field",
                field_name=self.field_name,
                message="Field value is null"
            )

        # Check if value is empty string (unless explicitly allowed)
        if not self.allow_empty_string and isinstance(value, str) and value.strip() == "":
            raise ValidationError(
                rule_name="required_field",
                field_name=self.field_name,
                message="Field value is empty string"
            )

    @property
    def rule_type(self) -> str:
        return "required_field"
