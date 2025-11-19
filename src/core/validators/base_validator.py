"""
Base validator interface for all validation rules.

All validators must inherit from BaseValidator and implement the validate() method.
"""

from abc import ABC, abstractmethod
from typing import Any


class ValidationError(Exception):
    """Raised when a validation rule fails."""

    def __init__(self, rule_name: str, field_name: str, message: str):
        self.rule_name = rule_name
        self.field_name = field_name
        self.message = message
        super().__init__(f"[{rule_name}] {field_name}: {message}")


class BaseValidator(ABC):
    """
    Abstract base class for all validators.

    Each validator implements a specific validation rule type
    (required_field, type_check, range, regex, custom).
    """

    def __init__(self, field_name: str, parameters: dict[str, Any] | None = None):
        """
        Initialize validator.

        Args:
            field_name: Name of the field to validate
            parameters: Rule-specific parameters (e.g., min/max for range)
        """
        self.field_name = field_name
        self.parameters = parameters or {}

    @abstractmethod
    def validate(self, value: Any, record: dict[str, Any]) -> None:
        """
        Validate a value against this rule.

        Args:
            value: The field value to validate
            record: The entire record (for context-dependent validation)

        Raises:
            ValidationError: If validation fails
        """
        pass

    @property
    @abstractmethod
    def rule_type(self) -> str:
        """Return the rule type identifier."""
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(field={self.field_name}, params={self.parameters})"
