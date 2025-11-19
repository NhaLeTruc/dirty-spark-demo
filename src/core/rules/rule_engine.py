"""
Rule engine for orchestrating validation rules on data records.

The rule engine loads validation rules, applies them to records,
and produces validation results.
"""

from typing import Any

from src.core.models import DataRecord, ValidationResult
from src.core.validators import (
    BaseValidator,
    CustomValidator,
    RangeValidator,
    RegexValidator,
    RequiredFieldValidator,
    TypeValidator,
    ValidationError,
)


class RuleEngine:
    """
    Orchestrates validation rules on data records.

    Loads rules from configuration and applies them to records in order,
    collecting all validation failures and transformations.
    """

    VALIDATOR_REGISTRY = {
        "required_field": RequiredFieldValidator,
        "type_check": TypeValidator,
        "range": RangeValidator,
        "regex": RegexValidator,
        "custom": CustomValidator,
    }

    def __init__(self, rules: list[dict[str, Any]]):
        """
        Initialize the rule engine with validation rules.

        Args:
            rules: List of rule configurations, each containing:
                   - rule_name: str
                   - rule_type: str (required_field, type_check, range, regex, custom)
                   - field_name: str
                   - parameters: Dict[str, Any] (optional)
                   - severity: str (error or warning)
                   - enabled: bool (default True)
        """
        self.rules = rules
        self.validators: list[tuple[str, str, BaseValidator]] = []
        self._build_validators()

    def _build_validators(self) -> None:
        """Build validator instances from rule configurations."""
        for rule in self.rules:
            # Skip disabled rules
            if not rule.get("enabled", True):
                continue

            rule_name = rule["rule_name"]
            rule_type = rule["rule_type"]
            field_name = rule["field_name"]
            parameters = rule.get("parameters", {})
            severity = rule.get("severity", "error")

            # Get validator class
            validator_class = self.VALIDATOR_REGISTRY.get(rule_type)
            if not validator_class:
                raise ValueError(f"Unknown rule type: {rule_type}")

            # Instantiate validator
            try:
                validator = validator_class(field_name, parameters)
                self.validators.append((rule_name, severity, validator))
            except Exception as e:
                raise ValueError(f"Failed to create validator for rule '{rule_name}': {e}")

    def validate_record(self, record: DataRecord) -> ValidationResult:
        """
        Validate a data record against all rules.

        Args:
            record: The DataRecord to validate

        Returns:
            ValidationResult containing pass/fail status and detailed results
        """
        passed_rules = []
        failed_rules = []
        warnings = []
        transformations = []

        # Use processed_payload if available, otherwise raw_payload
        payload = record.processed_payload if record.processed_payload else record.raw_payload

        for rule_name, severity, validator in self.validators:
            field_name = validator.field_name

            # Get field value (None if missing)
            value = payload.get(field_name)

            try:
                # Attempt validation
                validator.validate(value, payload)
                passed_rules.append(rule_name)

            except ValidationError:
                # Handle validation failure
                if severity == "error":
                    failed_rules.append(rule_name)
                else:
                    # Warning: log but don't fail the record
                    warnings.append(rule_name)

        # Determine overall pass/fail
        passed = len(failed_rules) == 0

        return ValidationResult(
            record_id=record.record_id,
            passed=passed,
            passed_rules=passed_rules,
            failed_rules=failed_rules,
            warnings=warnings,
            transformations_applied=transformations,
        )

    def validate_batch(self, records: list[DataRecord]) -> list[ValidationResult]:
        """
        Validate a batch of records.

        Args:
            records: List of DataRecord objects

        Returns:
            List of ValidationResult objects, one per record
        """
        return [self.validate_record(record) for record in records]

    def get_rule_summary(self) -> dict[str, Any]:
        """
        Get summary of loaded rules.

        Returns:
            Dictionary with rule counts and types
        """
        return {
            "total_rules": len(self.validators),
            "rules_by_type": self._count_by_type(),
            "rules_by_severity": self._count_by_severity(),
        }

    def _count_by_type(self) -> dict[str, int]:
        """Count validators by rule type."""
        counts: dict[str, int] = {}
        for _, _, validator in self.validators:
            rule_type = validator.rule_type
            counts[rule_type] = counts.get(rule_type, 0) + 1
        return counts

    def _count_by_severity(self) -> dict[str, int]:
        """Count validators by severity."""
        counts: dict[str, int] = {}
        for _, severity, _ in self.validators:
            counts[severity] = counts.get(severity, 0) + 1
        return counts
