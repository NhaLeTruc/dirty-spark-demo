"""
Rule configuration management.

Loads validation rules from YAML files and provides utilities
for managing rule configurations.
"""

from pathlib import Path
from typing import Any

import yaml


class RuleConfigLoader:
    """
    Loads validation rules from YAML configuration files.

    Expected YAML format:
    ```yaml
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
            max: 1000000.00
    ```
    """

    def __init__(self, config_path: str | Path):
        """
        Initialize the rule config loader.

        Args:
            config_path: Path to the YAML configuration file
        """
        self.config_path = Path(config_path)
        if not self.config_path.exists():
            raise FileNotFoundError(f"Rule configuration file not found: {config_path}")

    def load_rules(self) -> list[dict[str, Any]]:
        """
        Load and parse validation rules from YAML file.

        Returns:
            List of rule dictionaries suitable for RuleEngine

        Raises:
            ValueError: If YAML is invalid or missing required fields
        """
        with open(self.config_path) as f:
            config = yaml.safe_load(f)

        if not config or "rules" not in config:
            raise ValueError("Configuration file must contain 'rules' section")

        rules = []
        field_rules = config["rules"]

        for field_name, field_rule_list in field_rules.items():
            if not isinstance(field_rule_list, list):
                raise ValueError(f"Rules for field '{field_name}' must be a list")

            for idx, rule_def in enumerate(field_rule_list):
                rule = self._parse_rule(field_name, rule_def, idx)
                rules.append(rule)

        return rules

    def _parse_rule(self, field_name: str, rule_def: dict[str, Any], idx: int) -> dict[str, Any]:
        """
        Parse a single rule definition.

        Args:
            field_name: The field this rule applies to
            rule_def: The rule definition from YAML
            idx: Index of this rule for the field (for naming)

        Returns:
            Parsed rule dictionary

        Raises:
            ValueError: If rule definition is invalid
        """
        if "type" not in rule_def:
            raise ValueError(f"Rule for field '{field_name}' is missing 'type'")

        rule_type = rule_def["type"]

        # Generate rule name if not provided
        rule_name = rule_def.get("name", f"{field_name}_{rule_type}_{idx}")

        # Extract parameters
        parameters = rule_def.get("params", rule_def.get("parameters", {}))

        # Extract severity (default: error)
        severity = rule_def.get("severity", "error")
        if severity not in ("error", "warning"):
            raise ValueError(f"Invalid severity '{severity}' for rule '{rule_name}'. Must be 'error' or 'warning'")

        # Extract enabled flag (default: True)
        enabled = rule_def.get("enabled", True)

        return {
            "rule_name": rule_name,
            "rule_type": rule_type,
            "field_name": field_name,
            "parameters": parameters,
            "severity": severity,
            "enabled": enabled,
        }


class RuleConfigBuilder:
    """
    Programmatically build rule configurations (for testing or dynamic rules).
    """

    def __init__(self):
        """Initialize empty rule configuration."""
        self.rules: list[dict[str, Any]] = []

    def add_required_field(self, field_name: str, allow_empty_string: bool = False) -> "RuleConfigBuilder":
        """Add a required field rule."""
        self.rules.append({
            "rule_name": f"{field_name}_required",
            "rule_type": "required_field",
            "field_name": field_name,
            "parameters": {"allow_empty_string": allow_empty_string},
            "severity": "error",
            "enabled": True,
        })
        return self

    def add_type_check(
        self,
        field_name: str,
        expected_type: str,
        coerce: bool = True
    ) -> "RuleConfigBuilder":
        """Add a type check rule."""
        self.rules.append({
            "rule_name": f"{field_name}_type_check",
            "rule_type": "type_check",
            "field_name": field_name,
            "parameters": {"expected_type": expected_type, "coerce": coerce},
            "severity": "error",
            "enabled": True,
        })
        return self

    def add_range(
        self,
        field_name: str,
        min_value: float | None = None,
        max_value: float | None = None
    ) -> "RuleConfigBuilder":
        """Add a range validation rule."""
        params = {}
        if min_value is not None:
            params["min"] = min_value
        if max_value is not None:
            params["max"] = max_value

        self.rules.append({
            "rule_name": f"{field_name}_range",
            "rule_type": "range",
            "field_name": field_name,
            "parameters": params,
            "severity": "error",
            "enabled": True,
        })
        return self

    def add_regex(self, field_name: str, pattern: str) -> "RuleConfigBuilder":
        """Add a regex validation rule."""
        self.rules.append({
            "rule_name": f"{field_name}_regex",
            "rule_type": "regex",
            "field_name": field_name,
            "parameters": {"pattern": pattern},
            "severity": "error",
            "enabled": True,
        })
        return self

    def build(self) -> list[dict[str, Any]]:
        """Build and return the rule configuration."""
        return self.rules
