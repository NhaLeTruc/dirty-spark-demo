"""
ValidationRule model representing a configurable constraint applied to incoming data.
"""

from pydantic import BaseModel, Field
from typing import Literal, Dict, Any
from datetime import datetime


class ValidationRule(BaseModel):
    """
    A configurable constraint applied to incoming data.

    Attributes:
        rule_id: Auto-increment primary key
        rule_name: Human-readable name ("require_transaction_id")
        rule_type: Type: "required_field", "type_check", "range", "regex", "custom"
        field_name: Which field this rule applies to
        parameters: Rule-specific params (e.g., {"min": 0, "max": 100})
        enabled: Whether rule is active
        severity: "error" (quarantine) or "warning" (log only)
        created_at: Rule creation timestamp
    """

    rule_id: int | None = None
    rule_name: str = Field(..., min_length=1)
    rule_type: Literal["required_field", "type_check", "range", "regex", "custom"]
    field_name: str
    parameters: Dict[str, Any] | None = None
    enabled: bool = True
    severity: Literal["error", "warning"] = "error"
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        json_schema_extra = {
            "example": {
                "rule_id": 1,
                "rule_name": "require_transaction_id",
                "rule_type": "required_field",
                "field_name": "transaction_id",
                "parameters": None,
                "enabled": True,
                "severity": "error"
            }
        }
