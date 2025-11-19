"""
ValidationResult model representing the outcome of validating a record (ephemeral).
"""

from pydantic import BaseModel, Field, field_validator
from typing import List


class ValidationResult(BaseModel):
    """
    Outcome of validating a record (ephemeral, used during processing).

    Note: ValidationResult is ephemeral, not persisted to database
    (used in-memory during processing).

    Attributes:
        record_id: Which record was validated
        passed: Overall validation status
        passed_rules: Rules that succeeded
        failed_rules: Rules that failed
        warnings: Non-blocking validation warnings (issues that don't fail the record)
        transformations_applied: List of transformations (e.g., "date_format_normalized")
        confidence_score: Schema inference confidence (0.0-1.0)
    """

    record_id: str
    passed: bool
    passed_rules: List[str] = Field(default_factory=list)
    failed_rules: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    transformations_applied: List[str] = Field(default_factory=list)
    confidence_score: float | None = Field(None, ge=0.0, le=1.0)

    @field_validator('failed_rules')
    @classmethod
    def check_passed_consistency(cls, v, info):
        """Validate that passed=True implies failed_rules is empty."""
        if info.data.get('passed') and len(v) > 0:
            raise ValueError("passed=True but failed_rules is not empty")
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "record_id": "TXN0001234567",
                "passed": True,
                "passed_rules": [
                    "require_transaction_id",
                    "transaction_id_regex",
                    "amount_range"
                ],
                "failed_rules": [],
                "warnings": [
                    "unusual_amount_for_category",
                    "timestamp_in_future"
                ],
                "transformations_applied": [
                    "amount_str_to_float",
                    "date_iso_format"
                ],
                "confidence_score": 0.95
            }
        }
