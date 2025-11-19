"""
QuarantineRecord model representing invalid records with detailed error context.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


class QuarantineRecord(BaseModel):
    """
    Invalid records with detailed error context.

    Attributes:
        quarantine_id: Auto-increment primary key
        record_id: Original record_id (may be missing if malformed)
        source_id: Source of the invalid record
        raw_payload: Original data before validation
        failed_rules: Array of rule names that failed
        error_messages: Corresponding error messages
        quarantined_at: When quarantined
        reviewed: Whether analyst has reviewed
        reprocess_requested: Whether to retry with updated rules
        reprocessed_at: When reprocessing occurred
    """

    quarantine_id: int | None = None
    record_id: str | None = None
    source_id: str
    raw_payload: dict[str, Any]
    failed_rules: list[str] = Field(..., min_length=1)
    error_messages: list[str] = Field(..., min_length=1)
    quarantined_at: datetime = Field(default_factory=datetime.utcnow)
    reviewed: bool = False
    reprocess_requested: bool = False
    reprocessed_at: datetime | None = None

    @field_validator('error_messages')
    @classmethod
    def check_arrays_same_length(cls, v, info):
        """Validate that failed_rules and error_messages have the same length."""
        failed_rules = info.data.get('failed_rules', [])
        if len(v) != len(failed_rules):
            raise ValueError(
                f"error_messages length ({len(v)}) must match failed_rules length ({len(failed_rules)})"
            )
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "quarantine_id": 1,
                "record_id": "TXN0001234567",
                "source_id": "sales_csv_source",
                "raw_payload": {
                    "transaction_id": "INVALID",
                    "amount": "not_a_number"
                },
                "failed_rules": ["transaction_id_regex", "amount_type_check"],
                "error_messages": [
                    "transaction_id does not match pattern ^TXN[0-9]{10}$",
                    "amount must be a valid float"
                ],
                "reviewed": False,
                "reprocess_requested": False
            }
        }
