"""
DataRecord model representing a single unit of data being processed (ephemeral).
"""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class DataRecord(BaseModel):
    """
    A single unit of data being processed (ephemeral, not stored long-term).

    Note: DataRecord is primarily an in-memory structure during processing.
    Only persisted to warehouse (if valid) or quarantine (if invalid).

    Attributes:
        record_id: Unique identifier for the record (business key)
        source_id: Which source this came from (FK)
        raw_payload: Original unmodified data
        processed_payload: Data after transformations/coercions
        validation_status: "pending", "valid", "invalid", "manual_review"
        processing_timestamp: When record entered pipeline
        batch_id: Batch identifier for grouping (batch mode)
    """

    record_id: str = Field(..., min_length=1)
    source_id: str
    raw_payload: dict[str, Any]
    processed_payload: dict[str, Any] | None = None
    validation_status: Literal["pending", "valid", "invalid", "manual_review"] = "pending"
    processing_timestamp: datetime = Field(default_factory=datetime.utcnow)
    batch_id: str | None = None

    class Config:
        json_schema_extra = {
            "example": {
                "record_id": "TXN0001234567",
                "source_id": "sales_csv_source",
                "raw_payload": {
                    "transaction_id": "TXN0001234567",
                    "amount": "99.99",
                    "date": "2025-11-17"
                },
                "processed_payload": {
                    "transaction_id": "TXN0001234567",
                    "amount": 99.99,
                    "date": "2025-11-17T00:00:00Z"
                },
                "validation_status": "valid",
                "batch_id": "batch_20251117_001"
            }
        }
