"""
WarehouseData model representing clean, validated data stored in the warehouse.
"""

from pydantic import BaseModel, Field
from typing import Dict, Any
from datetime import datetime


class WarehouseData(BaseModel):
    """
    Clean, validated data stored in the warehouse.

    Attributes:
        record_id: Business key (PK, must match DataRecord.record_id)
        source_id: Source of the record
        data: The actual validated data payload
        schema_version_id: Which schema version was used
        processed_at: When record was loaded
        checksum: Data fingerprint for change detection
    """

    record_id: str = Field(..., min_length=1)
    source_id: str
    data: Dict[str, Any]
    schema_version_id: int | None = None
    processed_at: datetime = Field(default_factory=datetime.utcnow)
    checksum: str | None = None

    class Config:
        json_schema_extra = {
            "example": {
                "record_id": "TXN0001234567",
                "source_id": "sales_csv_source",
                "data": {
                    "transaction_id": "TXN0001234567",
                    "amount": 99.99,
                    "date": "2025-11-17T00:00:00Z",
                    "customer_id": "CUST12345"
                },
                "schema_version_id": 1,
                "checksum": "a3b2c1d4e5f6..."
            }
        }
