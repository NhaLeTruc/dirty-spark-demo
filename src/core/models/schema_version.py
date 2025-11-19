"""
SchemaVersion model representing a snapshot of a data schema at a point in time.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class SchemaVersion(BaseModel):
    """
    Snapshot of a data schema at a point in time.

    Attributes:
        schema_id: Auto-increment primary key
        source_id: Which source this schema applies to
        version: Version number (increments with each change)
        schema_definition: Spark schema as JSON (fields, types, nullable)
        inferred: Whether schema was auto-inferred or manually defined
        confidence: Inference confidence if auto-inferred (0.0-1.0)
        created_at: When this version was created
        deprecated_at: When schema became obsolete
    """

    schema_id: int | None = None
    source_id: str
    version: int = Field(..., gt=0)
    schema_definition: dict[str, Any]  # Spark schema as dict
    inferred: bool = True
    confidence: float | None = Field(None, ge=0.0, le=1.0)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    deprecated_at: datetime | None = None

    class Config:
        json_schema_extra = {
            "example": {
                "schema_id": 1,
                "source_id": "sales_csv_source",
                "version": 1,
                "schema_definition": {
                    "fields": [
                        {"name": "transaction_id", "type": "string", "nullable": False},
                        {"name": "amount", "type": "double", "nullable": False},
                        {"name": "date", "type": "timestamp", "nullable": False}
                    ]
                },
                "inferred": True,
                "confidence": 0.92
            }
        }
