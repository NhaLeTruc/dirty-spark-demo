"""
AuditLog model representing lineage entry tracking data transformations.
"""

from datetime import datetime

from pydantic import BaseModel, Field


class AuditLog(BaseModel):
    """
    Lineage entry tracking data transformations.

    Attributes:
        log_id: Auto-increment primary key
        record_id: Which record was transformed
        source_id: Source of the record
        transformation_type: Type of transformation (e.g., "type_coercion", "date_normalization")
        field_name: Which field was affected
        old_value: Original value (as string)
        new_value: Transformed value (as string)
        rule_applied: Which rule/logic caused the transformation
        created_at: When transformation occurred
    """

    log_id: int | None = None
    record_id: str
    source_id: str
    transformation_type: str
    field_name: str | None = None
    old_value: str | None = None
    new_value: str | None = None
    rule_applied: str | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        json_schema_extra = {
            "example": {
                "log_id": 1,
                "record_id": "TXN0001234567",
                "source_id": "sales_csv_source",
                "transformation_type": "type_coercion",
                "field_name": "amount",
                "old_value": "99.99",
                "new_value": "99.99",
                "rule_applied": "amount_type_check"
            }
        }
