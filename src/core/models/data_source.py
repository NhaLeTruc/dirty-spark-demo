"""
DataSource model representing a data origin (CSV file, Kafka topic, API endpoint).
"""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class DataSource(BaseModel):
    """
    Represents an origin of data (CSV file, Kafka topic, API endpoint).

    Attributes:
        source_id: Unique identifier for the data source (PK)
        source_type: Type of source: "csv_file", "kafka_topic", "json_stream", "api_endpoint"
        schema_version_id: Current active schema version (FK to SchemaVersion)
        connection_info: Connection details (file path, Kafka bootstrap servers, etc.)
        enabled: Whether source is actively being processed
        created_at: When source was registered
        updated_at: Last configuration update
    """

    source_id: str = Field(..., min_length=1, max_length=255)
    source_type: Literal["csv_file", "kafka_topic", "json_stream", "api_endpoint"]
    schema_version_id: int | None = None
    connection_info: dict[str, Any]
    enabled: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        json_schema_extra = {
            "example": {
                "source_id": "sales_csv_source",
                "source_type": "csv_file",
                "schema_version_id": 1,
                "connection_info": {
                    "path": "/data/sales.csv",
                    "delimiter": ",",
                    "has_header": True
                },
                "enabled": True
            }
        }
