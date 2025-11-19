# Data Model: Dirty Data Validation Pipeline

**Feature**: 001-dirty-data-validation
**Date**: 2025-11-17
**Purpose**: Define all data entities, schemas, relationships, and validation rules

## Overview

The data model supports the core workflow:
1. **Ingest** data from sources → **DataSource**, **DataRecord**
2. **Validate** against rules → **ValidationRule**, **ValidationResult**
3. **Route** based on validation → **WarehouseData** (valid) or **QuarantineRecord** (invalid)
4. **Track** transformations → **AuditLog**
5. **Manage** schema evolution → **SchemaVersion**

---

## Entity Definitions

### 1. DataSource

**Purpose**: Represents an origin of data (CSV file, Kafka topic, API endpoint)

**Attributes**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| source_id | TEXT | Yes | Unique identifier for the data source (PK) |
| source_type | TEXT | Yes | Type of source: "csv_file", "kafka_topic", "json_stream" |
| schema_version_id | INTEGER | No | Current active schema version (FK to SchemaVersion) |
| connection_info | JSONB | Yes | Connection details (file path, Kafka bootstrap servers, etc.) |
| enabled | BOOLEAN | Yes | Whether source is actively being processed |
| created_at | TIMESTAMP | Yes | When source was registered |
| updated_at | TIMESTAMP | Yes | Last configuration update |

**Relationships**:
- Has many **DataRecord** (1:N)
- Has many **SchemaVersion** (1:N)

**Validation Rules**:
- source_id must be unique
- source_type must be in allowed list
- connection_info must be valid JSON

**PostgreSQL DDL**:
```sql
CREATE TABLE data_source (
    source_id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL CHECK (source_type IN ('csv_file', 'kafka_topic', 'json_stream', 'api_endpoint')),
    schema_version_id INTEGER REFERENCES schema_version(schema_id),
    connection_info JSONB NOT NULL,
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_data_source_enabled ON data_source(enabled) WHERE enabled = TRUE;
```

**Pydantic Model**:
```python
from pydantic import BaseModel, Field
from typing import Literal, Dict, Any
from datetime import datetime

class DataSource(BaseModel):
    source_id: str = Field(..., min_length=1, max_length=255)
    source_type: Literal["csv_file", "kafka_topic", "json_stream", "api_endpoint"]
    schema_version_id: int | None = None
    connection_info: Dict[str, Any]
    enabled: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
```

---

### 2. ValidationRule

**Purpose**: A configurable constraint applied to incoming data

**Attributes**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| rule_id | SERIAL | Yes | Auto-increment primary key |
| rule_name | TEXT | Yes | Human-readable name ("require_transaction_id") |
| rule_type | TEXT | Yes | Type: "required_field", "type_check", "range", "regex", "custom" |
| field_name | TEXT | Yes | Which field this rule applies to |
| parameters | JSONB | No | Rule-specific params (e.g., {"min": 0, "max": 100}) |
| enabled | BOOLEAN | Yes | Whether rule is active |
| severity | TEXT | Yes | "error" (quarantine) or "warning" (log only) |
| created_at | TIMESTAMP | Yes | Rule creation timestamp |

**Relationships**:
- Applied to many **DataRecord** (N:M through ValidationResult)

**Validation Rules**:
- rule_type must be in allowed types
- parameters must be valid for rule_type
- severity must be "error" or "warning"

**PostgreSQL DDL**:
```sql
CREATE TABLE validation_rule (
    rule_id SERIAL PRIMARY KEY,
    rule_name TEXT NOT NULL UNIQUE,
    rule_type TEXT NOT NULL CHECK (rule_type IN ('required_field', 'type_check', 'range', 'regex', 'custom')),
    field_name TEXT NOT NULL,
    parameters JSONB,
    enabled BOOLEAN DEFAULT TRUE,
    severity TEXT NOT NULL CHECK (severity IN ('error', 'warning')),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_validation_rule_enabled ON validation_rule(enabled) WHERE enabled = TRUE;
CREATE INDEX idx_validation_rule_field ON validation_rule(field_name);
```

**Pydantic Model**:
```python
from pydantic import BaseModel, Field
from typing import Literal, Dict, Any

class ValidationRule(BaseModel):
    rule_id: int | None = None
    rule_name: str = Field(..., min_length=1)
    rule_type: Literal["required_field", "type_check", "range", "regex", "custom"]
    field_name: str
    parameters: Dict[str, Any] | None = None
    enabled: bool = True
    severity: Literal["error", "warning"] = "error"
    created_at: datetime = Field(default_factory=datetime.utcnow)
```

---

### 3. DataRecord

**Purpose**: A single unit of data being processed (ephemeral, not stored long-term)

**Attributes**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| record_id | TEXT | Yes | Unique identifier for the record (business key) |
| source_id | TEXT | Yes | Which source this came from (FK) |
| raw_payload | JSONB | Yes | Original unmodified data |
| processed_payload | JSONB | No | Data after transformations/coercions |
| validation_status | TEXT | Yes | "pending", "valid", "invalid", "manual_review" |
| processing_timestamp | TIMESTAMP | Yes | When record entered pipeline |
| batch_id | TEXT | No | Batch identifier for grouping (batch mode) |

**Relationships**:
- Belongs to **DataSource** (N:1)
- Has one **ValidationResult** (1:1)
- May have one **QuarantineRecord** if invalid (1:0..1)
- May have many **AuditLog** entries (1:N)

**Validation Rules**:
- record_id must be unique within source
- validation_status must be in allowed values

**Note**: DataRecord is primarily an in-memory structure during processing. Only persisted to warehouse (if valid) or quarantine (if invalid).

**Pydantic Model**:
```python
from pydantic import BaseModel, Field
from typing import Dict, Any, Literal

class DataRecord(BaseModel):
    record_id: str = Field(..., min_length=1)
    source_id: str
    raw_payload: Dict[str, Any]
    processed_payload: Dict[str, Any] | None = None
    validation_status: Literal["pending", "valid", "invalid", "manual_review"] = "pending"
    processing_timestamp: datetime = Field(default_factory=datetime.utcnow)
    batch_id: str | None = None
```

---

### 4. WarehouseData

**Purpose**: Clean, validated data stored in the warehouse

**Attributes**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| record_id | TEXT | Yes | Business key (PK, must match DataRecord.record_id) |
| source_id | TEXT | Yes | Source of the record |
| data | JSONB | Yes | The actual validated data payload |
| schema_version_id | INTEGER | No | Which schema version was used |
| processed_at | TIMESTAMP | Yes | When record was loaded |
| checksum | TEXT | No | Data fingerprint for change detection |

**Relationships**:
- Originated from **DataSource** (N:1)
- May have **AuditLog** entries (1:N)

**Validation Rules**:
- record_id is unique (enforces idempotency)
- data must not be null

**PostgreSQL DDL**:
```sql
CREATE TABLE warehouse_data (
    record_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES data_source(source_id),
    data JSONB NOT NULL,
    schema_version_id INTEGER REFERENCES schema_version(schema_id),
    processed_at TIMESTAMP DEFAULT NOW(),
    checksum TEXT
);

CREATE INDEX idx_warehouse_source_time ON warehouse_data(source_id, processed_at DESC);
CREATE INDEX idx_warehouse_data_gin ON warehouse_data USING gin(data);  -- For JSONB queries
```

**Pydantic Model**:
```python
class WarehouseData(BaseModel):
    record_id: str = Field(..., min_length=1)
    source_id: str
    data: Dict[str, Any]
    schema_version_id: int | None = None
    processed_at: datetime = Field(default_factory=datetime.utcnow)
    checksum: str | None = None
```

---

### 5. QuarantineRecord

**Purpose**: Invalid records with detailed error context

**Attributes**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| quarantine_id | SERIAL | Yes | Auto-increment primary key |
| record_id | TEXT | No | Original record_id (may be missing if malformed) |
| source_id | TEXT | Yes | Source of the invalid record |
| raw_payload | JSONB | Yes | Original data before validation |
| failed_rules | TEXT[] | Yes | Array of rule names that failed |
| error_messages | TEXT[] | Yes | Corresponding error messages |
| quarantined_at | TIMESTAMP | Yes | When quarantined |
| reviewed | BOOLEAN | Yes | Whether analyst has reviewed |
| reprocess_requested | BOOLEAN | Yes | Whether to retry with updated rules |
| reprocessed_at | TIMESTAMP | No | When reprocessing occurred |

**Relationships**:
- Originated from **DataSource** (N:1)

**Validation Rules**:
- failed_rules and error_messages must have same array length
- raw_payload must not be null

**PostgreSQL DDL**:
```sql
CREATE UNLOGGED TABLE quarantine_record (  -- UNLOGGED for faster writes
    quarantine_id SERIAL PRIMARY KEY,
    record_id TEXT,
    source_id TEXT NOT NULL REFERENCES data_source(source_id),
    raw_payload JSONB NOT NULL,
    failed_rules TEXT[] NOT NULL,
    error_messages TEXT[] NOT NULL,
    quarantined_at TIMESTAMP DEFAULT NOW(),
    reviewed BOOLEAN DEFAULT FALSE,
    reprocess_requested BOOLEAN DEFAULT FALSE,
    reprocessed_at TIMESTAMP,
    CONSTRAINT arrays_same_length CHECK (array_length(failed_rules, 1) = array_length(error_messages, 1))
);

CREATE INDEX idx_quarantine_source ON quarantine_record(source_id, quarantined_at DESC);
CREATE INDEX idx_quarantine_reviewed ON quarantine_record(reviewed) WHERE reviewed = FALSE;
CREATE INDEX idx_quarantine_reprocess ON quarantine_record(reprocess_requested) WHERE reprocess_requested = TRUE;
```

**Pydantic Model**:
```python
from typing import List

class QuarantineRecord(BaseModel):
    quarantine_id: int | None = None
    record_id: str | None = None
    source_id: str
    raw_payload: Dict[str, Any]
    failed_rules: List[str] = Field(..., min_length=1)
    error_messages: List[str] = Field(..., min_length=1)
    quarantined_at: datetime = Field(default_factory=datetime.utcnow)
    reviewed: bool = False
    reprocess_requested: bool = False
    reprocessed_at: datetime | None = None
```

---

### 6. ValidationResult

**Purpose**: Outcome of validating a record (ephemeral, used during processing)

**Attributes**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| record_id | TEXT | Yes | Which record was validated |
| passed | BOOLEAN | Yes | Overall validation status |
| passed_rules | TEXT[] | Yes | Rules that succeeded |
| failed_rules | TEXT[] | Yes | Rules that failed |
| warnings | TEXT[] | No | Non-blocking validation warnings (data quality issues that don't fail the record) |
| transformations_applied | TEXT[] | No | List of transformations (e.g., "date_format_normalized") |
| confidence_score | FLOAT | No | Schema inference confidence (0.0-1.0) |

**Relationships**:
- Belongs to **DataRecord** (1:1)

**Validation Rules**:
- confidence_score must be between 0.0 and 1.0
- passed = True implies failed_rules is empty

**Note**: ValidationResult is ephemeral, not persisted to database (used in-memory during processing).

**Pydantic Model**:
```python
from typing import List
from pydantic import Field, field_validator

class ValidationResult(BaseModel):
    record_id: str
    passed: bool
    passed_rules: List[str] = Field(default_factory=list)
    failed_rules: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)  # NEW: Non-blocking warnings
    transformations_applied: List[str] = Field(default_factory=list)
    confidence_score: float | None = Field(None, ge=0.0, le=1.0)

    @field_validator('failed_rules')
    @classmethod
    def check_passed_consistency(cls, v, info):
        if info.data.get('passed') and len(v) > 0:
            raise ValueError("passed=True but failed_rules is not empty")
        return v
```

---

### 7. SchemaVersion

**Purpose**: Snapshot of a data schema at a point in time

**Attributes**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| schema_id | SERIAL | Yes | Auto-increment primary key |
| source_id | TEXT | Yes | Which source this schema applies to |
| version | INTEGER | Yes | Version number (increments with each change) |
| schema_definition | JSONB | Yes | Spark schema as JSON (fields, types, nullable) |
| inferred | BOOLEAN | Yes | Whether schema was auto-inferred or manually defined |
| confidence | FLOAT | No | Inference confidence if auto-inferred (0.0-1.0) |
| created_at | TIMESTAMP | Yes | When this version was created |
| deprecated_at | TIMESTAMP | No | When schema became obsolete |

**Relationships**:
- Belongs to **DataSource** (N:1)

**Validation Rules**:
- (source_id, version) must be unique
- version must be positive integer
- schema_definition must be valid Spark schema JSON

**PostgreSQL DDL**:
```sql
CREATE TABLE schema_version (
    schema_id SERIAL PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES data_source(source_id),
    version INTEGER NOT NULL CHECK (version > 0),
    schema_definition JSONB NOT NULL,
    inferred BOOLEAN DEFAULT TRUE,
    confidence FLOAT CHECK (confidence >= 0.0 AND confidence <= 1.0),
    created_at TIMESTAMP DEFAULT NOW(),
    deprecated_at TIMESTAMP,
    UNIQUE(source_id, version)
);

CREATE INDEX idx_schema_version_source ON schema_version(source_id, version DESC);
CREATE INDEX idx_schema_version_active ON schema_version(source_id) WHERE deprecated_at IS NULL;
```

**Pydantic Model**:
```python
class SchemaVersion(BaseModel):
    schema_id: int | None = None
    source_id: str
    version: int = Field(..., gt=0)
    schema_definition: Dict[str, Any]  # Spark schema as dict
    inferred: bool = True
    confidence: float | None = Field(None, ge=0.0, le=1.0)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    deprecated_at: datetime | None = None
```

---

### 8. AuditLog

**Purpose**: Lineage entry tracking data transformations

**Attributes**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| log_id | BIGSERIAL | Yes | Auto-increment primary key |
| record_id | TEXT | Yes | Which record was transformed |
| source_id | TEXT | Yes | Source of the record |
| transformation_type | TEXT | Yes | Type of transformation (e.g., "type_coercion", "date_normalization") |
| field_name | TEXT | No | Which field was affected |
| old_value | TEXT | No | Original value (as string) |
| new_value | TEXT | No | Transformed value (as string) |
| rule_applied | TEXT | No | Which rule/logic caused the transformation |
| created_at | TIMESTAMP | Yes | When transformation occurred |

**Relationships**:
- Tracks changes to **DataRecord** (N:1)

**Validation Rules**:
- transformation_type must be from allowed list
- created_at must be in the past

**PostgreSQL DDL**:
```sql
-- Partitioned by month for performance
CREATE TABLE audit_log (
    log_id BIGSERIAL,
    record_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    transformation_type TEXT NOT NULL,
    field_name TEXT,
    old_value TEXT,
    new_value TEXT,
    rule_applied TEXT,
    created_at TIMESTAMP DEFAULT NOW()
) PARTITION BY RANGE (created_at);

-- Create monthly partitions (example)
CREATE TABLE audit_log_2025_11 PARTITION OF audit_log
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');

CREATE INDEX idx_audit_log_record ON audit_log(record_id, created_at DESC);
CREATE INDEX idx_audit_log_source ON audit_log(source_id, created_at DESC);
```

**Pydantic Model**:
```python
class AuditLog(BaseModel):
    log_id: int | None = None
    record_id: str
    source_id: str
    transformation_type: str
    field_name: str | None = None
    old_value: str | None = None
    new_value: str | None = None
    rule_applied: str | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
```

---

## Entity Relationship Diagram

```
┌──────────────┐
│  DataSource  │
│--------------│
│ source_id PK │◄────┐
│ source_type  │     │
│ schema_ver_id│     │
└──────────────┘     │
       │ 1           │
       │             │
       │ N           │
┌──────────────┐     │
│ DataRecord   │     │
│--------------│     │
│ record_id PK │     │
│ source_id FK │─────┘
│ raw_payload  │
│ status       │
└──────────────┘
       │ 1
       │
       │ 1
┌──────────────────┐
│ ValidationResult │
│------------------│
│ record_id PK     │
│ passed           │
│ failed_rules[]   │
└──────────────────┘
       │
       ├──── valid ────►┌────────────────┐
       │                │ WarehouseData  │
       │                │----------------│
       │                │ record_id PK   │
       │                │ source_id FK   │
       │                │ data JSONB     │
       │                └────────────────┘
       │
       └─── invalid ───►┌──────────────────┐
                        │ QuarantineRecord │
                        │------------------│
                        │ quarantine_id PK │
                        │ source_id FK     │
                        │ failed_rules[]   │
                        └──────────────────┘

┌──────────────────┐
│ ValidationRule   │
│------------------│
│ rule_id PK       │
│ rule_type        │
│ field_name       │
└──────────────────┘

┌──────────────────┐
│ SchemaVersion    │
│------------------│
│ schema_id PK     │
│ source_id FK     │
│ version          │
│ schema_def JSONB │
└──────────────────┘

┌──────────────────┐
│ AuditLog         │
│------------------│
│ log_id PK        │
│ record_id        │
│ source_id        │
│ transformation   │
└──────────────────┘
```

---

## State Transitions

### DataRecord Status Flow

```
        ┌─────────┐
START ──►│ pending │
        └─────────┘
             │
             ▼
     [ Validation Engine ]
             │
      ┌──────┴──────┐
      │             │
      ▼             ▼
 ┌───────┐     ┌─────────┐
 │ valid │     │ invalid │
 └───────┘     └─────────┘
      │             │
      ▼             ▼
 [ Warehouse ] [ Quarantine ]
      │             │
      │             ▼
      │        ┌──────────────┐
      │        │manual_review │
      │        └──────────────┘
      │             │
      │             ▼
      │       [ Rule Update ]
      │             │
      │             ▼
      └─────── [ Reprocess ]
                    │
                    ▼
               (back to validation)
```

---

## Data Volumes & Growth Estimates

| Entity | Initial Size | Daily Growth | 1 Year Estimate | Retention |
|--------|-------------|--------------|-----------------|-----------|
| DataSource | 10 | ~1/month | 20 | Permanent |
| ValidationRule | 50 | ~2/week | 150 | Permanent |
| WarehouseData | 100K | 10K/day | 3.7M | Permanent |
| QuarantineRecord | 5K | 500/day | 185K | 90 days |
| SchemaVersion | 10 | ~1/week | 60 | Permanent |
| AuditLog | 50K | 5K/day | 1.85M | 1 year |

**Storage Estimates**:
- WarehouseData: ~2KB/record → 3.7M * 2KB = 7.4GB/year
- QuarantineRecord: ~3KB/record → 185K * 3KB = 555MB (with 90-day retention)
- AuditLog (partitioned): ~500B/record → 1.85M * 500B = 925MB/year

---

## Indexes Strategy

**Primary Access Patterns**:
1. Query warehouse by source + time range → `idx_warehouse_source_time`
2. Find unreviewed quarantine records → `idx_quarantine_reviewed`
3. Lookup audit trail for record → `idx_audit_log_record`
4. Get active validation rules → `idx_validation_rule_enabled`
5. Find current schema for source → `idx_schema_version_active`

**Composite Indexes**:
- (source_id, processed_at) for time-series queries
- (source_id, version) for schema lookups
- (reviewed=FALSE) partial index for quarantine dashboard

**JSONB GIN Indexes**:
- `warehouse_data.data` for ad-hoc field queries
- `schema_version.schema_definition` for schema searches

---

## Summary

All 8 entities from the specification are fully defined with:
- ✅ PostgreSQL DDL schemas
- ✅ Pydantic validation models
- ✅ Relationships and foreign keys
- ✅ Indexes for performance
- ✅ Constraints for data integrity
- ✅ State transition diagrams
- ✅ Growth and retention estimates

This data model supports all functional requirements (FR-001 through FR-020) and aligns with constitutional principles for data quality, observability, and reliability.
