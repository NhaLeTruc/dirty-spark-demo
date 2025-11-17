# Technical Research: Dirty Data Validation Pipeline

**Feature**: 001-dirty-data-validation
**Date**: 2025-11-17
**Purpose**: Document technology choices, best practices, and architectural decisions

## Executive Summary

This document captures all technology selections and architectural patterns for the dirty data validation pipeline. All choices prioritize open-source stability, community support, and alignment with constitutional principles (data quality, dual-mode processing, observability).

---

## 1. Processing Engine Selection

### Decision: Apache Spark 3.5.1 (Batch + Structured Streaming)

**Rationale**:
- **Unified API**: Spark Structured Streaming shares DataFrame/Dataset API with batch, enabling code reuse for validation logic (aligns with Principle II: Dual-Mode Processing)
- **Mature ecosystem**: 10+ years of production use, extensive documentation, large community
- **Built-in fault tolerance**: Checkpointing, exactly-once semantics, automatic retries
- **Scalability**: Linear horizontal scaling from single node to thousands of workers
- **Schema handling**: Native schema inference, schema evolution support via `mergeSchema` option
- **Performance**: In-memory processing, catalyst optimizer, Tungsten execution engine

**Alternatives Considered**:
- **Apache Flink**: More complex setup, smaller community, better for pure streaming but less mature batch support
- **Kafka Streams**: Java-only, limited batch capabilities, would require separate batch framework
- **Pandas + multiprocessing**: No distributed support, manual fault tolerance, memory bottlenecks at scale
- **Dask**: Good for batch, limited streaming support, smaller ecosystem

**Best Practices**:
- Use DataFrame API for type safety and optimization
- Partition data by source_id for parallelism
- Configure spark.sql.shuffle.partitions based on cluster size (rule of thumb: 2-4x CPU cores)
- Enable adaptive query execution (spark.sql.adaptive.enabled=true)
- Use broadcast joins for small reference data (validation rules)

**Configuration Recommendations**:
```yaml
batch:
  spark.sql.adaptive.enabled: true
  spark.sql.shuffle.partitions: 200  # Adjust based on data volume
  spark.sql.files.maxPartitionBytes: 128MB

streaming:
  spark.sql.streaming.checkpointLocation: /checkpoints
  spark.sql.streaming.stateStore.providerClass: org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
  trigger: processingTime=10 seconds  # Micro-batch interval
```

---

## 2. Data Warehouse Selection

### Decision: PostgreSQL 16.2

**Rationale**:
- **ACID guarantees**: Full transactional support for batch loads (Principle IV: Idempotency & Reliability)
- **JSONB support**: Native semi-structured data storage for nested fields, schema registry
- **Upsert capability**: INSERT ... ON CONFLICT for idempotent writes
- **Performance**: Improved query parallelism in PG 16, faster COPY operations
- **Open source**: No licensing costs, extensive tooling
- **Connection pooling**: Built-in connection limits, external pooling via pgBouncer

**Alternatives Considered**:
- **MySQL**: Weaker JSON support, less robust transaction isolation
- **Snowflake/BigQuery**: Proprietary, cost implications, overkill for demo
- **Cassandra**: No ACID, eventual consistency conflicts with data quality requirements
- **DuckDB**: Embedded only, no concurrent write support for streaming

**Best Practices**:
- Create composite indexes on business keys for upsert performance
- Use UNLOGGED tables for quarantine (faster writes, acceptable for recoverable data)
- Partition large audit tables by timestamp for query performance
- Configure shared_buffers = 25% of RAM for caching
- Use COPY protocol for bulk inserts (10-100x faster than INSERT)

**Schema Design**:
```sql
-- Warehouse table with upsert key
CREATE TABLE warehouse_data (
    record_id TEXT PRIMARY KEY,  -- Business key for idempotency
    source_id TEXT NOT NULL,
    data JSONB NOT NULL,
    processed_at TIMESTAMP DEFAULT NOW(),
    schema_version INTEGER
);
CREATE INDEX idx_source_processed ON warehouse_data(source_id, processed_at);

-- Quarantine table for invalid records
CREATE UNLOGGED TABLE quarantine (
    quarantine_id SERIAL PRIMARY KEY,
    record_id TEXT,
    source_id TEXT NOT NULL,
    raw_payload JSONB NOT NULL,
    failed_rules TEXT[] NOT NULL,
    error_messages TEXT[] NOT NULL,
    quarantined_at TIMESTAMP DEFAULT NOW(),
    reviewed BOOLEAN DEFAULT FALSE
);

-- Audit log for lineage
CREATE TABLE audit_log (
    log_id BIGSERIAL PRIMARY KEY,
    record_id TEXT NOT NULL,
    transformation_type TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    rule_applied TEXT,
    created_at TIMESTAMP DEFAULT NOW()
) PARTITION BY RANGE (created_at);

-- Schema registry
CREATE TABLE schema_registry (
    schema_id SERIAL PRIMARY KEY,
    source_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    schema_definition JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(source_id, version)
);
```

---

## 3. Python Ecosystem & Libraries

### Decision: Python 3.11 + PySpark + Pydantic 2.6

**Rationale**:
- **Python 3.11**: 25% faster than 3.10, improved error messages, latest stable
- **PySpark 3.5.1**: Official Spark Python API, type hints support
- **Pydantic 2.6**: 5-50x faster than v1, robust validation, automatic coercion
- **Type safety**: Static type checking with mypy, runtime validation with Pydantic

**Key Libraries**:

**Data Processing**:
- `pyspark==3.5.1`: Spark Python API
- `pyarrow==15.0.0`: Columnar format, efficient Spark<->Pandas conversion
- `pandas==2.2.1`: Small data manipulation (if needed)

**Database**:
- `psycopg[binary,pool]==3.1.18`: PostgreSQL adapter, connection pooling
- `sqlalchemy==2.0.28`: ORM and query builder (optional, for migrations)

**Validation**:
- `pydantic==2.6.3`: Data validation and settings
- `pydantic-core==2.16.3`: Rust-based validation core (auto-installed)

**Observability**:
- `python-json-logger==2.0.7`: Structured logging
- `prometheus-client==0.20.0`: Metrics exposition

**Configuration**:
- `pyyaml==6.0.1`: YAML config parsing
- `python-dotenv==1.0.1`: Environment variable management

**Testing**:
- `pytest==8.1.0`: Testing framework
- `pytest-spark==0.6.0`: Spark testing fixtures
- `testcontainers==4.0.1`: Docker-based integration testing
- `hypothesis==6.98.0`: Property-based testing
- `great-expectations==0.18.12`: Data quality assertions
- `pytest-cov==4.1.0`: Coverage reporting

**Development**:
- `black==24.2.0`: Code formatting
- `ruff==0.3.0`: Fast linting (replaces flake8, isort, pylint)
- `mypy==1.8.0`: Static type checking

---

## 4. Streaming Data Source

### Decision: Apache Kafka 3.7.0 (Optional, file-based alternative provided)

**Rationale**:
- **Industry standard**: Most common streaming platform
- **Spark integration**: Native StructuredStreaming source
- **Replay capability**: Offset-based consumption enables reprocessing
- **Durability**: Persistent message storage

**Alternatives Considered**:
- **AWS Kinesis**: Proprietary, cloud-only
- **RabbitMQ**: Less suitable for high-throughput data pipelines
- **File-based streaming**: readStream from directory (simpler for demo)

**For Demo/Local Testing**:
Use Spark's file stream source (watches directory for new files):
```python
# Simpler than Kafka for local testing
stream_df = spark.readStream \
    .schema(schema) \
    .json("/input/stream/")
```

**Best Practices (if using Kafka)**:
- Partition by record key for ordering guarantees
- Configure retention based on reprocessing needs (7 days typical)
- Use Schema Registry (Confluent) for schema evolution management
- Set `startingOffsets=earliest` for full replay, `latest` for real-time only

---

## 5. Local Testing Infrastructure

### Decision: Docker Compose + Testcontainers

**Rationale**:
- **Reproducibility**: Identical environments across developers
- **Integration testing**: Real PostgreSQL, Kafka, Spark in containers
- **No manual setup**: `docker-compose up` starts full stack
- **CI/CD ready**: Same containers in GitHub Actions

**Docker Compose Services**:
```yaml
services:
  postgres:
    image: postgres:16.2-alpine
    environment:
      POSTGRES_DB: datawarehouse
      POSTGRES_USER: pipeline
      POSTGRES_PASSWORD: dev_password
    ports:
      - "5432:5432"
    volumes:
      - ./docker/init-db.sql:/docker-entrypoint-initdb.d/init.sql

  spark-master:
    image: bitnami/spark:3.5.1
    environment:
      - SPARK_MODE=master
    ports:
      - "8080:8080"  # Spark UI
      - "7077:7077"  # Spark master

  spark-worker:
    image: bitnami/spark:3.5.1
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
    depends_on:
      - spark-master

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
    ports:
      - "9092:9092"
    depends_on:
      - zookeeper

  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
```

**Testcontainers Usage**:
```python
# tests/conftest.py
import pytest
from testcontainers.postgres import PostgresContainer

@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:16.2-alpine") as postgres:
        yield postgres
```

---

## 6. Validation Architecture

### Decision: Pluggable Rule Engine + Pydantic Models

**Rationale**:
- **Extensibility**: Add custom validators without core changes
- **Type safety**: Pydantic enforces types at runtime
- **Configuration-driven**: Rules in YAML, no code changes needed
- **Testability**: Each validator is a pure function

**Architecture Pattern**:
```python
# Core validator interface
from abc import ABC, abstractmethod
from typing import Any, Dict

class Validator(ABC):
    @abstractmethod
    def validate(self, value: Any, params: Dict[str, Any]) -> tuple[bool, str]:
        """Returns (is_valid, error_message)"""
        pass

# Example validators
class RequiredFieldValidator(Validator):
    def validate(self, value: Any, params: Dict) -> tuple[bool, str]:
        if value is None or value == "":
            return False, f"Field is required"
        return True, ""

class RangeValidator(Validator):
    def validate(self, value: Any, params: Dict) -> tuple[bool, str]:
        min_val = params.get("min")
        max_val = params.get("max")
        if not (min_val <= value <= max_val):
            return False, f"Value {value} not in range [{min_val}, {max_val}]"
        return True, ""

# Rule engine
class ValidationEngine:
    def __init__(self, rules_config: Dict):
        self.rules = self._load_rules(rules_config)
        self.validators = {
            "required": RequiredFieldValidator(),
            "range": RangeValidator(),
            # ... register all validators
        }

    def validate_record(self, record: Dict) -> ValidationResult:
        errors = []
        for field, field_rules in self.rules.items():
            value = record.get(field)
            for rule in field_rules:
                validator = self.validators[rule["type"]]
                is_valid, error_msg = validator.validate(value, rule.get("params", {}))
                if not is_valid:
                    errors.append({
                        "field": field,
                        "rule": rule["type"],
                        "message": error_msg
                    })
        return ValidationResult(passed=len(errors) == 0, errors=errors)
```

**Configuration Example** (`validation_rules.yaml`):
```yaml
rules:
  transaction_id:
    - type: required
    - type: regex
      params:
        pattern: "^TXN[0-9]{10}$"

  amount:
    - type: required
    - type: type_check
      params:
        expected_type: float
    - type: range
      params:
        min: 0.01
        max: 1000000.00

  timestamp:
    - type: required
    - type: type_check
      params:
        expected_type: datetime
```

---

## 7. Schema Evolution Strategy

### Decision: Spark mergeSchema + Schema Registry

**Rationale**:
- **Automatic detection**: Spark can merge schemas from multiple files
- **Version tracking**: Registry stores all historical schemas
- **Backward compatibility**: Validate new schemas against previous versions
- **Confidence scoring**: Statistical analysis to infer types

**Implementation Approach**:
```python
# Schema inference
def infer_schema(sample_df: DataFrame, confidence_threshold: float = 0.95):
    """Infer schema from sample data with confidence scoring"""
    inferred_schema = sample_df.schema

    # Analyze type consistency across sample
    for field in inferred_schema.fields:
        consistency = calculate_type_consistency(sample_df, field.name)
        if consistency < confidence_threshold:
            # Low confidence, fall back to string type
            field.dataType = StringType()
            field.metadata = {"confidence": consistency, "needs_review": True}

    return inferred_schema

# Schema evolution
def evolve_schema(old_schema: StructType, new_schema: StructType):
    """Merge schemas with backward compatibility checks"""
    merged_fields = {}

    # Add all old fields
    for field in old_schema.fields:
        merged_fields[field.name] = field

    # Add new fields from new schema
    for field in new_schema.fields:
        if field.name not in merged_fields:
            # New field added
            merged_fields[field.name] = field
        elif merged_fields[field.name].dataType != field.dataType:
            # Type change detected - requires manual review
            raise SchemaEvolutionError(
                f"Type change for {field.name}: "
                f"{merged_fields[field.name].dataType} -> {field.dataType}"
            )

    return StructType(list(merged_fields.values()))
```

---

## 8. Idempotency & Fault Tolerance

### Decision: Upsert + Checkpointing + Transaction Boundaries

**Rationale**:
- **Upsert**: PostgreSQL UPSERT prevents duplicates on retry
- **Checkpointing**: Spark tracks processed offsets/files
- **Transactional writes**: All-or-nothing batch commits

**Batch Idempotency**:
```python
# Use foreachBatch for transactional writes
def write_batch_idempotent(batch_df: DataFrame, batch_id: int):
    """Write batch with idempotent upsert"""
    batch_df.write \
        .format("jdbc") \
        .option("url", postgres_url) \
        .option("dbtable", "warehouse_data") \
        .option("upsertOnConflict", "record_id") \
        .mode("append") \
        .save()

# Streaming with checkpoints
query = validated_stream \
    .writeStream \
    .foreachBatch(write_batch_idempotent) \
    .option("checkpointLocation", "/checkpoints/stream1") \
    .start()
```

**PostgreSQL Upsert**:
```sql
INSERT INTO warehouse_data (record_id, source_id, data, schema_version)
VALUES (%s, %s, %s, %s)
ON CONFLICT (record_id) DO UPDATE
SET
    data = EXCLUDED.data,
    processed_at = NOW(),
    schema_version = EXCLUDED.schema_version;
```

---

## 9. Observability Implementation

### Decision: Structured Logging + Prometheus Metrics + Lineage Table

**Rationale**:
- **Structured logs**: JSON format for easy parsing/querying
- **Metrics**: Standard Prometheus exposition for monitoring
- **Lineage**: Database table for auditable history

**Logging Setup**:
```python
import logging
from pythonjsonlogger import jsonlogger

# Configure JSON logging
logger = logging.getLogger("pipeline")
handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    fmt="%(asctime)s %(name)s %(levelname)s %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Usage
logger.info("Record validated", extra={
    "record_id": record_id,
    "source_id": source_id,
    "validation_result": "passed",
    "rules_checked": 5
})
```

**Metrics Collection**:
```python
from prometheus_client import Counter, Histogram, Gauge

# Define metrics
records_processed = Counter(
    "pipeline_records_processed_total",
    "Total records processed",
    ["source_id", "status"]  # Labels
)

processing_latency = Histogram(
    "pipeline_processing_seconds",
    "Record processing latency",
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0]
)

quarantine_size = Gauge(
    "pipeline_quarantine_records",
    "Current quarantine size"
)

# Usage
with processing_latency.time():
    result = validate_record(record)
    records_processed.labels(
        source_id=record.source_id,
        status="valid" if result.passed else "quarantined"
    ).inc()
```

---

## 10. Performance Optimization Techniques

### Best Practices Research

**Spark Optimization**:
1. **Data locality**: Partition by most-queried fields (source_id)
2. **Broadcast joins**: Use for small reference data (< 100MB)
3. **Caching**: Cache frequently reused DataFrames
4. **Coalesce vs repartition**: Use coalesce for reducing partitions (no shuffle)
5. **Predicate pushdown**: Filter early in the pipeline
6. **Column pruning**: Select only needed columns

**PostgreSQL Optimization**:
1. **COPY over INSERT**: 10-100x faster for bulk loads
2. **UNLOGGED tables**: 2-3x faster writes for quarantine (non-critical data)
3. **Batch commits**: Commit every 10K records, not per-record
4. **Index strategy**: Composite index on (source_id, processed_at) for queries
5. **VACUUM scheduling**: Regular maintenance for performance

**Memory Management**:
1. **Chunked processing**: Process batches of 10K-100K records
2. **Lazy evaluation**: Spark actions trigger computation, transformations are lazy
3. **Spillage monitoring**: Watch for disk spills in Spark UI

**Example Optimized Pipeline**:
```python
# Read with schema (avoids inference overhead)
df = spark.read.schema(known_schema).csv("input.csv")

# Filter early (predicate pushdown)
df = df.filter(col("timestamp") > cutoff_date)

# Select only needed columns (column pruning)
df = df.select("record_id", "source_id", "data", "timestamp")

# Repartition for parallelism
df = df.repartition(200, "source_id")

# Broadcast small reference data
broadcast_rules = spark.sparkContext.broadcast(validation_rules)

# Apply validation
validated_df = df.mapPartitions(
    lambda partition: validate_partition(partition, broadcast_rules.value)
)

# Cache if reused
validated_df.cache()

# Write efficiently
validated_df.write.mode("append").format("jdbc").save()
```

---

## Summary of Decisions

| Component | Choice | Version | Rationale |
|-----------|--------|---------|-----------|
| Language | Python | 3.11 | Latest stable, performance, ecosystem |
| Batch Engine | Apache Spark | 3.5.1 | Mature, scalable, schema support |
| Stream Engine | Spark Structured Streaming | 3.5.1 | Unified with batch, exactly-once |
| Warehouse | PostgreSQL | 16.2 | ACID, JSONB, upsert, open-source |
| Validation | Pydantic | 2.6 | Type safety, performance, Pythonic |
| Testing | pytest + testcontainers | 8.1 / 4.0 | Standard, Docker integration |
| Logging | python-json-logger | 2.0.7 | Structured, queryable |
| Metrics | prometheus-client | 0.20.0 | Standard exposition format |
| Container | Docker Compose | Latest | Local dev consistency |
| Message Queue | Kafka (optional) | 3.7.0 | Industry standard, or file-based |

All choices align with constitutional principles and prioritize open-source, stability, and community support.
