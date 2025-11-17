# Implementation Plan: Dirty Data Validation Pipeline

**Branch**: `001-dirty-data-validation` | **Date**: 2025-11-17 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-dirty-data-validation/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/commands/plan.md` for the execution workflow.

## Summary

Build a data validation and transformation pipeline that ingests messy, real-world data from CSV files and message queues, validates against configurable rules, quarantines invalid records with detailed error context, and safely loads clean data into a PostgreSQL data warehouse. The system supports both batch processing (Apache Spark) and real-time streaming (Spark Structured Streaming) using identical validation logic, with full data lineage tracking and idempotent warehouse writes.

## Technical Context

**Language/Version**: Python 3.11 (latest stable with type hints and performance improvements)
**Primary Dependencies**:
- Apache Spark 3.5.1 (batch processing, latest stable)
- Spark Structured Streaming 3.5.1 (streaming mode)
- PySpark 3.5.1 (Python API for Spark)
- PostgreSQL 16.2 (data warehouse, latest stable with improved performance)
- psycopg3 3.1.18 (PostgreSQL adapter with connection pooling)
- Pydantic 2.6 (data validation and schema management)
- PyArrow 15.0 (efficient columnar data format)

**Storage**:
- PostgreSQL 16.2 (primary warehouse for clean data)
- File system (quarantine storage for invalid records, checkpoint storage)
- Schema registry: PostgreSQL JSONB columns

**Testing**:
- pytest 8.1 (unit and integration testing)
- pytest-spark 0.6.0 (Spark testing utilities)
- testcontainers 4.0 (PostgreSQL and Kafka containers for local testing)
- hypothesis 6.98 (property-based testing for validation rules)
- great-expectations 0.18 (data quality assertions)
- localstack 3.2 (local AWS services if needed)

**Target Platform**: Linux server (Ubuntu 22.04 LTS or similar)

**Project Type**: Single project (data pipeline)

**Performance Goals**:
- Batch: 10,000+ records/minute throughput
- Streaming: <2 second end-to-end latency
- Schema inference: <10 seconds for 100K records

**Constraints**:
- Memory efficient: constant memory usage via chunked processing
- Fault tolerant: resume from checkpoint within 10 seconds
- Zero data loss: all records accounted for (loaded or quarantined)

**Scale/Scope**:
- Handle 1K to 100M records per batch
- Support hundreds of schema versions
- Process multiple concurrent data sources

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### ✅ Principle I: Data Quality First (NON-NEGOTIABLE)

- **Validation before insertion**: Pydantic validators + custom rule engine ✓
- **Quarantine with context**: Separate quarantine table + error metadata ✓
- **Lineage tracking**: Audit log table with transformation history ✓
- **No silent failures**: All records accounted for in metrics ✓

### ✅ Principle II: Dual-Mode Processing (Batch + Stream)

- **Unified validation logic**: Shared validation module used by both Spark batch and Structured Streaming ✓
- **Mode-agnostic core**: Validation rules independent of processing engine ✓
- **Adapters for modes**: Batch reader/writer and stream source/sink as separate adapters ✓

### ✅ Principle III: Schema Flexibility & Evolution

- **Schema inference**: Spark schema inference + Pydantic dynamic models ✓
- **Schema versioning**: Schema registry with JSONB storage ✓
- **Evolution handling**: Schema merging logic with backward compatibility checks ✓

### ✅ Principle IV: Idempotency & Reliability (NON-NEGOTIABLE)

- **Upsert-based writes**: PostgreSQL INSERT ... ON CONFLICT UPDATE ✓
- **Transactional batch**: Spark write transactions + PostgreSQL ACID ✓
- **Exactly-once streaming**: Spark checkpointing + idempotent writes ✓
- **Checkpoint management**: Spark checkpoint directory on persistent storage ✓

### ✅ Principle V: Observability & Debugging

- **Structured logging**: Python logging with JSON formatter ✓
- **Metrics collection**: Spark metrics + custom counters (Prometheus compatible) ✓
- **Lineage tracking**: Audit table with record transformations ✓
- **Failed record sampling**: Configurable sampling rate to quarantine ✓

### ✅ Principle VI: Performance & Scalability

- **Horizontal scaling**: Spark cluster with multiple workers ✓
- **Partition-based parallelism**: Spark partitioning by source/key ✓
- **Bulk operations**: Batch warehouse writes (COPY for PostgreSQL) ✓
- **Resource monitoring**: Spark UI + system metrics ✓

### ✅ Principle VII: Type Safety & Validation

- **Type validation**: Pydantic models with type checking ✓
- **Coercion rules**: Custom type coercers with error handling ✓
- **Constraint validation**: Pydantic validators for ranges, regex, etc. ✓

### ✅ Principle VIII: Test-Driven Development (NON-NEGOTIABLE)

- **Unit tests**: pytest for validation logic, schema inference ✓
- **Integration tests**: testcontainers for database operations ✓
- **E2E tests**: pytest-spark for full pipeline with sample dirty data ✓
- **Property-based tests**: hypothesis for validation rule fuzzing ✓

**Gate Status**: ✅ PASS - All constitutional principles satisfied

## Project Structure

### Documentation (this feature)

```text
specs/001-dirty-data-validation/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
│   ├── validation-api.yaml   # Validation rule management API
│   └── schemas.json          # Data schemas and entity definitions
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
src/
├── core/                      # Mode-agnostic validation logic
│   ├── validators/            # Validation rule implementations
│   │   ├── required_field.py
│   │   ├── type_validator.py
│   │   ├── range_validator.py
│   │   └── custom_validator.py
│   ├── schema/                # Schema inference and evolution
│   │   ├── inference.py
│   │   ├── evolution.py
│   │   └── registry.py
│   ├── models/                # Pydantic data models
│   │   ├── data_record.py
│   │   ├── validation_result.py
│   │   └── quarantine_record.py
│   └── rules/                 # Validation rule engine
│       ├── rule_engine.py
│       └── rule_config.py
├── batch/                     # Spark batch processing
│   ├── readers/               # Batch data source readers
│   │   ├── csv_reader.py
│   │   └── file_reader.py
│   ├── writers/               # Batch data sink writers
│   │   ├── warehouse_writer.py
│   │   └── quarantine_writer.py
│   └── pipeline.py            # Batch pipeline orchestration
├── streaming/                 # Spark Structured Streaming
│   ├── sources/               # Stream data sources
│   │   ├── kafka_source.py
│   │   └── file_stream_source.py
│   ├── sinks/                 # Stream data sinks
│   │   ├── warehouse_sink.py
│   │   └── quarantine_sink.py
│   └── pipeline.py            # Streaming pipeline orchestration
├── warehouse/                 # PostgreSQL interactions
│   ├── connection.py          # Connection pool management
│   ├── upsert.py              # Idempotent upsert operations
│   ├── schema_mgmt.py         # Schema DDL operations
│   └── audit.py               # Lineage and audit logging
├── observability/             # Logging and metrics
│   ├── logger.py              # Structured JSON logging
│   ├── metrics.py             # Custom metrics collection
│   └── lineage.py             # Data lineage tracking
├── cli/                       # Command-line interface
│   ├── batch_cli.py           # Batch processing commands
│   ├── stream_cli.py          # Streaming commands
│   └── admin_cli.py           # Admin/config commands
└── config/                    # Configuration management
    ├── validation_rules.yaml  # Validation rule definitions
    ├── batch_config.yaml      # Batch processing config
    └── stream_config.yaml     # Streaming config

tests/
├── unit/                      # Unit tests
│   ├── test_validators.py
│   ├── test_schema_inference.py
│   ├── test_type_coercion.py
│   └── test_rule_engine.py
├── integration/               # Integration tests
│   ├── test_batch_pipeline.py
│   ├── test_stream_pipeline.py
│   ├── test_warehouse_ops.py
│   └── test_quarantine.py
├── e2e/                       # End-to-end tests
│   ├── test_dirty_csv_batch.py
│   ├── test_streaming_flow.py
│   └── test_reprocessing.py
├── fixtures/                  # Test data fixtures
│   ├── dirty_data.csv
│   ├── malformed_events.json
│   └── schema_evolution_data.csv
└── conftest.py                # pytest configuration

docker/                        # Local testing infrastructure
├── docker-compose.yml         # PostgreSQL, Kafka, Spark cluster
├── Dockerfile.spark           # Spark master/worker image
└── init-db.sql                # Database initialization

config/                        # Environment configurations
├── local.env                  # Local development config
├── test.env                   # Test environment config
└── validation-rules-example.yaml
```

**Structure Decision**: Single project structure chosen because this is a data pipeline application without separate frontend/backend or mobile components. The structure separates concerns by processing mode (batch vs streaming) while sharing core validation logic, which aligns with Principle II (Dual-Mode Processing).

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

No violations - constitution check passed all gates.
