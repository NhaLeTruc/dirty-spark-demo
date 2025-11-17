# Tasks: Dirty Data Validation Pipeline

**Input**: Design documents from `/specs/001-dirty-data-validation/`
**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Tests are REQUIRED per Constitution Principle VIII (Test-Driven Development - NON-NEGOTIABLE). All tasks include test-first approach.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Single project**: `src/`, `tests/` at repository root
- Paths shown below follow plan.md structure with batch/stream separation

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [x] T001 Create project root directories (src/, tests/, docker/, config/)
- [x] T002 Initialize Python 3.11 project with pyproject.toml and setup.py
- [x] T003 [P] Create requirements.txt with pinned versions (pyspark==3.5.1, psycopg[binary,pool]==3.1.18, pydantic==2.6.3, pytest==8.1.0, etc.)
- [x] T004 [P] Create .gitignore for Python (__pycache__, .venv, .pytest_cache, checkpoints/)
- [x] T005 [P] Create docker/docker-compose.yml with PostgreSQL 16.2, Spark 3.5.1, Kafka 3.7.0, Zookeeper
- [x] T006 [P] Create docker/Dockerfile.spark for Spark master/worker image
- [x] T007 [P] Create docker/init-db.sql with DDL from data-model.md (all tables, indexes, partitions)
- [x] T008 [P] Create config/local.env with database connection strings and Spark config
- [x] T009 [P] Create config/test.env for test environment
- [x] T010 [P] Configure pytest in tests/conftest.py with testcontainers fixtures
- [x] T011 [P] Configure black, ruff, mypy in pyproject.toml
- [x] T012 [P] Create src/__init__.py and all submodule __init__.py files
- [x] T013 Create README.md with quickstart instructions

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [x] T014 [P] Create src/warehouse/connection.py with PostgreSQL connection pool using psycopg3
- [x] T015 [P] Create src/observability/logger.py with structured JSON logging (python-json-logger)
- [x] T016 [P] Create src/observability/metrics.py with Prometheus metrics (Counter, Histogram, Gauge)
- [x] T017 [P] Create tests/unit/test_connection_pool.py to verify database connectivity
- [x] T018 Create src/core/models/data_source.py with Pydantic DataSource model from data-model.md
- [x] T019 Create src/core/models/validation_rule.py with Pydantic ValidationRule model from data-model.md
- [x] T020 Create src/core/models/data_record.py with Pydantic DataRecord model from data-model.md
- [x] T021 Create src/core/models/warehouse_data.py with Pydantic WarehouseData model from data-model.md
- [x] T022 Create src/core/models/quarantine_record.py with Pydantic QuarantineRecord model from data-model.md
- [x] T023 Create src/core/models/validation_result.py with Pydantic ValidationResult model from data-model.md
- [x] T024 Create src/core/models/schema_version.py with Pydantic SchemaVersion model from data-model.md
- [x] T025 Create src/core/models/audit_log.py with Pydantic AuditLog model from data-model.md
- [x] T026 [P] Create tests/unit/test_pydantic_models.py to validate all models with valid/invalid data
- [x] T027 Create src/core/validators/base_validator.py with abstract Validator interface
- [x] T028 [P] Create src/core/validators/required_field.py with RequiredFieldValidator
- [x] T029 [P] Create src/core/validators/type_validator.py with TypeCheckValidator (string, int, float, datetime)
- [x] T030 [P] Create src/core/validators/range_validator.py with RangeValidator (min/max)
- [x] T031 [P] Create src/core/validators/regex_validator.py with RegexValidator
- [x] T032 [P] Create src/core/validators/custom_validator.py with extensible CustomValidator framework
- [x] T033 [P] Create tests/unit/test_validators.py with property-based tests (hypothesis) for all validators
- [x] T034 Create src/core/rules/rule_config.py to load validation rules from YAML config
- [x] T035 Create src/core/rules/rule_engine.py with ValidationEngine that runs all rules on a record
- [x] T036 Create tests/unit/test_rule_engine.py to verify rule engine with multiple rules
- [ ] T037 Create config/validation_rules.yaml with sample rules (transaction_id, amount, timestamp, email)
- [x] T038 Create src/warehouse/schema_mgmt.py with DDL execution and table creation functions
- [x] T039 Create src/warehouse/upsert.py with idempotent upsert using INSERT ... ON CONFLICT
- [x] T040 Create tests/integration/test_warehouse_ops.py with testcontainers PostgreSQL to verify upsert idempotency

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Batch Historical Data Ingestion (Priority: P1) 🎯 MVP

**Goal**: Process dirty CSV files from legacy systems, validate data, load valid records to warehouse, quarantine invalid records with error context

**Independent Test**: Provide a dirty CSV file with missing fields, duplicates, type errors → Run batch pipeline → Verify valid records in warehouse, invalid records in quarantine with specific error messages

### Tests for User Story 1 ⚠️

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation (TDD - NON-NEGOTIABLE)**

- [ ] T041 [P] [US1] Create tests/fixtures/dirty_data.csv with intentionally dirty data (missing fields, wrong types, duplicates, nulls)
- [ ] T042 [P] [US1] Create tests/e2e/test_dirty_csv_batch.py with end-to-end test: CSV → validation → warehouse + quarantine
- [ ] T043 [P] [US1] Create tests/integration/test_batch_pipeline.py to verify batch processing with Spark (using pytest-spark)

### Implementation for User Story 1

- [ ] T044 [P] [US1] Create src/core/schema/inference.py with Spark schema inference and confidence scoring
- [ ] T045 [P] [US1] Create tests/unit/test_schema_inference.py with various CSV schemas and edge cases
- [ ] T046 [US1] Create src/core/schema/registry.py to save/load schemas from schema_version table
- [ ] T047 [US1] Create src/batch/readers/csv_reader.py to read CSV files using Spark with schema inference
- [ ] T048 [US1] Create src/batch/readers/file_reader.py for generic file reading (CSV, JSON, Parquet)
- [ ] T049 [US1] Create src/batch/pipeline.py to orchestrate batch processing: read → validate → route → write
- [ ] T050 [US1] Integrate ValidationEngine from Phase 2 into batch pipeline (apply rules to DataFrame)
- [ ] T051 [US1] Create src/batch/writers/warehouse_writer.py to write valid records to warehouse_data table using bulk COPY
- [ ] T052 [US1] Create src/batch/writers/quarantine_writer.py to write invalid records to quarantine_record table with error details
- [ ] T053 [US1] Add duplicate detection logic in batch pipeline using record_id groupBy and deduplication
- [ ] T054 [US1] Add type coercion logic (string → int/float/datetime) with fallback to quarantine on failure
- [ ] T055 [US1] Create tests/unit/test_type_coercion.py to verify coercion rules and error handling
- [ ] T056 [US1] Create src/cli/batch_cli.py with "process" command (argparse) for batch processing
- [ ] T057 [US1] Add --dry-run flag to batch CLI for validation without writing
- [ ] T058 [US1] Add batch metrics collection (records_processed, valid, invalid, throughput) to src/observability/metrics.py
- [ ] T059 [US1] Add structured logging for batch pipeline (start, end, errors) to src/observability/logger.py
- [ ] T060 [US1] Run tests/e2e/test_dirty_csv_batch.py and verify all assertions pass

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently (MVP complete!)

---

## Phase 4: User Story 4 - Data Lineage and Audit Trail (Priority: P2)

**Goal**: Track every record from source to warehouse with full transformation history, enable audit queries for compliance

**Independent Test**: Process a record through batch pipeline → Query audit_log table → Verify complete lineage (source, transformations, destination)

### Tests for User Story 4 ⚠️

- [ ] T061 [P] [US4] Create tests/integration/test_audit_trail.py to verify lineage tracking end-to-end
- [ ] T062 [P] [US4] Create tests/unit/test_lineage.py to verify AuditLog creation logic

### Implementation for User Story 4

- [ ] T063 [P] [US4] Create src/warehouse/audit.py with functions to insert audit log entries
- [ ] T064 [US4] Create src/observability/lineage.py to track transformations (type coercion, date normalization, etc.)
- [ ] T065 [US4] Integrate lineage tracking into batch pipeline (log every transformation)
- [ ] T066 [US4] Add lineage tracking to warehouse writer (log successful warehouse writes)
- [ ] T067 [US4] Add lineage tracking to quarantine writer (log quarantine events)
- [ ] T068 [US4] Create src/cli/admin_cli.py with "trace-record" command to query audit trail by record_id
- [ ] T069 [US4] Add audit report generation command to admin CLI (total records, loaded, quarantined, transformations)
- [ ] T070 [US4] Create tests for lineage query functions (verify audit_log table queries)
- [ ] T071 [US4] Run tests/integration/test_audit_trail.py and verify lineage completeness

**Checkpoint**: At this point, User Stories 1 AND 4 should both work independently (batch + audit operational)

---

## Phase 5: User Story 2 - Real-Time Stream Data Validation (Priority: P2)

**Goal**: Consume JSON events from Kafka or file stream in real-time, validate using same rules as batch, write to warehouse within 2 seconds

**Independent Test**: Publish test JSON events to Kafka → Start streaming pipeline → Verify events appear in warehouse within 2 seconds, malformed events in quarantine

### Tests for User Story 2 ⚠️

- [ ] T072 [P] [US2] Create tests/fixtures/malformed_events.json with valid and invalid JSON events
- [ ] T073 [P] [US2] Create tests/e2e/test_streaming_flow.py with file stream test (watch directory, process events)
- [ ] T074 [P] [US2] Create tests/integration/test_stream_pipeline.py to verify Spark Structured Streaming logic

### Implementation for User Story 2

- [ ] T075 [P] [US2] Create src/streaming/sources/file_stream_source.py to read JSON files from directory using Spark readStream
- [ ] T076 [P] [US2] Create src/streaming/sources/kafka_source.py to consume from Kafka using Spark Structured Streaming
- [ ] T077 [US2] Create src/streaming/pipeline.py to orchestrate streaming: read → validate → route → write
- [ ] T078 [US2] Integrate ValidationEngine from Phase 2 into streaming pipeline (apply rules to streaming DataFrame)
- [ ] T079 [US2] Add schema evolution detection in streaming pipeline (detect new fields mid-stream)
- [ ] T080 [US2] Create src/core/schema/evolution.py with schema merging and backward compatibility checks
- [ ] T081 [US2] Create tests/unit/test_schema_evolution.py to verify schema merging logic
- [ ] T082 [US2] Create tests/fixtures/schema_evolution_data.csv with schema changes (new columns, type changes)
- [ ] T083 [US2] Create src/streaming/sinks/warehouse_sink.py to write valid stream records to warehouse using foreachBatch
- [ ] T084 [US2] Create src/streaming/sinks/quarantine_sink.py to write invalid stream records to quarantine
- [ ] T085 [US2] Add checkpoint management for exactly-once semantics (checkpointLocation in streaming config)
- [ ] T086 [US2] Add streaming metrics (latency, throughput, backpressure) to src/observability/metrics.py
- [ ] T087 [US2] Create src/cli/stream_cli.py with "start" command to start streaming pipeline
- [ ] T088 [US2] Add "stop" command to stream CLI to gracefully stop streaming query
- [ ] T089 [US2] Add "status" command to stream CLI to show streaming query status
- [ ] T090 [US2] Integrate lineage tracking into streaming pipeline (reuse src/observability/lineage.py)
- [ ] T091 [US2] Create config/stream_config.yaml with Spark streaming configuration (trigger interval, checkpoint location)
- [ ] T092 [US2] Run tests/e2e/test_streaming_flow.py and verify events processed within 2 seconds

**Checkpoint**: At this point, User Stories 1, 2, AND 4 should all work independently (batch + streaming + audit operational)

---

## Phase 6: User Story 3 - Quarantine Review and Reprocessing (Priority: P3)

**Goal**: Enable analysts to review quarantined records, update validation rules, and reprocess failed records

**Independent Test**: Quarantine records with overly strict rule → View quarantine dashboard → Update rule → Trigger reprocessing → Verify records move to warehouse

### Tests for User Story 3 ⚠️

- [ ] T093 [P] [US3] Create tests/e2e/test_reprocessing.py to verify reprocessing workflow end-to-end
- [ ] T094 [P] [US3] Create tests/integration/test_quarantine.py to verify quarantine queries and updates

### Implementation for User Story 3

- [ ] T095 [P] [US3] Add "quarantine-review" command to admin CLI to display quarantined records grouped by error type
- [ ] T096 [US3] Add quarantine filtering by source_id, date range, error type in admin CLI
- [ ] T097 [US3] Add quarantine statistics query (count by error type, top failed rules)
- [ ] T098 [US3] Add "add-rule" command to admin CLI to insert validation rules into validation_rule table
- [ ] T099 [US3] Add "update-rule" command to admin CLI to modify existing rules
- [ ] T100 [US3] Add "disable-rule" command to admin CLI to set enabled=false
- [ ] T101 [US3] Create src/batch/reprocess.py to fetch quarantine records and re-run validation with updated rules
- [ ] T102 [US3] Add "reprocess" command to admin CLI with --source and --quarantine-ids options
- [ ] T103 [US3] Add reprocessing metrics (reprocessed_count, success_rate) to metrics.py
- [ ] T104 [US3] Add "mark-reviewed" command to admin CLI to set reviewed=true for quarantine records
- [ ] T105 [US3] Run tests/e2e/test_reprocessing.py and verify records move from quarantine to warehouse

**Checkpoint**: All user stories should now be independently functional and tested

---

## Phase N: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] T106 [P] Create config/validation-rules-example.yaml with comprehensive rule examples for documentation
- [ ] T107 [P] Add docstrings to all public functions and classes (Google style)
- [ ] T108 [P] Run black formatter on entire codebase
- [ ] T109 [P] Run ruff linter and fix all warnings
- [ ] T110 [P] Run mypy type checker and fix all type errors
- [ ] T111 [P] Add error handling and retry logic for database connection failures
- [ ] T112 [P] Add graceful shutdown for streaming pipelines (handle SIGTERM/SIGINT)
- [ ] T113 Create .github/workflows/ci.yml with pytest, linting, type checking on every push
- [ ] T114 Run pytest with coverage report (pytest --cov=src --cov-report=html)
- [ ] T115 Verify coverage >80% for all modules
- [ ] T116 [P] Add performance benchmarks for batch processing (measure throughput with 100K records)
- [ ] T117 [P] Add performance benchmarks for streaming (measure latency with event bursts)
- [ ] T118 Create tests/fixtures/large_dataset.csv with 100K+ records for performance testing
- [ ] T119 Validate quickstart.md instructions work end-to-end on clean environment
- [ ] T120 Create CONTRIBUTING.md with development guidelines

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-6)**: All depend on Foundational phase completion
  - User Story 1 (Phase 3): Can start after Foundational - No dependencies on other stories
  - User Story 4 (Phase 4): Can start after Foundational - Independent (but logically extends US1)
  - User Story 2 (Phase 5): Can start after Foundational - Independent (reuses validation from US1)
  - User Story 3 (Phase 6): Depends on US1 completing (needs quarantine data)
- **Polish (Final Phase)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories ✅ MVP
- **User Story 4 (P2)**: Can start after Foundational (Phase 2) - Extends US1 for audit compliance
- **User Story 2 (P2)**: Can start after Foundational (Phase 2) - Reuses validation logic from US1
- **User Story 3 (P3)**: Requires US1 to complete first (needs quarantine records to exist)

### Within Each User Story

- Tests (TDD - NON-NEGOTIABLE) MUST be written and FAIL before implementation
- Models before services
- Services before CLI/endpoints
- Core implementation before integration
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel (T003-T013)
- All Foundational tasks marked [P] can run in parallel within categories:
  - Models (T018-T025)
  - Validators (T028-T032)
  - Tests (T026, T033, T036)
- Once Foundational phase completes, user stories can start in parallel (if team capacity allows):
  - US1 + US2 can be developed in parallel by different developers
  - US4 can be developed in parallel with US1 or US2
- Tests for a user story marked [P] can run in parallel (e.g., T041-T043)
- Models within a story marked [P] can run in parallel

---

## Parallel Example: User Story 1 (Batch)

```bash
# Launch all tests for User Story 1 together (TDD - write first):
Task T041: Create dirty CSV fixture
Task T042: Create E2E test (will fail)
Task T043: Create integration test (will fail)

# Launch all independent implementation tasks together:
Task T044: Schema inference
Task T045: Schema inference tests
Task T051: Warehouse writer
Task T052: Quarantine writer

# Sequential dependencies:
Task T046 (schema registry) → depends on T044 completing
Task T047 (CSV reader) → depends on T044 completing
Task T049 (batch pipeline) → depends on T047, T050, T051, T052 completing
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 1 (Batch Processing)
4. **STOP and VALIDATE**: Test User Story 1 independently with dirty CSV
5. Deploy/demo if ready

**MVP Scope**: T001-T060 (60 tasks)
**Estimated Effort**: ~3-4 weeks for single developer

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → Deploy/Demo (MVP! 🎯)
3. Add User Story 4 → Test independently → Deploy/Demo (Compliance ready)
4. Add User Story 2 → Test independently → Deploy/Demo (Streaming operational)
5. Add User Story 3 → Test independently → Deploy/Demo (Full admin UI)
6. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (Batch)
   - Developer B: User Story 4 (Audit)
   - Developer C: User Story 2 (Streaming)
3. User Story 3 starts after US1 completes
4. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- **TDD is NON-NEGOTIABLE**: Verify tests fail before implementing
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence

**Total Tasks**: 120
- Setup: 13 tasks
- Foundational: 27 tasks
- User Story 1 (Batch - P1): 20 tasks 🎯 MVP
- User Story 4 (Audit - P2): 11 tasks
- User Story 2 (Streaming - P2): 21 tasks
- User Story 3 (Quarantine - P3): 13 tasks
- Polish: 15 tasks

**Test Tasks**: 22 (following TDD - tests written before implementation)
**Parallel Tasks**: 47 (marked with [P])

**Suggested First Sprint**: T001-T060 (Setup + Foundational + US1 Batch) = MVP in ~3-4 weeks
