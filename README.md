# Dirty Spark Data Validation Pipeline

A robust data validation and transformation pipeline that ingests messy, real-world data from various sources and safely loads it into a PostgreSQL data warehouse. Handles dirty data, schema evolution, and impossible-to-normalize records in both batch and streaming modes.

## Features

- ✅ **Dual-Mode Processing**: Unified validation logic for both batch (Apache Spark) and streaming (Spark Structured Streaming)
- ✅ **Data Quality First**: Validates all data before warehouse insertion, quarantines invalid records with full error context
- ✅ **Warning-Level Validation**: Non-blocking validation warnings for data quality monitoring without failing records
- ✅ **Schema Flexibility**: Handles schema-less, un-tabulated, and evolving data structures without breaking
- ✅ **Idempotent & Reliable**: Upsert-based warehouse writes, exactly-once semantics, checkpoint-based recovery
- ✅ **Full Observability**: Structured JSON logging, Prometheus metrics, complete data lineage tracking
- ✅ **Type Safety**: Pydantic models with runtime validation, explicit type coercion rules
- ✅ **Input Validation**: Comprehensive validation utilities preventing SQL injection and path traversal attacks
- ✅ **Resource Safety**: Context managers for automatic database connection cleanup
- ✅ **Test-Driven**: Comprehensive unit, integration, and E2E tests with >80% coverage

## Quick Start

### Prerequisites

- **Docker** and **Docker Compose** (for local infrastructure)
- **Python 3.11+**
- **8GB RAM minimum**

### 1. Clone and Setup

```bash
git clone <repository-url>
cd dirty-spark-demo

# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Start Infrastructure

```bash
# Start PostgreSQL, Spark, and Kafka
docker-compose -f docker/docker-compose.yml up -d

# Verify all services are running
docker-compose -f docker/docker-compose.yml ps
```

### 3. Run Sample Batch Processing

```bash
# Process a dirty CSV file
python -m src.cli.batch_cli process \
  --source sample_transactions \
  --input tests/fixtures/dirty_data.csv

# View quarantined records
python -m src.cli.admin_cli quarantine-review --limit 10
```

### 4. Run Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
open htmlcov/index.html
```

## Project Structure

```
dirty-spark-demo/
├── src/
│   ├── core/              # Mode-agnostic validation logic
│   │   ├── models/        # Pydantic data models
│   │   ├── validators/    # Validation rule implementations
│   │   ├── schema/        # Schema inference and evolution
│   │   └── rules/         # Validation rule engine
│   ├── batch/             # Spark batch processing
│   │   ├── readers/       # CSV, file readers
│   │   ├── writers/       # Warehouse, quarantine writers
│   │   └── pipeline.py    # Batch orchestration
│   ├── streaming/         # Spark Structured Streaming
│   │   ├── sources/       # Kafka, file stream sources
│   │   ├── sinks/         # Streaming sinks
│   │   └── pipeline.py    # Streaming orchestration
│   ├── warehouse/         # PostgreSQL interactions
│   ├── observability/     # Logging, metrics, lineage
│   ├── utils/             # Validation utilities, helpers
│   └── cli/               # Command-line interfaces
├── tests/
│   ├── unit/              # Unit tests
│   ├── integration/       # Integration tests (testcontainers)
│   ├── e2e/               # End-to-end tests
│   └── fixtures/          # Test data
├── docker/                # Local infrastructure
│   ├── docker-compose.yml
│   ├── Dockerfile.spark
│   └── init-db.sql
├── docs/                  # Documentation
│   ├── CODE_REVIEW_FINDINGS.md
│   ├── IMPROVEMENTS_COMPLETED.md
│   └── LINTING_AND_PRECOMMIT_PLAN.md
└── config/                # Configuration files
    ├── validation_rules.yaml
    ├── local.env
    └── test.env
```

## Architecture

### Data Flow

```
CSV/Kafka → Spark Batch/Streaming → Validation Engine → [Valid] → PostgreSQL Warehouse
                                                      ↓
                                                   [Invalid] → Quarantine Table
                                                      ↓
                                            (with full error context)
```

### Key Components

- **Validation Engine**: Pluggable rule system (required field, type check, range, regex, custom) with warning-level support
- **Schema Registry**: Tracks schema versions with confidence scoring
- **Warehouse Writer**: Idempotent upserts using `INSERT ... ON CONFLICT` with safe resource management
- **Quarantine System**: Stores invalid records with detailed error messages for review
- **Audit Log**: Partitioned table tracking all transformations for compliance
- **Input Validation**: Security-focused utilities for SQL injection and path traversal prevention
- **Connection Pool**: Context-managed database connections with automatic cleanup

## Configuration

### Validation Rules

Edit `config/validation_rules.yaml`:

```yaml
rules:
  transaction_id:
    - rule_name: require_transaction_id
      rule_type: required_field
      field_name: transaction_id
      severity: error  # or "warning" for non-blocking
      enabled: true
    - rule_name: transaction_id_regex
      rule_type: regex
      field_name: transaction_id
      severity: error
      parameters:
        pattern: "^TXN[0-9]{10}$"

  amount:
    - rule_name: require_amount
      rule_type: required_field
      field_name: amount
      severity: error
    - rule_name: amount_type_check
      rule_type: type_check
      field_name: amount
      severity: error
      parameters:
        expected_type: float
    - rule_name: amount_range
      rule_type: range
      field_name: amount
      severity: error
      parameters:
        min: 0.01
        max: 1000000.00
    - rule_name: unusual_amount_check
      rule_type: range
      field_name: amount
      severity: warning  # Non-blocking warning
      parameters:
        min: 0.01
        max: 50000.00
```

### Environment Variables

See `config/local.env` for all configuration options:
- Database connection strings
- Spark configuration
- Kafka settings
- Validation thresholds
- Logging and metrics

## CLI Commands

### Batch Processing

```bash
# Process a batch file
python -m src.cli.batch_cli process \
  --source <source_id> \
  --input <file_path> \
  [--validation-rules <rules_file>] \
  [--dry-run]
```

### Streaming

```bash
# Start streaming pipeline
python -m src.cli.stream_cli start \
  --source <source_id> \
  --stream-source [kafka|file_stream] \
  --checkpoint-location <checkpoint_dir>

# Stop streaming
python -m src.cli.stream_cli stop --query-id <query_id>
```

### Administration

```bash
# Trace record lineage
python -m src.cli.admin_cli trace-record \
  --record-id <record_id> \
  [--limit <count>]

# Generate audit report
python -m src.cli.admin_cli audit-report \
  [--source <source_id>] \
  [--detailed] \
  [--limit <count>]

# Review quarantined records (Phase 6 feature - coming soon)
python -m src.cli.admin_cli quarantine-review \
  [--source <source_id>] \
  [--limit <count>]
```

## Development

### Running Tests

```bash
# Unit tests only
pytest tests/unit/ -v

# Integration tests (requires Docker)
pytest tests/integration/ -v -m integration

# End-to-end tests
pytest tests/e2e/ -v -m e2e

# With coverage
pytest tests/ --cov=src --cov-report=term-missing
```

### Code Quality

```bash
# Format code
black src/ tests/

# Lint code
ruff check src/ tests/

# Type checking
mypy src/
```

### Building Docker Images

```bash
# Build Spark image
cd docker
docker build -t dirty-spark:3.5.1 -f Dockerfile.spark .
```

## Monitoring

### Access Points

- **Spark Master UI**: http://localhost:8080
- **Spark Worker UI**: http://localhost:8081
- **Prometheus Metrics**: http://localhost:8000/metrics

### Metrics Exposed

- `pipeline_records_processed_total{source_id, status}`: Total records processed
- `pipeline_processing_duration_seconds{source_id, mode}`: Record processing latency histogram
- `pipeline_quarantine_size{source_id}`: Current quarantine size
- `pipeline_validation_failures_total{source_id, rule_type, field_name}`: Validation failures by rule
- `pipeline_validation_warnings_total{source_id, rule_name}`: Non-blocking validation warnings
- `pipeline_streaming_latency_seconds{source_id}`: End-to-end streaming latency

## Troubleshooting

### PostgreSQL Connection Failed

```bash
# Check if container is running
docker-compose -f docker/docker-compose.yml ps postgres

# Test connection
psql -h localhost -U pipeline -d datawarehouse -c "SELECT 1;"
# Password: dev_password
```

### Spark Worker Not Connecting

```bash
# Check master logs
docker logs dirty-spark-master

# Restart Spark cluster
docker-compose -f docker/docker-compose.yml restart spark-master spark-worker
```

### Tests Failing

```bash
# Ensure Docker is running for testcontainers
docker info

# Clean test artifacts
rm -rf .pytest_cache __pycache__ .mypy_cache

# Re-run with verbose output
pytest tests/ -vv --tb=short
```

## Documentation

### Specifications
- **Specification**: `specs/001-dirty-data-validation/spec.md`
- **Implementation Plan**: `specs/001-dirty-data-validation/plan.md`
- **Tasks Breakdown**: `specs/001-dirty-data-validation/tasks.md`
- **Data Model**: `specs/001-dirty-data-validation/data-model.md`
- **Quickstart Guide**: `specs/001-dirty-data-validation/quickstart.md`

### Code Quality & Improvements
- **Code Review Findings**: `docs/CODE_REVIEW_FINDINGS.md` - Comprehensive code analysis
- **Improvements Completed**: `docs/IMPROVEMENTS_COMPLETED.md` - All implemented enhancements
- **Linting & Pre-commit Plan**: `docs/LINTING_AND_PRECOMMIT_PLAN.md` - Code quality automation

### API Documentation
- **Input Validation**: See `src/utils/validation.py` for security-focused validation functions
- **Connection Management**: See `src/warehouse/connection.py` for context managers

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests and code quality checks
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## License

MIT License - See LICENSE file for details

## Support

For issues, questions, or contributions, please open an issue on GitHub.

---

**Built with ❤️ using Apache Spark, PostgreSQL, and Python**
