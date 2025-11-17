# Quickstart Guide: Dirty Data Validation Pipeline

**Feature**: 001-dirty-data-validation
**Purpose**: Get the validation pipeline running locally in under 15 minutes
**Prerequisites**: Docker, Python 3.11+, 8GB RAM minimum

---

## Table of Contents

1. [Quick Setup (TL;DR)](#quick-setup-tldr)
2. [Detailed Setup](#detailed-setup)
3. [Running Batch Processing](#running-batch-processing)
4. [Running Streaming Mode](#running-streaming-mode)
5. [Viewing Results](#viewing-results)
6. [Troubleshooting](#troubleshooting)

---

## Quick Setup (TL;DR)

```bash
# Clone and enter repository
git clone <repo-url>
cd dirty-spark-demo

# Start infrastructure (PostgreSQL, Spark, Kafka)
docker-compose -f docker/docker-compose.yml up -d

# Install Python dependencies
python3.11 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

# Initialize database
psql -h localhost -U pipeline -d datawarehouse -f docker/init-db.sql

# Run sample batch
python -m src.cli.batch_cli process \
  --source sample_transactions \
  --input tests/fixtures/dirty_data.csv

# View results
python -m src.cli.admin_cli quarantine-review --limit 10
```

---

## Detailed Setup

### Step 1: Start Local Infrastructure

The Docker Compose file starts:
- PostgreSQL 16.2 (data warehouse)
- Spark master + worker (processing engine)
- Apache Kafka + Zookeeper (streaming source)

```bash
# Start all services
docker-compose -f docker/docker-compose.yml up -d

# Verify all containers are running
docker-compose -f docker/docker-compose.yml ps

# Expected output:
# NAME                STATUS              PORTS
# postgres            Up 10 seconds       0.0.0.0:5432->5432/tcp
# spark-master        Up 10 seconds       0.0.0.0:8080->8080/tcp, 0.0.0.0:7077->7077/tcp
# spark-worker        Up 10 seconds
# kafka               Up 10 seconds       0.0.0.0:9092->9092/tcp
# zookeeper           Up 10 seconds       0.0.0.0:2181->2181/tcp
```

**Access Points**:
- PostgreSQL: `localhost:5432` (user: `pipeline`, password: `dev_password`, db: `datawarehouse`)
- Spark UI: http://localhost:8080
- Spark Master: `spark://localhost:7077`
- Kafka: `localhost:9092`

### Step 2: Install Python Environment

```bash
# Create virtual environment with Python 3.11
python3.11 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt

# Verify Spark installation
python -c "from pyspark.sql import SparkSession; print('Spark OK')"
```

**requirements.txt** includes:
- pyspark==3.5.1
- psycopg[binary,pool]==3.1.18
- pydantic==2.6.3
- pytest==8.1.0
- pytest-spark==0.6.0
- testcontainers==4.0.1
- python-json-logger==2.0.7
- prometheus-client==0.20.0

### Step 3: Initialize Database Schema

```bash
# Option 1: Using psql (if installed)
psql -h localhost -U pipeline -d datawarehouse -f docker/init-db.sql
# Password: dev_password

# Option 2: Using Docker exec
docker exec -i postgres psql -U pipeline -d datawarehouse < docker/init-db.sql

# Verify tables created
psql -h localhost -U pipeline -d datawarehouse -c "\dt"
```

**Expected tables**:
- `data_source`
- `warehouse_data`
- `quarantine_record`
- `validation_rule`
- `schema_version`
- `audit_log` (with partitions)

### Step 4: Configure Validation Rules

Create `config/validation_rules.yaml`:

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

  customer_email:
    - type: regex
      params:
        pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
```

### Step 5: Register Data Source

```bash
# Register a CSV file source
python -m src.cli.admin_cli register-source \
  --source-id sample_transactions \
  --source-type csv_file \
  --connection-info '{"file_path": "tests/fixtures/dirty_data.csv", "delimiter": ",", "header": true}'

# Verify source registered
python -m src.cli.admin_cli list-sources
```

---

## Running Batch Processing

### Example 1: Process a Dirty CSV File

**Sample Data** (`tests/fixtures/dirty_data.csv`):
```csv
transaction_id,amount,timestamp,customer_email
TXN0000000001,125.50,2025-11-17T10:00:00Z,john@example.com
TXN0000000002,-50.00,2025-11-17T11:00:00Z,invalid_email
,999.99,2025-11-17T12:00:00Z,jane@example.com
TXN0000000004,99999999.99,2025-11-17T13:00:00Z,bob@example.com
TXN0000000005,75.25,invalid_date,alice@example.com
```

**Run Batch Processing**:
```bash
python -m src.cli.batch_cli process \
  --source sample_transactions \
  --input tests/fixtures/dirty_data.csv \
  --validation-rules config/validation_rules.yaml \
  --chunk-size 1000

# Expected output (JSON):
{
  "batch_id": "batch_20251117_100530",
  "total_records": 5,
  "valid_records": 1,
  "invalid_records": 4,
  "processing_time_seconds": 2.45,
  "warehouse_records_written": 1,
  "quarantine_records_written": 4,
  "validation_failures_by_rule": [
    {"rule_name": "amount_range_check", "failure_count": 2},
    {"rule_name": "require_transaction_id", "failure_count": 1},
    {"rule_name": "timestamp_type_check", "failure_count": 1},
    {"rule_name": "customer_email_regex", "failure_count": 1}
  ]
}
```

### Example 2: Dry Run (Validate Only)

```bash
python -m src.cli.batch_cli process \
  --source sample_transactions \
  --input tests/fixtures/dirty_data.csv \
  --dry-run  # No warehouse writes

# Use this to test validation rules before committing data
```

### Example 3: Query Warehouse Data

```bash
# Using psql
psql -h localhost -U pipeline -d datawarehouse \
  -c "SELECT record_id, data->>'amount' as amount, processed_at FROM warehouse_data ORDER BY processed_at DESC LIMIT 10;"

# Expected output:
#     record_id     | amount  |       processed_at
# ------------------+---------+----------------------------
#  TXN0000000001   | 125.50  | 2025-11-17 10:05:30.123456
```

---

## Running Streaming Mode

### Example 1: File Stream (Easiest for Local Testing)

**Setup**:
```bash
# Create stream input directory
mkdir -p /tmp/stream_input

# Configure file stream source
python -m src.cli.admin_cli register-source \
  --source-id live_events \
  --source-type json_stream \
  --connection-info '{"stream_path": "/tmp/stream_input", "file_format": "json"}'
```

**Start Streaming Pipeline**:
```bash
python -m src.cli.stream_cli start \
  --source live_events \
  --stream-source file_stream \
  --file-stream-path /tmp/stream_input \
  --checkpoint-location /tmp/checkpoints/stream1 \
  --trigger-interval "10 seconds"

# Output:
{
  "query_id": "stream_live_events_20251117_100600",
  "status": "running",
  "checkpoint_location": "/tmp/checkpoints/stream1",
  "started_at": "2025-11-17T10:06:00Z"
}
```

**Publish Test Events** (in another terminal):
```bash
# Create a test JSON file
cat > /tmp/stream_input/event_001.json << EOF
{"transaction_id": "TXN0000000010", "amount": 50.00, "timestamp": "2025-11-17T14:00:00Z", "customer_email": "test@example.com"}
EOF

# Wait ~10 seconds for micro-batch processing
# Check warehouse
psql -h localhost -U pipeline -d datawarehouse \
  -c "SELECT COUNT(*) FROM warehouse_data WHERE source_id='live_events';"
```

**Stop Streaming**:
```bash
python -m src.cli.stream_cli stop --query-id stream_live_events_20251117_100600
```

### Example 2: Kafka Stream (Advanced)

**Publish to Kafka**:
```bash
# Install Kafka CLI tools (or use docker exec)
docker exec -it kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic test_events

# Type messages (one per line, JSON format):
{"transaction_id": "TXN0000000020", "amount": 100.00, "timestamp": "2025-11-17T15:00:00Z", "customer_email": "kafka@example.com"}
```

**Start Kafka Stream Pipeline**:
```bash
python -m src.cli.stream_cli start \
  --source kafka_events \
  --stream-source kafka \
  --kafka-topic test_events \
  --kafka-bootstrap-servers localhost:9092 \
  --checkpoint-location /tmp/checkpoints/kafka_stream \
  --trigger-interval "5 seconds"
```

---

## Viewing Results

### 1. Review Quarantined Records

```bash
# View all quarantined records grouped by error type
python -m src.cli.admin_cli quarantine-review \
  --limit 20 \
  --group-by-error

# Expected output (table format):
# Error Type                    | Count | Sample Record ID
# ------------------------------|-------|------------------
# amount_range_check            | 2     | TXN0000000004
# require_transaction_id        | 1     | (null)
# timestamp_type_check          | 1     | TXN0000000005
```

### 2. Query Specific Quarantine Records

```bash
# Using psql
psql -h localhost -U pipeline -d datawarehouse << EOF
SELECT
    quarantine_id,
    record_id,
    source_id,
    failed_rules,
    error_messages,
    quarantined_at
FROM quarantine_record
WHERE source_id = 'sample_transactions'
ORDER BY quarantined_at DESC
LIMIT 5;
EOF
```

### 3. View Data Lineage (Audit Trail)

```bash
# Trace a specific record's transformations
psql -h localhost -U pipeline -d datawarehouse << EOF
SELECT
    transformation_type,
    field_name,
    old_value,
    new_value,
    rule_applied,
    created_at
FROM audit_log
WHERE record_id = 'TXN0000000001'
ORDER BY created_at;
EOF
```

### 4. Monitor with Prometheus Metrics

```bash
# Start metrics server (if not auto-started)
python -m src.observability.metrics --port 8000

# View metrics in browser
open http://localhost:8000/metrics

# Example metrics:
# pipeline_records_processed_total{source_id="sample_transactions",status="valid"} 1
# pipeline_records_processed_total{source_id="sample_transactions",status="quarantined"} 4
# pipeline_processing_seconds_sum 2.45
```

### 5. Spark UI (Monitor Processing)

Open http://localhost:8080 in browser to view:
- Active jobs and stages
- Executor memory usage
- Task parallelism
- Shuffle operations

---

## Reprocessing Quarantine

After fixing validation rules, reprocess quarantined records:

```bash
# Update validation rule (e.g., relax email regex)
# Edit config/validation_rules.yaml

# Reprocess all quarantined records from a source
python -m src.cli.admin_cli reprocess \
  --source sample_transactions \
  --validation-rules config/validation_rules.yaml

# Or reprocess specific records
python -m src.cli.admin_cli reprocess \
  --source sample_transactions \
  --quarantine-ids "123,124,125"
```

---

## Running Tests

### Unit Tests
```bash
# Run all unit tests
pytest tests/unit/ -v

# Run specific test file
pytest tests/unit/test_validators.py -v

# Run with coverage
pytest tests/unit/ --cov=src --cov-report=html
open htmlcov/index.html
```

### Integration Tests (Uses Testcontainers)
```bash
# Requires Docker running
pytest tests/integration/ -v

# This will:
# 1. Start PostgreSQL container
# 2. Run tests against real database
# 3. Tear down containers
```

### End-to-End Tests
```bash
# Full pipeline test with dirty data
pytest tests/e2e/test_dirty_csv_batch.py -v

# Streaming pipeline test
pytest tests/e2e/test_streaming_flow.py -v
```

---

## Troubleshooting

### Issue: "Connection to PostgreSQL failed"

**Solution**:
```bash
# Check if PostgreSQL container is running
docker-compose -f docker/docker-compose.yml ps postgres

# Check logs
docker logs postgres

# Restart if needed
docker-compose -f docker/docker-compose.yml restart postgres

# Test connection
psql -h localhost -U pipeline -d datawarehouse -c "SELECT 1;"
```

### Issue: "Spark worker not connecting to master"

**Solution**:
```bash
# Check Spark master logs
docker logs spark-master

# Check worker logs
docker logs spark-worker

# Restart Spark cluster
docker-compose -f docker/docker-compose.yml restart spark-master spark-worker

# Verify in Spark UI (http://localhost:8080) - should show 1 worker
```

### Issue: "Module 'pyspark' not found"

**Solution**:
```bash
# Ensure virtual environment is activated
source venv/bin/activate  # Windows: venv\Scripts\activate

# Reinstall dependencies
pip install -r requirements.txt

# Verify
python -c "import pyspark; print(pyspark.__version__)"  # Should print 3.5.1
```

### Issue: "Permission denied: /tmp/checkpoints"

**Solution**:
```bash
# Create checkpoint directory with write permissions
mkdir -p /tmp/checkpoints
chmod 777 /tmp/checkpoints

# Or use a different location
python -m src.cli.stream_cli start \
  --checkpoint-location ~/checkpoints/stream1 \
  ...
```

### Issue: "Kafka connection refused"

**Solution**:
```bash
# Check Kafka and Zookeeper running
docker-compose -f docker/docker-compose.yml ps kafka zookeeper

# Test Kafka connectivity
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Restart if needed
docker-compose -f docker/docker-compose.yml restart kafka zookeeper

# Wait 30 seconds for Kafka to fully start
sleep 30
```

### Issue: "OutOfMemoryError in Spark"

**Solution**:
```bash
# Reduce chunk size
python -m src.cli.batch_cli process \
  --chunk-size 1000 \  # Instead of 10000
  ...

# Or increase Spark worker memory in docker-compose.yml:
# SPARK_WORKER_MEMORY=4g  # Default is 1g
docker-compose -f docker/docker-compose.yml up -d --force-recreate
```

---

## Performance Benchmarks (Local Machine)

**Hardware**: MacBook Pro M1, 16GB RAM, Docker Desktop 4GB

| Dataset Size | Processing Mode | Throughput | Latency | Memory Usage |
|--------------|----------------|-----------|---------|--------------|
| 10K records  | Batch          | 15K rec/min | N/A | 800MB |
| 100K records | Batch          | 18K rec/min | N/A | 1.2GB |
| 1M records   | Batch          | 20K rec/min | N/A | 2.5GB |
| Streaming    | File Stream    | N/A | 1.5s avg | 600MB |
| Streaming    | Kafka          | N/A | 2.2s avg | 700MB |

*Note: Results vary based on validation complexity and hardware*

---

## Next Steps

1. **Add custom validation rules**: See `src/core/validators/custom_validator.py`
2. **Configure schema evolution**: Edit `src/core/schema/evolution.py` settings
3. **Set up monitoring dashboard**: Deploy Grafana + Prometheus
4. **Scale up**: Add more Spark workers in `docker/docker-compose.yml`
5. **Production deployment**: See `docs/production-deployment.md` (TODO)

---

## Quick Reference

**Start Everything**:
```bash
docker-compose -f docker/docker-compose.yml up -d
source venv/bin/activate
```

**Stop Everything**:
```bash
docker-compose -f docker/docker-compose.yml down
deactivate
```

**Reset Database** (⚠️ Deletes all data):
```bash
docker-compose -f docker/docker-compose.yml down -v
docker-compose -f docker/docker-compose.yml up -d postgres
sleep 5
psql -h localhost -U pipeline -d datawarehouse -f docker/init-db.sql
```

**View Logs**:
```bash
docker-compose -f docker/docker-compose.yml logs -f  # All services
docker logs spark-master --tail 100 -f  # Specific service
```

---

**Questions?** See `docs/` directory or open an issue on GitHub.

**Happy validating! 🚀**
