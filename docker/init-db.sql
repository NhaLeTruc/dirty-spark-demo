-- Dirty Spark Data Validation Pipeline - Database Initialization
-- PostgreSQL 16.2
-- This script creates all required tables, indexes, and partitions

-- =======================
-- SCHEMA VERSION TABLE
-- =======================
-- Must be created first as it's referenced by data_source
CREATE TABLE schema_version (
    schema_id SERIAL PRIMARY KEY,
    source_id TEXT NOT NULL,  -- Will add FK later after data_source is created
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

-- =======================
-- DATA SOURCE TABLE
-- =======================
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

-- Now add foreign key constraint to schema_version
ALTER TABLE schema_version
    ADD CONSTRAINT fk_schema_version_source
    FOREIGN KEY (source_id) REFERENCES data_source(source_id);

-- =======================
-- VALIDATION RULE TABLE
-- =======================
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

-- =======================
-- WAREHOUSE DATA TABLE
-- =======================
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

-- =======================
-- QUARANTINE RECORD TABLE
-- =======================
-- UNLOGGED for faster writes (acceptable for recoverable quarantine data)
CREATE UNLOGGED TABLE quarantine_record (
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

-- =======================
-- AUDIT LOG TABLE (PARTITIONED)
-- =======================
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

-- Create partitions for current and next 3 months
CREATE TABLE audit_log_2025_11 PARTITION OF audit_log
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');

CREATE TABLE audit_log_2025_12 PARTITION OF audit_log
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

CREATE TABLE audit_log_2026_01 PARTITION OF audit_log
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE audit_log_2026_02 PARTITION OF audit_log
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

-- Indexes on audit_log (applied to all partitions)
CREATE INDEX idx_audit_log_record ON audit_log(record_id, created_at DESC);
CREATE INDEX idx_audit_log_source ON audit_log(source_id, created_at DESC);

-- =======================
-- GRANT PERMISSIONS
-- =======================
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO pipeline;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO pipeline;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO pipeline;

-- =======================
-- SUMMARY
-- =======================
-- Tables created: 6 (data_source, schema_version, validation_rule, warehouse_data, quarantine_record, audit_log)
-- Indexes created: 14
-- Partitions created: 4 (audit_log monthly partitions)
