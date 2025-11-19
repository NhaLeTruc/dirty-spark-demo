"""Unit tests for schema evolution logic."""

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.core.schema.evolution import (
    SchemaEvolutionError,
    SchemaEvolutionManager,
    merge_streaming_schemas,
)


class TestSchemaEvolutionManager:
    """Test schema evolution and merging."""

    def test_merge_schemas_add_field(self):
        """Test adding a new field to schema."""
        manager = SchemaEvolutionManager()

        old_schema = StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
        ])

        new_schema = StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),  # New field
        ])

        merged, changes = manager.merge_schemas(old_schema, new_schema)

        assert len(merged.fields) == 3
        assert merged.fieldNames() == ["id", "name", "email"]
        assert len(changes) == 1
        assert "Added field: email" in changes[0]

    def test_merge_schemas_no_changes(self):
        """Test merging identical schemas."""
        manager = SchemaEvolutionManager()

        schema = StructType([
            StructField("id", StringType(), False),
            StructField("value", IntegerType(), True),
        ])

        merged, changes = manager.merge_schemas(schema, schema)

        assert len(merged.fields) == 2
        assert len(changes) == 0

    def test_merge_schemas_compatible_type_change(self):
        """Test compatible type changes (int -> long, float -> double)."""
        manager = SchemaEvolutionManager()

        old_schema = StructType([
            StructField("count", IntegerType(), False),
        ])

        new_schema = StructType([
            StructField("count", LongType(), False),  # Compatible upgrade
        ])

        merged, changes = manager.merge_schemas(
            old_schema,
            new_schema,
            allow_type_changes=True
        )

        assert isinstance(merged["count"].dataType, LongType)
        assert len(changes) == 1

    def test_merge_schemas_incompatible_type_change(self):
        """Test that incompatible type changes raise error."""
        manager = SchemaEvolutionManager()

        old_schema = StructType([
            StructField("value", StringType(), False),
        ])

        new_schema = StructType([
            StructField("value", IntegerType(), False),  # Incompatible
        ])

        with pytest.raises(SchemaEvolutionError):
            manager.merge_schemas(old_schema, new_schema, allow_type_changes=True)

    def test_merge_schemas_type_change_not_allowed(self):
        """Test that type changes fail when not allowed."""
        manager = SchemaEvolutionManager()

        old_schema = StructType([
            StructField("count", IntegerType(), False),
        ])

        new_schema = StructType([
            StructField("count", LongType(), False),
        ])

        with pytest.raises(SchemaEvolutionError):
            manager.merge_schemas(
                old_schema,
                new_schema,
                allow_type_changes=False  # Not allowed
            )

    def test_detect_schema_changes_added_fields(self):
        """Test detection of added fields."""
        manager = SchemaEvolutionManager()

        old_schema = StructType([
            StructField("id", StringType(), False),
        ])

        new_schema = StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
        ])

        changes = manager.detect_schema_changes(old_schema, new_schema)

        assert len(changes["added_fields"]) == 2
        assert "name" in changes["added_fields"]
        assert "email" in changes["added_fields"]
        assert not changes["has_breaking_changes"]

    def test_detect_schema_changes_removed_fields(self):
        """Test detection of removed fields (breaking change)."""
        manager = SchemaEvolutionManager()

        old_schema = StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
        ])

        new_schema = StructType([
            StructField("id", StringType(), False),
        ])

        changes = manager.detect_schema_changes(old_schema, new_schema)

        assert len(changes["removed_fields"]) == 1
        assert "name" in changes["removed_fields"]
        assert changes["has_breaking_changes"]

    def test_detect_schema_changes_type_changes(self):
        """Test detection of type changes."""
        manager = SchemaEvolutionManager()

        old_schema = StructType([
            StructField("count", IntegerType(), False),
            StructField("price", StringType(), True),
        ])

        new_schema = StructType([
            StructField("count", LongType(), False),  # Compatible
            StructField("price", DoubleType(), True),  # Incompatible
        ])

        changes = manager.detect_schema_changes(old_schema, new_schema)

        assert len(changes["type_changes"]) == 2
        assert changes["type_changes"][0]["compatible"]  # int -> long
        assert not changes["type_changes"][1]["compatible"]  # string -> double
        assert changes["has_breaking_changes"]

    def test_nullable_change_safe(self):
        """Test that non-nullable -> nullable is safe."""
        manager = SchemaEvolutionManager()

        old_schema = StructType([
            StructField("value", StringType(), False),  # Required
        ])

        new_schema = StructType([
            StructField("value", StringType(), True),  # Now optional
        ])

        merged, changes = manager.merge_schemas(old_schema, new_schema)

        assert merged["value"].nullable
        assert len(changes) == 1

    def test_create_schema_version(self):
        """Test schema version creation."""
        manager = SchemaEvolutionManager()

        schema = StructType([
            StructField("id", StringType(), False),
            StructField("value", IntegerType(), True),
        ])

        version = manager.create_schema_version(
            source_id="test_source",
            schema=schema,
            version=1,
            inferred=True,
            confidence=0.95,
        )

        assert version.source_id == "test_source"
        assert version.version == 1
        assert version.inferred
        assert version.confidence == 0.95
        assert version.schema_definition is not None


class TestStreamingSchemaEvolution:
    """Test schema evolution in streaming context."""

    def test_merge_streaming_schemas_add_field(self):
        """Test adding field in streaming is allowed."""
        current = StructType([
            StructField("id", StringType(), False),
        ])

        incoming = StructType([
            StructField("id", StringType(), False),
            StructField("new_field", StringType(), True),
        ])

        merged, evolved, changes = merge_streaming_schemas(
            current,
            incoming,
            "test_source"
        )

        assert evolved
        assert len(changes) == 1
        assert len(merged.fields) == 2

    def test_merge_streaming_schemas_breaking_change(self):
        """Test that breaking changes in streaming raise error."""
        current = StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
        ])

        incoming = StructType([
            StructField("id", StringType(), False),
            # "name" field removed - breaking!
        ])

        with pytest.raises(SchemaEvolutionError):
            merge_streaming_schemas(current, incoming, "test_source")

    def test_merge_streaming_schemas_no_evolution(self):
        """Test identical schemas result in no evolution."""
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("value", IntegerType(), True),
        ])

        merged, evolved, changes = merge_streaming_schemas(
            schema,
            schema,
            "test_source"
        )

        assert not evolved
        assert len(changes) == 0
        assert merged == schema
