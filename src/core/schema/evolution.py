"""
Schema evolution and merging logic.

Handles schema changes over time with backward compatibility checks.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

from pyspark.sql.types import (
    StructType,
    StructField,
    DataType,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    BooleanType,
    TimestampType,
)

from src.core.models.schema_version import SchemaVersion

logger = logging.getLogger(__name__)


class SchemaEvolutionError(Exception):
    """Raised when schema evolution fails."""
    pass


class SchemaEvolutionManager:
    """
    Manages schema evolution with backward compatibility.

    Handles:
    - Schema merging (adding new fields)
    - Type compatibility checking
    - Version management
    """

    # Compatible type transitions (old_type -> new_type)
    COMPATIBLE_TYPE_TRANSITIONS = {
        (IntegerType, LongType),
        (FloatType, DoubleType),
        (StringType, StringType),  # Always compatible with itself
    }

    def __init__(self):
        """Initialize schema evolution manager."""
        self.schema_history: List[SchemaVersion] = []

    def merge_schemas(
        self,
        old_schema: StructType,
        new_schema: StructType,
        allow_type_changes: bool = False,
    ) -> Tuple[StructType, List[str]]:
        """
        Merge two schemas, adding new fields from new_schema.

        Args:
            old_schema: Existing schema
            new_schema: New schema with potential additions
            allow_type_changes: If True, allow compatible type changes

        Returns:
            Tuple of (merged_schema, list_of_changes)

        Raises:
            SchemaEvolutionError: If incompatible changes detected
        """
        logger.info("Merging schemas")

        changes = []
        merged_fields = {}

        # Add all existing fields
        for field in old_schema.fields:
            merged_fields[field.name] = field

        # Process new fields
        for new_field in new_schema.fields:
            field_name = new_field.name

            if field_name not in merged_fields:
                # New field - add it
                merged_fields[field_name] = new_field
                changes.append(f"Added field: {field_name} ({new_field.dataType})")
                logger.info(f"Schema evolution: Added field '{field_name}'")

            else:
                # Field exists - check compatibility
                old_field = merged_fields[field_name]

                if old_field.dataType != new_field.dataType:
                    # Type changed
                    if allow_type_changes:
                        if self._is_compatible_type_change(
                            old_field.dataType,
                            new_field.dataType
                        ):
                            merged_fields[field_name] = new_field
                            changes.append(
                                f"Type changed: {field_name} "
                                f"({old_field.dataType} -> {new_field.dataType})"
                            )
                            logger.warning(
                                f"Schema evolution: Type change for '{field_name}'"
                            )
                        else:
                            raise SchemaEvolutionError(
                                f"Incompatible type change for field '{field_name}': "
                                f"{old_field.dataType} -> {new_field.dataType}"
                            )
                    else:
                        raise SchemaEvolutionError(
                            f"Type change not allowed for field '{field_name}': "
                            f"{old_field.dataType} -> {new_field.dataType}"
                        )

                # Check nullability changes
                if old_field.nullable != new_field.nullable:
                    if not old_field.nullable and new_field.nullable:
                        # Becoming nullable is safe
                        merged_fields[field_name] = new_field
                        changes.append(f"Field '{field_name}' now nullable")
                    else:
                        # Becoming non-nullable is dangerous
                        logger.warning(
                            f"Field '{field_name}' changing to non-nullable - "
                            f"may cause issues with existing data"
                        )

        # Create merged schema
        merged_schema = StructType(list(merged_fields.values()))

        logger.info(
            f"Schema merge complete: {len(changes)} changes, "
            f"{len(merged_schema.fields)} total fields"
        )

        return merged_schema, changes

    def _is_compatible_type_change(
        self,
        old_type: DataType,
        new_type: DataType
    ) -> bool:
        """
        Check if type change is compatible.

        Args:
            old_type: Original data type
            new_type: New data type

        Returns:
            True if change is compatible
        """
        # Same type is always compatible
        if type(old_type) == type(new_type):
            return True

        # Check known compatible transitions
        for (from_type, to_type) in self.COMPATIBLE_TYPE_TRANSITIONS:
            if isinstance(old_type, from_type) and isinstance(new_type, to_type):
                return True

        return False

    def detect_schema_changes(
        self,
        old_schema: StructType,
        new_schema: StructType
    ) -> Dict[str, Any]:
        """
        Detect all changes between two schemas.

        Args:
            old_schema: Original schema
            new_schema: New schema

        Returns:
            Dictionary with detected changes
        """
        changes = {
            "added_fields": [],
            "removed_fields": [],
            "type_changes": [],
            "nullability_changes": [],
            "has_breaking_changes": False,
        }

        old_fields = {f.name: f for f in old_schema.fields}
        new_fields = {f.name: f for f in new_schema.fields}

        # Find added fields
        for name in new_fields:
            if name not in old_fields:
                changes["added_fields"].append(name)

        # Find removed fields (breaking change)
        for name in old_fields:
            if name not in new_fields:
                changes["removed_fields"].append(name)
                changes["has_breaking_changes"] = True

        # Find type changes
        for name in old_fields:
            if name in new_fields:
                old_field = old_fields[name]
                new_field = new_fields[name]

                if old_field.dataType != new_field.dataType:
                    changes["type_changes"].append({
                        "field": name,
                        "old_type": str(old_field.dataType),
                        "new_type": str(new_field.dataType),
                        "compatible": self._is_compatible_type_change(
                            old_field.dataType,
                            new_field.dataType
                        ),
                    })

                    # Incompatible type change is breaking
                    if not self._is_compatible_type_change(
                        old_field.dataType,
                        new_field.dataType
                    ):
                        changes["has_breaking_changes"] = True

                if old_field.nullable != new_field.nullable:
                    changes["nullability_changes"].append({
                        "field": name,
                        "old_nullable": old_field.nullable,
                        "new_nullable": new_field.nullable,
                    })

                    # Non-nullable to nullable is safe, reverse is breaking
                    if not old_field.nullable and new_field.nullable:
                        pass  # Safe change
                    else:
                        changes["has_breaking_changes"] = True

        return changes

    def create_schema_version(
        self,
        source_id: str,
        schema: StructType,
        version: int,
        inferred: bool = True,
        confidence: Optional[float] = None,
    ) -> SchemaVersion:
        """
        Create a new schema version.

        Args:
            source_id: Data source identifier
            schema: The schema definition
            version: Version number
            inferred: Whether schema was inferred or manually defined
            confidence: Inference confidence (0.0-1.0)

        Returns:
            SchemaVersion instance
        """
        # Convert Spark schema to JSON dict
        schema_dict = schema.jsonValue()

        schema_version = SchemaVersion(
            source_id=source_id,
            version=version,
            schema_definition=schema_dict,
            inferred=inferred,
            confidence=confidence,
            created_at=datetime.utcnow(),
        )

        logger.info(
            f"Created schema version {version} for {source_id} "
            f"(inferred={inferred}, confidence={confidence})"
        )

        return schema_version

    def get_next_version(self, source_id: str) -> int:
        """
        Get next version number for a source.

        Args:
            source_id: Data source identifier

        Returns:
            Next version number
        """
        # Get max version from history
        source_versions = [
            sv.version for sv in self.schema_history
            if sv.source_id == source_id
        ]

        if source_versions:
            return max(source_versions) + 1
        else:
            return 1


def merge_streaming_schemas(
    current_schema: StructType,
    incoming_schema: StructType,
    source_id: str,
) -> Tuple[StructType, bool, List[str]]:
    """
    Merge schemas in streaming context with safety checks.

    Args:
        current_schema: Current active schema
        incoming_schema: Schema from new data
        source_id: Data source identifier

    Returns:
        Tuple of (merged_schema, schema_evolved, changes)
    """
    manager = SchemaEvolutionManager()

    try:
        # Detect changes first
        changes_detected = manager.detect_schema_changes(
            current_schema,
            incoming_schema
        )

        if changes_detected["has_breaking_changes"]:
            logger.error(
                f"Breaking schema changes detected for {source_id}: "
                f"{changes_detected}"
            )
            raise SchemaEvolutionError(
                f"Breaking changes not allowed in streaming: "
                f"removed fields or incompatible type changes"
            )

        # Merge schemas (allow compatible type changes)
        merged_schema, changes = manager.merge_schemas(
            current_schema,
            incoming_schema,
            allow_type_changes=True,
        )

        schema_evolved = len(changes) > 0

        if schema_evolved:
            logger.info(
                f"Schema evolved for {source_id}: {len(changes)} changes"
            )

        return merged_schema, schema_evolved, changes

    except SchemaEvolutionError as e:
        logger.error(f"Schema evolution failed: {e}")
        raise
