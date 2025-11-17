"""
Schema management operations for the data warehouse.

Handles schema versioning, evolution, and DDL operations.
"""

from typing import Dict, Any, List
from datetime import datetime
from .connection import DatabaseConnectionPool


class SchemaManager:
    """
    Manages schema versions and evolution in the warehouse.

    Handles:
    - Creating schema versions
    - Tracking schema changes
    - Deprecating old schemas
    - Querying schema history
    """

    def __init__(self, pool: DatabaseConnectionPool):
        """
        Initialize schema manager.

        Args:
            pool: Database connection pool
        """
        self.pool = pool

    def create_schema_version(
        self,
        source_id: str,
        schema_definition: Dict[str, Any],
        inferred: bool = True,
        confidence: float | None = None
    ) -> int:
        """
        Create a new schema version for a data source.

        Args:
            source_id: The data source ID
            schema_definition: Spark schema as dictionary
            inferred: Whether schema was auto-inferred
            confidence: Confidence score for inferred schemas (0.0-1.0)

        Returns:
            schema_id: The ID of the created schema version

        Raises:
            ValueError: If confidence is outside 0-1 range
        """
        if confidence is not None and (confidence < 0.0 or confidence > 1.0):
            raise ValueError(f"Confidence must be between 0.0 and 1.0, got {confidence}")

        # Get next version number for this source
        version = self._get_next_version(source_id)

        # Insert schema version
        query = """
            INSERT INTO schema_version (source_id, version, schema_definition, inferred, confidence)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING schema_id
        """

        import json
        schema_json = json.dumps(schema_definition)

        result = self.pool.execute_query(
            query,
            (source_id, version, schema_json, inferred, confidence)
        )

        return result[0]["schema_id"]

    def get_active_schema(self, source_id: str) -> Dict[str, Any] | None:
        """
        Get the active (non-deprecated) schema for a source.

        Args:
            source_id: The data source ID

        Returns:
            Schema version record or None if no active schema exists
        """
        query = """
            SELECT schema_id, source_id, version, schema_definition,
                   inferred, confidence, created_at
            FROM schema_version
            WHERE source_id = %s AND deprecated_at IS NULL
            ORDER BY version DESC
            LIMIT 1
        """

        result = self.pool.execute_query(query, (source_id,))
        return result[0] if result else None

    def get_schema_by_version(self, source_id: str, version: int) -> Dict[str, Any] | None:
        """
        Get a specific schema version.

        Args:
            source_id: The data source ID
            version: The schema version number

        Returns:
            Schema version record or None if not found
        """
        query = """
            SELECT schema_id, source_id, version, schema_definition,
                   inferred, confidence, created_at, deprecated_at
            FROM schema_version
            WHERE source_id = %s AND version = %s
        """

        result = self.pool.execute_query(query, (source_id, version))
        return result[0] if result else None

    def deprecate_schema(self, source_id: str, version: int) -> None:
        """
        Mark a schema version as deprecated.

        Args:
            source_id: The data source ID
            version: The schema version to deprecate
        """
        query = """
            UPDATE schema_version
            SET deprecated_at = NOW()
            WHERE source_id = %s AND version = %s AND deprecated_at IS NULL
        """

        self.pool.execute_command(query, (source_id, version))

    def list_schema_history(self, source_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        List schema version history for a source.

        Args:
            source_id: The data source ID
            limit: Maximum number of versions to return

        Returns:
            List of schema version records, newest first
        """
        query = """
            SELECT schema_id, version, inferred, confidence,
                   created_at, deprecated_at
            FROM schema_version
            WHERE source_id = %s
            ORDER BY version DESC
            LIMIT %s
        """

        return self.pool.execute_query(query, (source_id, limit))

    def _get_next_version(self, source_id: str) -> int:
        """
        Get the next version number for a source.

        Args:
            source_id: The data source ID

        Returns:
            Next version number (starts at 1)
        """
        query = """
            SELECT COALESCE(MAX(version), 0) + 1 as next_version
            FROM schema_version
            WHERE source_id = %s
        """

        result = self.pool.execute_query(query, (source_id,))
        return result[0]["next_version"]

    def compare_schemas(
        self,
        schema1: Dict[str, Any],
        schema2: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Compare two schemas and identify differences.

        Args:
            schema1: First schema definition
            schema2: Second schema definition

        Returns:
            Dictionary with:
            - added_fields: List of fields in schema2 but not schema1
            - removed_fields: List of fields in schema1 but not schema2
            - type_changes: List of fields with type changes
            - compatible: Boolean indicating backward compatibility
        """
        fields1 = {f["name"]: f for f in schema1.get("fields", [])}
        fields2 = {f["name"]: f for f in schema2.get("fields", [])}

        added_fields = [name for name in fields2 if name not in fields1]
        removed_fields = [name for name in fields1 if name not in fields2]

        type_changes = []
        for name in fields1:
            if name in fields2:
                if fields1[name].get("type") != fields2[name].get("type"):
                    type_changes.append({
                        "field": name,
                        "old_type": fields1[name].get("type"),
                        "new_type": fields2[name].get("type")
                    })

        # Schema is backward compatible if:
        # - No fields were removed
        # - No field types changed
        # - Any added fields are nullable
        added_non_nullable = [
            name for name in added_fields
            if not fields2[name].get("nullable", True)
        ]

        compatible = (
            len(removed_fields) == 0 and
            len(type_changes) == 0 and
            len(added_non_nullable) == 0
        )

        return {
            "added_fields": added_fields,
            "removed_fields": removed_fields,
            "type_changes": type_changes,
            "compatible": compatible
        }
