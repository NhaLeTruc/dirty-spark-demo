"""
Schema registry for managing schema versions.

Saves and loads schemas from the schema_version table in PostgreSQL.
"""

from typing import Dict, Any, Optional
from pyspark.sql.types import StructType
from src.warehouse.connection import DatabaseConnectionPool
from src.warehouse.schema_mgmt import SchemaManager
from .inference import SchemaInferrer


class SchemaRegistry:
    """
    Registry for managing schema versions across data sources.

    Integrates SchemaManager (database operations) with SchemaInferrer
    (schema inference) to provide a complete schema versioning solution.
    """

    def __init__(self, pool: DatabaseConnectionPool, schema_inferrer: SchemaInferrer):
        """
        Initialize schema registry.

        Args:
            pool: Database connection pool
            schema_inferrer: Schema inference engine
        """
        self.pool = pool
        self.schema_manager = SchemaManager(pool)
        self.inferrer = schema_inferrer

    def get_or_infer_schema(
        self,
        source_id: str,
        sample_data_path: Optional[str] = None,
        sample_df: Optional[Any] = None
    ) -> Tuple[StructType, int, float]:
        """
        Get existing schema or infer new one.

        Args:
            source_id: Data source ID
            sample_data_path: Path to sample data file for inference
            sample_df: Sample DataFrame for inference

        Returns:
            Tuple of (schema, schema_id, confidence)

        Raises:
            ValueError: If neither sample_data_path nor sample_df provided when schema doesn't exist
        """
        from typing import Tuple  # Local import to avoid circular dependency

        # Try to get existing active schema
        existing_schema = self.schema_manager.get_active_schema(source_id)

        if existing_schema:
            # Load existing schema
            schema_dict = existing_schema["schema_definition"]
            schema = self.inferrer.dict_to_schema(schema_dict)
            schema_id = existing_schema["schema_id"]
            confidence = existing_schema.get("confidence", 1.0)

            return schema, schema_id, confidence

        # No existing schema - must infer
        if sample_data_path:
            schema, confidence = self.inferrer.infer_from_csv(sample_data_path)
        elif sample_df:
            schema, confidence = self.inferrer.infer_from_dataframe(sample_df)
        else:
            raise ValueError(
                f"No schema exists for source '{source_id}' and no sample data provided for inference"
            )

        # Save inferred schema
        schema_id = self.save_schema(
            source_id=source_id,
            schema=schema,
            inferred=True,
            confidence=confidence
        )

        return schema, schema_id, confidence

    def save_schema(
        self,
        source_id: str,
        schema: StructType,
        inferred: bool = True,
        confidence: Optional[float] = None
    ) -> int:
        """
        Save schema to registry.

        Args:
            source_id: Data source ID
            schema: Spark schema
            inferred: Whether schema was inferred or manually defined
            confidence: Confidence score for inferred schemas

        Returns:
            schema_id: ID of created schema version
        """
        # Convert schema to dictionary
        schema_dict = self.inferrer.schema_to_dict(schema)

        # Create schema version
        schema_id = self.schema_manager.create_schema_version(
            source_id=source_id,
            schema_definition=schema_dict,
            inferred=inferred,
            confidence=confidence
        )

        return schema_id

    def get_schema(self, source_id: str, version: Optional[int] = None) -> Optional[StructType]:
        """
        Get schema by source and version.

        Args:
            source_id: Data source ID
            version: Schema version (None for active version)

        Returns:
            Spark schema or None if not found
        """
        if version is None:
            # Get active schema
            schema_record = self.schema_manager.get_active_schema(source_id)
        else:
            # Get specific version
            schema_record = self.schema_manager.get_schema_by_version(source_id, version)

        if not schema_record:
            return None

        # Convert dictionary to Spark schema
        schema_dict = schema_record["schema_definition"]
        return self.inferrer.dict_to_schema(schema_dict)

    def evolve_schema(
        self,
        source_id: str,
        new_schema: StructType,
        confidence: float
    ) -> int:
        """
        Create new schema version (schema evolution).

        Args:
            source_id: Data source ID
            new_schema: New schema version
            confidence: Confidence score for new schema

        Returns:
            schema_id: ID of new schema version
        """
        # Get current active schema for comparison
        current_schema_record = self.schema_manager.get_active_schema(source_id)

        if current_schema_record:
            current_schema_dict = current_schema_record["schema_definition"]
            new_schema_dict = self.inferrer.schema_to_dict(new_schema)

            # Compare schemas
            comparison = self.schema_manager.compare_schemas(current_schema_dict, new_schema_dict)

            # If schemas are identical, return current schema_id
            if (not comparison["added_fields"] and
                not comparison["removed_fields"] and
                not comparison["type_changes"]):
                return current_schema_record["schema_id"]

            # Deprecate old schema if incompatible
            if not comparison["compatible"]:
                self.schema_manager.deprecate_schema(
                    source_id=source_id,
                    version=current_schema_record["version"]
                )

        # Create new schema version
        return self.save_schema(
            source_id=source_id,
            schema=new_schema,
            inferred=True,
            confidence=confidence
        )

    def list_schema_versions(self, source_id: str, limit: int = 10) -> list:
        """
        List schema version history.

        Args:
            source_id: Data source ID
            limit: Maximum versions to return

        Returns:
            List of schema version records
        """
        return self.schema_manager.list_schema_history(source_id, limit)
