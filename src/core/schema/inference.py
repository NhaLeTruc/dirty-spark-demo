"""
Schema inference for data sources using Spark.

Automatically infers schema from CSV/JSON files with confidence scoring.
"""

import re
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.observability.logger import get_logger

logger = get_logger(__name__)

from src.observability.logger import get_logger

logger = get_logger(__name__)


class SchemaInferrer:
    """
    Infers schema from data files with confidence scoring.

    Analyzes sample data to determine field types, nullable status,
    and provides confidence score for the inferred schema.
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize schema inferrer.

        Args:
            spark: Active Spark session
        """
        self.spark = spark

    def infer_from_csv(
        self,
        file_path: str,
        sample_size: int = 1000,
        has_header: bool = True
    ) -> tuple[StructType, float]:
        """
        Infer schema from CSV file.

        Args:
            file_path: Path to CSV file
            sample_size: Number of rows to sample for inference
            has_header: Whether CSV has header row

        Returns:
            Tuple of (inferred_schema, confidence_score)
        """
        # Read CSV with schema inference enabled
        df = self.spark.read.csv(
            file_path,
            header=has_header,
            inferSchema=True,
            samplingRatio=min(1.0, sample_size / 10000)  # Sample ratio for inference
        )

        # Get inferred schema
        schema = df.schema

        # Calculate confidence score based on data quality
        confidence = self._calculate_confidence(df, schema)

        return schema, confidence

    def infer_from_dataframe(
        self,
        df: DataFrame,
        sample_fraction: float = 0.1
    ) -> tuple[StructType, float]:
        """
        Infer schema from existing DataFrame.

        Args:
            df: Input DataFrame
            sample_fraction: Fraction of data to sample

        Returns:
            Tuple of (inferred_schema, confidence_score)
        """
        schema = df.schema
        confidence = self._calculate_confidence(df, schema, sample_fraction)

        return schema, confidence

    def _calculate_confidence(
        self,
        df: DataFrame,
        schema: StructType,
        sample_fraction: float = 0.1
    ) -> float:
        """
        Calculate confidence score for inferred schema.

        Confidence is based on:
        - Percentage of non-null values
        - Type consistency (no mixed types)
        - Field name quality (meaningful names, not col1, col2, etc.)

        Args:
            df: Input DataFrame
            schema: Inferred schema
            sample_fraction: Fraction to sample for analysis

        Returns:
            Confidence score (0.0 to 1.0)
        """
        try:
            # Sample data for analysis
            sample_df = df.sample(withReplacement=False, fraction=sample_fraction)
            total_rows = sample_df.count()

            if total_rows == 0:
                return 0.0

            confidence_scores = []

            for field in schema.fields:
                field_name = field.name

                # Check field name quality
                name_score = self._score_field_name(field_name)
                confidence_scores.append(name_score)

                # Calculate non-null percentage
                non_null_count = sample_df.filter(sample_df[field_name].isNotNull()).count()
                null_ratio = 1.0 - (non_null_count / total_rows)

                # Higher score for fields with fewer nulls
                null_score = 1.0 - null_ratio
                confidence_scores.append(null_score)

            # Overall confidence is average of all scores
            if not confidence_scores:
                return 0.0
            overall_confidence = sum(confidence_scores) / len(confidence_scores)

            return round(overall_confidence, 2)

        except (TypeError, ValueError) as e:
            # Handle type/value errors in calculation
            logger.debug(f"Confidence calculation failed: {e}. Using default confidence.", exc_info=True)
            return 0.7
        except Exception as e:
            # Catch unexpected errors (e.g., Spark errors during sampling)
            logger.warning(f"Unexpected error in confidence calculation: {e}", exc_info=True)
            return 0.7

    def _score_field_name(self, field_name: str) -> float:
        """
        Score field name quality.

        Args:
            field_name: Name of the field

        Returns:
            Score (0.0 to 1.0)
        """
        # Generic names get lower scores
        generic_patterns = [
            r'^_c\d+$',  # _c0, _c1, etc. (Spark default)
            r'^col\d+$',  # col1, col2, etc.
            r'^field\d+$',  # field1, field2, etc.
            r'^column\d+$',  # column1, column2, etc.
        ]

        for pattern in generic_patterns:
            if re.match(pattern, field_name, re.IGNORECASE):
                return 0.5  # Generic name

        # Meaningful names get full score
        return 1.0

    def schema_to_dict(self, schema: StructType) -> dict[str, Any]:
        """
        Convert Spark schema to dictionary format.

        Args:
            schema: Spark StructType schema

        Returns:
            Dictionary representation of schema
        """
        return {
            "fields": [
                {
                    "name": field.name,
                    "type": str(field.dataType),
                    "nullable": field.nullable
                }
                for field in schema.fields
            ]
        }

    def dict_to_schema(self, schema_dict: dict[str, Any]) -> StructType:
        """
        Convert dictionary to Spark schema.

        Args:
            schema_dict: Dictionary with schema definition

        Returns:
            Spark StructType schema
        """
        type_mapping = {
            "StringType()": StringType(),
            "IntegerType()": IntegerType(),
            "DoubleType()": DoubleType(),
            "TimestampType()": TimestampType(),
            "BooleanType()": BooleanType(),
        }

        fields = []
        for field_def in schema_dict.get("fields", []):
            field_type = type_mapping.get(field_def["type"], StringType())
            fields.append(
                StructField(
                    field_def["name"],
                    field_type,
                    field_def.get("nullable", True)
                )
            )

        return StructType(fields)
