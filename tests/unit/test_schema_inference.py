"""
Unit tests for schema inference.
"""

import pytest
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from src.core.schema.inference import SchemaInferrer


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for tests."""
    spark = SparkSession.builder \
        .appName("schema_inference_tests") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_infer_from_csv(spark):
    """Test schema inference from CSV file."""
    inferrer = SchemaInferrer(spark)

    # Use the dirty_data.csv fixture
    test_csv = Path(__file__).parent.parent / "fixtures" / "dirty_data.csv"

    schema, confidence = inferrer.infer_from_csv(str(test_csv))

    assert schema is not None
    assert isinstance(schema, StructType)
    assert len(schema.fields) > 0
    assert confidence > 0.0
    assert confidence <= 1.0

    # Verify expected fields
    field_names = [f.name for f in schema.fields]
    assert "transaction_id" in field_names
    assert "amount" in field_names


def test_infer_from_dataframe(spark):
    """Test schema inference from DataFrame."""
    inferrer = SchemaInferrer(spark)

    # Create test DataFrame
    data = [
        ("TXN001", 99.99, "user@example.com"),
        ("TXN002", 150.50, "admin@test.com"),
        ("TXN003", 250.00, "invalid@domain.com"),
    ]
    df = spark.createDataFrame(data, ["transaction_id", "amount", "email"])

    schema, confidence = inferrer.infer_from_dataframe(df)

    assert schema is not None
    assert len(schema.fields) == 3
    assert confidence > 0.0


def test_confidence_score_high_quality_data(spark):
    """Test confidence score for high-quality data."""
    inferrer = SchemaInferrer(spark)

    # Create high-quality DataFrame (no nulls, good field names)
    data = [
        ("TXN001", 99.99, "user@example.com"),
        ("TXN002", 150.50, "admin@test.com"),
        ("TXN003", 250.00, "test@domain.com"),
    ]
    df = spark.createDataFrame(data, ["transaction_id", "amount", "email"])

    schema, confidence = inferrer.infer_from_dataframe(df, sample_fraction=1.0)

    # High-quality data should have high confidence
    assert confidence > 0.8


def test_confidence_score_low_quality_data(spark):
    """Test confidence score for low-quality data."""
    inferrer = SchemaInferrer(spark)

    # Create low-quality DataFrame (many nulls, generic field names)
    data = [
        ("TXN001", None, None),
        (None, 150.50, "admin@test.com"),
        (None, None, None),
    ]
    df = spark.createDataFrame(data, ["_c0", "_c1", "_c2"])

    schema, confidence = inferrer.infer_from_dataframe(df, sample_fraction=1.0)

    # Low-quality data should have lower confidence
    assert confidence < 0.8


def test_schema_to_dict(spark):
    """Test converting schema to dictionary."""
    inferrer = SchemaInferrer(spark)

    schema = StructType([
        StructField("id", StringType(), nullable=False),
        StructField("amount", DoubleType(), nullable=True),
    ])

    schema_dict = inferrer.schema_to_dict(schema)

    assert "fields" in schema_dict
    assert len(schema_dict["fields"]) == 2
    assert schema_dict["fields"][0]["name"] == "id"
    assert schema_dict["fields"][0]["nullable"] is False


def test_dict_to_schema(spark):
    """Test converting dictionary to schema."""
    inferrer = SchemaInferrer(spark)

    schema_dict = {
        "fields": [
            {"name": "id", "type": "StringType()", "nullable": False},
            {"name": "amount", "type": "DoubleType()", "nullable": True},
        ]
    }

    schema = inferrer.dict_to_schema(schema_dict)

    assert isinstance(schema, StructType)
    assert len(schema.fields) == 2
    assert schema.fields[0].name == "id"
    assert isinstance(schema.fields[1].dataType, DoubleType)


def test_field_name_scoring(spark):
    """Test field name quality scoring."""
    inferrer = SchemaInferrer(spark)

    # Generic names should get lower scores
    assert inferrer._score_field_name("_c0") == 0.5
    assert inferrer._score_field_name("col1") == 0.5
    assert inferrer._score_field_name("field2") == 0.5

    # Meaningful names should get full score
    assert inferrer._score_field_name("transaction_id") == 1.0
    assert inferrer._score_field_name("customer_email") == 1.0
