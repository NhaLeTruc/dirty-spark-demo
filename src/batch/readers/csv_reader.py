"""
CSV reader using Spark for batch processing.
"""

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType


class CSVReader:
    """
    Reads CSV files using Spark with schema inference or explicit schema.
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize CSV reader.

        Args:
            spark: Active Spark session
        """
        self.spark = spark

    def read(
        self,
        file_path: str,
        schema: Optional[StructType] = None,
        header: bool = True,
        delimiter: str = ",",
        infer_schema: bool = True
    ) -> DataFrame:
        """
        Read CSV file into Spark DataFrame.

        Args:
            file_path: Path to CSV file
            schema: Optional explicit schema
            header: Whether CSV has header row
            delimiter: Field delimiter
            infer_schema: Whether to infer schema if not provided

        Returns:
            Spark DataFrame
        """
        reader = self.spark.read

        if schema:
            reader = reader.schema(schema)
        elif infer_schema:
            reader = reader.option("inferSchema", "true")

        df = reader \
            .option("header", str(header).lower()) \
            .option("delimiter", delimiter) \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .csv(file_path)

        return df
