"""
Generic file reader for multiple formats (CSV, JSON, Parquet).
"""


from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from .csv_reader import CSVReader


class FileReader:
    """
    Generic file reader supporting multiple formats.
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize file reader.

        Args:
            spark: Active Spark session
        """
        self.spark = spark
        self.csv_reader = CSVReader(spark)

    def read(
        self,
        file_path: str,
        file_format: str = "csv",
        schema: StructType | None = None,
        **options
    ) -> DataFrame:
        """
        Read file into Spark DataFrame.

        Args:
            file_path: Path to file
            file_format: Format (csv, json, parquet)
            schema: Optional explicit schema
            **options: Format-specific options

        Returns:
            Spark DataFrame

        Raises:
            ValueError: If file format is unsupported
        """
        if file_format.lower() == "csv":
            return self.csv_reader.read(
                file_path,
                schema=schema,
                **options
            )
        elif file_format.lower() == "json":
            reader = self.spark.read
            if schema:
                reader = reader.schema(schema)
            return reader.json(file_path)
        elif file_format.lower() == "parquet":
            return self.spark.read.parquet(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
