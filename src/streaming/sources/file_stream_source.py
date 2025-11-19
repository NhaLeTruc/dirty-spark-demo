"""
File stream source for Spark Structured Streaming.

Reads JSON files from a directory as a stream, allowing real-time processing
of files as they arrive.
"""

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from src.core.models.data_source import DataSource

logger = logging.getLogger(__name__)


class FileStreamSource:
    """
    Spark Structured Streaming source for reading JSON files from a directory.

    Watches a directory for new JSON files and processes them as a stream.
    Supports schema evolution and handles malformed files.
    """

    def __init__(
        self,
        spark: SparkSession,
        data_source: DataSource,
        schema: StructType | None = None,
    ):
        """
        Initialize file stream source.

        Args:
            spark: Active Spark session
            data_source: DataSource configuration with connection_info containing:
                - stream_path: Directory to watch for files
                - file_format: Format (json, csv, parquet) - default json
            schema: Optional schema for parsing. If None, will infer schema.
        """
        self.spark = spark
        self.data_source = data_source
        self.schema = schema

        # Extract configuration from connection_info
        self.stream_path = data_source.connection_info.get("stream_path")
        self.file_format = data_source.connection_info.get("file_format", "json")

        if not self.stream_path:
            raise ValueError("stream_path must be specified in connection_info")

        # Validate stream path exists
        stream_dir = Path(self.stream_path)
        if not stream_dir.exists():
            logger.warning(f"Stream directory {self.stream_path} does not exist, creating it")
            stream_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Initialized FileStreamSource for {data_source.source_id} "
            f"watching {self.stream_path}"
        )

    def read_stream(self) -> DataFrame:
        """
        Create streaming DataFrame from file directory.

        Returns:
            Streaming DataFrame that monitors the directory for new files

        Raises:
            ValueError: If stream configuration is invalid
        """
        logger.info(
            f"Starting file stream from {self.stream_path} "
            f"(format: {self.file_format})"
        )

        # Build stream reader
        stream_reader = self.spark.readStream.format(self.file_format)

        # Apply schema if provided
        if self.schema:
            stream_reader = stream_reader.schema(self.schema)
            logger.info(f"Using provided schema with {len(self.schema.fields)} fields")
        else:
            logger.warning(
                "No schema provided - Spark will infer schema from first file. "
                "This may cause issues if schema changes."
            )

        # Configure stream options
        stream_reader = stream_reader.option("maxFilesPerTrigger", 10)  # Process up to 10 files per micro-batch
        stream_reader = stream_reader.option("latestFirst", False)  # Process oldest files first

        # For JSON, handle malformed records
        if self.file_format == "json":
            stream_reader = stream_reader.option("mode", "PERMISSIVE")  # Don't fail on malformed records
            stream_reader = stream_reader.option("columnNameOfCorruptRecord", "_corrupt_record")

        # Load the stream
        stream_df = stream_reader.load(self.stream_path)

        # Add source metadata
        from pyspark.sql.functions import lit
        stream_df = stream_df.withColumn(
            "source_id",
            lit(self.data_source.source_id)
        )

        logger.info(
            f"File stream created successfully. "
            f"Schema: {stream_df.schema.simpleString()}"
        )

        return stream_df

    def validate_configuration(self) -> bool:
        """
        Validate stream source configuration.

        Returns:
            True if configuration is valid, False otherwise
        """
        try:
            # Check stream path
            stream_dir = Path(self.stream_path)
            if not stream_dir.exists():
                logger.error(f"Stream path does not exist: {self.stream_path}")
                return False

            if not stream_dir.is_dir():
                logger.error(f"Stream path is not a directory: {self.stream_path}")
                return False

            # Check file format is supported
            supported_formats = ["json", "csv", "parquet"]
            if self.file_format not in supported_formats:
                logger.error(
                    f"Unsupported file format: {self.file_format}. "
                    f"Supported: {supported_formats}"
                )
                return False

            logger.info("Stream source configuration is valid")
            return True

        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            return False


def create_file_stream_source(
    spark: SparkSession,
    stream_path: str,
    source_id: str,
    file_format: str = "json",
    schema: StructType | None = None,
) -> FileStreamSource:
    """
    Factory function to create FileStreamSource with minimal configuration.

    Args:
        spark: Active Spark session
        stream_path: Directory to watch for files
        source_id: Identifier for this data source
        file_format: File format (json, csv, parquet)
        schema: Optional schema for parsing

    Returns:
        Configured FileStreamSource instance

    Example:
        >>> spark = SparkSession.builder.getOrCreate()
        >>> source = create_file_stream_source(
        ...     spark,
        ...     stream_path="/tmp/events",
        ...     source_id="live_events",
        ...     file_format="json"
        ... )
        >>> stream_df = source.read_stream()
    """
    data_source = DataSource(
        source_id=source_id,
        source_type="json_stream",
        connection_info={
            "stream_path": stream_path,
            "file_format": file_format,
        },
        enabled=True,
    )

    return FileStreamSource(spark, data_source, schema)
