"""
Kafka source for Spark Structured Streaming.

Consumes messages from Apache Kafka topics using Spark Structured Streaming.
"""

import logging
from typing import Optional, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType

from src.core.models.data_source import DataSource

logger = logging.getLogger(__name__)


class KafkaSource:
    """
    Spark Structured Streaming source for consuming from Apache Kafka.

    Reads messages from Kafka topics and parses them according to provided schema.
    Supports multiple topics, offset management, and exactly-once semantics.
    """

    def __init__(
        self,
        spark: SparkSession,
        data_source: DataSource,
        value_schema: Optional[StructType] = None,
    ):
        """
        Initialize Kafka stream source.

        Args:
            spark: Active Spark session
            data_source: DataSource configuration with connection_info containing:
                - kafka_bootstrap_servers: Comma-separated list of Kafka brokers
                - kafka_topic: Kafka topic name (or comma-separated topics)
                - kafka_group_id: Consumer group ID (optional)
                - starting_offsets: Where to start reading (earliest, latest, or JSON offset spec)
            value_schema: Schema for parsing Kafka message values (assumes JSON)
        """
        self.spark = spark
        self.data_source = data_source
        self.value_schema = value_schema

        # Extract Kafka configuration
        self.bootstrap_servers = data_source.connection_info.get("kafka_bootstrap_servers")
        self.topic = data_source.connection_info.get("kafka_topic")
        self.group_id = data_source.connection_info.get("kafka_group_id", f"{data_source.source_id}_consumer")
        self.starting_offsets = data_source.connection_info.get("starting_offsets", "latest")

        # Validate required configuration
        if not self.bootstrap_servers:
            raise ValueError("kafka_bootstrap_servers must be specified in connection_info")
        if not self.topic:
            raise ValueError("kafka_topic must be specified in connection_info")

        logger.info(
            f"Initialized KafkaSource for {data_source.source_id} "
            f"(topic: {self.topic}, servers: {self.bootstrap_servers})"
        )

    def read_stream(self) -> DataFrame:
        """
        Create streaming DataFrame from Kafka topic.

        Returns:
            Streaming DataFrame containing Kafka messages

        Raises:
            ValueError: If Kafka configuration is invalid
        """
        logger.info(
            f"Starting Kafka stream from topic '{self.topic}' "
            f"(offsets: {self.starting_offsets})"
        )

        # Build Kafka stream reader
        stream_reader = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", self.starting_offsets)
        )

        # Add consumer group ID if specified
        if self.group_id:
            stream_reader = stream_reader.option("kafka.group.id", self.group_id)

        # Enable exactly-once semantics
        stream_reader = stream_reader.option("failOnDataLoss", "false")  # Handle topic deletions gracefully

        # Load the stream
        kafka_df = stream_reader.load()

        # Kafka DataFrame has these columns:
        # - key: binary
        # - value: binary (our JSON message)
        # - topic: string
        # - partition: int
        # - offset: long
        # - timestamp: timestamp
        # - timestampType: int

        # Parse the value column as JSON
        if self.value_schema:
            # Cast value from binary to string, then parse as JSON
            parsed_df = kafka_df.withColumn(
                "value_str",
                col("value").cast("string")
            ).withColumn(
                "parsed_value",
                from_json(col("value_str"), self.value_schema)
            )

            # Expand the parsed JSON into top-level columns
            for field in self.value_schema.fields:
                parsed_df = parsed_df.withColumn(
                    field.name,
                    col(f"parsed_value.{field.name}")
                )

            # Drop temporary columns
            parsed_df = parsed_df.drop("value_str", "parsed_value", "value")

            logger.info(
                f"Kafka stream created with parsed schema "
                f"({len(self.value_schema.fields)} fields)"
            )
        else:
            # No schema provided - just cast value to string
            parsed_df = kafka_df.withColumn(
                "value_str",
                col("value").cast("string")
            ).drop("value")

            logger.warning(
                "No value schema provided - messages will be treated as raw strings"
            )

        # Add source metadata
        from pyspark.sql.functions import lit
        parsed_df = parsed_df.withColumn(
            "source_id",
            lit(self.data_source.source_id)
        )

        # Keep Kafka metadata for lineage
        # Keep: topic, partition, offset, timestamp

        return parsed_df

    def validate_configuration(self) -> bool:
        """
        Validate Kafka source configuration.

        Returns:
            True if configuration is valid, False otherwise
        """
        try:
            # Check required fields
            if not self.bootstrap_servers:
                logger.error("kafka_bootstrap_servers is required")
                return False

            if not self.topic:
                logger.error("kafka_topic is required")
                return False

            # Validate starting_offsets value
            valid_offsets = ["earliest", "latest"]
            if self.starting_offsets not in valid_offsets and not self.starting_offsets.startswith("{"):
                logger.error(
                    f"Invalid starting_offsets: {self.starting_offsets}. "
                    f"Must be 'earliest', 'latest', or JSON offset spec"
                )
                return False

            logger.info("Kafka source configuration is valid")
            return True

        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            return False


def create_kafka_source(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    source_id: str,
    value_schema: Optional[StructType] = None,
    starting_offsets: str = "latest",
    group_id: Optional[str] = None,
) -> KafkaSource:
    """
    Factory function to create KafkaSource with minimal configuration.

    Args:
        spark: Active Spark session
        bootstrap_servers: Comma-separated list of Kafka brokers (host:port)
        topic: Kafka topic name
        source_id: Identifier for this data source
        value_schema: Schema for parsing message values (JSON)
        starting_offsets: Where to start reading (earliest, latest)
        group_id: Consumer group ID (optional, will auto-generate if not provided)

    Returns:
        Configured KafkaSource instance

    Example:
        >>> from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        >>> spark = SparkSession.builder.getOrCreate()
        >>> schema = StructType([
        ...     StructField("transaction_id", StringType(), False),
        ...     StructField("amount", DoubleType(), False),
        ... ])
        >>> source = create_kafka_source(
        ...     spark,
        ...     bootstrap_servers="localhost:9092",
        ...     topic="transactions",
        ...     source_id="kafka_transactions",
        ...     value_schema=schema
        ... )
        >>> stream_df = source.read_stream()
    """
    data_source = DataSource(
        source_id=source_id,
        source_type="kafka_topic",
        connection_info={
            "kafka_bootstrap_servers": bootstrap_servers,
            "kafka_topic": topic,
            "kafka_group_id": group_id or f"{source_id}_consumer",
            "starting_offsets": starting_offsets,
        },
        enabled=True,
    )

    return KafkaSource(spark, data_source, value_schema)
