"""Base class for Spark streaming jobs."""

from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType
import os


class BaseStreamingJob(ABC):
    """Abstract base class for Kafka to DuckDB streaming jobs."""

    def __init__(
        self,
        app_name: str,
        kafka_bootstrap_servers: str = "kafka:9092",
        duckdb_path: str = "/opt/data/warehouse/portfolio.duckdb"
    ):
        """
        Initialize the streaming job.

        Args:
            app_name: Name of the Spark application
            kafka_bootstrap_servers: Kafka broker addresses
            duckdb_path: Path to DuckDB database file
        """
        self.app_name = app_name
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.duckdb_path = duckdb_path

        # Create Spark session
        self.spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.shuffle.partitions", "3")
            .config("spark.sql.streaming.checkpointLocation", f"/opt/data/checkpoints/{app_name}")
            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel("WARN")

    @abstractmethod
    def get_topic_name(self) -> str:
        """Return the Kafka topic to consume from."""
        pass

    @abstractmethod
    def get_schema(self) -> StructType:
        """Return the schema for deserializing messages."""
        pass

    @abstractmethod
    def get_table_name(self) -> str:
        """Return the DuckDB table name to write to."""
        pass

    def read_kafka_stream(self) -> DataFrame:
        """Read streaming data from Kafka."""
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .option("subscribe", self.get_topic_name())
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )

    def deserialize_messages(self, df: DataFrame) -> DataFrame:
        """Deserialize Avro messages from Kafka."""
        # For this scaffold, we'll use JSON deserialization
        # In production, use Avro deserializer with schema registry
        schema = self.get_schema()

        return (
            df
            .select(
                col("key").cast("string").alias("_key"),
                from_json(col("value").cast("string"), schema).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            )
            .select("data.*", "kafka_timestamp")
            .withColumn("ingestion_time", current_timestamp())
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply transformations to the data.
        Override this method for custom transformations.
        """
        return df

    def write_to_duckdb(self, df: DataFrame, epoch_id: int):
        """
        Write batch to DuckDB.

        Args:
            df: Batch DataFrame
            epoch_id: Batch ID
        """
        if df.isEmpty():
            return

        # Convert to Pandas for DuckDB insertion
        pandas_df = df.toPandas()

        # Write to DuckDB
        import duckdb
        conn = duckdb.connect(self.duckdb_path)

        try:
            # Use INSERT OR REPLACE for idempotency
            # Assuming table already exists from init_warehouse.py
            table_name = self.get_table_name()

            # Append to table
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM pandas_df")

            print(f"Batch {epoch_id}: Inserted {len(pandas_df)} rows into {table_name}")

        finally:
            conn.close()

    def run(self):
        """Run the streaming job."""
        print(f"Starting streaming job: {self.app_name}")
        print(f"Topic: {self.get_topic_name()}")
        print(f"Table: {self.get_table_name()}")

        # Read from Kafka
        raw_stream = self.read_kafka_stream()

        # Deserialize messages
        deserialized = self.deserialize_messages(raw_stream)

        # Apply transformations
        transformed = self.transform(deserialized)

        # Write to DuckDB using foreachBatch
        query = (
            transformed.writeStream
            .outputMode("append")
            .foreachBatch(self.write_to_duckdb)
            .option("checkpointLocation", f"/opt/data/checkpoints/{self.app_name}")
            .trigger(processingTime="10 seconds")
            .start()
        )

        query.awaitTermination()

    def stop(self):
        """Stop the Spark session."""
        if self.spark:
            self.spark.stop()
