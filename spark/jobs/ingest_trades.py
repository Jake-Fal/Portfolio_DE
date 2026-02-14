"""Spark streaming job to ingest trade events from Kafka to DuckDB."""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from jobs.base_streaming_job import BaseStreamingJob
from schemas.event_schemas import TRADE_SCHEMA
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_unixtime, col


class IngestTradesJob(BaseStreamingJob):
    """Ingest trade events from Kafka to DuckDB."""

    def __init__(self):
        super().__init__(
            app_name="ingest_trades",
            kafka_bootstrap_servers="kafka:9092",
            duckdb_path="/opt/data/warehouse/portfolio.duckdb"
        )

    def get_topic_name(self) -> str:
        return "trade_events"

    def get_schema(self):
        return TRADE_SCHEMA

    def get_table_name(self) -> str:
        return "raw_trades"

    def transform(self, df: DataFrame) -> DataFrame:
        """Transform trade events before writing."""
        return (
            df
            # Convert timestamp from milliseconds to timestamp type
            .withColumn("timestamp", from_unixtime(col("timestamp") / 1000))
            # Select only columns that match the table schema
            .select(
                "trade_id",
                "account_id",
                "symbol",
                "side",
                "quantity",
                "price",
                "timestamp",
                "event_version",
                "ingestion_time"
            )
        )


if __name__ == "__main__":
    job = IngestTradesJob()
    try:
        job.run()
    except KeyboardInterrupt:
        print("\nStopping trade ingestion job...")
        job.stop()
