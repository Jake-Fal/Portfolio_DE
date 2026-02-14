"""Spark streaming job to ingest price events from Kafka to DuckDB."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from jobs.base_streaming_job import BaseStreamingJob
from schemas.event_schemas import PRICE_SCHEMA
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_unixtime, col


class IngestPricesJob(BaseStreamingJob):
    """Ingest price events from Kafka to DuckDB."""

    def __init__(self):
        super().__init__(
            app_name="ingest_prices",
            kafka_bootstrap_servers="kafka:9092",
            duckdb_path="/opt/data/warehouse/portfolio.duckdb"
        )

    def get_topic_name(self) -> str:
        return "price_events"

    def get_schema(self):
        return PRICE_SCHEMA

    def get_table_name(self) -> str:
        return "raw_prices"

    def transform(self, df: DataFrame) -> DataFrame:
        """Transform price events before writing."""
        return (
            df
            .withColumn("timestamp", from_unixtime(col("timestamp") / 1000))
            .select(
                "symbol",
                "price",
                "timestamp",
                "source",
                "event_version",
                "ingestion_time"
            )
        )


if __name__ == "__main__":
    job = IngestPricesJob()
    try:
        job.run()
    except KeyboardInterrupt:
        print("\nStopping price ingestion job...")
        job.stop()
