"""Spark streaming job to ingest cash events from Kafka to DuckDB."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from jobs.base_streaming_job import BaseStreamingJob
from schemas.event_schemas import CASH_SCHEMA
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_unixtime, col


class IngestCashEventsJob(BaseStreamingJob):
    """Ingest cash events from Kafka to DuckDB."""

    def __init__(self):
        super().__init__(
            app_name="ingest_cash_events",
            kafka_bootstrap_servers="kafka:9092",
            duckdb_path="/opt/data/warehouse/portfolio.duckdb"
        )

    def get_topic_name(self) -> str:
        return "cash_events"

    def get_schema(self):
        return CASH_SCHEMA

    def get_table_name(self) -> str:
        return "raw_cash_events"

    def transform(self, df: DataFrame) -> DataFrame:
        """Transform cash events before writing."""
        return (
            df
            .withColumn("timestamp", from_unixtime(col("timestamp") / 1000))
            .select(
                "event_id",
                "account_id",
                "amount",
                "type",
                "timestamp",
                "description",
                "event_version",
                "ingestion_time"
            )
        )


if __name__ == "__main__":
    job = IngestCashEventsJob()
    try:
        job.run()
    except KeyboardInterrupt:
        print("\nStopping cash event ingestion job...")
        job.stop()
