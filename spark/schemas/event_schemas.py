"""PySpark StructType schemas for event deserialization."""

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, LongType
)

# Trade event schema
TRADE_SCHEMA = StructType([
    StructField("trade_id", StringType(), nullable=False),
    StructField("account_id", StringType(), nullable=False),
    StructField("symbol", StringType(), nullable=False),
    StructField("side", StringType(), nullable=False),
    StructField("quantity", DoubleType(), nullable=False),
    StructField("price", DoubleType(), nullable=False),
    StructField("timestamp", LongType(), nullable=False),
    StructField("event_version", StringType(), nullable=True)
])

# Price event schema
PRICE_SCHEMA = StructType([
    StructField("symbol", StringType(), nullable=False),
    StructField("price", DoubleType(), nullable=False),
    StructField("timestamp", LongType(), nullable=False),
    StructField("source", StringType(), nullable=True),
    StructField("event_version", StringType(), nullable=True)
])

# Cash event schema
CASH_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("account_id", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=False),
    StructField("type", StringType(), nullable=False),
    StructField("timestamp", LongType(), nullable=False),
    StructField("description", StringType(), nullable=True),
    StructField("event_version", StringType(), nullable=True)
])
