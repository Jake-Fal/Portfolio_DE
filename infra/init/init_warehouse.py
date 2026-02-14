#!/usr/bin/env python3
"""
Initialize DuckDB warehouse with raw tables for portfolio data.

This script creates the DuckDB database file and sets up the raw tables
that will receive data from Spark streaming jobs.
"""

import duckdb
import os
from pathlib import Path

# Determine warehouse path
script_dir = Path(__file__).parent
warehouse_dir = script_dir.parent.parent / 'data' / 'warehouse'
warehouse_dir.mkdir(parents=True, exist_ok=True)
db_path = warehouse_dir / 'portfolio.duckdb'

print(f"Initializing warehouse at: {db_path}")

# Connect to DuckDB
conn = duckdb.connect(str(db_path))

# Create raw_trades table
print("Creating raw_trades table...")
conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_trades (
        trade_id VARCHAR PRIMARY KEY,
        account_id VARCHAR NOT NULL,
        symbol VARCHAR NOT NULL,
        side VARCHAR NOT NULL CHECK (side IN ('BUY', 'SELL')),
        quantity DOUBLE NOT NULL CHECK (quantity > 0),
        price DOUBLE NOT NULL CHECK (price > 0),
        timestamp TIMESTAMP NOT NULL,
        event_version VARCHAR,
        ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")

# Create indexes on raw_trades
conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_trades_account_symbol
    ON raw_trades(account_id, symbol, timestamp)
""")

conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_trades_timestamp
    ON raw_trades(timestamp)
""")

# Create raw_prices table
print("Creating raw_prices table...")
conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_prices (
        symbol VARCHAR NOT NULL,
        price DOUBLE NOT NULL CHECK (price > 0),
        timestamp TIMESTAMP NOT NULL,
        source VARCHAR,
        event_version VARCHAR,
        ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (symbol, timestamp)
    )
""")

# Create index on raw_prices
conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_prices_symbol
    ON raw_prices(symbol, timestamp)
""")

# Create raw_cash_events table
print("Creating raw_cash_events table...")
conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_cash_events (
        event_id VARCHAR PRIMARY KEY,
        account_id VARCHAR NOT NULL,
        amount DOUBLE NOT NULL,
        type VARCHAR NOT NULL CHECK (type IN ('DEPOSIT', 'WITHDRAWAL', 'DIVIDEND', 'INTEREST')),
        timestamp TIMESTAMP NOT NULL,
        description VARCHAR,
        event_version VARCHAR,
        ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")

# Create index on raw_cash_events
conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_cash_account
    ON raw_cash_events(account_id, timestamp)
""")

# Verify tables were created
print("\nVerifying tables...")
tables = conn.execute("SHOW TABLES").fetchall()
print(f"Tables created: {[t[0] for t in tables]}")

# Display schema for each table
for table_name in ['raw_trades', 'raw_prices', 'raw_cash_events']:
    print(f"\n{table_name} schema:")
    schema = conn.execute(f"DESCRIBE {table_name}").fetchall()
    for col in schema:
        print(f"  {col[0]}: {col[1]}")

conn.close()
print(f"\n✓ Warehouse initialized successfully at: {db_path}")
