"""Direct DuckDB writer for events."""

import duckdb
from typing import Dict, Any
from pathlib import Path
from .logger import setup_logger

logger = setup_logger(__name__)


class DuckDBWriter:
    """Writes events directly to DuckDB raw tables."""

    def __init__(self, db_path: str):
        """Initialize DuckDB connection."""
        self.db_path = Path(db_path)
        self.conn = duckdb.connect(str(self.db_path))
        logger.info(f"Connected to DuckDB at {self.db_path}")

    def write_trade(self, trade: Dict[str, Any]):
        """Write trade event to raw_trades table."""
        try:
            self.conn.execute("""
                INSERT INTO raw_trades (
                    trade_id, account_id, symbol, side, quantity, price, timestamp, event_version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                trade['trade_id'],
                trade['account_id'],
                trade['symbol'],
                trade['side'],
                trade['quantity'],
                trade['price'],
                self._millis_to_timestamp(trade['timestamp']),
                trade['event_version']
            ])
        except Exception as e:
            logger.error(f"Error writing trade: {e}")
            raise

    def write_price(self, price_event: Dict[str, Any]):
        """Write price event to raw_prices table."""
        try:
            self.conn.execute("""
                INSERT INTO raw_prices (
                    symbol, price, timestamp, source, event_version
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (symbol, timestamp) DO UPDATE SET price = EXCLUDED.price
            """, [
                price_event['symbol'],
                price_event['price'],
                self._millis_to_timestamp(price_event['timestamp']),
                price_event['source'],
                price_event['event_version']
            ])
        except Exception as e:
            logger.error(f"Error writing price: {e}")
            raise

    def write_cash(self, cash_event: Dict[str, Any]):
        """Write cash event to raw_cash_events table."""
        try:
            self.conn.execute("""
                INSERT INTO raw_cash_events (
                    event_id, account_id, amount, type, timestamp, description, event_version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                cash_event['event_id'],
                cash_event['account_id'],
                cash_event['amount'],
                cash_event['type'],
                self._millis_to_timestamp(cash_event['timestamp']),
                cash_event['description'],
                cash_event['event_version']
            ])
        except Exception as e:
            logger.error(f"Error writing cash event: {e}")
            raise

    def _millis_to_timestamp(self, millis: int):
        """Convert milliseconds since epoch to timestamp."""
        from datetime import datetime
        return datetime.fromtimestamp(millis / 1000.0)

    def close(self):
        """Close DuckDB connection."""
        if self.conn:
            self.conn.close()
            logger.info("DuckDB connection closed")
