"""Price event producer - generates simulated market price ticks."""

import time
import random
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
import numpy as np

from common.duckdb_writer import DuckDBWriter
from common.logger import setup_logger
import config

logger = setup_logger(__name__)


class PriceProducer:
    """Generates simulated price movements using random walk."""

    def __init__(self, db_path: str = '../data/warehouse/portfolio.duckdb'):
        """Initialize price producer."""
        self.writer = DuckDBWriter(db_path)
        self.current_prices = config.INITIAL_PRICES.copy()
        logger.info("Price producer initialized")

    def update_prices(self):
        """Update all prices using random walk with drift."""
        for symbol in config.SYMBOLS:
            current_price = self.current_prices[symbol]

            # Generate random return using geometric Brownian motion
            dt = config.PRICE_UPDATE_INTERVAL
            drift = config.PRICE_DRIFT * dt
            shock = config.PRICE_VOLATILITY * np.random.normal() * np.sqrt(dt)

            # Calculate new price (with lower bound to prevent negative prices)
            price_change = current_price * (drift + shock)
            new_price = max(current_price + price_change, current_price * 0.5)

            self.current_prices[symbol] = round(new_price, 2)

    def generate_price_events(self) -> list[Dict[str, Any]]:
        """Generate price events for all symbols."""
        timestamp = int(datetime.now().timestamp() * 1000)
        events = []

        for symbol in config.SYMBOLS:
            event = {
                'symbol': symbol,
                'price': self.current_prices[symbol],
                'timestamp': timestamp,
                'source': 'simulated',
                'event_version': '1.0'
            }
            events.append(event)

        return events

    def run(self, duration: float = None):
        """
        Run the price producer.

        Args:
            duration: How long to run (seconds). None for infinite.
        """
        logger.info(f"Starting price producer (update interval: {config.PRICE_UPDATE_INTERVAL}s)")
        start_time = time.time()
        updates_sent = 0

        try:
            while True:
                if duration and (time.time() - start_time) > duration:
                    break

                # Update prices and generate events
                self.update_prices()
                events = self.generate_price_events()

                # Write all price events
                for event in events:
                    self.writer.write_price(event)
                    updates_sent += 1

                if updates_sent % 100 == 0:
                    logger.info(f"Sent {updates_sent} price updates")

                time.sleep(config.PRICE_UPDATE_INTERVAL)

        except KeyboardInterrupt:
            logger.info("Price producer stopped by user")
        finally:
            self.writer.close()
            logger.info(f"Price producer finished. Total updates sent: {updates_sent}")

    def get_current_prices(self) -> Dict[str, float]:
        """Get the current prices for all symbols."""
        return self.current_prices.copy()


if __name__ == '__main__':
    producer = PriceProducer()
    producer.run()
