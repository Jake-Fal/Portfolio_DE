"""Trade event producer - generates simulated trade executions."""

import time
import random
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from common.duckdb_writer import DuckDBWriter
from common.logger import setup_logger
import config

logger = setup_logger(__name__)


class TradeProducer:
    """Generates random trade events."""

    def __init__(self, db_path: str = '../data/warehouse/portfolio.duckdb'):
        """Initialize trade producer."""
        self.writer = DuckDBWriter(db_path)
        self.current_prices = config.INITIAL_PRICES.copy()
        logger.info("Trade producer initialized")

    def update_price(self, symbol: str, market_price: float):
        """Update the current market price for a symbol."""
        self.current_prices[symbol] = market_price

    def generate_trade(self) -> Dict[str, Any]:
        """Generate a random trade event."""
        symbol = random.choice(config.SYMBOLS)
        side = random.choice(['BUY', 'SELL'])
        quantity = random.randint(config.MIN_QUANTITY, config.MAX_QUANTITY)

        # Use market price with small random spread
        market_price = self.current_prices.get(symbol, 100.0)
        spread = market_price * config.PRICE_SPREAD_PERCENT * random.uniform(-1, 1)
        price = round(market_price + spread, 2)

        trade = {
            'trade_id': f"TRD_{uuid.uuid4()}",
            'account_id': random.choice(config.ACCOUNTS),
            'symbol': symbol,
            'side': side,
            'quantity': float(quantity),
            'price': price,
            'timestamp': int(datetime.now().timestamp() * 1000),
            'event_version': '1.0'
        }

        return trade

    def run(self, duration: float = None):
        """
        Run the trade producer.

        Args:
            duration: How long to run (seconds). None for infinite.
        """
        logger.info(f"Starting trade producer (rate: {config.TRADE_FREQUENCY} trades/sec)")
        start_time = time.time()
        trades_sent = 0

        try:
            while True:
                if duration and (time.time() - start_time) > duration:
                    break

                # Generate and write trades
                num_trades = int(config.TRADE_FREQUENCY * config.PRODUCER_INTERVAL)
                for _ in range(num_trades):
                    trade = self.generate_trade()
                    self.writer.write_trade(trade)
                    trades_sent += 1

                if trades_sent % 100 == 0:
                    logger.info(f"Sent {trades_sent} trades")

                time.sleep(config.PRODUCER_INTERVAL)

        except KeyboardInterrupt:
            logger.info("Trade producer stopped by user")
        finally:
            self.writer.close()
            logger.info(f"Trade producer finished. Total trades sent: {trades_sent}")


if __name__ == '__main__':
    producer = TradeProducer()
    producer.run()
