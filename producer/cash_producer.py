"""Cash event producer - generates simulated deposits and withdrawals."""

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


class CashProducer:
    """Generates random cash movement events."""

    def __init__(self, db_path: str = '../data/warehouse/portfolio.duckdb'):
        """Initialize cash event producer."""
        self.writer = DuckDBWriter(db_path)
        logger.info("Cash producer initialized")

    def generate_cash_event(self) -> Dict[str, Any]:
        """Generate a random cash event."""
        # Determine if deposit or withdrawal
        if random.random() < config.CASH_DEPOSIT_PROBABILITY:
            event_type = 'DEPOSIT'
        else:
            event_type = 'WITHDRAWAL'

        # Generate random amount
        amount = round(
            random.uniform(config.MIN_CASH_AMOUNT, config.MAX_CASH_AMOUNT),
            2
        )

        event = {
            'event_id': f"CASH_{uuid.uuid4()}",
            'account_id': random.choice(config.ACCOUNTS),
            'amount': amount,
            'type': event_type,
            'timestamp': int(datetime.now().timestamp() * 1000),
            'description': f"{event_type.lower()} transaction",
            'event_version': '1.0'
        }

        return event

    def run(self, duration: float = None):
        """
        Run the cash event producer.

        Args:
            duration: How long to run (seconds). None for infinite.
        """
        logger.info(
            f"Starting cash producer "
            f"(rate: {config.CASH_FREQUENCY} events/sec)"
        )
        start_time = time.time()
        events_sent = 0

        try:
            while True:
                if duration and (time.time() - start_time) > duration:
                    break

                # Generate events based on frequency
                # With low frequency, use probability-based generation
                if random.random() < (config.CASH_FREQUENCY * config.PRODUCER_INTERVAL):
                    event = self.generate_cash_event()
                    self.writer.write_cash(event)
                    events_sent += 1
                    logger.info(
                        f"Sent cash event: {event['type']} "
                        f"${event['amount']:,.2f} for {event['account_id']}"
                    )

                time.sleep(config.PRODUCER_INTERVAL)

        except KeyboardInterrupt:
            logger.info("Cash producer stopped by user")
        finally:
            self.writer.close()
            logger.info(f"Cash producer finished. Total events sent: {events_sent}")


if __name__ == '__main__':
    producer = CashProducer()
    producer.run()
