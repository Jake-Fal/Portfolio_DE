"""Orchestrator to run all event producers simultaneously."""

import sys
import time
import threading
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from trade_producer import TradeProducer
from price_producer import PriceProducer
from cash_producer import CashProducer
from common.logger import setup_logger

logger = setup_logger(__name__)


def run_trade_producer():
    """Run trade producer in a thread."""
    try:
        producer = TradeProducer()
        producer.run()
    except Exception as e:
        logger.error(f"Trade producer error: {e}")


def run_price_producer():
    """Run price producer in a thread."""
    try:
        producer = PriceProducer()
        producer.run()
    except Exception as e:
        logger.error(f"Price producer error: {e}")


def run_cash_producer():
    """Run cash producer in a thread."""
    try:
        producer = CashProducer()
        producer.run()
    except Exception as e:
        logger.error(f"Cash producer error: {e}")


def main():
    """Run all producers in parallel threads."""
    logger.info("="*60)
    logger.info("Starting Portfolio Event Producers")
    logger.info("="*60)

    # Create threads for each producer
    threads = [
        threading.Thread(target=run_trade_producer, name="TradeProducer", daemon=True),
        threading.Thread(target=run_price_producer, name="PriceProducer", daemon=True),
        threading.Thread(target=run_cash_producer, name="CashProducer", daemon=True)
    ]

    # Start all threads
    for thread in threads:
        thread.start()
        logger.info(f"Started {thread.name}")

    logger.info("All producers running. Press Ctrl+C to stop.")

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("\nShutting down producers...")
        logger.info("Waiting for threads to complete...")
        time.sleep(2)  # Give threads time to clean up
        logger.info("Shutdown complete")


if __name__ == '__main__':
    main()
