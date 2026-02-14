"""Configuration for event producers."""

import os

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

KAFKA_TOPICS = {
    'trades': 'trade_events',
    'prices': 'price_events',
    'cash': 'cash_events'
}

# Producer settings
BATCH_SIZE = 100
PRODUCER_INTERVAL = 1.0  # seconds between batches
FLUSH_TIMEOUT = 10.0  # seconds

# Simulation parameters
SYMBOLS = [
    'AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA',
    'META', 'NVDA', 'JPM', 'V', 'WMT'
]

ACCOUNTS = ['ACC001', 'ACC002', 'ACC003']

# Initial prices for each symbol
INITIAL_PRICES = {
    'AAPL': 150.0,
    'GOOGL': 140.0,
    'MSFT': 380.0,
    'AMZN': 175.0,
    'TSLA': 250.0,
    'META': 485.0,
    'NVDA': 875.0,
    'JPM': 195.0,
    'V': 275.0,
    'WMT': 165.0
}

# Trade generation parameters
TRADE_FREQUENCY = 5  # trades per second
MIN_QUANTITY = 1
MAX_QUANTITY = 100
PRICE_SPREAD_PERCENT = 0.1  # 0.1% spread from market price

# Price movement parameters
PRICE_VOLATILITY = 0.005  # 0.5% standard deviation
PRICE_DRIFT = 0.0  # No drift in prices
PRICE_UPDATE_INTERVAL = 1.0  # Update prices every second

# Cash event parameters
CASH_FREQUENCY = 0.1  # events per second (1 every 10 seconds)
MIN_CASH_AMOUNT = 1000.0
MAX_CASH_AMOUNT = 50000.0
CASH_DEPOSIT_PROBABILITY = 0.7  # 70% deposits, 30% withdrawals
