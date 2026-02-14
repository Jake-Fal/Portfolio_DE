# Portfolio Performance & Risk Data Platform

![Python](https://img.shields.io/badge/python-3.13-blue.svg)
![Kafka](https://img.shields.io/badge/kafka-latest-black.svg)
![dbt](https://img.shields.io/badge/dbt-1.9-ff6849.svg)
![DuckDB](https://img.shields.io/badge/duckdb-1.4+-yellow.svg)
![Status](https://img.shields.io/badge/status-operational-green.svg)

A production-grade data platform for tracking trading activity, calculating positions, and analyzing portfolio performance. Designed to showcase end-to-end data engineering skills for fintech applications.

## Overview

This project simulates the data infrastructure used by brokerages and asset managers to:
- **Generate** simulated trading events, market prices, and cash movements
- **Ingest** data directly into an analytical warehouse (DuckDB)
- **Transform** data with dbt to calculate positions, PnL, and risk metrics
- **Analyze** portfolio performance with SQL queries

**Tech Stack:** Python → DuckDB → dbt

## Architecture

```
┌─────────────────┐
│ Event Producers │  (Python)
│ • Trades        │  Generates simulated trading data
│ • Prices        │  Random walk price movements (10 symbols)
│ • Cash Flows    │  Deposits and withdrawals
└────────┬────────┘
         │
         ▼
    ┌─────────┐
    │ DuckDB  │  (Data Warehouse)
    │ Raw Tbls│  raw_trades, raw_prices, raw_cash_events
    └────┬────┘
         │
         ▼
   ┌──────────┐
   │   dbt    │  (Analytics Engineering)
   │ Staging  │  Clean and standardize (3 views)
   │ Dims     │  Account, symbol, date dimensions (3 tables)
   │ Facts    │  Trades, positions, portfolio value, PnL (4 tables)
   │ Metrics  │  Returns, drawdown, volatility (4 views)
   └──────────┘
```

**Note:** This implementation uses direct DuckDB writes for Windows compatibility. The original design included Kafka → Spark streaming, but was simplified to achieve the same analytical goals with fewer dependencies.

## Quick Start

### Prerequisites
- Docker and Docker Compose (for Kafka - optional)
- Python 3.11+
- 4GB RAM

### Installation

1. **Clone and navigate to project**
   ```bash
   git clone https://github.com/yourusername/Portfolio_DE.git
   cd Portfolio_DE
   ```

2. **Install Python dependencies**
   ```bash
   pip install duckdb kafka-python-ng faker numpy pyyaml python-dateutil
   pip install dbt-core==1.9.0 dbt-duckdb==1.9.0
   ```

3. **Initialize warehouse**
   ```bash
   python infra/init/init_warehouse.py
   ```

4. **Install dbt dependencies**
   ```bash
   cd dbt
   dbt deps
   ```

### Running the Pipeline

**Option 1: Quick Demo (Recommended)**

```bash
# Generate initial data (run for 30 seconds)
cd producer
python -c "
from trade_producer import TradeProducer
from price_producer import PriceProducer
from cash_producer import CashProducer
import threading

# Create initial deposits
cp = CashProducer()
for acc in ['ACC001', 'ACC002', 'ACC003']:
    import uuid
    from datetime import datetime
    event = {
        'event_id': f'CASH_{uuid.uuid4()}',
        'account_id': acc,
        'amount': 100000.00,
        'type': 'DEPOSIT',
        'timestamp': int(datetime.now().timestamp() * 1000),
        'description': 'initial deposit',
        'event_version': '1.0'
    }
    cp.writer.write_cash(event)
cp.writer.close()

# Generate trades and prices
tp = TradeProducer()
pp = PriceProducer()

t1 = threading.Thread(target=lambda: tp.run(duration=30))
t2 = threading.Thread(target=lambda: pp.run(duration=30))

t1.start()
t2.start()
t1.join()
t2.join()
"

# Run dbt transformations
cd ../dbt
dbt run && dbt test
```

**Option 2: Continuous Generation**

Terminal 1 - Trades:
```bash
cd producer
python trade_producer.py
```

Terminal 2 - Prices:
```bash
cd producer
python price_producer.py
```

Terminal 3 - dbt (run periodically):
```bash
cd dbt
dbt run && dbt test
```

### View Results

```bash
# Query DuckDB directly
python -c "
import duckdb
conn = duckdb.connect('data/warehouse/portfolio.duckdb')

# Portfolio values
print(conn.execute('''
    SELECT account_id, total_value, cash_balance, positions_value
    FROM main_facts.fact_portfolio_value_daily
    ORDER BY account_id
''').fetchall())

conn.close()
"

# View dbt documentation
cd dbt
dbt docs generate
dbt docs serve
```

## Example Queries

### Portfolio Value Over Time
```sql
SELECT
    account_id,
    value_date,
    total_value,
    cash_balance,
    positions_value
FROM main_facts.fact_portfolio_value_daily
WHERE account_id = 'ACC001'
ORDER BY value_date DESC
LIMIT 30;
```

### Position Holdings
```sql
SELECT
    account_id,
    symbol,
    cumulative_position,
    avg_cost_per_share,
    unrealized_pnl
FROM main_facts.fact_positions_daily
WHERE cumulative_position <> 0
ORDER BY unrealized_pnl DESC;
```

### Daily Returns
```sql
SELECT
    account_id,
    value_date,
    daily_return_pct,
    cumulative_return_pct
FROM main_metrics.daily_returns dr
JOIN main_metrics.cumulative_returns cr USING (account_id, value_date)
ORDER BY account_id, value_date DESC
LIMIT 30;
```

### Risk Metrics
```sql
SELECT
    account_id,
    MAX(cumulative_return_pct) as total_return_pct,
    MIN(max_drawdown_pct) as max_drawdown_pct,
    AVG(annualized_volatility_pct) as avg_volatility_pct
FROM main_metrics.cumulative_returns cr
LEFT JOIN main_metrics.drawdown dd USING (account_id, value_date)
LEFT JOIN main_metrics.rolling_volatility rv USING (account_id, value_date)
GROUP BY account_id;
```

## Project Structure

```
Portfolio_DE/
├── producer/                # Python event generators
│   ├── common/
│   │   ├── duckdb_writer.py # Direct DuckDB ingestion
│   │   └── logger.py
│   ├── schemas/             # Avro schema definitions (reference)
│   ├── trade_producer.py    # Trade event generator
│   ├── price_producer.py    # Price event generator
│   └── cash_producer.py     # Cash event generator
├── dbt/                     # Analytics engineering
│   ├── models/
│   │   ├── staging/         # Clean and standardize (3 views)
│   │   ├── dimensions/      # Account, symbol, date (3 tables)
│   │   ├── facts/           # Trades, positions, PnL (4 tables)
│   │   └── metrics/         # Returns, volatility, drawdown (4 views)
│   ├── profiles.yml         # DuckDB connection config
│   └── dbt_project.yml
├── infra/                   # Infrastructure
│   └── init/
│       ├── init_warehouse.py # Creates DuckDB tables
│       └── create_topics.sh  # Kafka topics (optional)
├── data/                    # Local data (gitignored)
│   └── warehouse/
│       └── portfolio.duckdb # DuckDB database
├── docker-compose.yml       # Kafka services (optional)
├── claude.md                # Implementation guide
└── IMPLEMENTATION_SUMMARY.md # Project summary
```

## Key Features

- ✅ **End-to-End Pipeline:** Data generation → storage → transformation → analytics
- ✅ **Financial Calculations:** Position tracking, PnL (realized/unrealized), returns, drawdown, volatility
- ✅ **Data Quality:** 59 dbt tests validating accuracy and completeness
- ✅ **Production Patterns:** Proper logging, error handling, modular design
- ✅ **Dimensional Modeling:** Star schema with facts and dimensions

## Data Models

### Staging Layer (3 views)
- `stg_trades` - Cleaned trade events
- `stg_prices` - Standardized price updates
- `stg_cash_events` - Normalized cash movements

### Dimensions (3 tables)
- `dim_account` - Account master (3 accounts)
- `dim_symbol` - Symbol reference (10 stocks: AAPL, GOOGL, MSFT, AMZN, TSLA, META, JPM, V, WMT, JNJ)
- `dim_date` - Date dimension (2024-2026)

### Facts (4 tables)
- `fact_trades` - Trade transactions
- `fact_positions_daily` - Daily position holdings by account/symbol
- `fact_portfolio_value_daily` - Aggregate portfolio value by account
- `fact_pnl_daily` - Profit & loss by account/symbol

### Metrics (4 views)
- `daily_returns` - Daily performance percentage
- `cumulative_returns` - Total returns over time
- `drawdown` - Maximum drawdown from peak
- `rolling_volatility` - 30-day rolling volatility (annualized)

## Testing

```bash
# Run all dbt tests (59 tests)
cd dbt
dbt test

# Test specific model
dbt test --select stg_trades

# Run models and tests together
dbt build
```

**Test Coverage:**
- ✅ Uniqueness constraints on primary keys
- ✅ Not-null validations on required fields
- ✅ Accepted values for enums (BUY/SELL, DEPOSIT/WITHDRAWAL)
- ✅ Relationship integrity between models

## Development Guide

See **[IMPLEMENTATION_SUMMARY.md](./IMPLEMENTATION_SUMMARY.md)** for:
- Complete implementation details
- Architecture decisions
- How to extend the pipeline
- Query examples
- Troubleshooting guide

See **[claude.md](./claude.md)** for:
- Phase-by-phase development roadmap
- Financial formulas and assumptions
- Code patterns and best practices

## Monitoring (Optional - with Docker)

If running with Docker Compose:
- **Kafka UI:** http://localhost:8080
- **dbt Docs:** `cd dbt && dbt docs serve` → http://localhost:8080

## Roadmap

- [x] Project setup and infrastructure
- [x] Event producers with schema definitions
- [x] Direct DuckDB ingestion
- [x] dbt models (staging, dimensions, facts, metrics)
- [x] Data quality testing (59 tests)
- [x] End-to-end pipeline verification
- [ ] Streamlit dashboard for visualization
- [ ] Advanced risk metrics (VaR, CVaR, Sharpe ratio)
- [ ] Backtest framework for trading strategies
- [ ] Cloud deployment (Snowflake/BigQuery)
- [ ] Real-time data integration

## Contributing

This is a portfolio project demonstrating data engineering skills. Suggestions and feedback are welcome via issues!

## License

MIT License - See [LICENSE](LICENSE) for details.

## Technical Notes

**Built with:**
- Python 3.13
- DuckDB 1.4.4 (embedded analytical database)
- dbt-core 1.9.0 with dbt-duckdb adapter
- kafka-python-ng 2.2.3
- numpy, faker for data generation

**Demonstrates:**
- Data Engineering: pipeline architecture, data modeling
- Analytics Engineering: dbt transformations, testing
- Financial Modeling: positions, PnL, risk metrics
- Production Patterns: logging, error handling, modularity

---

⭐ If you find this project useful, please consider starring the repo!

**Status:** ✅ Fully operational - ready for portfolio demonstration
