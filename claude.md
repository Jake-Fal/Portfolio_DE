# Portfolio_DE Implementation Guide

**For: AI Agents & Developers**
**Purpose:** Detailed phase-by-phase instructions for building the Portfolio Performance & Risk Data Platform

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture Deep Dive](#architecture-deep-dive)
3. [Phase-by-Phase Implementation](#phase-by-phase-implementation)
4. [Financial Formulas & Assumptions](#financial-formulas--assumptions)
5. [Development Workflow](#development-workflow)
6. [Troubleshooting Guide](#troubleshooting-guide)

---

## Project Overview

### Goals
Build a production-grade data platform that:
- Ingests real-time trading events from Kafka
- Processes streams with Spark Structured Streaming
- Stores data in DuckDB warehouse
- Transforms data with dbt for analytics
- Calculates positions, PnL, returns, and risk metrics

### Tech Stack
- **Event Generation:** Python producers with Avro schemas
- **Message Broker:** Apache Kafka (KRaft mode)
- **Stream Processing:** Apache Spark 3.5
- **Data Warehouse:** DuckDB (embedded)
- **Analytics Engineering:** dbt-core with dbt-duckdb
- **Orchestration:** Docker Compose

### Success Criteria
- [ ] End-to-end pipeline runs locally without errors
- [ ] Events flow: Producers → Kafka → Spark → DuckDB → dbt
- [ ] All dbt models build successfully
- [ ] Data quality tests pass
- [ ] Can query portfolio value, PnL, and risk metrics

---

## Architecture Deep Dive

### Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    PRODUCER LAYER                           │
│  Python Scripts (trade_producer.py, price_producer.py,     │
│  cash_producer.py) generate simulated events               │
│  • Avro serialization                                       │
│  • Random walk price simulation                             │
│  • Configurable frequency                                   │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    MESSAGE BROKER                           │
│  Apache Kafka (KRaft mode, single broker)                  │
│  Topics:                                                     │
│  • trade_events (3 partitions)                              │
│  • price_events (3 partitions)                              │
│  • cash_events (3 partitions)                               │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                 STREAM PROCESSING                           │
│  Spark Structured Streaming (3 jobs)                       │
│  • Reads from Kafka topics                                  │
│  • Deserializes Avro messages                               │
│  • Applies schema validation                                │
│  • Writes to DuckDB raw tables                              │
│  • Checkpointing for fault tolerance                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  DATA WAREHOUSE                             │
│  DuckDB (embedded analytical database)                     │
│  Raw Tables:                                                 │
│  • raw_trades (trade_id PK, indexes on account+symbol)     │
│  • raw_prices (symbol+timestamp PK)                         │
│  • raw_cash_events (event_id PK, index on account)         │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                ANALYTICS ENGINEERING                        │
│  dbt Models (SQL transformations)                           │
│  Staging → Dimensions → Facts → Metrics                    │
│  • stg_* : Clean and standardize                            │
│  • dim_* : Account, symbol, date dimensions                 │
│  • fact_* : Trades, positions, portfolio value, PnL         │
│  • metrics: Returns, drawdown, volatility                   │
└─────────────────────────────────────────────────────────────┘
```

### Event Schemas

#### Trade Event
```json
{
  "trade_id": "TRD_uuid",
  "account_id": "ACC001",
  "symbol": "AAPL",
  "side": "BUY",  // or "SELL"
  "quantity": 50.0,
  "price": 150.25,
  "timestamp": 1704067200000,  // milliseconds since epoch
  "event_version": "1.0"
}
```

**Business Rules:**
- Trade IDs are unique and immutable
- Quantity must be positive
- Price must be positive
- Side is either BUY or SELL
- BUY increases position, SELL decreases position

#### Price Event
```json
{
  "symbol": "AAPL",
  "price": 150.50,
  "timestamp": 1704067200000,
  "source": "simulated",
  "event_version": "1.0"
}
```

**Business Rules:**
- Prices update every second for all symbols
- Price movement follows geometric Brownian motion
- Standard deviation: 0.5% per update
- No drift in simulation

#### Cash Event
```json
{
  "event_id": "CASH_uuid",
  "account_id": "ACC001",
  "amount": 10000.00,
  "type": "DEPOSIT",  // or "WITHDRAWAL", "DIVIDEND", "INTEREST"
  "timestamp": 1704067200000,
  "description": "deposit transaction",
  "event_version": "1.0"
}
```

**Business Rules:**
- DEPOSIT and DIVIDEND add to cash balance
- WITHDRAWAL subtracts from cash balance
- Amount is always positive (direction determined by type)
- Cash events are less frequent than trades

### Storage Strategy

**Raw Layer (DuckDB):**
- Append-only tables (no updates)
- Primary keys enforce uniqueness
- Indexes on frequently joined columns
- `ingestion_time` for audit trail

**dbt Layers:**
- **Staging (Views):** Lightweight, no storage, always fresh
- **Intermediate (Ephemeral):** Not materialized, computed on demand
- **Dimensions (Tables):** Persisted, relatively static
- **Facts (Tables):** Persisted, large volume
- **Metrics (Views):** Computed on query, flexible

---

## Phase-by-Phase Implementation

### Phase 1: Project Setup & Infrastructure

**Goal:** Bootstrap infrastructure and verify connectivity

**Prerequisites:**
- Docker and Docker Compose installed
- Python 3.11+ installed
- 8GB RAM available for Docker

**Steps:**

1. **Verify folder structure**
   ```bash
   cd Portfolio_DE
   ls -R
   # Should see producer/, spark/, dbt/, infra/, data/ folders
   ```

2. **Start Docker infrastructure**
   ```bash
   docker-compose up -d
   ```

   Wait 30-60 seconds for services to start.

3. **Verify containers are running**
   ```bash
   docker ps
   # Should see 4 containers: kafka, kafka-ui, spark-master, spark-worker
   ```

4. **Create Kafka topics**
   ```bash
   # On Windows (Git Bash or WSL)
   bash infra/init/create_topics.sh

   # Or manually:
   docker exec portfolio-kafka kafka-topics.sh \
     --bootstrap-server localhost:9092 --list
   ```

5. **Initialize DuckDB warehouse**
   ```bash
   python infra/init/init_warehouse.py
   ```

   Verify: `data/warehouse/portfolio.duckdb` file exists

6. **Initialize dbt project**
   ```bash
   cd dbt
   pip install dbt-core dbt-duckdb
   dbt deps
   dbt debug
   ```

   Should see: "All checks passed!"

**Validation Checklist:**
- [ ] All 4 Docker containers running
- [ ] Kafka UI accessible at http://localhost:8080
- [ ] Spark Master UI accessible at http://localhost:8081
- [ ] 3 Kafka topics exist (check Kafka UI)
- [ ] DuckDB file created with 3 raw tables
- [ ] dbt connection successful

**Troubleshooting:**
- If Kafka fails to start: Check port 9092 not in use
- If Spark fails: Increase Docker memory to 6GB minimum
- If dbt debug fails: Check profiles.yml path is correct

---

### Phase 2: Event Producers

**Goal:** Generate simulated trading data flowing to Kafka

**Steps:**

1. **Install producer dependencies**
   ```bash
   cd producer
   pip install -r requirements.txt
   ```

2. **Test individual producers**
   ```bash
   # Test trade producer (Ctrl+C to stop after ~10 seconds)
   python trade_producer.py

   # Test price producer
   python price_producer.py

   # Test cash producer
   python cash_producer.py
   ```

3. **Verify messages in Kafka UI**
   - Open http://localhost:8080
   - Navigate to Topics
   - Check `trade_events`, `price_events`, `cash_events`
   - View messages - should see JSON/Avro data

4. **Run all producers together**
   ```bash
   python run_all.py
   ```

   Let it run for 1-2 minutes to generate test data.

**Expected Behavior:**
- Trade events: ~5 per second
- Price events: 10 per second (1 per symbol)
- Cash events: ~1 every 10 seconds

**Code Patterns:**

The producers use a common pattern:

```python
# 1. Initialize producer with schema
producer = AvroKafkaProducer(
    bootstrap_servers='localhost:9092',
    schema_path='schemas/trade_event.avsc'
)

# 2. Generate event data
event = {
    'trade_id': f"TRD_{uuid.uuid4()}",
    'account_id': random.choice(ACCOUNTS),
    # ... more fields
}

# 3. Send to Kafka
producer.send(topic='trade_events', record=event, key=event['trade_id'])

# 4. Flush on shutdown
producer.flush()
producer.close()
```

**Validation Checklist:**
- [ ] No import errors when running producers
- [ ] Messages appearing in Kafka UI
- [ ] Trade messages have BUY/SELL sides
- [ ] Prices are realistic (not negative or extreme)
- [ ] Cash events have correct types (DEPOSIT/WITHDRAWAL)
- [ ] No serialization errors in logs

**Troubleshooting:**
- `confluent_kafka` import error: Run `pip install confluent-kafka`
- Connection refused: Ensure Kafka is running and accessible on localhost:9092
- Avro serialization errors: Check schema files are valid JSON

---

### Phase 3: Spark Streaming Ingestion

**Goal:** Ingest Kafka messages into DuckDB raw tables

**Important Note:** The Spark jobs as scaffolded assume JSON serialization for simplicity. In production, you'd use a full Avro deserializer with Schema Registry.

**Steps:**

1. **Install Spark dependencies (if running locally)**
   ```bash
   cd spark
   pip install -r requirements.txt
   ```

2. **Submit Spark jobs to cluster**
   ```bash
   # Make script executable
   chmod +x submit_jobs.sh

   # Submit all jobs
   bash submit_jobs.sh
   ```

   **Note:** The jobs are configured to run inside the Spark container and read/write from mounted volumes.

3. **Monitor Spark UI**
   - Open http://localhost:8081
   - Check "Running Applications"
   - Should see 3 streaming jobs

4. **Verify data in DuckDB**
   ```bash
   duckdb data/warehouse/portfolio.duckdb
   ```

   ```sql
   SELECT COUNT(*) FROM raw_trades;
   SELECT COUNT(*) FROM raw_prices;
   SELECT COUNT(*) FROM raw_cash_events;

   -- View sample data
   SELECT * FROM raw_trades LIMIT 5;
   ```

**Expected Behavior:**
- Rows continuously appear in raw tables
- Trade counts increase by ~50 per 10 seconds
- Price counts increase by ~100 per 10 seconds
- Cash counts increase by ~1 per 10 seconds

**Code Patterns:**

Base streaming job pattern:

```python
class IngestTradesJob(BaseStreamingJob):
    def get_topic_name(self) -> str:
        return "trade_events"

    def get_schema(self):
        return TRADE_SCHEMA  # PySpark StructType

    def get_table_name(self) -> str:
        return "raw_trades"

    def transform(self, df: DataFrame) -> DataFrame:
        # Convert milliseconds to timestamp
        return df.withColumn(
            "timestamp",
            from_unixtime(col("timestamp") / 1000)
        )
```

**Validation Checklist:**
- [ ] All 3 Spark jobs running (check Spark UI)
- [ ] No errors in Spark logs
- [ ] Data appearing in DuckDB tables
- [ ] Timestamps are correctly converted
- [ ] No duplicate primary keys (trade_id, event_id)
- [ ] Checkpoints being created in data/checkpoints/

**Troubleshooting:**
- Jobs fail immediately: Check Kafka is reachable from Spark container
- "Table does not exist" error: Ensure init_warehouse.py ran successfully
- Checkpoint errors: Clear `data/checkpoints/` and restart jobs
- Memory errors: Reduce Spark worker memory or batch size

---

### Phase 4: dbt Staging Models

**Goal:** Create clean, standardized views of raw data

**Steps:**

1. **Navigate to dbt project**
   ```bash
   cd dbt
   ```

2. **Run staging models**
   ```bash
   dbt run --select staging
   ```

   Should see: "Completed successfully" for 3 models

3. **Query staging tables**
   ```bash
   duckdb ../data/warehouse/portfolio.duckdb
   ```

   ```sql
   SELECT * FROM staging.stg_trades LIMIT 10;
   SELECT * FROM staging.stg_prices LIMIT 10;
   SELECT * FROM staging.stg_cash_events LIMIT 10;
   ```

4. **Run tests**
   ```bash
   dbt test --select staging
   ```

**Expected Results:**
- All models build as views (fast, no storage)
- Side values uppercase (BUY, SELL)
- Cash types uppercase (DEPOSIT, WITHDRAWAL)
- No nulls in required fields

**Validation Checklist:**
- [ ] 3 staging models build successfully
- [ ] Tests pass (or expected failures documented)
- [ ] Row counts match raw tables
- [ ] Timestamps converted to date fields correctly

---

### Phase 5: dbt Dimensions

**Goal:** Create dimension tables for accounts, symbols, and dates

**Steps:**

1. **Run dimension models**
   ```bash
   dbt run --select dimensions
   ```

2. **Verify dimensions**
   ```sql
   SELECT * FROM dimensions.dim_account;
   SELECT * FROM dimensions.dim_symbol;
   SELECT COUNT(*) FROM dimensions.dim_date;
   ```

**Expected Results:**
- `dim_account`: 3 accounts (ACC001, ACC002, ACC003)
- `dim_symbol`: 10 symbols (AAPL, GOOGL, ...)
- `dim_date`: ~730 dates (2024-01-01 to 2026-01-01)

**Validation Checklist:**
- [ ] All accounts from trades and cash events present
- [ ] All symbols from trades and prices present
- [ ] Date dimension covers required range

---

### Phase 6: dbt Facts

**Goal:** Build fact tables for trades, positions, portfolio value, and PnL

**Steps:**

1. **Run fact models**
   ```bash
   dbt run --select facts
   ```

2. **Verify fact tables**
   ```sql
   SELECT COUNT(*) FROM facts.fact_trades;
   SELECT COUNT(*) FROM facts.fact_positions_daily;
   SELECT COUNT(*) FROM facts.fact_portfolio_value_daily;
   SELECT COUNT(*) FROM facts.fact_pnl_daily;
   ```

3. **Check data quality**
   ```sql
   -- Positions should be cumulative
   SELECT account_id, symbol, position_date, cumulative_position
   FROM facts.fact_positions_daily
   WHERE account_id = 'ACC001' AND symbol = 'AAPL'
   ORDER BY position_date;

   -- Portfolio value should be positive
   SELECT * FROM facts.fact_portfolio_value_daily
   WHERE total_value <= 0;  -- Should return 0 rows
   ```

**Validation Checklist:**
- [ ] Fact tables build successfully
- [ ] Positions accumulate correctly (BUY adds, SELL subtracts)
- [ ] Portfolio value = positions value + cash balance
- [ ] No negative portfolio values (unless shorts allowed)

---

### Phase 7: dbt Metrics

**Goal:** Calculate returns, drawdown, and volatility

**Steps:**

1. **Run metric models**
   ```bash
   dbt run --select metrics
   ```

2. **Verify metrics**
   ```sql
   -- Daily returns
   SELECT * FROM metrics.daily_returns
   WHERE account_id = 'ACC001'
   ORDER BY value_date DESC
   LIMIT 10;

   -- Cumulative returns
   SELECT * FROM metrics.cumulative_returns
   WHERE account_id = 'ACC001'
   ORDER BY value_date DESC
   LIMIT 10;

   -- Maximum drawdown
   SELECT account_id, MAX(max_drawdown_pct) as worst_drawdown
   FROM metrics.drawdown
   GROUP BY account_id;

   -- Current volatility
   SELECT * FROM metrics.rolling_volatility
   WHERE account_id = 'ACC001'
   ORDER BY value_date DESC
   LIMIT 1;
   ```

**Expected Results:**
- Daily returns typically between -5% and +5%
- Cumulative returns vary by trading activity
- Drawdowns are negative values
- Volatility around 20-40% annualized

**Validation Checklist:**
- [ ] All metric models build successfully
- [ ] Returns are reasonable (not 1000%+)
- [ ] Drawdown is negative or zero
- [ ] Volatility increases with more trading

---

### Phase 8: Full dbt Build

**Goal:** Build entire dbt project end-to-end

**Steps:**

1. **Run all models**
   ```bash
   dbt run
   ```

2. **Run all tests**
   ```bash
   dbt test
   ```

3. **Generate documentation**
   ```bash
   dbt docs generate
   dbt docs serve
   ```

   Open http://localhost:8080 to view dbt docs

**Validation Checklist:**
- [ ] All models build successfully
- [ ] Tests pass (or failures are expected/documented)
- [ ] Documentation renders correctly
- [ ] Lineage graph shows correct dependencies

---

### Phase 9: End-to-End Testing

**Goal:** Verify the entire pipeline works together

**Steps:**

1. **Stop all producers and Spark jobs**
   ```bash
   # Ctrl+C on producer terminal
   # Stop Spark jobs (if running)
   ```

2. **Clear existing data (optional)**
   ```bash
   rm data/warehouse/portfolio.duckdb
   python infra/init/init_warehouse.py
   ```

3. **Start producers**
   ```bash
   cd producer
   python run_all.py
   ```

4. **Submit Spark jobs**
   ```bash
   cd spark
   bash submit_jobs.sh
   ```

5. **Wait 2-3 minutes for data to accumulate**

6. **Run dbt models**
   ```bash
   cd dbt
   dbt run && dbt test
   ```

7. **Query final results**
   ```sql
   -- Portfolio value over time
   SELECT
       account_id,
       value_date,
       total_value,
       cash_balance,
       positions_value
   FROM facts.fact_portfolio_value_daily
   WHERE account_id = 'ACC001'
   ORDER BY value_date DESC
   LIMIT 30;

   -- PnL by symbol
   SELECT
       symbol,
       SUM(realized_pnl) as total_realized_pnl,
       SUM(total_pnl) as total_pnl
   FROM facts.fact_pnl_daily
   GROUP BY symbol
   ORDER BY total_pnl DESC;

   -- Account performance summary
   SELECT
       pv.account_id,
       MAX(pv.total_value) as current_value,
       MAX(cr.cumulative_return_pct) as total_return_pct,
       MIN(dd.max_drawdown_pct) as max_drawdown_pct,
       AVG(vol.annualized_volatility_pct) as avg_volatility_pct
   FROM facts.fact_portfolio_value_daily pv
   LEFT JOIN metrics.cumulative_returns cr
       ON pv.account_id = cr.account_id
       AND pv.value_date = cr.value_date
   LEFT JOIN metrics.drawdown dd
       ON pv.account_id = dd.account_id
       AND pv.value_date = dd.value_date
   LEFT JOIN metrics.rolling_volatility vol
       ON pv.account_id = vol.account_id
       AND pv.value_date = vol.value_date
   WHERE pv.value_date = (SELECT MAX(value_date) FROM facts.fact_portfolio_value_daily)
   GROUP BY pv.account_id;
   ```

**Success Criteria:**
- [ ] Events flow from producers to Kafka (verify in Kafka UI)
- [ ] Spark jobs ingest data to DuckDB (check raw tables)
- [ ] dbt models build without errors
- [ ] All tests pass (or expected failures documented)
- [ ] Can query portfolio value, PnL, and risk metrics
- [ ] Numbers are realistic and make business sense

---

## Financial Formulas & Assumptions

### Position Tracking

**Formula:**
```
position(t) = position(t-1) + (buy_quantity - sell_quantity)
```

**Implementation:** `fact_positions_daily`
```sql
sum(
  case when side = 'BUY' then quantity
       when side = 'SELL' then -quantity
  end
) over (
  partition by account_id, symbol
  order by trade_date
  rows between unbounded preceding and current row
)
```

**Assumptions:**
- All positions start at 0
- Fractional shares allowed
- No short restrictions (position can go negative)
- Same-day trades are netted by end of day

### Unrealized PnL

**Formula:**
```
unrealized_pnl = position × (current_price - avg_cost_basis)
```

**Assumptions:**
- Mark-to-market using latest daily price
- Cost basis calculated using FIFO method
- No transaction costs or slippage
- Corporate actions (splits, dividends) not modeled

**Note:** The scaffolded PnL model uses a simplified calculation. Production systems would track cost basis per share using FIFO/LIFO.

### Realized PnL

**Formula:**
```
realized_pnl = CASE
  WHEN side = 'SELL' THEN quantity × (sell_price - cost_basis_per_share)
  ELSE 0
END
```

**Assumptions:**
- Realized on SELL trades only
- Uses FIFO cost basis
- Wash sale rules not applied
- Tax implications not considered

### Daily Returns

**Formula:**
```
daily_return = (portfolio_value_today - portfolio_value_yesterday) / portfolio_value_yesterday
```

**Implementation:** `metrics.daily_returns`
```sql
(current_value - lag(current_value)) / lag(current_value)
```

**Assumptions:**
- Time-weighted returns (not money-weighted)
- Deposits/withdrawals NOT excluded from calculation
- Calculated after market close
- Missing days treated as 0% return

**For accurate returns excluding cash flows, use:**
```
adjusted_return = (ending_value - beginning_value - net_deposits) / beginning_value
```

### Cumulative Returns

**Formula:**
```
cumulative_return = ∏(1 + daily_return_i) - 1
```

**Implementation:**
```sql
exp(sum(ln(1 + daily_return)) over (...)) - 1
```

**Assumptions:**
- Geometric linking of daily returns
- Starts from first data point (not calendar start)
- Includes impact of deposits/withdrawals

### Maximum Drawdown

**Formula:**
```
drawdown(t) = (portfolio_value(t) - peak_value(t)) / peak_value(t)
max_drawdown = min(all_drawdowns)
```

**Implementation:** `metrics.drawdown`
```sql
(total_value - max(total_value) over (...)) / max(total_value) over (...)
```

**Assumptions:**
- Peak is running maximum up to time t
- Drawdown is always ≤ 0
- Measured from highest point to current
- Recovery period not tracked

### Rolling Volatility

**Formula:**
```
volatility_30d = stdev(daily_returns over 30 days)
annualized_volatility = volatility_30d × √252
```

**Implementation:** `metrics.rolling_volatility`
```sql
stddev(daily_return) over (
  rows between 29 preceding and current row
) * sqrt(252)
```

**Assumptions:**
- 30-day rolling window (needs 30 days of data)
- 252 trading days per year for annualization
- Returns are normally distributed (often violated in practice)
- No adjustment for serial correlation

### Sharpe Ratio (Optional Extension)

**Formula:**
```
sharpe_ratio = (avg_return - risk_free_rate) / stdev(returns)
```

**Assumptions:**
- Risk-free rate = 0% (simplified)
- Annualize using √252 for daily returns
- Requires minimum 30 data points

**Not implemented in scaffold, but can be added as:**
```sql
SELECT
  account_id,
  AVG(daily_return * 252) / (STDDEV(daily_return) * SQRT(252)) as sharpe_ratio
FROM metrics.daily_returns
GROUP BY account_id
```

---

## Development Workflow

### Daily Development Loop

```bash
# 1. Start infrastructure (if not running)
docker-compose up -d

# 2. Terminal 1: Start producers
cd producer
python run_all.py

# 3. Terminal 2: Start Spark jobs
cd spark
bash submit_jobs.sh

# 4. Terminal 3: Run dbt
cd dbt
dbt run
dbt test

# 5. Query results
duckdb data/warehouse/portfolio.duckdb
```

### Making Changes

**To update producer logic:**
1. Edit Python file in `producer/`
2. Stop producer (Ctrl+C)
3. Restart: `python run_all.py`
4. Verify in Kafka UI

**To update Spark jobs:**
1. Edit job file in `spark/jobs/`
2. Stop Spark job
3. Resubmit: `bash submit_jobs.sh`
4. Check Spark UI for errors

**To update dbt models:**
1. Edit SQL file in `dbt/models/`
2. Run specific model: `dbt run --select model_name`
3. Run tests: `dbt test --select model_name`
4. Check lineage: `dbt docs generate && dbt docs serve`

### Testing Strategy

**Unit Tests (Future):**
- Test producer event generation logic
- Test Spark transformation functions
- Mock Kafka/DuckDB connections

**Integration Tests:**
- Run pipeline end-to-end
- Verify row counts at each stage
- Check data quality constraints

**Data Quality Tests (dbt):**
- `unique` - Primary keys
- `not_null` - Required fields
- `accepted_values` - Enums (BUY/SELL, DEPOSIT/WITHDRAWAL)
- `relationships` - Foreign keys
- `expression_is_true` - Business rules (price > 0)

**Run tests:**
```bash
dbt test                          # All tests
dbt test --select staging         # Staging only
dbt test --select tag:daily       # Tagged tests
```

### Performance Optimization

**For slow dbt models:**
```bash
# Profile model execution
dbt run --select fact_portfolio_value_daily --profile

# Materialize as table instead of view
# In dbt_project.yml or model config:
{{ config(materialized='table') }}

# Add indexes in DuckDB
CREATE INDEX idx_positions ON fact_positions_daily(account_id, symbol, position_date);
```

**For Kafka throughput:**
- Increase partitions: `--partitions 6`
- Batch producer sends: increase `BATCH_SIZE` in config.py
- Enable compression: `compression.type=snappy`

**For Spark performance:**
- Increase worker memory in docker-compose.yml
- Tune shuffle partitions: `spark.sql.shuffle.partitions`
- Adjust trigger interval: `.trigger(processingTime="5 seconds")`

---

## Troubleshooting Guide

### Kafka Issues

**Problem:** Producer can't connect to Kafka
```
ERROR: Failed to connect to broker localhost:9092
```

**Solutions:**
1. Check Kafka is running: `docker ps | grep kafka`
2. Verify port mapping: `docker port portfolio-kafka`
3. Test connection: `telnet localhost 9092`
4. Check Kafka logs: `docker logs portfolio-kafka`

**Problem:** Messages not appearing in Kafka
```
Producer sends messages but Kafka UI shows 0 messages
```

**Solutions:**
1. Check topic exists: Visit Kafka UI → Topics
2. Verify producer isn't erroring: Check console output
3. Check serialization: Ensure Avro schema is valid
4. Flush producer: Call `producer.flush()` before exit

### Spark Issues

**Problem:** Spark job fails with OutOfMemoryError
```
java.lang.OutOfMemoryError: Java heap space
```

**Solutions:**
1. Increase worker memory in `docker-compose.yml`:
   ```yaml
   environment:
     - SPARK_WORKER_MEMORY=4G
   ```
2. Reduce batch size: `.trigger(processingTime="30 seconds")`
3. Reduce shuffle partitions: `spark.sql.shuffle.partitions=2`

**Problem:** "Failed to find data source: kafka"
```
AnalysisException: Failed to find data source: kafka
```

**Solutions:**
1. Add Kafka package to spark-submit:
   ```bash
   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
   ```
2. Check package is downloaded (happens on first run)

**Problem:** Checkpoint corruption
```
Checkpoint directory is corrupted
```

**Solutions:**
1. Delete checkpoint directory:
   ```bash
   rm -rf data/checkpoints/ingest_trades
   ```
2. Restart Spark job

### DuckDB Issues

**Problem:** "Table does not exist" error
```
Catalog Error: Table with name "raw_trades" does not exist
```

**Solutions:**
1. Run init script: `python infra/init/init_warehouse.py`
2. Check database file exists: `ls data/warehouse/`
3. Verify in DuckDB CLI:
   ```bash
   duckdb data/warehouse/portfolio.duckdb
   > SHOW TABLES;
   ```

**Problem:** Database locked
```
IOException: database is locked
```

**Solutions:**
1. Close other DuckDB connections
2. Check no Spark jobs are writing
3. Restart Docker containers if persistent

### dbt Issues

**Problem:** "Relation does not exist"
```
Runtime Error: Relation "raw_trades" does not exist
```

**Solutions:**
1. Ensure Spark has written data: `SELECT COUNT(*) FROM raw_trades;`
2. Check source configuration in `_staging.yml`
3. Verify profiles.yml path is correct: `dbt debug`

**Problem:** Models fail with "column not found"
```
Catalog Error: Column "trade_id" not found
```

**Solutions:**
1. Check column names match raw table schema
2. Verify staging model selects correct columns
3. Use `dbt run --full-refresh` to rebuild from scratch

**Problem:** Circular dependency
```
Compilation Error: Circular dependency detected
```

**Solutions:**
1. Check model references with `{{ ref('model_name') }}`
2. Ensure no model references itself
3. Review lineage in dbt docs: `dbt docs generate && dbt docs serve`

### Docker Issues

**Problem:** Containers exit immediately
```
docker ps shows no containers
```

**Solutions:**
1. Check logs: `docker-compose logs kafka`
2. Verify ports not in use: `netstat -an | grep 9092`
3. Increase Docker resource limits (Mac/Windows)
4. Check Docker Compose file syntax

**Problem:** Can't access Kafka UI or Spark UI
```
Connection refused on localhost:8080
```

**Solutions:**
1. Check container is running: `docker ps | grep kafka-ui`
2. Verify port mapping: `docker port portfolio-kafka-ui`
3. Try 127.0.0.1:8080 instead of localhost:8080
4. Check firewall settings

### General Debugging

**Enable verbose logging:**

**Producers:**
```python
from common.logger import setup_logger
logger = setup_logger(__name__, level=logging.DEBUG)
```

**Spark:**
```python
spark.sparkContext.setLogLevel("INFO")  # or "DEBUG"
```

**dbt:**
```bash
dbt run --debug
```

**Check data flow at each stage:**
```bash
# 1. Kafka (via UI or CLI)
docker exec portfolio-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic trade_events \
  --from-beginning --max-messages 5

# 2. DuckDB raw tables
duckdb data/warehouse/portfolio.duckdb "SELECT COUNT(*) FROM raw_trades"

# 3. dbt staging
duckdb data/warehouse/portfolio.duckdb "SELECT COUNT(*) FROM staging.stg_trades"

# 4. dbt facts
duckdb data/warehouse/portfolio.duckdb "SELECT COUNT(*) FROM facts.fact_trades"
```

---

## Next Steps & Extensions

### Immediate Enhancements
1. Add Confluent Schema Registry for Avro schemas
2. Implement proper FIFO cost basis tracking in PnL
3. Add data freshness checks (dbt tests)
4. Create Streamlit dashboard for visualization
5. Add unit tests for producer and Spark logic

### Advanced Features
1. Real-time dashboard with WebSocket updates
2. Backtesting framework for trading strategies
3. Risk metrics: VaR, CVaR, beta, correlation matrix
4. Trade execution simulation with slippage
5. Multi-currency support
6. Options and derivatives support

### Production Readiness
1. Deploy to cloud (AWS EMR for Spark, RDS for Postgres)
2. Replace DuckDB with Snowflake or BigQuery
3. Add orchestration with Airflow or Prefect
4. Implement monitoring and alerting
5. Set up CI/CD pipeline
6. Add authentication and authorization

---

## Additional Resources

**Documentation:**
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [dbt Documentation](https://docs.getdbt.com/)
- [DuckDB Documentation](https://duckdb.org/docs/)

**Community:**
- Kafka Users Mailing List
- Spark StackOverflow
- dbt Slack Community
- DuckDB Discord

---

**Last Updated:** 2026-02-05
**Version:** 1.0.0
**Maintained by:** Portfolio_DE Project
