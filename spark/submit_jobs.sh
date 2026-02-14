#!/bin/bash
# Submit Spark streaming jobs to the cluster

SPARK_MASTER="spark://spark-master:7077"
SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

echo "Submitting Spark streaming jobs..."

# Submit trade ingestion job
echo "Submitting trade ingestion job..."
spark-submit \
  --master $SPARK_MASTER \
  --packages $SPARK_PACKAGES \
  --deploy-mode client \
  /opt/spark-apps/jobs/ingest_trades.py &

# Submit price ingestion job
echo "Submitting price ingestion job..."
spark-submit \
  --master $SPARK_MASTER \
  --packages $SPARK_PACKAGES \
  --deploy-mode client \
  /opt/spark-apps/jobs/ingest_prices.py &

# Submit cash event ingestion job
echo "Submitting cash event ingestion job..."
spark-submit \
  --master $SPARK_MASTER \
  --packages $SPARK_PACKAGES \
  --deploy-mode client \
  /opt/spark-apps/jobs/ingest_cash_events.py &

echo "All jobs submitted. Check Spark UI at http://localhost:8081"
echo "To stop jobs, use: pkill -f spark-submit"

# Wait for all background jobs
wait
