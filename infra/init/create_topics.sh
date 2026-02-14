#!/bin/bash
# Create Kafka topics for portfolio events

echo "Creating Kafka topics..."

# Trade events topic
docker exec portfolio-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic trade_events \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists \
  --config retention.ms=604800000

echo "Created topic: trade_events"

# Price events topic
docker exec portfolio-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic price_events \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists \
  --config retention.ms=604800000

echo "Created topic: price_events"

# Cash events topic
docker exec portfolio-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic cash_events \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists \
  --config retention.ms=604800000

echo "Created topic: cash_events"

# List all topics to verify
echo -e "\nVerifying topics:"
docker exec portfolio-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

echo -e "\nTopics created successfully!"
