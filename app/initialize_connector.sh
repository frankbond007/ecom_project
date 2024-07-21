#!/bin/bash

# Update package list and install required packages
apt-get update && apt-get install -y curl jq && apt-get install -y clickhouse-client

pip install confluent_kafka
pip install clickhouse-driver

# Create ClickHouse tables
clickhouse-client --host clickhouse --query "CREATE DATABASE IF NOT EXISTS ecommerce"
clickhouse-client --host clickhouse --query "CREATE TABLE IF NOT EXISTS ecommerce.customers (customer_id Int32, name String, email String, phone String, address String, gender String, created_at DateTime, day_id String) ENGINE = ReplacingMergeTree ORDER BY customer_id PARTITION BY day_id"
clickhouse-client --host clickhouse --query "CREATE TABLE IF NOT EXISTS ecommerce.products (product_id Int32, name String, description String, category String, price Decimal(10,2), seller_id Int32, seller_name String, created_at DateTime, day_id String) ENGINE = ReplacingMergeTree ORDER BY product_id PARTITION BY day_id"
clickhouse-client --host clickhouse --query "CREATE TABLE IF NOT EXISTS ecommerce.orders (order_id Int32, customer_id Int32, created_at DateTime, day_id String) ENGINE = ReplacingMergeTree ORDER BY order_id PARTITION BY day_id"
clickhouse-client --host clickhouse --query "CREATE TABLE IF NOT EXISTS ecommerce.order_items (order_item_id Int32, order_id Int32, product_id Int32, quantity Int32, unit_price Decimal(10,2), created_at DateTime, day_id String) ENGINE = ReplacingMergeTree ORDER BY order_item_id PARTITION BY day_id"
clickhouse-client --host clickhouse --query "CREATE TABLE IF NOT EXISTS ecommerce.denormalized_orders (order_id Int32, customer_id Int32, order_created_at DateTime, order_item_id Int32, product_id Int32, quantity Int32, unit_price Decimal(10,2), order_item_created_at DateTime, day_id String) ENGINE = ReplacingMergeTree ORDER BY order_id PARTITION BY day_id"

# Register MySQL connector with Debezium
MYSQL_CONNECTOR_CONFIG='{
  "name": "mysql-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "user",
    "database.password": "12345678",
    "database.server.id": "184054",
    "database.server.name": "mysql_server",
    "database.include.list": "ecommerce",
    "table.include.list": "ecommerce.products,ecommerce.customers,ecommerce.orders,ecommerce.order_items",
    "database.history.kafka.bootstrap.servers": "PLAINTEXT://kafka-broker:9092,PLAINTEXT://kafka-broker-2:9093",
    "database.history.kafka.topic": "schema-changes.mysql",
    "schema.history.internal.kafka.bootstrap.servers": "PLAINTEXT://kafka-broker:9092,PLAINTEXT://kafka-broker-2:9093",
    "schema.history.internal.kafka.topic": "schema-changes.mysql",
    "topic.prefix": "ecommerce_",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.add.fields": "op,table,source.ts_ms"
  }
}'

sleep 30

# Register MySQL connector
response=$(curl -s -w "\n%{http_code}" -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://debezium:8083/connectors/ -d "$MYSQL_CONNECTOR_CONFIG")
body=$(echo "$response" | sed '$d')
status=$(echo "$response" | tail -n1)

# Print response
echo "Response body from Debezium API: $body"
echo "Response status from Debezium API: $status"

if [ "$status" -ne 201 ]; then
    echo "Failed to register the MySQL connector or it already exists. Continuing..."
fi

# Wait until the MySQL connector is ready
echo "Waiting for MySQL connector to be ready..."
while true; do
    response=$(curl -s http://debezium:8083/connectors/mysql-connector/status)
    echo "Connector status response: $response"
    state=$(echo "$response" | jq -r '.connector.state')
    if [ "$state" == "RUNNING" ]; then
        echo "MySQL connector is ready."
        break
    else
        echo "Waiting for MySQL connector..."
        sleep 5
    fi
done

# Start Kafka consumers
nohup python /app/kafka_consumer.py ecommerce_.ecommerce.customers ecommerce.customers clickhouse_customers_group > /app/customers_consumer.log 2>&1 &
nohup python /app/kafka_consumer.py ecommerce_.ecommerce.order_items ecommerce.order_items clickhouse_order_items_group > /app/order_items_consumer.log 2>&1 &
nohup python /app/kafka_consumer.py ecommerce_.ecommerce.orders ecommerce.orders clickhouse_orders_group > /app//orders_consumer.log 2>&1 &
nohup python /app/kafka_consumer.py ecommerce_.ecommerce.products ecommerce.products clickhouse_products_group > /app/products_consumer.log 2>&1 &
# Keep the container running
tail -f /dev/null
