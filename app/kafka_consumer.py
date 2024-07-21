import json
import argparse
import logging
from confluent_kafka import Consumer, KafkaError
from clickhouse_driver import Client
from datetime import datetime, timedelta
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BATCH_SIZE = 500  # Adjust the batch size as needed
BATCH_TIMEOUT = 5  # Time in seconds to force batch write if batch size is not reached

def connect_clickhouse():
    try:
        client = Client('clickhouse')
        logger.info("Connected to ClickHouse")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {e}")
        sys.exit(1)

def generate_day_id():
    return datetime.now().strftime('%Y%m%d')

def convert_to_int(value):
    return int(value)

def convert_to_string(value):
    return str(value)

def convert_to_decimal(value):
    return float(int.from_bytes(bytes(value, 'utf-8'), 'big') / 100)

def convert_to_timestamp(value):
    return int(value / 1000)

conversion_functions = {
    'int32': convert_to_int,
    'string': convert_to_string,
    'bytes': convert_to_decimal,
    'int64': convert_to_timestamp
}

def process_messages(messages, table_name, column_map, client, batch):
    for message in messages:
        try:
            message_value = json.loads(message.value().decode('utf-8'))
            logger.info(f"Received message: {json.dumps(message_value, indent=2)}")  # Log the entire message

            payload = message_value.get('payload', {})
            op_type = payload.get('__op', 'unknown')

            if op_type == 'u':
                logger.info("Skipping update message")
                continue
            elif op_type == 'd':
                logger.info("Skipping delete message")
                continue
            else:
                logger.info(f"Processing {op_type} message")

            converted_payload = {}
            for field, (field_type, column_name) in column_map.items():
                value = payload.get(field, None)
                if value is not None:
                    converted_payload[column_name] = conversion_functions[field_type](value)
                else:
                    converted_payload[column_name] = None

            converted_payload['day_id'] = generate_day_id()
            batch.append(tuple(converted_payload.values()))

            logger.info(f"Processed message: {payload}")

        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Error processing message: {e}")
            continue

    if len(batch) >= BATCH_SIZE:
        write_batch_to_clickhouse(batch, table_name, client)

def write_batch_to_clickhouse(batch, table_name, client):
    if batch:
        try:
            client.execute(f"INSERT INTO {table_name} VALUES", batch)
            logger.info(f"Inserted {len(batch)} rows into {table_name}")
            batch.clear()
        except Exception as e:
            logger.error(f"Failed to insert data into ClickHouse: {e}")

def main(topic, table_name, column_map, consumer_group):
    consumer_conf = {
        'bootstrap.servers': 'kafka-broker:9092,kafka-broker-2:9093',
        'group.id': consumer_group,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    client = connect_clickhouse()

    logger.info(f"Starting consumer loop for topic: {topic}")

    batch = []
    last_batch_time = datetime.now()

    while True:
        messages = consumer.poll(timeout=1.0)
        if messages:
            if not isinstance(messages, list):
                messages = [messages]
            process_messages(messages, table_name, column_map, client, batch)
            consumer.commit(asynchronous=True)

        # Check if batch timeout has been reached
        if (datetime.now() - last_batch_time).seconds >= BATCH_TIMEOUT:
            write_batch_to_clickhouse(batch, table_name, client)
            last_batch_time = datetime.now()

    consumer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka to ClickHouse consumer")
    parser.add_argument("topic", help="Kafka topic to consume from")
    parser.add_argument("table_name", help="ClickHouse table name to insert data into")
    parser.add_argument("consumer_group", help="Kafka consumer group")
    args = parser.parse_args()

    schema_map = {
        'ecommerce_.ecommerce.customers': {
            'customer_id': ('int32', 'customer_id'),
            'name': ('string', 'name'),
            'email': ('string', 'email'),
            'phone': ('string', 'phone'),
            'address': ('string', 'address'),
            'gender': ('string', 'gender'),
            'created_at': ('int64', 'created_at')
        },
        'ecommerce_.ecommerce.order_items': {
            'order_item_id': ('int32', 'order_item_id'),
            'order_id': ('int32', 'order_id'),
            'product_id': ('int32', 'product_id'),
            'quantity': ('int32', 'quantity'),
            'unit_price': ('bytes', 'unit_price'),
            'created_at': ('int64', 'created_at')
        },
        'ecommerce_.ecommerce.orders': {
            'order_id': ('int32', 'order_id'),
            'customer_id': ('int32', 'customer_id'),
            'created_at': ('int64', 'created_at')
        },
        'ecommerce_.ecommerce.products': {
            'product_id': ('int32', 'product_id'),
            'name': ('string', 'name'),
            'description': ('string', 'description'),
            'category': ('string', 'category'),
            'price': ('bytes', 'price'),
            'seller_id': ('int32', 'seller_id'),
            'seller_name': ('string', 'seller_name'),
            'created_at': ('int64', 'created_at')
        }
    }

    if args.topic not in schema_map:
        logger.error(f"Unknown topic: {args.topic}")
        sys.exit(1)

    column_map = schema_map[args.topic]
    main(args.topic, args.table_name, column_map, args.consumer_group)
