import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create a Spark session
spark = SparkSession.builder \
    .appName("Kafka to ClickHouse") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.streaming.receiver.writeAheadLog.enable", "true") \
    .getOrCreate()

# Define schemas for each table
schemas = {
    "customers": StructType([
        StructField("eventTime", TimestampType(), True),
        StructField("id", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("op", StringType(), True)
    ]),
    "order_items": StructType([
        StructField("eventTime", TimestampType(), True),
        StructField("id", StringType(), True),
        StructField("order_item_id", IntegerType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DecimalType(10, 2), True),
        StructField("created_at", TimestampType(), True),
        StructField("op", StringType(), True)
    ]),
    "orders": StructType([
        StructField("eventTime", TimestampType(), True),
        StructField("id", StringType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("op", StringType(), True)
    ]),
    "products": StructType([
        StructField("eventTime", TimestampType(), True),
        StructField("id", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DecimalType(10, 2), True),
        StructField("seller_id", IntegerType(), True),
        StructField("seller_name", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("op", StringType(), True)
    ])
}

# Function to log the error and the problematic row
def log_error(row, error):
    logger.error(f"Error processing row: {row.asDict()}, error: {error}")

# Define function to process each table
def process_table(topic_name, table_name, schema):
    # Read streaming data from Kafka
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse the JSON data and extract the fields
    parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*")

    # Filter out update and delete messages and apply watermarking
    filtered_stream = parsed_stream \
        .filter((col("op") != "u") & (col("op") != "d")) \
        .withWatermark("eventTime", "10 minutes") \
        .dropDuplicates(["id"])

    # Add the day_id column
    with_day_id = filtered_stream.withColumn("day_id", date_format(col("eventTime"), "yyyyMMdd"))

    # Define a function to convert timestamps with error handling
    def safe_convert_timestamps(df):
        try:
            return df.withColumn("created_at", to_timestamp(col("created_at")))
        except Exception as e:
            log_error(df, e)
            return None

    # Apply the timestamp conversion
    final_stream = safe_convert_timestamps(with_day_id)

    # Write the processed data to ClickHouse
    clickhouse_sink = final_stream.writeStream \
        .outputMode("append") \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse-server:8123/default") \
        .option("dbtable", table_name) \
        .option("user", "default") \
        .option("password", "") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("checkpointLocation", f"/path/to/checkpoint/dir/{table_name}") \
        .trigger(processingTime='1 minute') \
        .start()

    return clickhouse_sink

# Get the list of topics and target tables from the command-line arguments
if __name__ == "__main__":
    if len(sys.argv) < 3 or len(sys.argv) % 2 != 1:
        print("Usage: script.py <topic1> <table1> <topic2> <table2> ... <topicN> <tableN>")
        sys.exit(1)

    args = sys.argv[1:]
    topic_table_pairs = zip(args[::2], args[1::2])

    # Process each topic-table pair
    sinks = []
    for topic_name, table_name in topic_table_pairs:
        # Extract the base table name (e.g., "customers" from "ecommerce_customers")
        base_table_name = table_name.split("_")[1] if "_" in table_name else table_name

        if base_table_name in schemas:
            schema = schemas[base_table_name]
            sink = process_table(topic_name, table_name, schema)
            if sink:
                sinks.append(sink)
        else:
            logger.error(f"Schema not defined for table: {base_table_name}")

    # Await termination for all streams
    for sink in sinks:
        sink.awaitTermination()
