"""
Spark Streaming Job — Kafka → Console Output
Nhận message từ Kafka topic về Customer (cus_id, cus_name) và in ra console

Requirements:
- PySpark 3.5.5
- Kafka broker: localhost:9092
- Topic: customer (cần tạo trước)

Usage:
    spark-submit \
      --master spark://localhost:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
      spark_kafka_streaming.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, schema_of_json
from pyspark.sql.types import StructType, StructField, StringType
import json
import sys

def create_spark_session(app_name="KafkaStreamingApp"):
    """Tạo SparkSession kết nối tới Spark master"""
    spark = SparkSession \
        .builder \
        .appName(app_name) \
        .master("spark://spark-master:7077") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✓ SparkSession created successfully")
    return spark


def create_kafka_stream(spark, brokers, topic):
    """
    Tạo Kafka streaming DataFrame
    
    Args:
        spark: SparkSession
        brokers: Kafka broker address (e.g., "kafka:9092")
        topic: Kafka topic name (e.g., "customer")
    
    Returns:
        DataFrame từ Kafka với columns: key, value, timestamp, etc.
    """
    try:
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", brokers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        print(f"✓ Kafka stream created for topic: {topic}")
        return df
    except Exception as e:
        print(f"✗ Error creating Kafka stream: {e}")
        sys.exit(1)


def parse_t24_account_message(df):
    """
    Parse Debezium CDC message từ topic oracle.FSS_STREAM.T24_ACCOUNT

    Debezium envelope (schemas.enable=false):
    {
        "op": "r|c|u|d",
        "before": "{...}",   -- JSON string, null với insert/snapshot
        "after":  "{...}",   -- JSON string, null với delete
        "source": "{...}",
        "ts_ms":  "..."
    }

    after/before chứa các cột của T24_ACCOUNT:
        ACCOUNT_ID, CUSTOMER_ID, WORKING_BALANCE, ONLINE_ACTUAL_BAL,
        CURRENCY_CODE, BRANCH_CODE, LAST_TX_TIME, EVENT_TIME,
        CREATED_AT, UPDATED_AT
    """
    from pyspark.sql.functions import when

    # Debezium envelope schema
    envelope_schema = StructType([
        StructField("op",     StringType(), True),
        StructField("before", StringType(), True),
        StructField("after",  StringType(), True),
        StructField("source", StringType(), True),
        StructField("ts_ms",  StringType(), True),
    ])

    # T24_ACCOUNT row schema
    account_schema = StructType([
        StructField("ACCOUNT_ID",       StringType(), True),
        StructField("CUSTOMER_ID",      StringType(), True),
        StructField("WORKING_BALANCE",  StringType(), True),
        StructField("ONLINE_ACTUAL_BAL",StringType(), True),
        StructField("CURRENCY_CODE",    StringType(), True),
        StructField("BRANCH_CODE",      StringType(), True),
        StructField("LAST_TX_TIME",     StringType(), True),
        StructField("EVENT_TIME",       StringType(), True),
        StructField("CREATED_AT",       StringType(), True),
        StructField("UPDATED_AT",       StringType(), True),
    ])

    # Parse envelope
    df_envelope = df.select(
        col("timestamp").alias("kafka_timestamp"),
        col("offset"),
        col("partition"),
        from_json(col("value").cast("string"), envelope_schema).alias("msg")
    ).select(
        "kafka_timestamp", "offset", "partition",
        col("msg.op").alias("op"),
        # lấy after cho r/c/u, before cho d
        when(col("msg.op").isin("r", "c", "u"), col("msg.after"))
        .otherwise(col("msg.before")).alias("row_data")
    ).filter(col("row_data").isNotNull())

    # Parse row_data → flat columns
    df_parsed = df_envelope.select(
        "kafka_timestamp", "offset", "partition", "op",
        from_json(col("row_data"), account_schema).alias("account")
    ).select(
        "kafka_timestamp", "offset", "partition", "op",
        col("account.ACCOUNT_ID"),
        col("account.CUSTOMER_ID"),
        col("account.WORKING_BALANCE"),
        col("account.ONLINE_ACTUAL_BAL"),
        col("account.CURRENCY_CODE"),
        col("account.BRANCH_CODE"),
        col("account.LAST_TX_TIME"),
        col("account.EVENT_TIME"),
        col("account.CREATED_AT"),
        col("account.UPDATED_AT"),
    )

    return df_parsed


def main():
    """Main function — orchestrate streaming pipeline"""
    
    # Configuration
    KAFKA_BROKERS = "kafka:29092"
    KAFKA_TOPIC = "oracle.FSS_STREAM.T24_ACCOUNT"
    TRIGGER_INTERVAL = "5 seconds"  # Batch interval
    
    print("\n" + "="*60)
    print("Spark Streaming — Kafka Consumer")
    print("="*60)
    print(f"Kafka Brokers: {KAFKA_BROKERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Trigger Interval: {TRIGGER_INTERVAL}")
    print("="*60 + "\n")
    
    # Step 1: Create SparkSession
    spark = create_spark_session("KafkaCustomerStreaming")
    
    # Step 2: Create Kafka stream
    df_kafka = create_kafka_stream(spark, KAFKA_BROKERS, KAFKA_TOPIC)
    
    # Step 3: Parse Debezium CDC messages
    df_parsed = parse_t24_account_message(df_kafka)
    df_parsed.printSchema()  # Kiểm tra schema sau khi parse
    # Step 4: Define output sink — console output
    # Mode: append (chỉ output new data) hoặc complete (tất cả data mỗi batch)
    query = df_parsed \
        .writeStream \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 20) \
        .trigger(processingTime=TRIGGER_INTERVAL) \
        .start()
    
    print("\n✓ Streaming query started!")
    print(f"Waiting for messages from Kafka topic '{KAFKA_TOPIC}'...")
    print("(Press Ctrl+C to stop)\n")
    
    # Step 5: Chờ cho đến khi user interrupt
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\nShutting down gracefully...")
        query.stop()
        print("✓ Streaming query stopped")


if __name__ == "__main__":
    main()
