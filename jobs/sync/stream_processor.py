"""
stream_processor.py — Spark Structured Streaming pipeline per table.

Mỗi bảng chạy một streaming query độc lập:
    Kafka topic → parse Debezium envelope → foreachBatch → Oracle writer
"""

import json
import logging
from typing import Dict, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.streaming import StreamingQuery

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_PREFIX,
    CHECKPOINT_BASE,
    DEBEZIUM_ENVELOPE_SCHEMA,
    TRIGGER_INTERVAL,
    MAX_OFFSETS_PER_TRIGGER,
)
from core.oracle_writer import upsert_rows, delete_rows

logger = logging.getLogger(__name__)

# Cột timestamp dùng làm last-write-wins guard cho từng bảng.
# Chỉ UPDATE khi event mới hơn row đang có — chống out-of-order CDC.
# Bảng không có cột thời gian phù hợp → None (không guard, safe vì data ít thay đổi).
_EVENT_TS_COL: Dict[str, Optional[str]] = {
    "T24_TRANSACTIONS": "TRANSACTION_TIME",  # timestamp của giao dịch trong Oracle
    "T24_ACCOUNT":      "UPDATED_AT",        # timestamp cập nhật số dư
    "T24_CUSTOMER":     "UPDATED_AT",        # timestamp cập nhật thông tin KH
    "T24_BRANCH":       None,                # BRANCH thay đổi rất hiếm, không cần guard
}


# ─────────────────────────────────────────────
# KAFKA READER
# ─────────────────────────────────────────────
def _read_kafka_stream(spark: SparkSession, topic: str) -> DataFrame:
    """Đọc raw Kafka stream cho một topic."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )


# ─────────────────────────────────────────────
# DEBEZIUM PARSER
# ─────────────────────────────────────────────
def _parse_debezium(raw_df: DataFrame) -> DataFrame:
    """
    Parse Debezium CDC envelope từ Kafka value (JSON string).

    Output schema: _op STRING, row_data STRING (JSON của after/before)
    """
    envelope_df = (
        raw_df
        .select(
            from_json(col("value").cast("string"), DEBEZIUM_ENVELOPE_SCHEMA).alias("msg")
        )
        .select("msg.*")
        .filter(col("op").isNotNull())
    )

    return (
        envelope_df
        .withColumn(
            "row_data",
            when(col("op").isin("r", "c", "u"), col("after"))
            .otherwise(col("before"))
        )
        .withColumn("_op", col("op"))
        .select("_op", "row_data")
        .filter(col("row_data").isNotNull())
    )


# ─────────────────────────────────────────────
# BATCH WRITER (foreachBatch callback)
# ─────────────────────────────────────────────
def _make_batch_writer(table_name: str, target_table: str, pk: str, col_types: dict):
    """
    Factory trả về hàm write_batch dùng cho foreachBatch.
    Đóng gói table_name, pk, col_types vào closure.
    """
    event_ts_col = _EVENT_TS_COL.get(table_name)

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        upsert_data: List[Dict] = []
        delete_data: List[Dict] = []

        for row in batch_df.collect():
            op       = row["_op"]
            row_dict = json.loads(row["row_data"])
            if op in ("r", "c", "u"):
                upsert_data.append(row_dict)
            elif op == "d":
                delete_data.append(row_dict)

        if upsert_data:
            upsert_rows(upsert_data, target_table, pk, col_types, event_ts_col)
            logger.info(f"[{table_name}] batch={batch_id} upserted {len(upsert_data)} rows")

        if delete_data:
            delete_rows(delete_data, target_table, pk)
            logger.info(f"[{table_name}] batch={batch_id} deleted {len(delete_data)} rows")

    return write_batch


# ─────────────────────────────────────────────
# PUBLIC: START ONE TABLE STREAM
# ─────────────────────────────────────────────
def start_table_stream(
    spark: SparkSession,
    table_name: str,
    pk: str,
    col_types: Dict[str, str],
) -> StreamingQuery:
    """
    Khởi động streaming query cho một bảng.

    Args:
        spark      : SparkSession đang chạy.
        table_name : Tên bảng nguồn (ví dụ: T24_TRANSACTIONS).
        pk         : Tên cột primary key.
        col_types  : Dict {col_name: oracle_type} từ schema_parser.

    Returns:
        StreamingQuery — có thể gọi .awaitTermination() hoặc .stop().
    """
    topic        = f"{TOPIC_PREFIX}.{table_name}"
    target_table = f"{table_name}_TARGET"
    checkpoint   = f"{CHECKPOINT_BASE}/{table_name.lower()}"

    logger.info(f"Starting stream: {topic} → {target_table} (pk={pk})")

    raw_df    = _read_kafka_stream(spark, topic)
    parsed_df = _parse_debezium(raw_df)

    return (
        parsed_df.writeStream
        .foreachBatch(_make_batch_writer(table_name, target_table, pk, col_types))
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
