"""
Spark Structured Streaming — Kafka (Debezium) → Oracle TARGET
Reads from  : oracle.FSS_STREAM.{TABLE}  (Kafka topics)
Writes to   : FSS_STREAM.{TABLE}_TARGET  (Oracle via python-oracledb)

Config mỗi bảng chỉ cần: tên bảng + primary key.
Schema parse động từ JSON trong từng batch.
Ghi Oracle dùng python-oracledb (thin mode) — không cần Oracle Client.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_PREFIX            = "oracle.FSS_STREAM"
TARGET_SCHEMA           = "FSS_STREAM"
CHECKPOINT_BASE         = "/opt/spark/checkpoints/oracle_sync"

ORACLE_HOST     = "192.168.26.180"
ORACLE_PORT     = 1521
ORACLE_SERVICE  = "dbpdb"
ORACLE_USER     = "FSS_STREAM"
ORACLE_PASSWORD = "FSS_STREAM"       # TODO: thay password thực tế

# Chỉ cần khai báo tên bảng và primary key
TABLES = [
    {"table": "T24_TRANSACTIONS", "pk": "TRANSACTION_ID"},
    {"table": "T24_ACCOUNT",      "pk": "ACCOUNT_ID"},
    {"table": "T24_CUSTOMER",     "pk": "CUSTOMER_ID"},
    # thêm bảng mới vào đây
    # {"table": "T24_BRANCH", "pk": "BRANCH_CODE"},
]

# Debezium envelope
DEBEZIUM_ENVELOPE_SCHEMA = StructType([
    StructField("op",     StringType(), True),
    StructField("before", StringType(), True),
    StructField("after",  StringType(), True),
    StructField("source", StringType(), True),
    StructField("ts_ms",  StringType(), True),
])


# ─────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────
def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("oracle-cdc-sync")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
        )
        .getOrCreate()
    )


# ─────────────────────────────────────────────
# ORACLE WRITE — dùng python-oracledb thuần
# chạy trên driver (trong foreachBatch)
# ─────────────────────────────────────────────
def get_oracle_conn():
    import oracledb
    return oracledb.connect(
        user=ORACLE_USER,
        password=ORACLE_PASSWORD,
        dsn=f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}",
    )


# Các suffix/keyword nhận biết cột date/timestamp
DATE_COL_KEYWORDS = ("_DATE", "_TIME", "_AT", "DATE", "TIME")


def is_date_col(col_name):
    upper = col_name.upper()
    return any(upper.endswith(k) or upper == k for k in DATE_COL_KEYWORDS)


def col_expr_for_select(col_name):
    """Cột date/timestamp giữ nguyên tên, convert ở Python trước khi bind."""
    return f":{col_name} AS {col_name}"


def convert_value(col_name, value):
    """Convert epoch microseconds → datetime cho cột date/timestamp."""
    if value is None:
        return None
    if is_date_col(col_name):
        try:
            from datetime import datetime, timezone
            micros = int(float(value))
            return datetime.fromtimestamp(micros / 1_000_000, tz=timezone.utc).replace(tzinfo=None)
        except Exception:
            return value
    return str(value)


def build_merge_sql(target_table: str, columns: list, pk: str) -> str:
    full_table    = f"{TARGET_SCHEMA}.{target_table}"
    update_cols   = [c for c in columns if c != pk]
    select_clause = ", ".join([f":{c} AS {c}" for c in columns])
    update_clause = ", ".join([f"t.{c} = s.{c}" for c in update_cols])
    insert_cols   = ", ".join(columns)
    insert_vals   = ", ".join([f"s.{c}" for c in columns])

    return f"""
        MERGE INTO {full_table} t
        USING (SELECT {select_clause} FROM DUAL) s
        ON (t.{pk} = s.{pk})
        WHEN MATCHED THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
    """


def upsert_rows(rows, target_table: str, pk: str) -> None:
    if not rows:
        return
    columns   = list(rows[0].keys())
    merge_sql = build_merge_sql(target_table, columns, pk)

    conn = get_oracle_conn()
    try:
        cursor = conn.cursor()
        batch  = [{k: convert_value(k, v) for k, v in row.items()} for row in rows]
        cursor.executemany(merge_sql, batch)
        conn.commit()
    finally:
        conn.close()


def delete_rows(rows, target_table: str, pk: str) -> None:
    if not rows:
        return
    full_table = f"{TARGET_SCHEMA}.{target_table}"
    delete_sql = f"DELETE FROM {full_table} WHERE {pk} = :{pk}"

    conn = get_oracle_conn()
    try:
        cursor = conn.cursor()
        batch  = [{pk: str(row[pk])} for row in rows if row.get(pk)]
        cursor.executemany(delete_sql, batch)
        conn.commit()
    finally:
        conn.close()


# ─────────────────────────────────────────────
# STREAM PER TABLE
# ─────────────────────────────────────────────
def process_table(spark: SparkSession, table_name: str, pk: str):
    topic        = f"{TOPIC_PREFIX}.{table_name}"
    target_table = f"{table_name}_TARGET"
    checkpoint   = f"{CHECKPOINT_BASE}/{table_name.lower()}"

    logger.info(f"Starting stream: {topic} -> {TARGET_SCHEMA}.{target_table} (pk={pk})")

    # 1. Đọc raw Kafka stream
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Parse Debezium envelope
    envelope_df = (
        raw_df
        .select(
            from_json(col("value").cast("string"), DEBEZIUM_ENVELOPE_SCHEMA).alias("msg")
        )
        .select("msg.*")
        .filter(col("op").isNotNull())
    )

    # 3. Chọn after (upsert) hoặc before (delete)
    parsed_df = (
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

    # 4. foreachBatch — collect về driver rồi ghi Oracle bằng python-oracledb
    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # Collect toàn bộ batch về driver
        rows = batch_df.collect()

        upsert_data = []
        delete_data = []

        for row in rows:
            op       = row["_op"]
            row_dict = json.loads(row["row_data"])
            if op in ("r", "c", "u"):
                upsert_data.append(row_dict)
            elif op == "d":
                delete_data.append(row_dict)

        # Log sample để debug format date
        if upsert_data:
            logger.info(f"[{table_name}] sample row: {upsert_data[0]}")

        if upsert_data:
            upsert_rows(upsert_data, target_table, pk)
            logger.info(f"[{table_name}] batch={batch_id} upserted {len(upsert_data)} rows")

        if delete_data:
            delete_rows(delete_data, target_table, pk)
            logger.info(f"[{table_name}] batch={batch_id} deleted {len(delete_data)} rows")

    # 5. Start stream
    return (
        parsed_df.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="30 seconds")
        .start()
    )


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    queries = [
        process_table(spark, cfg["table"], cfg["pk"])
        for cfg in TABLES
    ]
    logger.info(f"Started {len(queries)} streaming queries.")

    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
