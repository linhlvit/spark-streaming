"""
Spark Structured Streaming — Kafka (Debezium) → Oracle TARGET
Reads from  : oracle.LPB_POC.{TABLE}  (Kafka topics)
Writes to   : LPB_POC.{TABLE}_TARGET  (Oracle via python-oracledb)

Schema (column types + PK) được parse tự động từ file create_target_tables.sql.
Không cần khai báo thủ công — chỉ cần tên bảng trong TABLES.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType
import re
import json
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_PREFIX            = "oracle.LPB_POC"
TARGET_SCHEMA           = "LPB_POC"
CHECKPOINT_BASE         = "/opt/spark/checkpoints/oracle_schema_sync"

ORACLE_HOST     = "192.168.26.180"
ORACLE_PORT     = 1521
ORACLE_SERVICE  = "dbpdb"
ORACLE_USER     = "LPB_POC"
ORACLE_PASSWORD = "LPB_POC"

# Đường dẫn file SQL (mount vào container cùng thư mục jobs)
SQL_FILE_PATH = "/opt/spark/jobs/create_target_tables.sql"

# Chỉ cần tên bảng — PK và column types lấy từ SQL file
TABLES = [
    "T24_TRANSACTIONS",
    "T24_ACCOUNT",
    "T24_CUSTOMER",
    "T24_BRANCH", 
    # thêm bảng mới vào đây
]

# Debezium envelope
DEBEZIUM_ENVELOPE_SCHEMA = StructType([
    StructField("op",     StringType(), True),
    StructField("before", StringType(), True),
    StructField("after",  StringType(), True),
    StructField("source", StringType(), True),
    StructField("ts_ms",  StringType(), True),
])

# Map Oracle type → nhóm xử lý
DATE_TYPES    = {"DATE", "TIMESTAMP"}
NUMBER_TYPES  = {"NUMBER", "FLOAT", "INTEGER", "INT", "DECIMAL", "NUMERIC"}
STRING_TYPES  = {"VARCHAR2", "VARCHAR", "CHAR", "NVARCHAR2", "NCHAR", "CLOB"}


# ─────────────────────────────────────────────
# PARSE SQL FILE → TABLE METADATA
# ─────────────────────────────────────────────
def parse_sql_file(sql_file_path):
    """
    Parse file create_target_tables.sql để lấy:
    - columns: dict {col_name: oracle_type}  (ví dụ: {"ACCOUNT_ID": "VARCHAR2", "CREATED_AT": "TIMESTAMP"})
    - pk: tên cột primary key

    Trả về: dict { "TABLE_NAME": {"pk": "COL", "columns": {"COL": "TYPE", ...}} }
    """
    with open(sql_file_path, "r") as f:
        content = f.read()

    result = {}

    # Tìm từng block CREATE TABLE
    table_blocks = re.findall(
        r"CREATE\s+TABLE\s+\w+\.(\w+)\s*\((.*?)\);",
        content,
        re.IGNORECASE | re.DOTALL
    )

    for table_name, body in table_blocks:
        # Bỏ _TARGET suffix để map với tên bảng nguồn
        source_name = table_name.upper().replace("_TARGET", "")

        columns = {}
        pk      = None

        for line in body.split("\n"):
            line = line.strip().rstrip(",")
            if not line or line.startswith("--"):
                continue

            # Tìm PRIMARY KEY constraint
            pk_match = re.search(r"CONSTRAINT\s+\w+\s+PRIMARY\s+KEY\s*\((\w+)\)", line, re.IGNORECASE)
            if pk_match:
                pk = pk_match.group(1).upper()
                continue

            # Tìm column definition: COL_NAME TYPE[(precision)] [NOT NULL] ...
            col_match = re.match(r"(\w+)\s+(\w+)(\s*\([^)]*\))?", line)
            if col_match:
                col_name  = col_match.group(1).upper()
                col_type  = col_match.group(2).upper()
                # Bỏ qua các keyword không phải column
                if col_name in ("CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "FOREIGN"):
                    continue
                columns[col_name] = col_type

        result[source_name] = {"pk": pk, "columns": columns}
        logger.info(f"Parsed {source_name}: pk={pk}, cols={list(columns.keys())}")

    return result


# ─────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────
def build_spark_session():
    return (
        SparkSession.builder
        .appName("oracle-cdc-schema-sync")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
        )
        .getOrCreate()
    )


# ─────────────────────────────────────────────
# ORACLE WRITE
# ─────────────────────────────────────────────
def get_oracle_conn():
    import oracledb
    return oracledb.connect(
        user=ORACLE_USER,
        password=ORACLE_PASSWORD,
        dsn=f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}",
    )


def convert_value(col_name, value, col_type):
    """Convert giá trị theo đúng Oracle type từ SQL schema."""
    if value is None:
        return None

    base_type = col_type.upper()

    if base_type in DATE_TYPES:
        try:
            from datetime import datetime, timezone
            micros = int(float(value))
            return datetime.fromtimestamp(micros / 1_000_000, tz=timezone.utc).replace(tzinfo=None)
        except Exception:
            return value

    if base_type in NUMBER_TYPES:
        try:
            # Giữ nguyên float/int, Oracle tự xử lý precision
            f = float(value)
            return int(f) if f == int(f) else f
        except Exception:
            return value

    # VARCHAR2, CHAR, ... → string
    return str(value)


def build_merge_sql(target_table, columns, pk):
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


def upsert_rows(rows, target_table, pk, col_types):
    """col_types: dict {col_name: oracle_type}"""
    if not rows:
        return
    columns   = list(rows[0].keys())
    merge_sql = build_merge_sql(target_table, columns, pk)

    conn = get_oracle_conn()
    try:
        cursor = conn.cursor()
        batch = [
            {k: convert_value(k, v, col_types.get(k, "VARCHAR2")) for k, v in row.items()}
            for row in rows
        ]
        cursor.executemany(merge_sql, batch)
        conn.commit()
    finally:
        conn.close()


def delete_rows(rows, target_table, pk):
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
def process_table(spark, table_name, pk, col_types):
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
        .option("maxOffsetsPerTrigger", 10000)
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

    # 4. foreachBatch → convert đúng type theo schema SQL → ghi Oracle
    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        rows        = batch_df.collect()
        upsert_data = []
        delete_data = []

        for row in rows:
            op       = row["_op"]
            row_dict = json.loads(row["row_data"])
            if op in ("r", "c", "u"):
                upsert_data.append(row_dict)
            elif op == "d":
                delete_data.append(row_dict)

        if upsert_data:
            upsert_rows(upsert_data, target_table, pk, col_types)
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
        .option("maxOffsetsPerTrigger", 10000)
        .start()
    )


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Parse schema từ SQL file một lần lúc khởi động
    schema_map = parse_sql_file(SQL_FILE_PATH)

    queries = []
    for table_name in TABLES:
        meta = schema_map.get(table_name)
        if not meta:
            logger.error(f"Không tìm thấy schema cho bảng {table_name} trong {SQL_FILE_PATH}")
            continue
        pk        = meta["pk"]
        col_types = meta["columns"]
        q = process_table(spark, table_name, pk, col_types)
        queries.append(q)

    logger.info(f"Started {len(queries)} streaming queries.")

    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
