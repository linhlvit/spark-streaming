"""
config.py — Tập trung toàn bộ cấu hình của ứng dụng.
Các module khác chỉ import từ đây, không hardcode giá trị.
"""

from pyspark.sql.types import StructType, StructField, StringType

# ─────────────────────────────────────────────
# KAFKA
# ─────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_PREFIX            = "oracle.FSS_STREAM"

# ─────────────────────────────────────────────
# ORACLE SOURCE (CDC)
# ─────────────────────────────────────────────
ORACLE_HOST     = "192.168.26.180"
ORACLE_PORT     = 1521
ORACLE_SERVICE  = "dbpdb"
ORACLE_USER     = "FSS_STREAM"
ORACLE_PASSWORD = "FSS_STREAM"
ORACLE_DSN      = f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"

TARGET_SCHEMA   = "FSS_STREAM"

# ─────────────────────────────────────────────
# SPARK / CHECKPOINT
# ─────────────────────────────────────────────
CHECKPOINT_BASE = "/opt/spark/checkpoints/oracle_schema_sync"
APP_NAME        = "oracle-cdc-schema-sync"
SPARK_PACKAGES  = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"

TRIGGER_INTERVAL        = "30 seconds"
MAX_OFFSETS_PER_TRIGGER = 10000

# ─────────────────────────────────────────────
# SQL SCHEMA FILE
# ─────────────────────────────────────────────
SQL_FILE_PATH = "/opt/spark/jobs/sql/create_target_tables.sql"

# ─────────────────────────────────────────────
# TABLES TO SYNC
# ─────────────────────────────────────────────
TABLES = [
    "T24_TRANSACTIONS",
    "T24_ACCOUNT",
    "T24_CUSTOMER",
    "T24_BRANCH",
    # thêm bảng mới vào đây
]

# ─────────────────────────────────────────────
# DEBEZIUM ENVELOPE SCHEMA
# ─────────────────────────────────────────────
DEBEZIUM_ENVELOPE_SCHEMA = StructType([
    StructField("op",     StringType(), True),
    StructField("before", StringType(), True),
    StructField("after",  StringType(), True),
    StructField("source", StringType(), True),
    StructField("ts_ms",  StringType(), True),
])

# ─────────────────────────────────────────────
# ORACLE TYPE GROUPS
# ─────────────────────────────────────────────
DATE_TYPES   = {"DATE", "TIMESTAMP"}
NUMBER_TYPES = {"NUMBER", "FLOAT", "INTEGER", "INT", "DECIMAL", "NUMERIC"}
STRING_TYPES = {"VARCHAR2", "VARCHAR", "CHAR", "NVARCHAR2", "NCHAR", "CLOB"}
