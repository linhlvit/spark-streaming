"""
txn_acct_join.py — Spark Stream-Stream Join (Transaction × Account)
    Stream A : Kafka oracle.FSS_STREAM.T24_TRANSACTIONS  (Debezium CDC)
    Stream B : Kafka oracle.FSS_STREAM.T24_ACCOUNT       (Debezium CDC)

Mục đích:
    Ghi nhận trạng thái số dư tài khoản tại thời điểm xảy ra giao dịch.
    Đây là use-case điển hình cho stream-stream join vì cả hai bên đều
    thay đổi cùng lúc khi có giao dịch mới — khác với CUSTOMER (dimension ít thay đổi).

Output : T24_TXN_ACCOUNT_SNAPSHOT — giao dịch + snapshot số dư
DLQ    : T24_TXN_ACCT_PENDING_JOIN — TXN chưa join được ACCOUNT trong window ±30'
           ERROR_REASON = TXN_NO_ACCOUNT_MATCH

Watermark: kafka.timestamp (broker timestamp) — tránh evict sớm khi replay từ earliest.

Production note:
    Trước khi bật job này lần đầu, chạy batch bootstrap:
        python3 tools/bootstrap_txn_acct.py
    Sau đó bật streaming với startingOffsets=latest.
    Xem DESIGN.md §"Cold Start / Batch Bootstrap" để biết lý do.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import oracledb
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType,
)

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_PREFIX,
    CHECKPOINT_BASE,
    DEBEZIUM_ENVELOPE_SCHEMA,
    TRIGGER_INTERVAL,
    MAX_OFFSETS_PER_TRIGGER,
    ORACLE_DSN,
    ORACLE_USER,
    ORACLE_PASSWORD,
    TARGET_SCHEMA,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
TXN_TOPIC         = f"{TOPIC_PREFIX}.T24_TRANSACTIONS"
ACCT_TOPIC        = f"{TOPIC_PREFIX}.T24_ACCOUNT"
SNAPSHOT_TABLE    = f"{TARGET_SCHEMA}.T24_TXN_ACCOUNT_SNAPSHOT"
PENDING_TABLE     = f"{TARGET_SCHEMA}.T24_TXN_ACCT_PENDING_JOIN"
ACCT_TARGET       = f"{TARGET_SCHEMA}.T24_ACCOUNT_TARGET"
CHECKPOINT_JOIN   = f"{CHECKPOINT_BASE}/txn_acct_join/main"
CHECKPOINT_DELETE = f"{CHECKPOINT_BASE}/txn_acct_join/delete"

WATERMARK_DELAY = "30 minutes"
MAX_RETRY       = 5

STATUS_PENDING  = "PENDING"
STATUS_RESOLVED = "RESOLVED"
STATUS_FAILED   = "FAILED"

# DLQ error codes
EC_NO_ACCOUNT_MATCH = "TXN_NO_ACCOUNT_MATCH"

# Exponential backoff schedule cho stream_join DLQ (tính bằng phút).
# Base interval 10 phút — ACCOUNT_TARGET sync lag thường vài phút,
# nhưng watermark window ±30' nghĩa là TXN đã chờ ít nhất 30' trước khi vào DLQ.
BACKOFF_MINUTES_STREAM: List[int] = [10, 30, 120, 360, 1440]
#  retry 0 → +10'   (10 phút sau khi vào DLQ — ACCOUNT_TARGET có thể đã có)
#  retry 1 → +30'   (30 phút — đủ thời gian để sync job cập nhật)
#  retry 2 → +120'  (2 giờ — ACCOUNT có thể đang trong quá trình tạo)
#  retry 3 → +360'  (6 giờ — khả năng cao ACCOUNT là orphan)
#  retry 4 → +1440' (24 giờ — lần cuối trước khi FAILED)

# ─────────────────────────────────────────────
# PAYLOAD SCHEMAS
# ─────────────────────────────────────────────
TXN_SCHEMA = StructType([
    StructField("TRANSACTION_ID",     StringType(), True),
    StructField("ACCOUNT_ID",         StringType(), True),
    StructField("CUSTOMER_ID",        StringType(), True),
    StructField("TRANSACTION_DATE",   LongType(),   True),
    StructField("VALUE_DATE",         LongType(),   True),
    StructField("TRANSACTION_TIME",   LongType(),   True),
    StructField("TRANSACTION_TYPE",   StringType(), True),
    StructField("AMOUNT",             DoubleType(), True),
    StructField("CURRENCY_CODE",      StringType(), True),
    StructField("CHANNEL",            StringType(), True),
    StructField("BRANCH_CODE",        StringType(), True),
    StructField("REFERENCE_NO",       StringType(), True),
    StructField("TRANSACTION_STATUS", StringType(), True),
])

ACCT_SCHEMA = StructType([
    StructField("ACCOUNT_ID",        StringType(), True),
    StructField("CUSTOMER_ID",       StringType(), True),
    StructField("WORKING_BALANCE",   DoubleType(), True),
    StructField("ONLINE_ACTUAL_BAL", DoubleType(), True),
    StructField("CURRENCY_CODE",     StringType(), True),
    StructField("BRANCH_CODE",       StringType(), True),
    StructField("EVENT_TIME",        LongType(),   True),
])


# ─────────────────────────────────────────────
# ORACLE HELPERS
# ─────────────────────────────────────────────
def _get_conn() -> oracledb.Connection:
    return oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)


def _epoch_ms_to_dt(ms) -> Optional[datetime]:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).replace(tzinfo=None)
    except Exception:
        return None


def _next_retry_at(retry_count: int, schedule: List[int] = BACKOFF_MINUTES_STREAM) -> datetime:
    """
    Tính thời điểm được phép retry tiếp theo theo backoff schedule.
    retry_count: số lần đã retry (0 = lần đầu, chưa retry lần nào).
    """
    from datetime import timedelta
    idx     = min(retry_count, len(schedule) - 1)
    minutes = schedule[idx]
    return datetime.utcnow() + timedelta(minutes=minutes)


# ─────────────────────────────────────────────
# SQL
# ─────────────────────────────────────────────
_SNAPSHOT_MERGE = f"""
    MERGE INTO {SNAPSHOT_TABLE} t
    USING (SELECT
        :TRANSACTION_ID         AS TRANSACTION_ID,
        :ACCOUNT_ID             AS ACCOUNT_ID,
        :CUSTOMER_ID            AS CUSTOMER_ID,
        :TRANSACTION_DATE       AS TRANSACTION_DATE,
        :VALUE_DATE             AS VALUE_DATE,
        :TRANSACTION_TIME       AS TRANSACTION_TIME,
        :TRANSACTION_TYPE       AS TRANSACTION_TYPE,
        :AMOUNT                 AS AMOUNT,
        :CURRENCY_CODE          AS CURRENCY_CODE,
        :CHANNEL                AS CHANNEL,
        :BRANCH_CODE            AS BRANCH_CODE,
        :REFERENCE_NO           AS REFERENCE_NO,
        :TRANSACTION_STATUS     AS TRANSACTION_STATUS,
        :BALANCE_AT_TXN         AS BALANCE_AT_TXN,
        :WORKING_BALANCE_AT_TXN AS WORKING_BALANCE_AT_TXN,
        :ACCT_CURRENCY_CODE     AS ACCT_CURRENCY_CODE,
        :TXN_TS_MS                  AS TXN_TS_MS,
        :ACCT_TS_MS                 AS ACCT_TS_MS,
        :BALANCE_IS_APPROXIMATE     AS BALANCE_IS_APPROXIMATE
    FROM DUAL) s
    ON (t.TRANSACTION_ID = s.TRANSACTION_ID)
    WHEN MATCHED THEN UPDATE SET
        t.BALANCE_AT_TXN=s.BALANCE_AT_TXN,
        t.WORKING_BALANCE_AT_TXN=s.WORKING_BALANCE_AT_TXN,
        t.ACCT_TS_MS=s.ACCT_TS_MS,
        t.BALANCE_IS_APPROXIMATE=s.BALANCE_IS_APPROXIMATE,
        t.SNAPSHOT_AT=SYSTIMESTAMP
    WHEN NOT MATCHED THEN INSERT (
        TRANSACTION_ID, ACCOUNT_ID, CUSTOMER_ID,
        TRANSACTION_DATE, VALUE_DATE, TRANSACTION_TIME,
        TRANSACTION_TYPE, AMOUNT, CURRENCY_CODE, CHANNEL,
        BRANCH_CODE, REFERENCE_NO, TRANSACTION_STATUS,
        BALANCE_AT_TXN, WORKING_BALANCE_AT_TXN, ACCT_CURRENCY_CODE,
        TXN_TS_MS, ACCT_TS_MS, BALANCE_IS_APPROXIMATE
    ) VALUES (
        s.TRANSACTION_ID, s.ACCOUNT_ID, s.CUSTOMER_ID,
        s.TRANSACTION_DATE, s.VALUE_DATE, s.TRANSACTION_TIME,
        s.TRANSACTION_TYPE, s.AMOUNT, s.CURRENCY_CODE, s.CHANNEL,
        s.BRANCH_CODE, s.REFERENCE_NO, s.TRANSACTION_STATUS,
        s.BALANCE_AT_TXN, s.WORKING_BALANCE_AT_TXN, s.ACCT_CURRENCY_CODE,
        s.TXN_TS_MS, s.ACCT_TS_MS, s.BALANCE_IS_APPROXIMATE
    )
"""

# Idempotent: chỉ INSERT nếu chưa tồn tại
# NEXT_RETRY_AT: thời điểm được phép retry lần đầu (= now + backoff[0] = +10')
_PENDING_INSERT = f"""
    MERGE INTO {PENDING_TABLE} t
    USING (SELECT :TRANSACTION_ID AS TRANSACTION_ID FROM DUAL) s
    ON (t.TRANSACTION_ID = s.TRANSACTION_ID)
    WHEN NOT MATCHED THEN INSERT (
        TRANSACTION_ID, ACCOUNT_ID, CUSTOMER_ID,
        TRANSACTION_DATE, VALUE_DATE, TRANSACTION_TIME,
        TRANSACTION_TYPE, AMOUNT, CURRENCY_CODE, CHANNEL,
        BRANCH_CODE, REFERENCE_NO, TRANSACTION_STATUS,
        TXN_TS_MS, PENDING_SINCE, RETRY_COUNT, NEXT_RETRY_AT, STATUS, ERROR_CODE, ERROR_REASON
    ) VALUES (
        :TRANSACTION_ID, :ACCOUNT_ID, :CUSTOMER_ID,
        :TRANSACTION_DATE, :VALUE_DATE, :TRANSACTION_TIME,
        :TRANSACTION_TYPE, :AMOUNT, :CURRENCY_CODE, :CHANNEL,
        :BRANCH_CODE, :REFERENCE_NO, :TRANSACTION_STATUS,
        :TXN_TS_MS, SYSTIMESTAMP, 0, :NEXT_RETRY_AT, :STATUS, :ERROR_CODE, :ERROR_REASON
    )
"""

_PENDING_RESOLVE = f"""
    UPDATE {PENDING_TABLE}
    SET STATUS='{STATUS_RESOLVED}', RESOLVED_AT=SYSTIMESTAMP
    WHERE TRANSACTION_ID = :TRANSACTION_ID
"""

_PENDING_FAIL = f"""
    UPDATE {PENDING_TABLE}
    SET STATUS='{STATUS_FAILED}', RETRY_COUNT=RETRY_COUNT+1, RESOLVED_AT=SYSTIMESTAMP
    WHERE TRANSACTION_ID = :TRANSACTION_ID
"""

_PENDING_RETRY_INC = f"""
    UPDATE {PENDING_TABLE}
    SET RETRY_COUNT   = RETRY_COUNT + 1,
        NEXT_RETRY_AT = :NEXT_RETRY_AT
    WHERE TRANSACTION_ID = :TRANSACTION_ID
"""


# ─────────────────────────────────────────────
# BIND HELPERS
# ─────────────────────────────────────────────
def _to_snapshot_bind(txn: Dict, acct: Dict, txn_ts_ms, acct_ts_ms, approximate: bool = False) -> Dict:
    return {
        "TRANSACTION_ID":         txn.get("TRANSACTION_ID"),
        "ACCOUNT_ID":             txn.get("ACCOUNT_ID"),
        "CUSTOMER_ID":            txn.get("CUSTOMER_ID"),
        "TRANSACTION_DATE":       _epoch_ms_to_dt(txn.get("TRANSACTION_DATE")),
        "VALUE_DATE":             _epoch_ms_to_dt(txn.get("VALUE_DATE")),
        "TRANSACTION_TIME":       _epoch_ms_to_dt(txn.get("TRANSACTION_TIME")),
        "TRANSACTION_TYPE":       txn.get("TRANSACTION_TYPE"),
        "AMOUNT":                 txn.get("AMOUNT"),
        "CURRENCY_CODE":          txn.get("CURRENCY_CODE"),
        "CHANNEL":                txn.get("CHANNEL"),
        "BRANCH_CODE":            txn.get("BRANCH_CODE"),
        "REFERENCE_NO":           txn.get("REFERENCE_NO"),
        "TRANSACTION_STATUS":     txn.get("TRANSACTION_STATUS"),
        "BALANCE_AT_TXN":         acct.get("ONLINE_ACTUAL_BAL"),
        "WORKING_BALANCE_AT_TXN": acct.get("WORKING_BALANCE"),
        "ACCT_CURRENCY_CODE":     acct.get("CURRENCY_CODE"),
        "TXN_TS_MS":              txn_ts_ms,
        "ACCT_TS_MS":             acct_ts_ms,
        "BALANCE_IS_APPROXIMATE": 1 if approximate else 0,
    }


def _to_pending_bind(txn: Dict, txn_ts_ms) -> Dict:
    account_id = txn.get("ACCOUNT_ID")
    return {
        "TRANSACTION_ID":     txn.get("TRANSACTION_ID"),
        "ACCOUNT_ID":         account_id,
        "CUSTOMER_ID":        txn.get("CUSTOMER_ID"),
        "TRANSACTION_DATE":   _epoch_ms_to_dt(txn.get("TRANSACTION_DATE")),
        "VALUE_DATE":         _epoch_ms_to_dt(txn.get("VALUE_DATE")),
        "TRANSACTION_TIME":   _epoch_ms_to_dt(txn.get("TRANSACTION_TIME")),
        "TRANSACTION_TYPE":   txn.get("TRANSACTION_TYPE"),
        "AMOUNT":             txn.get("AMOUNT"),
        "CURRENCY_CODE":      txn.get("CURRENCY_CODE"),
        "CHANNEL":            txn.get("CHANNEL"),
        "BRANCH_CODE":        txn.get("BRANCH_CODE"),
        "REFERENCE_NO":       txn.get("REFERENCE_NO"),
        "TRANSACTION_STATUS": txn.get("TRANSACTION_STATUS"),
        "TXN_TS_MS":          txn_ts_ms,
        "STATUS":             STATUS_PENDING,
        "ERROR_CODE":         EC_NO_ACCOUNT_MATCH,
        "ERROR_REASON":       f"account_id='{account_id}' not synced within watermark window ±30min",
        "NEXT_RETRY_AT":      _next_retry_at(retry_count=0),
    }


# ─────────────────────────────────────────────
# WRITE HELPERS
# ─────────────────────────────────────────────
def _write_snapshot(rows: List[Dict]) -> None:
    if not rows:
        return
    conn = _get_conn()
    try:
        conn.cursor().executemany(_SNAPSHOT_MERGE, rows)
        conn.commit()
        logger.info(f"Snapshot {len(rows)} rows → {SNAPSHOT_TABLE}")
    except Exception:
        conn.rollback()
        logger.exception("Lỗi ghi snapshot")
        raise
    finally:
        conn.close()


def _write_pending(rows: List[Dict]) -> None:
    if not rows:
        return
    conn = _get_conn()
    try:
        conn.cursor().executemany(_PENDING_INSERT, rows)
        conn.commit()
        logger.info(f"DLQ insert {len(rows)} rows → {PENDING_TABLE}")
    except Exception:
        conn.rollback()
        logger.exception("Lỗi ghi DLQ")
        raise
    finally:
        conn.close()


# ─────────────────────────────────────────────
# KAFKA READER
# ─────────────────────────────────────────────
def _read_kafka(spark: SparkSession, topic: str) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        # .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )


# ─────────────────────────────────────────────
# PARSE STREAMS
# Dùng kafka.timestamp làm watermark — tránh evict sớm khi replay.
# startingOffsets=latest — batch bootstrap xử lý dữ liệu lịch sử riêng.
# ─────────────────────────────────────────────
def _build_txn_stream(spark: SparkSession) -> DataFrame:
    raw = _read_kafka(spark, TXN_TOPIC)
    return (
        raw
        .select(
            from_json(col("value").cast("string"), DEBEZIUM_ENVELOPE_SCHEMA).alias("e"),
            col("timestamp").alias("kafka_ts"),
        )
        .select("e.*", "kafka_ts")
        .filter(col("op").isin("c", "u", "r"))   # chỉ INSERT/UPDATE — không xử lý snapshot op=r
        .select(
            from_json(col("after"), TXN_SCHEMA).alias("t"),
            col("kafka_ts").alias("txn_event_ts"),
            col("ts_ms").cast("long").alias("txn_ts_ms"),
        )
        .select("t.*", "txn_event_ts", "txn_ts_ms")
        .filter(col("TRANSACTION_ID").isNotNull())
        .withWatermark("txn_event_ts", WATERMARK_DELAY)
    )


def _build_acct_stream(spark: SparkSession) -> DataFrame:
    raw = _read_kafka(spark, ACCT_TOPIC)
    return (
        raw
        .select(
            from_json(col("value").cast("string"), DEBEZIUM_ENVELOPE_SCHEMA).alias("e"),
            col("timestamp").alias("kafka_ts"),
        )
        .select("e.*", "kafka_ts")
        .filter(col("op").isin("r", "c", "u", "r"))
        .select(
            from_json(col("after"), ACCT_SCHEMA).alias("a"),
            col("kafka_ts").alias("acct_event_ts"),
            col("ts_ms").cast("long").alias("acct_ts_ms"),
        )
        .select("a.*", "acct_event_ts", "acct_ts_ms")
        .filter(col("ACCOUNT_ID").isNotNull())
        .withWatermark("acct_event_ts", WATERMARK_DELAY)
    )


# ─────────────────────────────────────────────
# FOREACHBATCH — Stream-Stream Join
# ─────────────────────────────────────────────
def _make_join_batch_writer():
    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        spark = batch_df.sparkSession
        spark.sparkContext.setJobDescription(
            f"[txn_acct_join] batch={batch_id} — classify snapshot/pending"
        )

        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number

        # TXN matched với ACCOUNT trong window
        matched   = batch_df.filter(
            col("TRANSACTION_ID").isNotNull() & col("TXN_ACCT_ACCOUNT_ID").isNotNull()
        )
        # TXN không tìm được ACCOUNT trong window → DLQ
        txn_only  = batch_df.filter(
            col("TRANSACTION_ID").isNotNull() & col("TXN_ACCT_ACCOUNT_ID").isNull()
        )

        # Dedup: mỗi TXN chỉ lấy ACCOUNT update SỚM NHẤT sau giao dịch
        # (lần update đầu tiên của core banking sau khi ghi TXN)
        w = Window.partitionBy("TRANSACTION_ID").orderBy("acct_ts_ms")
        matched = (
            matched
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

        matched_rows = matched.collect()
        txn_only_rows = txn_only.collect()

        logger.info(
            f"[batch={batch_id}] matched={len(matched_rows)} txn_only={len(txn_only_rows)}"
        )
        for r in txn_only_rows:
            logger.warning(
                f"[batch={batch_id}] TXN_NO_ACCOUNT_MATCH: "
                f"TXN={r['TRANSACTION_ID']} ACCOUNT_ID={r['ACCOUNT_ID']}"
            )

        snapshot_rows: List[Dict] = []
        pending_rows:  List[Dict] = []

        for r in matched_rows:
            txn  = {k: r[k] for k in TXN_SCHEMA.fieldNames() if k in r}
            acct = {
                "ONLINE_ACTUAL_BAL": r["ONLINE_ACTUAL_BAL"],
                "WORKING_BALANCE":   r["WORKING_BALANCE"],
                "CURRENCY_CODE":     r["ACCT_CURRENCY_CODE"],
            }
            snapshot_rows.append(_to_snapshot_bind(txn, acct, r["txn_ts_ms"], r["acct_ts_ms"]))

        for r in txn_only_rows:
            txn = {k: r[k] for k in TXN_SCHEMA.fieldNames() if k in r}
            pending_rows.append(_to_pending_bind(txn, r["txn_ts_ms"]))

        _write_snapshot(snapshot_rows)
        _write_pending(pending_rows)
        logger.info(
            f"[batch={batch_id}] total={len(matched_rows)+len(txn_only_rows)} "
            f"snapshot={len(snapshot_rows)} pending={len(pending_rows)}"
        )

    return write_batch


# ─────────────────────────────────────────────
# RETRY LOGIC — public, dùng bởi tools/retry_txn_acct_dlq.py
# Lookup T24_ACCOUNT_TARGET (mirror luôn cập nhật qua sync job)
# ─────────────────────────────────────────────
def retry_pending_once(max_retry: int = MAX_RETRY) -> Dict[str, int]:
    """
    Đọc PENDING rows từ DLQ, lookup T24_ACCOUNT_TARGET, retry join.

    Returns: {"resolved": N, "failed": N, "retrying": N}
    Gọi từ tools/retry_txn_acct_dlq.py (standalone) hoặc Airflow task.
    """
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT TRANSACTION_ID, ACCOUNT_ID, CUSTOMER_ID,
                   TRANSACTION_DATE, VALUE_DATE, TRANSACTION_TIME,
                   TRANSACTION_TYPE, AMOUNT, CURRENCY_CODE, CHANNEL,
                   BRANCH_CODE, REFERENCE_NO, TRANSACTION_STATUS,
                   TXN_TS_MS, RETRY_COUNT
            FROM {PENDING_TABLE}
            WHERE STATUS = '{STATUS_PENDING}'
              AND NEXT_RETRY_AT <= SYSTIMESTAMP
            ORDER BY PENDING_SINCE
        """)
        cols         = [d[0] for d in cur.description]
        pending_rows = [dict(zip(cols, r)) for r in cur.fetchall()]

        cur.execute(f"""
            SELECT ACCOUNT_ID, CUSTOMER_ID, WORKING_BALANCE,
                   ONLINE_ACTUAL_BAL, CURRENCY_CODE
            FROM {ACCT_TARGET}
        """)
        acct_map: Dict[str, Dict] = {
            r[0]: {
                "CUSTOMER_ID":       r[1],
                "WORKING_BALANCE":   r[2],
                "ONLINE_ACTUAL_BAL": r[3],
                "CURRENCY_CODE":     r[4],
            }
            for r in cur.fetchall()
        }
    finally:
        conn.close()

    if not pending_rows:
        logger.info("DLQ trống — không có gì để retry.")
        return {"resolved": 0, "failed": 0, "retrying": 0}

    logger.info(f"Retry {len(pending_rows)} pending rows, acct_target={len(acct_map)}...")

    snapshot_rows: List[Dict] = []
    resolved_ids:  List[Dict] = []
    failed_ids:    List[Dict] = []
    retry_ids:     List[Dict] = []

    for row in pending_rows:
        retry_count = row.get("RETRY_COUNT") or 0
        acct_info   = acct_map.get(row["ACCOUNT_ID"])
        bind_key    = {"TRANSACTION_ID": row["TRANSACTION_ID"]}

        if acct_info:
            txn = {k: row[k] for k in [
                "TRANSACTION_ID", "ACCOUNT_ID", "CUSTOMER_ID",
                "TRANSACTION_DATE", "VALUE_DATE", "TRANSACTION_TIME",
                "TRANSACTION_TYPE", "AMOUNT", "CURRENCY_CODE",
                "CHANNEL", "BRANCH_CODE", "REFERENCE_NO", "TRANSACTION_STATUS",
            ]}
            snapshot_rows.append(_to_snapshot_bind(txn, acct_info, row["TXN_TS_MS"], None, approximate=True))
            resolved_ids.append(bind_key)
        elif retry_count >= max_retry:
            logger.warning(
                f"FAILED: TXN={row['TRANSACTION_ID']} "
                f"ACCOUNT_ID={row['ACCOUNT_ID']} retry={retry_count}/{max_retry}"
            )
            failed_ids.append(bind_key)
        else:
            next_at = _next_retry_at(retry_count + 1)
            retry_ids.append({**bind_key, "NEXT_RETRY_AT": next_at})

    _write_snapshot(snapshot_rows)

    conn2 = _get_conn()
    try:
        cur2 = conn2.cursor()
        if resolved_ids:
            cur2.executemany(_PENDING_RESOLVE, resolved_ids)
        if failed_ids:
            cur2.executemany(_PENDING_FAIL, failed_ids)
        if retry_ids:
            cur2.executemany(_PENDING_RETRY_INC, retry_ids)
        conn2.commit()
    except Exception:
        conn2.rollback()
        logger.exception("Lỗi batch update DLQ status")
        raise
    finally:
        conn2.close()

    result = {
        "resolved": len(resolved_ids),
        "failed":   len(failed_ids),
        "retrying": len(retry_ids),
    }
    logger.info(f"Retry done: {result}")
    return result


# ─────────────────────────────────────────────
# PUBLIC ENTRYPOINT — gọi từ main.py
# ─────────────────────────────────────────────
def start_txn_acct_join(spark: SparkSession) -> List[StreamingQuery]:
    """
    Khởi động 2 streaming queries:
      1. join_query   : T24_TRANSACTIONS ⋈ T24_ACCOUNT (INNER JOIN với watermark ±30')
                        → T24_TXN_ACCOUNT_SNAPSHOT / T24_TXN_ACCT_PENDING_JOIN (DLQ)
      2. delete_query : T24_TRANSACTIONS op=d → DELETE khỏi T24_TXN_ACCOUNT_SNAPSHOT

    Lưu ý: dùng startingOffsets=latest — chạy bootstrap_txn_acct.py trước.
    DLQ retry: python3 tools/retry_txn_acct_dlq.py

    Returns: [join_query, delete_query]
    """
    logger.info(f"Starting txn-acct stream-stream join: {TXN_TOPIC} ⋈ {ACCT_TOPIC}")

    txn_df  = _build_txn_stream(spark)
    acct_df = _build_acct_stream(spark)

    txn_df.createOrReplaceTempView("txn_stream")
    acct_df.createOrReplaceTempView("acct_stream_b")

    # LEFT OUTER JOIN với one-sided window, tách biệt 2 mục đích:
    # Logic: core banking ghi TXN trước, sau đó mới UPDATE ACCOUNT với số dư mới
    # → ACCT event luôn xuất hiện SAU TXN event.
    #
    # acct_ts_ms / txn_ts_ms  (Oracle ts_ms): xác định đúng window business,
    #   bền vững khi Kafka replay vì ts_ms là timestamp gốc từ Oracle redo log.
    # acct_event_ts (kafka_ts, upper bound only): cho Spark biết state bound để
    #   evict state an toàn. Không dùng lower bound kafka_ts vì 2 topic có thể
    #   bị lỗi và replay độc lập → kafka_ts lệch nhau → join sai.
    #
    # TXN không match → Spark emit NULL phía ACCOUNT sau khi watermark vượt qua
    # → foreachBatch phân loại thành TXN_ONLY → DLQ.
    joined_df = spark.sql("""
        SELECT
            t.TRANSACTION_ID,
            t.ACCOUNT_ID,
            t.CUSTOMER_ID,
            t.TRANSACTION_DATE,
            t.VALUE_DATE,
            t.TRANSACTION_TIME,
            t.TRANSACTION_TYPE,
            t.AMOUNT,
            t.CURRENCY_CODE,
            t.CHANNEL,
            t.BRANCH_CODE,
            t.REFERENCE_NO,
            t.TRANSACTION_STATUS,
            t.txn_event_ts,
            t.txn_ts_ms,
            a.ACCOUNT_ID        AS TXN_ACCT_ACCOUNT_ID,
            a.ONLINE_ACTUAL_BAL,
            a.WORKING_BALANCE,
            a.CURRENCY_CODE     AS ACCT_CURRENCY_CODE,
            a.acct_ts_ms
        FROM txn_stream t
        LEFT OUTER JOIN acct_stream_b a
            ON  t.ACCOUNT_ID = a.ACCOUNT_ID
            AND a.acct_event_ts >= t.txn_event_ts
            AND a.acct_event_ts <= t.txn_event_ts + INTERVAL 30 MINUTES
    """)

    join_query = (
        joined_df.writeStream
        .foreachBatch(_make_join_batch_writer())
        .option("checkpointLocation", CHECKPOINT_JOIN)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName("txn_acct_stream_join")
        .start()
    )

    # DELETE handler: TXN bị xóa → xóa snapshot tương ứng
    raw_txn_del = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TXN_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    txn_delete_df = (
        raw_txn_del
        .select(from_json(col("value").cast("string"), DEBEZIUM_ENVELOPE_SCHEMA).alias("e"))
        .select("e.*")
        .filter(col("op") == "d")
        .select(from_json(col("before"), TXN_SCHEMA).alias("t"))
        .select(col("t.TRANSACTION_ID").alias("TRANSACTION_ID"))
        .filter(col("TRANSACTION_ID").isNotNull())
    )

    def _delete_batch(batch_df: DataFrame, batch_id: int) -> None:
        ids = [r["TRANSACTION_ID"] for r in batch_df.collect() if r["TRANSACTION_ID"]]
        if not ids:
            return
        conn = _get_conn()
        try:
            conn.cursor().executemany(
                f"DELETE FROM {SNAPSHOT_TABLE} WHERE TRANSACTION_ID = :1",
                [(tid,) for tid in ids],
            )
            conn.commit()
            logger.info(f"[delete_batch={batch_id}] Deleted {len(ids)} snapshots")
        except Exception:
            conn.rollback()
            logger.exception("Lỗi xóa snapshot")
            raise
        finally:
            conn.close()

    delete_query = (
        txn_delete_df.writeStream
        .foreachBatch(_delete_batch)
        .option("checkpointLocation", CHECKPOINT_DELETE)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName("txn_delete_handler")
        .start()
    )

    logger.info("txn-acct join queries started.")
    return [join_query, delete_query]
