"""
branch_sales_agg.py — Spark Stateful Aggregation (Pattern 4)
    Stream : Kafka oracle.FSS_STREAM.T24_TRANSACTIONS  (Debezium CDC)

Mục đích:
    Tính tổng doanh số theo chi nhánh (BRANCH_CODE) và ngày giao dịch,
    cập nhật near-realtime vào bảng summary T24_BRANCH_SALES_SUMMARY.

    Đây là pattern cộng dồn (incremental aggregation):
    - op=c (INSERT mới)  → SIGNED_AMOUNT = +AMOUNT  → cộng vào tổng
    - op=u (revert/hủy) → SIGNED_AMOUNT = -before.AMOUNT → trừ ngược phần đã cộng
    - Mỗi batch tính DELTA (SUM SIGNED_AMOUNT, SUM COUNT_DELTA) rồi MERGE vào Oracle.
    - KHÔNG dùng Spark window function để tránh join state memory lớn.

Idempotency:
    Spark có thể chạy lại cùng batch_id khi executor lỗi.
    Nếu batch đã commit → cộng lại delta sẽ sai số liệu.
    Giải pháp: T24_STREAM_BATCH_LOG ghi batch_id đã xử lý trong cùng
    DB transaction với MERGE → đảm bảo exactly-once tại Oracle layer.

Latency:
    Trigger interval 30s → end-to-end latency ~35–70s.
    Phù hợp cho dashboard near-realtime tại ngân hàng (refresh 1 phút).

Output:
    T24_BRANCH_SALES_SUMMARY  — tổng doanh số tích lũy theo BRANCH + ngày
    T24_STREAM_BATCH_LOG      — idempotency guard (batch_id đã xử lý)
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import oracledb
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, sum as _sum, count as _count, lit
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

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
TXN_TOPIC       = f"{TOPIC_PREFIX}.T24_TRANSACTIONS"
SUMMARY_TABLE   = f"{TARGET_SCHEMA}.T24_BRANCH_SALES_SUMMARY"
BATCH_LOG_TABLE = f"{TARGET_SCHEMA}.T24_STREAM_BATCH_LOG"
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/branch_sales_agg"
JOB_NAME        = "branch_sales_agg"

# ─────────────────────────────────────────────
# PAYLOAD SCHEMA
# ─────────────────────────────────────────────
TXN_SCHEMA = StructType([
    StructField("TRANSACTION_ID",   StringType(), True),
    StructField("ACCOUNT_ID",       StringType(), True),
    StructField("CUSTOMER_ID",      StringType(), True),
    StructField("TRANSACTION_DATE", LongType(),   True),   # epoch ms (Oracle DATE → CDC)
    StructField("TRANSACTION_TYPE", StringType(), True),
    StructField("AMOUNT",           DoubleType(), True),
    StructField("CURRENCY_CODE",    StringType(), True),
    StructField("BRANCH_CODE",      StringType(), True),
    StructField("TRANSACTION_STATUS", StringType(), True),
])

# ─────────────────────────────────────────────
# SQL
# ─────────────────────────────────────────────
# MERGE cộng delta vào tổng tích lũy.
# Khi row đã tồn tại → cộng thêm delta (TOTAL += DELTA).
# Khi chưa tồn tại → INSERT với giá trị ban đầu bằng delta.
_SUMMARY_MERGE = f"""
    MERGE INTO {SUMMARY_TABLE} t
    USING (SELECT
        :BRANCH_CODE      AS BRANCH_CODE,
        :RPT_DATE         AS RPT_DATE,
        :CURRENCY_CODE    AS CURRENCY_CODE,
        :DELTA_AMOUNT     AS DELTA_AMOUNT,
        :DELTA_COUNT      AS DELTA_COUNT,
        :DELTA_CREDIT_AMT AS DELTA_CREDIT_AMT,
        :DELTA_DEBIT_AMT  AS DELTA_DEBIT_AMT
    FROM DUAL) s
    ON (t.BRANCH_CODE   = s.BRANCH_CODE
    AND t.RPT_DATE      = s.RPT_DATE
    AND t.CURRENCY_CODE = s.CURRENCY_CODE)
    WHEN MATCHED THEN UPDATE SET
        t.TOTAL_AMOUNT     = t.TOTAL_AMOUNT     + s.DELTA_AMOUNT,
        t.TXN_COUNT        = t.TXN_COUNT        + s.DELTA_COUNT,
        t.CREDIT_AMOUNT    = t.CREDIT_AMOUNT    + s.DELTA_CREDIT_AMT,
        t.DEBIT_AMOUNT     = t.DEBIT_AMOUNT     + s.DELTA_DEBIT_AMT,
        t.UPDATED_AT       = SYSTIMESTAMP
    WHEN NOT MATCHED THEN INSERT (
        BRANCH_CODE, RPT_DATE, CURRENCY_CODE,
        TOTAL_AMOUNT, TXN_COUNT, CREDIT_AMOUNT, DEBIT_AMOUNT,
        CREATED_AT, UPDATED_AT
    ) VALUES (
        s.BRANCH_CODE, s.RPT_DATE, s.CURRENCY_CODE,
        s.DELTA_AMOUNT, s.DELTA_COUNT, s.DELTA_CREDIT_AMT, s.DELTA_DEBIT_AMT,
        SYSTIMESTAMP, SYSTIMESTAMP
    )
"""

# Guard: kiểm tra batch_id đã xử lý chưa
_BATCH_LOG_CHECK = f"""
    SELECT COUNT(*) FROM {BATCH_LOG_TABLE}
    WHERE JOB_NAME = :JOB_NAME AND BATCH_ID = :BATCH_ID
"""

# Ghi batch_id vào log sau khi MERGE thành công — cùng 1 transaction
_BATCH_LOG_INSERT = f"""
    INSERT INTO {BATCH_LOG_TABLE} (JOB_NAME, BATCH_ID, PROCESSED_AT)
    VALUES (:JOB_NAME, :BATCH_ID, SYSTIMESTAMP)
"""

# Cleanup batch log cũ (giữ 7 ngày gần nhất) — chạy định kỳ để tránh bloat
_BATCH_LOG_CLEANUP = f"""
    DELETE FROM {BATCH_LOG_TABLE}
    WHERE JOB_NAME = :JOB_NAME
      AND PROCESSED_AT < SYSTIMESTAMP - INTERVAL '7' DAY
"""


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def _get_conn() -> oracledb.Connection:
    return oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)


def _epoch_ms_to_date(ms) -> Optional[datetime]:
    """Chuyển epoch ms → datetime (chỉ lấy date phần, giờ = 0)."""
    if ms is None:
        return None
    try:
        dt = datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).replace(tzinfo=None)
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    except Exception:
        return None


# ─────────────────────────────────────────────
# FOREACHBATCH — core logic
# ─────────────────────────────────────────────
def _make_batch_writer():
    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        spark = batch_df.sparkSession
        spark.sparkContext.setJobDescription(
            f"[branch_sales_agg] batch={batch_id} — aggregate delta"
        )

        # ── Idempotency check ──────────────────────────────────────
        conn = _get_conn()
        try:
            cur = conn.cursor()
            cur.execute(_BATCH_LOG_CHECK, {"JOB_NAME": JOB_NAME, "BATCH_ID": batch_id})
            already_processed = cur.fetchone()[0] > 0
        finally:
            conn.close()

        if already_processed:
            logger.info(f"[batch={batch_id}] đã xử lý trước đó, skip.")
            return

        # ── Parse envelope — tách op=c (INSERT mới) và op=u (REVERT) ──
        #
        # Quan điểm nghiệp vụ:
        #   op=c : giao dịch phát sinh mới → cộng +AMOUNT vào doanh số
        #   op=u : giao dịch bị revert/hủy trong ngày (TRANSACTION_STATUS thay đổi,
        #          AMOUNT không đổi) → trừ -before.AMOUNT để hoàn tác phần đã cộng
        #   op=d : không xảy ra với T24_TRANSACTIONS (giao dịch không bị xóa vật lý)
        #   op=r : Debezium snapshot — bỏ qua, bootstrap xử lý riêng
        #
        # Cách tính SIGNED_AMOUNT:
        #   op=c → signed = +after.AMOUNT   (cộng vào tổng)
        #   op=u → signed = -before.AMOUNT  (trừ ngược phần đã cộng trước đó)
        #          COUNT delta = -1          (giảm số GD đang tính)
        from pyspark.sql.functions import when

        parsed_df = (
            batch_df
            .select(
                from_json(col("value").cast("string"), DEBEZIUM_ENVELOPE_SCHEMA).alias("e"),
            )
            .select("e.*")
            .filter(col("op").isin("c", "u", "r"))
        )

        # op=c: lấy after, signed_amount = +AMOUNT, count_delta = +1
        insert_df = (
            parsed_df
            .filter(col("op").isin("c", "r"))
            .select(from_json(col("after"), TXN_SCHEMA).alias("t"))
            .select("t.*")
            .filter(
                col("TRANSACTION_ID").isNotNull() &
                col("BRANCH_CODE").isNotNull() &
                col("AMOUNT").isNotNull()
            )
            .withColumn("SIGNED_AMOUNT", col("AMOUNT"))
            .withColumn("COUNT_DELTA",   lit(1))
            .withColumn(
                "RPT_DATE",
                (col("TRANSACTION_DATE") / 1000).cast("timestamp").cast("date")
            )
        )

        # op=u (revert): lấy before, signed_amount = -AMOUNT, count_delta = -1
        # before chứa giá trị gốc trước khi update — đây là AMOUNT đã được cộng lúc op=c
        revert_df = (
            parsed_df
            .filter(col("op") == "u")
            .select(from_json(col("before"), TXN_SCHEMA).alias("t"))
            .select("t.*")
            .filter(
                col("TRANSACTION_ID").isNotNull() &
                col("BRANCH_CODE").isNotNull() &
                col("AMOUNT").isNotNull()
            )
            .withColumn("SIGNED_AMOUNT", -col("AMOUNT"))
            .withColumn("COUNT_DELTA",   lit(-1))
            .withColumn(
                "RPT_DATE",
                (col("TRANSACTION_DATE") / 1000).cast("timestamp").cast("date")
            )
        )

        combined_df = insert_df.unionByName(revert_df)

        # ── Tính delta theo (BRANCH_CODE, RPT_DATE, CURRENCY_CODE) ──
        # CREDIT / DEBIT phân loại theo TRANSACTION_TYPE của after (insert_df)
        # và before (revert_df) — dùng SIGNED_AMOUNT nên tự triệt tiêu đúng chiều
        delta_df = (
            combined_df
            .groupBy("BRANCH_CODE", "RPT_DATE", "CURRENCY_CODE")
            .agg(
                _sum("SIGNED_AMOUNT").alias("DELTA_AMOUNT"),
                _sum("COUNT_DELTA").alias("DELTA_COUNT"),
                _sum(
                    when(col("TRANSACTION_TYPE").contains("CREDIT"), col("SIGNED_AMOUNT"))
                    .otherwise(lit(0.0))
                ).alias("DELTA_CREDIT_AMT"),
                _sum(
                    when(~col("TRANSACTION_TYPE").contains("CREDIT"), col("SIGNED_AMOUNT"))
                    .otherwise(lit(0.0))
                ).alias("DELTA_DEBIT_AMT"),
            )
        )

        rows = delta_df.collect()
        if not rows:
            logger.info(f"[batch={batch_id}] Không có row hợp lệ sau filter.")
            return

        bind_rows: List[Dict] = [
            {
                "BRANCH_CODE":      r["BRANCH_CODE"],
                "RPT_DATE":         r["RPT_DATE"],
                "CURRENCY_CODE":    r["CURRENCY_CODE"] or "VND",
                "DELTA_AMOUNT":     float(r["DELTA_AMOUNT"] or 0),
                "DELTA_COUNT":      int(r["DELTA_COUNT"] or 0),
                "DELTA_CREDIT_AMT": float(r["DELTA_CREDIT_AMT"] or 0),
                "DELTA_DEBIT_AMT":  float(r["DELTA_DEBIT_AMT"] or 0),
            }
            for r in rows
        ]

        logger.info(
            f"[batch={batch_id}] delta rows={len(bind_rows)} "
            f"total_txn={sum(r['DELTA_COUNT'] for r in bind_rows)}"
        )

        # ── MERGE delta + ghi batch_id — cùng 1 transaction ────────
        conn2 = _get_conn()
        try:
            cur2 = conn2.cursor()
            cur2.executemany(_SUMMARY_MERGE, bind_rows)
            cur2.execute(_BATCH_LOG_INSERT, {"JOB_NAME": JOB_NAME, "BATCH_ID": batch_id})
            conn2.commit()
            logger.info(f"[batch={batch_id}] MERGE OK — {len(bind_rows)} branch-date rows updated")
        except Exception:
            conn2.rollback()
            logger.exception(f"[batch={batch_id}] Lỗi MERGE summary")
            raise
        finally:
            conn2.close()

        # ── Cleanup batch log cũ (best-effort, không ảnh hưởng kết quả) ──
        try:
            conn3 = _get_conn()
            conn3.cursor().execute(_BATCH_LOG_CLEANUP, {"JOB_NAME": JOB_NAME})
            conn3.commit()
            conn3.close()
        except Exception:
            logger.warning("Cleanup batch log thất bại (non-critical)", exc_info=True)

    return write_batch


# ─────────────────────────────────────────────
# PUBLIC ENTRYPOINT — gọi từ main.py
# ─────────────────────────────────────────────
def start_branch_sales_agg(spark: SparkSession) -> StreamingQuery:
    """
    Khởi động streaming aggregation job:
        T24_TRANSACTIONS → T24_BRANCH_SALES_SUMMARY (cộng dồn delta)

    Idempotent qua T24_STREAM_BATCH_LOG.
    Trigger: 30s (config.TRIGGER_INTERVAL).
    Latency end-to-end: ~35–70s.

    Returns: StreamingQuery
    """
    logger.info(f"Starting branch_sales_agg: {TXN_TOPIC} → {SUMMARY_TABLE}")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TXN_TOPIC)
        .option("startingOffsets", "earliest")
        # .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    query = (
        raw_df.writeStream
        .foreachBatch(_make_batch_writer())
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName("branch_sales_agg")
        .start()
    )

    logger.info("branch_sales_agg query started.")
    return query
