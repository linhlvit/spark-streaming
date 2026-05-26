"""
txn_branch_join.py — Spark Stream-Static Join
    Stream : Kafka oracle.LPB_POC.T24_TRANSACTIONS  (Debezium CDC)
    Static : Oracle LPB_POC.T24_BRANCH              (oracledb, refresh mỗi batch)

Output:
    T24_TXN_ENRICHED      — giao dịch đã join được branch
    T24_TXN_PENDING_JOIN  — DLQ: giao dịch chưa join được (STATUS=PENDING)

DLQ retry KHÔNG chạy trong job này — tách ra tools/retry_txn_branch_dlq.py
để tránh ảnh hưởng tới streaming job chính khi retry gặp lỗi.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import oracledb
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, when
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
BRANCH_SOURCE   = f"{TARGET_SCHEMA}.T24_BRANCH"
ENRICHED_TABLE  = f"{TARGET_SCHEMA}.T24_TXN_ENRICHED"
PENDING_TABLE   = f"{TARGET_SCHEMA}.T24_TXN_PENDING_JOIN"
CHECKPOINT_JOIN = f"{CHECKPOINT_BASE}/txn_branch_join"

MAX_RETRY = 5

# DLQ status values
STATUS_PENDING  = "PENDING"
STATUS_RESOLVED = "RESOLVED"
STATUS_FAILED   = "FAILED"

# DLQ error codes — dùng để filter/alert theo loại lỗi
EC_BRANCH_NOT_FOUND   = "BRANCH_NOT_FOUND"
EC_BRANCH_TABLE_EMPTY = "BRANCH_TABLE_EMPTY"

# Exponential backoff schedule cho static_join DLQ (tính bằng phút).
# Base interval 5 phút — BRANCH lag thường resolve trong vài phút.
# RETRY_COUNT → số phút chờ trước lần retry tiếp theo.
BACKOFF_MINUTES_STATIC: list[int] = [5, 15, 60, 240, 1440]
#  retry 0 → +5'    (5 phút sau khi vào DLQ)
#  retry 1 → +15'   (đã thử 1 lần, chờ thêm 15')
#  retry 2 → +60'   (1 giờ — BRANCH có thể đang trong quá trình sync chậm)
#  retry 3 → +240'  (4 giờ — khả năng cao BRANCH không tồn tại)
#  retry 4 → +1440' (24 giờ — lần cuối trước khi FAILED)

TXN_PAYLOAD_SCHEMA = StructType([
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
    StructField("INPUT_ID",           StringType(), True),
    StructField("AUTHOR_ID",          StringType(), True),
    StructField("BRANCH_CODE",        StringType(), True),
    StructField("REFERENCE_NO",       StringType(), True),
    StructField("TRANSACTION_STATUS", StringType(), True),
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


def _next_retry_at(retry_count: int, schedule: list[int] = BACKOFF_MINUTES_STATIC) -> datetime:
    """
    Tính thời điểm được phép retry tiếp theo theo backoff schedule.
    retry_count: số lần đã retry (0 = lần đầu tiên, chưa retry lần nào).
    Trả về datetime UTC naive — dùng để so sánh với SYSTIMESTAMP Oracle.
    """
    idx     = min(retry_count, len(schedule) - 1)
    minutes = schedule[idx]
    from datetime import timedelta
    return datetime.utcnow() + timedelta(minutes=minutes)


# ─────────────────────────────────────────────
# LOAD STATIC DATA
# Dùng oracledb thuần thay vì spark.read.jdbc vì:
# spark.read.jdbc bên trong foreachBatch gây ClassCastException
# do JAR serialization conflict giữa driver và executor.
# ─────────────────────────────────────────────
def _load_branch_map() -> Dict[str, Dict]:
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT BRANCH_CODE, BRANCH_NAME, REGION_CODE, REGION_NAME FROM {BRANCH_SOURCE}"
        )
        result = {
            r[0]: {"BRANCH_NAME": r[1], "REGION_CODE": r[2], "REGION_NAME": r[3]}
            for r in cur.fetchall()
        }
        logger.info(f"Loaded {len(result)} branches from {BRANCH_SOURCE}")
        return result
    finally:
        conn.close()


# ─────────────────────────────────────────────
# SQL
# ─────────────────────────────────────────────
_ENRICHED_MERGE = f"""
    MERGE INTO {ENRICHED_TABLE} t
    USING (SELECT
        :TRANSACTION_ID AS TRANSACTION_ID, :ACCOUNT_ID AS ACCOUNT_ID,
        :CUSTOMER_ID AS CUSTOMER_ID, :TRANSACTION_DATE AS TRANSACTION_DATE,
        :VALUE_DATE AS VALUE_DATE, :TRANSACTION_TIME AS TRANSACTION_TIME,
        :TRANSACTION_TYPE AS TRANSACTION_TYPE, :AMOUNT AS AMOUNT,
        :CURRENCY_CODE AS CURRENCY_CODE, :CHANNEL AS CHANNEL,
        :INPUT_ID AS INPUT_ID, :AUTHOR_ID AS AUTHOR_ID,
        :BRANCH_CODE AS BRANCH_CODE, :BRANCH_NAME AS BRANCH_NAME,
        :REGION_CODE AS REGION_CODE, :REGION_NAME AS REGION_NAME,
        :REFERENCE_NO AS REFERENCE_NO, :TRANSACTION_STATUS AS TRANSACTION_STATUS
    FROM DUAL) s
    ON (t.TRANSACTION_ID = s.TRANSACTION_ID)
    WHEN MATCHED THEN UPDATE SET
        t.BRANCH_NAME=s.BRANCH_NAME, t.REGION_CODE=s.REGION_CODE,
        t.REGION_NAME=s.REGION_NAME, t.ENRICHED_AT=SYSTIMESTAMP
    WHEN NOT MATCHED THEN INSERT (
        TRANSACTION_ID, ACCOUNT_ID, CUSTOMER_ID, TRANSACTION_DATE, VALUE_DATE,
        TRANSACTION_TIME, TRANSACTION_TYPE, AMOUNT, CURRENCY_CODE, CHANNEL,
        INPUT_ID, AUTHOR_ID, BRANCH_CODE, BRANCH_NAME, REGION_CODE, REGION_NAME,
        REFERENCE_NO, TRANSACTION_STATUS
    ) VALUES (
        s.TRANSACTION_ID, s.ACCOUNT_ID, s.CUSTOMER_ID, s.TRANSACTION_DATE, s.VALUE_DATE,
        s.TRANSACTION_TIME, s.TRANSACTION_TYPE, s.AMOUNT, s.CURRENCY_CODE, s.CHANNEL,
        s.INPUT_ID, s.AUTHOR_ID, s.BRANCH_CODE, s.BRANCH_NAME, s.REGION_CODE, s.REGION_NAME,
        s.REFERENCE_NO, s.TRANSACTION_STATUS
    )
"""

# INSERT khi lần đầu vào DLQ, SKIP nếu đã tồn tại (idempotent — batch có thể retry)
# NEXT_RETRY_AT: thời điểm được phép retry lần đầu (= PENDING_SINCE + backoff[0])
_PENDING_INSERT = f"""
    MERGE INTO {PENDING_TABLE} t
    USING (SELECT :TRANSACTION_ID AS TRANSACTION_ID FROM DUAL) s
    ON (t.TRANSACTION_ID = s.TRANSACTION_ID)
    WHEN NOT MATCHED THEN INSERT (
        TRANSACTION_ID, ACCOUNT_ID, CUSTOMER_ID, TRANSACTION_DATE, VALUE_DATE,
        TRANSACTION_TIME, TRANSACTION_TYPE, AMOUNT, CURRENCY_CODE, CHANNEL,
        INPUT_ID, AUTHOR_ID, BRANCH_CODE, REFERENCE_NO, TRANSACTION_STATUS,
        PENDING_SINCE, RETRY_COUNT, NEXT_RETRY_AT, STATUS, ERROR_CODE, ERROR_REASON
    ) VALUES (
        :TRANSACTION_ID, :ACCOUNT_ID, :CUSTOMER_ID, :TRANSACTION_DATE, :VALUE_DATE,
        :TRANSACTION_TIME, :TRANSACTION_TYPE, :AMOUNT, :CURRENCY_CODE, :CHANNEL,
        :INPUT_ID, :AUTHOR_ID, :BRANCH_CODE, :REFERENCE_NO, :TRANSACTION_STATUS,
        SYSTIMESTAMP, 0, :NEXT_RETRY_AT, :STATUS, :ERROR_CODE, :ERROR_REASON
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

# NEXT_RETRY_AT được tính từ Python theo backoff schedule, truyền vào bind variable
_PENDING_RETRY_INC = f"""
    UPDATE {PENDING_TABLE}
    SET RETRY_COUNT   = RETRY_COUNT + 1,
        NEXT_RETRY_AT = :NEXT_RETRY_AT
    WHERE TRANSACTION_ID = :TRANSACTION_ID
"""


# ─────────────────────────────────────────────
# BIND HELPERS
# ─────────────────────────────────────────────
def _to_enriched_bind(row: Dict, branch: Dict) -> Dict:
    return {
        "TRANSACTION_ID":     row.get("TRANSACTION_ID"),
        "ACCOUNT_ID":         row.get("ACCOUNT_ID"),
        "CUSTOMER_ID":        row.get("CUSTOMER_ID"),
        "TRANSACTION_DATE":   _epoch_ms_to_dt(row.get("TRANSACTION_DATE")),
        "VALUE_DATE":         _epoch_ms_to_dt(row.get("VALUE_DATE")),
        "TRANSACTION_TIME":   _epoch_ms_to_dt(row.get("TRANSACTION_TIME")),
        "TRANSACTION_TYPE":   row.get("TRANSACTION_TYPE"),
        "AMOUNT":             row.get("AMOUNT"),
        "CURRENCY_CODE":      row.get("CURRENCY_CODE"),
        "CHANNEL":            row.get("CHANNEL"),
        "INPUT_ID":           row.get("INPUT_ID"),
        "AUTHOR_ID":          row.get("AUTHOR_ID"),
        "BRANCH_CODE":        row.get("BRANCH_CODE"),
        "BRANCH_NAME":        branch.get("BRANCH_NAME"),
        "REGION_CODE":        branch.get("REGION_CODE"),
        "REGION_NAME":        branch.get("REGION_NAME"),
        "REFERENCE_NO":       row.get("REFERENCE_NO"),
        "TRANSACTION_STATUS": row.get("TRANSACTION_STATUS"),
    }


def _to_pending_bind(row: Dict, error_code: str = EC_BRANCH_NOT_FOUND) -> Dict:
    branch_code = row.get("BRANCH_CODE")
    error_reason = (
        f"branch_code='{branch_code}' not found in T24_BRANCH"
        if error_code == EC_BRANCH_NOT_FOUND
        else "T24_BRANCH table is empty"
    )
    return {
        "TRANSACTION_ID":     row.get("TRANSACTION_ID"),
        "ACCOUNT_ID":         row.get("ACCOUNT_ID"),
        "CUSTOMER_ID":        row.get("CUSTOMER_ID"),
        "TRANSACTION_DATE":   _epoch_ms_to_dt(row.get("TRANSACTION_DATE")),
        "VALUE_DATE":         _epoch_ms_to_dt(row.get("VALUE_DATE")),
        "TRANSACTION_TIME":   _epoch_ms_to_dt(row.get("TRANSACTION_TIME")),
        "TRANSACTION_TYPE":   row.get("TRANSACTION_TYPE"),
        "AMOUNT":             row.get("AMOUNT"),
        "CURRENCY_CODE":      row.get("CURRENCY_CODE"),
        "CHANNEL":            row.get("CHANNEL"),
        "INPUT_ID":           row.get("INPUT_ID"),
        "AUTHOR_ID":          row.get("AUTHOR_ID"),
        "BRANCH_CODE":        branch_code,
        "REFERENCE_NO":       row.get("REFERENCE_NO"),
        "TRANSACTION_STATUS": row.get("TRANSACTION_STATUS"),
        "NEXT_RETRY_AT":      _next_retry_at(retry_count=0),  # lần đầu: backoff[0] = 5'
        "STATUS":             STATUS_PENDING,
        "ERROR_CODE":         error_code,
        "ERROR_REASON":       error_reason,
    }


# ─────────────────────────────────────────────
# WRITE HELPERS
# ─────────────────────────────────────────────
def _write_enriched(rows: List[Dict]) -> None:
    if not rows:
        return
    conn = _get_conn()
    try:
        conn.cursor().executemany(_ENRICHED_MERGE, rows)
        conn.commit()
        logger.info(f"Enriched {len(rows)} rows → {ENRICHED_TABLE}")
    except Exception:
        conn.rollback()
        logger.exception("Lỗi ghi enriched")
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
# FOREACHBATCH — streaming job chính
# ─────────────────────────────────────────────
def _make_join_batch_writer():
    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        spark = batch_df.sparkSession
        spark.sparkContext.setJobDescription(
            f"[txn_branch_join] batch={batch_id} — join branch + write"
        )

        batch_df.createOrReplaceTempView("txn_raw")
        txn_rows = spark.sql("""
            SELECT _op, row_data FROM txn_raw
            WHERE _op IN ('r','c','u') AND row_data IS NOT NULL
        """).collect()

        if not txn_rows:
            return

        try:
            branch_map = _load_branch_map()
        except Exception:
            logger.exception(f"[batch={batch_id}] Không load được branch — bỏ qua batch")
            return

        enriched: List[Dict] = []
        pending:  List[Dict] = []

        for row in txn_rows:
            try:
                txn = json.loads(row["row_data"])
            except Exception:
                continue

            branch_code = txn.get("BRANCH_CODE")
            branch_info = branch_map.get(branch_code) if branch_map else None

            if branch_info:
                enriched.append(_to_enriched_bind(txn, branch_info))
            else:
                error_code = EC_BRANCH_TABLE_EMPTY if not branch_map else EC_BRANCH_NOT_FOUND
                logger.warning(
                    f"[batch={batch_id}] {error_code}: "
                    f"BRANCH_CODE='{branch_code}' TXN={txn.get('TRANSACTION_ID')} → DLQ"
                )
                pending.append(_to_pending_bind(txn, error_code=error_code))

        _write_enriched(enriched)
        _write_pending(pending)
        logger.info(
            f"[batch={batch_id}] total={len(txn_rows)} "
            f"enriched={len(enriched)} pending={len(pending)}"
        )

    return write_batch


# ─────────────────────────────────────────────
# RETRY LOGIC — dùng chung với standalone script
# Tách thành hàm public để tools/retry_txn_branch_dlq.py import trực tiếp
# ─────────────────────────────────────────────
def retry_pending_once(max_retry: int = MAX_RETRY) -> Dict[str, int]:
    """
    Đọc toàn bộ PENDING rows từ DLQ, load branch mới nhất, retry join.

    Returns: {"resolved": N, "failed": N, "retrying": N}
    Hàm này được gọi bởi tools/retry_txn_branch_dlq.py (standalone script).
    """
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT TRANSACTION_ID, ACCOUNT_ID, CUSTOMER_ID,
                   TRANSACTION_DATE, VALUE_DATE, TRANSACTION_TIME,
                   TRANSACTION_TYPE, AMOUNT, CURRENCY_CODE, CHANNEL,
                   INPUT_ID, AUTHOR_ID, BRANCH_CODE, REFERENCE_NO,
                   TRANSACTION_STATUS, RETRY_COUNT
            FROM {PENDING_TABLE}
            WHERE STATUS = '{STATUS_PENDING}'
              AND NEXT_RETRY_AT <= SYSTIMESTAMP
            ORDER BY PENDING_SINCE
        """)
        cols         = [d[0] for d in cur.description]
        pending_rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()

    if not pending_rows:
        logger.info("DLQ: không có row nào eligible (tất cả đang trong backoff window).")
        return {"resolved": 0, "failed": 0, "retrying": 0}

    logger.info(f"Retry {len(pending_rows)} eligible rows (NEXT_RETRY_AT <= now)...")

    branch_map = _load_branch_map()
    if not branch_map:
        logger.warning("T24_BRANCH rỗng — bỏ qua lần retry này.")
        return {"resolved": 0, "failed": 0, "retrying": len(pending_rows)}

    enriched_binds: List[Dict] = []
    resolved_ids:   List[Dict] = []
    failed_ids:     List[Dict] = []
    retry_ids:      List[Dict] = []

    for row in pending_rows:
        retry_count = row.get("RETRY_COUNT") or 0
        branch_code = row.get("BRANCH_CODE")
        branch_info = branch_map.get(branch_code)
        bind        = {"TRANSACTION_ID": row["TRANSACTION_ID"]}

        if branch_info:
            # RESOLVED: branch đã có trong lookup table
            enriched_binds.append({
                "TRANSACTION_ID":     row["TRANSACTION_ID"],
                "ACCOUNT_ID":         row["ACCOUNT_ID"],
                "CUSTOMER_ID":        row["CUSTOMER_ID"],
                "TRANSACTION_DATE":   row["TRANSACTION_DATE"],
                "VALUE_DATE":         row["VALUE_DATE"],
                "TRANSACTION_TIME":   row["TRANSACTION_TIME"],
                "TRANSACTION_TYPE":   row["TRANSACTION_TYPE"],
                "AMOUNT":             row["AMOUNT"],
                "CURRENCY_CODE":      row["CURRENCY_CODE"],
                "CHANNEL":            row["CHANNEL"],
                "INPUT_ID":           row["INPUT_ID"],
                "AUTHOR_ID":          row["AUTHOR_ID"],
                "BRANCH_CODE":        branch_code,
                "BRANCH_NAME":        branch_info.get("BRANCH_NAME"),
                "REGION_CODE":        branch_info.get("REGION_CODE"),
                "REGION_NAME":        branch_info.get("REGION_NAME"),
                "REFERENCE_NO":       row["REFERENCE_NO"],
                "TRANSACTION_STATUS": row["TRANSACTION_STATUS"],
            })
            resolved_ids.append(bind)
        elif retry_count >= max_retry:
            # FAILED: đã hết lượt retry, branch vẫn không tồn tại
            logger.warning(
                f"FAILED: TXN={row['TRANSACTION_ID']} "
                f"BRANCH='{branch_code}' retry={retry_count}/{max_retry}"
            )
            failed_ids.append(bind)
        else:
            # Còn lượt retry — tính NEXT_RETRY_AT theo backoff schedule
            # retry_count hiện tại đã được tăng lên 1 sau lần này → dùng retry_count+1
            next_at = _next_retry_at(retry_count + 1)
            retry_ids.append({**bind, "NEXT_RETRY_AT": next_at})
            logger.debug(
                f"RETRY_LATER: TXN={row['TRANSACTION_ID']} "
                f"retry={retry_count+1}/{max_retry} next_at={next_at.isoformat()}"
            )

    # Ghi enriched trước, rồi update DLQ status
    _write_enriched(enriched_binds)

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
        logger.exception("Lỗi update DLQ status")
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
def start_stream_static_join(spark: SparkSession) -> StreamingQuery:
    """
    Khởi động streaming query: T24_TRANSACTIONS (Kafka) ⋈ T24_BRANCH (Oracle)
    → T24_TXN_ENRICHED  (join thành công)
    → T24_TXN_PENDING_JOIN / DLQ (BRANCH_NOT_FOUND, STATUS=PENDING)

    DLQ retry KHÔNG chạy ở đây.
    Chạy retry độc lập: python3 tools/retry_txn_branch_dlq.py
    """
    logger.info(f"Starting stream-static join: {TXN_TOPIC} ⋈ {BRANCH_SOURCE}")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TXN_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    parsed_df = (
        raw_df
        .select(from_json(col("value").cast("string"), DEBEZIUM_ENVELOPE_SCHEMA).alias("msg"))
        .select("msg.*")
        .filter(col("op").isNotNull())
        .withColumn(
            "row_data",
            when(col("op").isin("r", "c", "u"), col("after")).otherwise(col("before"))
        )
        .withColumn("_op", col("op"))
        .select("_op", "row_data")
        .filter(col("row_data").isNotNull())
    )

    return (
        parsed_df.writeStream
        .foreachBatch(_make_join_batch_writer())
        .option("checkpointLocation", CHECKPOINT_JOIN)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
