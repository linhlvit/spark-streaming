
"""
txn_customer_join.py — Spark Stream + CDC Cache Join (Transaction × Customer)
    Stream   : Kafka oracle.FSS_STREAM.T24_TRANSACTIONS  (Debezium CDC)
    CDC Cache: Kafka oracle.FSS_STREAM.T24_CUSTOMER      (Debezium CDC) — polled via kafka-python
               + Oracle FSS_STREAM.T24_CUSTOMER          (full load 1 lần lúc startup)

Tại sao không dùng static reload như txn_branch_join?
    T24_BRANCH có vài trăm rows → reload mỗi batch OK.
    T24_CUSTOMER có ~10 triệu rows → full reload mỗi batch = hàng phút latency.
    Giải pháp: load 1 lần lúc startup, sau đó chỉ apply delta từ CDC.

Architecture:
    Startup : Oracle T24_CUSTOMER → _init_customer_cache() → _customer_cache (Dict)
    Runtime : _poll_customer_cdc() → _apply_customer_cdc() → patch cache
              T24_TRANSACTIONS batch → join với _customer_cache
              → T24_TXN_CUSTOMER_ENRICHED (hit)
              → T24_TXN_CUSTOMER_PENDING  (miss → DLQ)

RAM estimate: 10M × ~100 bytes (4 cột) ≈ ~1 GB trên driver.

DLQ retry: python3 tools/retry_txn_customer_dlq.py
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import oracledb
from kafka import KafkaConsumer
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, when
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
TXN_TOPIC             = f"{TOPIC_PREFIX}.T24_TRANSACTIONS"
CUSTOMER_CDC_TOPIC    = f"{TOPIC_PREFIX}.T24_CUSTOMER"
CUSTOMER_SOURCE       = f"{TARGET_SCHEMA}.T24_CUSTOMER"
ENRICHED_TABLE        = f"{TARGET_SCHEMA}.T24_TXN_CUSTOMER_ENRICHED"
PENDING_TABLE         = f"{TARGET_SCHEMA}.T24_TXN_CUSTOMER_PENDING"
CHECKPOINT_JOIN       = f"{CHECKPOINT_BASE}/txn_customer_join"

# Consumer group riêng — không ảnh hưởng Debezium connector hay TXN Spark stream.
# Đặt group_id cố định để offset được lưu trong Kafka → tiếp tục từ điểm dừng sau restart.
CDC_CONSUMER_GROUP    = "spark-txn-customer-cdc-cache"

MAX_RETRY = 5

STATUS_PENDING  = "PENDING"
STATUS_RESOLVED = "RESOLVED"
STATUS_FAILED   = "FAILED"

EC_CUSTOMER_NOT_FOUND   = "CUSTOMER_NOT_FOUND"
EC_CUSTOMER_CACHE_WARMING = "CUSTOMER_CACHE_WARMING"

BACKOFF_MINUTES_STATIC: List[int] = [5, 15, 60, 240, 1440]

# ─────────────────────────────────────────────
# MODULE-LEVEL CACHE  (Driver memory only)
# ─────────────────────────────────────────────
# Dict[customer_id, {"CUSTOMER_NAME": ..., "SEGMENT": ..., "BRANCH_CODE": ..., "_ts_ms": int}]
_customer_cache: Dict[str, Dict] = {}
_cache_initialized: bool = False

# Kafka consumer singleton — tái dùng qua các batch để giữ offset state
_cdc_consumer: Optional[KafkaConsumer] = None

# ─────────────────────────────────────────────
# TXN PAYLOAD SCHEMA
# ─────────────────────────────────────────────
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

# Schema của payload T24_CUSTOMER từ Debezium CDC (chỉ các cột cần cho cache)
CUSTOMER_PAYLOAD_SCHEMA = StructType([
    StructField("CUSTOMER_ID",        StringType(), True),
    StructField("CUSTOMER_NAME",      StringType(), True),
    StructField("SEGMENT",            StringType(), True),
    StructField("MANAGE_BRANCH_CODE", StringType(), True),
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


def _next_retry_at(retry_count: int, schedule: List[int] = BACKOFF_MINUTES_STATIC) -> datetime:
    """
    Tính thời điểm retry tiếp theo theo backoff schedule.
    retry_count = số lần đã retry (0 = chưa retry lần nào).
    """
    from datetime import timedelta
    idx     = min(retry_count, len(schedule) - 1)
    minutes = schedule[idx]
    return datetime.utcnow() + timedelta(minutes=minutes)


# ─────────────────────────────────────────────
# CACHE INIT — chạy 1 lần lúc startup
# ─────────────────────────────────────────────
def _init_customer_cache() -> None:
    """
    Full load T24_CUSTOMER vào _customer_cache.
    Chỉ lấy 4 cột cần thiết để giữ RAM ở mức ~1 GB cho 10M rows.
    _ts_ms = 0 → mọi CDC event thực đều có ts_ms > 0 → sẽ ghi đè khi apply CDC.

    Raise exception nếu load thất bại — caller (start_stream_cdc_join) sẽ fail fast
    thay vì tiếp tục với cache rỗng khiến toàn bộ TXN vào DLQ.
    """
    global _customer_cache, _cache_initialized

    logger.info(f"[cache_init] Loading T24_CUSTOMER from Oracle {CUSTOMER_SOURCE} ...")
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.arraysize = 10_000  # fetch 10k rows/lần để tránh OOM
        sql = f"""
        SELECT CUSTOMER_ID, CUSTOMER_NAME, SEGMENT, MANAGE_BRANCH_CODE
        FROM {CUSTOMER_SOURCE}"""
        print(sql)
        cur.execute(sql)
        count = 0
        new_cache: Dict[str, Dict] = {}
        while True:
            rows = cur.fetchmany()
            if not rows:
                break
            for r in rows:
                cid = str(r[0]).strip() if r[0] is not None else None
                if cid:
                    new_cache[cid] = {
                        "CUSTOMER_NAME":      r[1],
                        "SEGMENT":            r[2],
                        "MANAGE_BRANCH_CODE": r[3],
                        "_ts_ms":             0,  # sentinel: mọi CDC event thực đều ghi đè
                    }
                    count += 1

        if count == 0:
            logger.warning(
                f"[cache_init] T24_CUSTOMER trả về 0 rows — "
                f"bảng rỗng hoặc user không có quyền SELECT?"
            )

        # Swap atomically: không dùng _customer_cache trực tiếp trong vòng lặp
        # để tránh trạng thái trung gian nếu có batch đang chạy song song
        _customer_cache = new_cache
        _cache_initialized = True
        logger.info(f"[cache_init] Loaded {count:,} customers into _customer_cache.")
    except Exception:
        logger.exception(
            "[cache_init] FATAL: Failed to load customer cache from Oracle. "
            "Kiểm tra: (1) Oracle connectivity, (2) quyền SELECT trên T24_CUSTOMER, "
            "(3) tên bảng CUSTOMER_SOURCE đúng chưa."
        )
        raise  # fail fast — không để job chạy với cache rỗng
    finally:
        conn.close()


# ─────────────────────────────────────────────
# KAFKA CDC CONSUMER — singleton, non-blocking poll
# ─────────────────────────────────────────────
def _get_cdc_consumer() -> KafkaConsumer:
    """
    Tạo hoặc trả về KafkaConsumer singleton cho T24_CUSTOMER CDC topic.

    - consumer_timeout_ms=0 : poll non-blocking — trả về ngay nếu không có message.
    - enable_auto_commit=True: offset tự commit sau mỗi lần poll → checkpoint trong Kafka.
    - group_id cố định      : tiếp tục từ offset cuối sau khi job restart.
    - value_deserializer    : raw bytes → để _apply_customer_cdc tự parse JSON.
    """
    global _cdc_consumer
    if _cdc_consumer is None:
        logger.info(f"[cdc_consumer] Creating KafkaConsumer group={CDC_CONSUMER_GROUP}")
        _cdc_consumer = KafkaConsumer(
            CUSTOMER_CDC_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CDC_CONSUMER_GROUP,
            auto_offset_reset="earliest",       # đọc từ đầu nếu group mới (sau warm-up)
            enable_auto_commit=True,
            consumer_timeout_ms=200,            # block tối đa 200ms rồi return
            value_deserializer=lambda b: b,     # raw bytes — parse trong _apply_customer_cdc
            fetch_max_bytes=52_428_800,         # 50 MB per fetch
            max_poll_records=50_000,
            # Tăng timeout để tránh broker kick consumer khi foreachBatch block lâu.
            # foreachBatch có thể mất 60-120s (ghi Oracle batch lớn).
            # max_poll_interval_ms: thời gian tối đa giữa 2 lần poll() trước khi
            #   broker coi consumer là dead và kick ra khỏi group.
            # session_timeout_ms: thời gian broker chờ heartbeat trước khi kick.
            # heartbeat_interval_ms: tần suất gửi heartbeat (nên = session_timeout/3).
            max_poll_interval_ms=600_000,       # 10 phút — đủ cho foreachBatch chậm nhất
            session_timeout_ms=120_000,          # 60 giây
            heartbeat_interval_ms=40_000,       # 20 giây = session_timeout / 3
        )
    return _cdc_consumer


def _poll_customer_cdc() -> List[Dict]:
    """
    Poll tất cả CDC events có sẵn từ T24_CUSTOMER topic (non-blocking).
    Trả về list dict: {"op": str, "after": dict|None, "before": dict|None, "ts_ms": int}
    """
    consumer = _get_cdc_consumer()
    events: List[Dict] = []

    try:
        for msg in consumer:
            if msg.value is None:
                continue
            try:
                envelope = json.loads(msg.value.decode("utf-8"))
            except Exception:
                continue

            op     = envelope.get("op")
            ts_ms  = int(envelope.get("ts_ms") or 0)
            after  = envelope.get("after")
            before = envelope.get("before")

            # after/before có thể là string (Debezium SMT flattened) hoặc dict
            if isinstance(after, str):
                try:
                    after = json.loads(after)
                except Exception:
                    after = None
            if isinstance(before, str):
                try:
                    before = json.loads(before)
                except Exception:
                    before = None

            events.append({"op": op, "after": after, "before": before, "ts_ms": ts_ms})
    except StopIteration:
        # consumer_timeout_ms reached — không còn message nào trong window
        pass
    except Exception:
        logger.exception("[cdc_poll] Lỗi khi poll T24_CUSTOMER CDC")

    return events


# ─────────────────────────────────────────────
# APPLY CDC — last-write-wins guard
# ─────────────────────────────────────────────
def _apply_customer_cdc(cdc_events: List[Dict]) -> int:
    """
    Áp dụng CDC events vào _customer_cache.

    last-write-wins: nếu ts_ms của event < ts_ms hiện tại trong cache → bỏ qua
    (bảo vệ khỏi out-of-order events do Kafka partition lag).

    Returns: số events đã apply thực sự (bỏ qua stale events).
    """
    applied = 0
    for event in cdc_events:
        op    = event.get("op")
        ts_ms = event.get("ts_ms", 0)

        if op in ("c", "u", "r"):
            payload = event.get("after") or {}
            cid = str(payload.get("CUSTOMER_ID", "") or "").strip()
            if not cid:
                continue

            existing = _customer_cache.get(cid)
            if existing and existing.get("_ts_ms", 0) > ts_ms:
                # Out-of-order: cache đã có version mới hơn → bỏ qua
                logger.debug(
                    f"[cdc_lww] Stale event skipped: CUSTOMER_ID={cid} "
                    f"event_ts={ts_ms} cache_ts={existing['_ts_ms']}"
                )
                continue

            _customer_cache[cid] = {
                "CUSTOMER_NAME":      payload.get("CUSTOMER_NAME"),
                "SEGMENT":            payload.get("SEGMENT"),
                "MANAGE_BRANCH_CODE": payload.get("MANAGE_BRANCH_CODE"),
                "_ts_ms":             ts_ms,
            }
            applied += 1

        elif op == "d":
            payload = event.get("before") or {}
            cid = str(payload.get("CUSTOMER_ID", "") or "").strip()
            if cid:
                _customer_cache.pop(cid, None)
                applied += 1

    return applied


# ─────────────────────────────────────────────
# SQL
# ─────────────────────────────────────────────
_ENRICHED_MERGE = f"""
    MERGE INTO {ENRICHED_TABLE} t
    USING (SELECT
        :TRANSACTION_ID     AS TRANSACTION_ID,
        :ACCOUNT_ID         AS ACCOUNT_ID,
        :CUSTOMER_ID        AS CUSTOMER_ID,
        :TRANSACTION_DATE   AS TRANSACTION_DATE,
        :VALUE_DATE         AS VALUE_DATE,
        :TRANSACTION_TIME   AS TRANSACTION_TIME,
        :TRANSACTION_TYPE   AS TRANSACTION_TYPE,
        :AMOUNT             AS AMOUNT,
        :CURRENCY_CODE      AS CURRENCY_CODE,
        :CHANNEL            AS CHANNEL,
        :INPUT_ID           AS INPUT_ID,
        :AUTHOR_ID          AS AUTHOR_ID,
        :BRANCH_CODE        AS BRANCH_CODE,
        :CUSTOMER_NAME      AS CUSTOMER_NAME,
        :SEGMENT            AS SEGMENT,
        :REFERENCE_NO       AS REFERENCE_NO,
        :TRANSACTION_STATUS AS TRANSACTION_STATUS
    FROM DUAL) s
    ON (t.TRANSACTION_ID = s.TRANSACTION_ID)
    WHEN MATCHED THEN UPDATE SET
        t.CUSTOMER_NAME=s.CUSTOMER_NAME,
        t.SEGMENT=s.SEGMENT,
        t.ENRICHED_AT=SYSTIMESTAMP
    WHEN NOT MATCHED THEN INSERT (
        TRANSACTION_ID, ACCOUNT_ID, CUSTOMER_ID,
        TRANSACTION_DATE, VALUE_DATE, TRANSACTION_TIME,
        TRANSACTION_TYPE, AMOUNT, CURRENCY_CODE, CHANNEL,
        INPUT_ID, AUTHOR_ID, BRANCH_CODE,
        CUSTOMER_NAME, SEGMENT, REFERENCE_NO, TRANSACTION_STATUS
    ) VALUES (
        s.TRANSACTION_ID, s.ACCOUNT_ID, s.CUSTOMER_ID,
        s.TRANSACTION_DATE, s.VALUE_DATE, s.TRANSACTION_TIME,
        s.TRANSACTION_TYPE, s.AMOUNT, s.CURRENCY_CODE, s.CHANNEL,
        s.INPUT_ID, s.AUTHOR_ID, s.BRANCH_CODE,
        s.CUSTOMER_NAME, s.SEGMENT, s.REFERENCE_NO, s.TRANSACTION_STATUS
    )
"""

# Idempotent: MERGE WHEN NOT MATCHED → skip nếu TXN đã có trong DLQ (batch retry)
_PENDING_INSERT = f"""
    MERGE INTO {PENDING_TABLE} t
    USING (SELECT :TRANSACTION_ID AS TRANSACTION_ID FROM DUAL) s
    ON (t.TRANSACTION_ID = s.TRANSACTION_ID)
    WHEN NOT MATCHED THEN INSERT (
        TRANSACTION_ID, ACCOUNT_ID, CUSTOMER_ID,
        TRANSACTION_DATE, VALUE_DATE, TRANSACTION_TIME,
        TRANSACTION_TYPE, AMOUNT, CURRENCY_CODE, CHANNEL,
        INPUT_ID, AUTHOR_ID, BRANCH_CODE, REFERENCE_NO, TRANSACTION_STATUS,
        PENDING_SINCE, RETRY_COUNT, NEXT_RETRY_AT, STATUS, ERROR_CODE, ERROR_REASON
    ) VALUES (
        :TRANSACTION_ID, :ACCOUNT_ID, :CUSTOMER_ID,
        :TRANSACTION_DATE, :VALUE_DATE, :TRANSACTION_TIME,
        :TRANSACTION_TYPE, :AMOUNT, :CURRENCY_CODE, :CHANNEL,
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

_PENDING_RETRY_INC = f"""
    UPDATE {PENDING_TABLE}
    SET RETRY_COUNT   = RETRY_COUNT + 1,
        NEXT_RETRY_AT = :NEXT_RETRY_AT
    WHERE TRANSACTION_ID = :TRANSACTION_ID
"""


# ─────────────────────────────────────────────
# BIND HELPERS
# ─────────────────────────────────────────────
def _to_enriched_bind(row: Dict, customer: Dict) -> Dict:
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
        "CUSTOMER_NAME":      customer.get("CUSTOMER_NAME"),
        "SEGMENT":            customer.get("SEGMENT"),
        "REFERENCE_NO":       row.get("REFERENCE_NO"),
        "TRANSACTION_STATUS": row.get("TRANSACTION_STATUS"),
    }


def _to_pending_bind(row: Dict, error_code: str = EC_CUSTOMER_NOT_FOUND) -> Dict:
    customer_id = row.get("CUSTOMER_ID")
    if error_code == EC_CUSTOMER_CACHE_WARMING:
        reason = "Customer cache is still warming up — full load not completed yet"
    else:
        reason = f"customer_id='{customer_id}' not found in _customer_cache"
    return {
        "TRANSACTION_ID":     row.get("TRANSACTION_ID"),
        "ACCOUNT_ID":         row.get("ACCOUNT_ID"),
        "CUSTOMER_ID":        customer_id,
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
        "REFERENCE_NO":       row.get("REFERENCE_NO"),
        "TRANSACTION_STATUS": row.get("TRANSACTION_STATUS"),
        "NEXT_RETRY_AT":      _next_retry_at(retry_count=0),
        "STATUS":             STATUS_PENDING,
        "ERROR_CODE":         error_code,
        "ERROR_REASON":       reason,
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
    """
    Trả về hàm write_batch dùng cho foreachBatch.

    Mỗi batch:
      1. Poll CDC T24_CUSTOMER (kafka-python, non-blocking) → patch _customer_cache
      2. Lấy TXN rows từ batch_df
      3. Join với _customer_cache
      4. Ghi enriched → T24_TXN_CUSTOMER_ENRICHED
      5. Ghi miss    → T24_TXN_CUSTOMER_PENDING (DLQ)
    """
    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        spark = batch_df.sparkSession
        spark.sparkContext.setJobDescription(
            f"[txn_customer_join] batch={batch_id} — cdc_apply + join + write"
        )

        # ── Step 1: Apply customer CDC delta ──────────────────────
        cdc_events = _poll_customer_cdc()
        if cdc_events:
            applied = _apply_customer_cdc(cdc_events)
            logger.info(
                f"[batch={batch_id}] CDC applied: {applied}/{len(cdc_events)} events "
                f"(cache_size={len(_customer_cache):,})"
            )

        # ── Step 2: Lấy TXN rows (chỉ insert/update) ─────────────
        batch_df.createOrReplaceTempView("txn_raw")
        txn_rows = spark.sql("""
            SELECT _op, row_data
            FROM txn_raw
            WHERE _op IN ('r','c','u') AND row_data IS NOT NULL
        """).collect()

        if not txn_rows:
            return

        logger.info(
            f"[batch={batch_id}] cache_size={len(_customer_cache):,} "
            f"cache_initialized={_cache_initialized} txn_count={len(txn_rows)}"
        )

        # ── Step 3: Join TXN với _customer_cache ─────────────────
        enriched: List[Dict] = []
        pending:  List[Dict] = []

        # Log sample key để phát hiện type/format mismatch
        if txn_rows and _cache_initialized and len(_customer_cache) > 0:
            sample_txn = json.loads(txn_rows[0]["row_data"])
            sample_cid = str(sample_txn.get("CUSTOMER_ID", "") or "").strip()
            sample_cache_key = next(iter(_customer_cache))
            # In repr() để thấy rõ whitespace, ký tự ẩn, leading zeros
            logger.info(
                f"[batch={batch_id}] KEY COMPARE: "
                f"txn_key={repr(sample_cid)} len={len(sample_cid)} | "
                f"cache_key={repr(sample_cache_key)} len={len(sample_cache_key)} | "
                f"direct_hit={sample_cid in _customer_cache} | "
                f"cache_sample_5={list(list(_customer_cache.keys())[:5])}"
            )

        for row in txn_rows:
            try:
                txn = json.loads(row["row_data"])
            except Exception:
                logger.warning(f"[batch={batch_id}] Cannot parse TXN JSON: {row['row_data']!r}")
                continue

            customer_id   = str(txn.get("CUSTOMER_ID", "") or "").strip()
            # print(customer_id)
            customer_info = _customer_cache.get(customer_id) if _cache_initialized else None

            if customer_info:
                enriched.append(_to_enriched_bind(txn, customer_info))
            else:
                error_code = EC_CUSTOMER_CACHE_WARMING if not _cache_initialized else EC_CUSTOMER_NOT_FOUND
                # logger.warning(
                #     f"[batch={batch_id}] {error_code}: "
                #     f"CUSTOMER_ID='{customer_id}' TXN={txn.get('TRANSACTION_ID')} → DLQ"
                # )
                pending.append(_to_pending_bind(txn, error_code=error_code))

        # ── Step 4-5: Ghi Oracle ──────────────────────────────────
        _write_enriched(enriched)
        _write_pending(pending)
        logger.info(
            f"[batch={batch_id}] total={len(txn_rows)} "
            f"enriched={len(enriched)} pending={len(pending)}"
        )

    return write_batch


# ─────────────────────────────────────────────
# RETRY LOGIC — public, dùng bởi tools/retry_txn_customer_dlq.py
# ─────────────────────────────────────────────
def retry_pending_once(max_retry: int = MAX_RETRY) -> Dict[str, int]:
    """
    Đọc PENDING rows từ DLQ, thử join lại với _customer_cache hiện tại.
    Nếu cache chưa init (job vừa restart) → load lại cache trước.

    Returns: {"resolved": N, "failed": N, "retrying": N}
    """
    if not _cache_initialized:
        logger.info("[retry] Cache chưa init — load lại từ Oracle trước khi retry.")
        _init_customer_cache()

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

    logger.info(f"Retry {len(pending_rows)} eligible rows (cache_size={len(_customer_cache):,})...")

    # Apply CDC mới nhất trước khi retry để cache fresh nhất có thể
    cdc_events = _poll_customer_cdc()
    if cdc_events:
        applied = _apply_customer_cdc(cdc_events)
        logger.info(f"[retry] Applied {applied} CDC events trước khi retry.")

    enriched_binds: List[Dict] = []
    resolved_ids:   List[Dict] = []
    failed_ids:     List[Dict] = []
    retry_ids:      List[Dict] = []

    for row in pending_rows:
        retry_count   = row.get("RETRY_COUNT") or 0
        customer_id   = row.get("CUSTOMER_ID")
        customer_info = _customer_cache.get(customer_id)
        bind_key      = {"TRANSACTION_ID": row["TRANSACTION_ID"]}

        if customer_info:
            enriched_binds.append({
                "TRANSACTION_ID":     row["TRANSACTION_ID"],
                "ACCOUNT_ID":         row["ACCOUNT_ID"],
                "CUSTOMER_ID":        customer_id,
                "TRANSACTION_DATE":   row["TRANSACTION_DATE"],
                "VALUE_DATE":         row["VALUE_DATE"],
                "TRANSACTION_TIME":   row["TRANSACTION_TIME"],
                "TRANSACTION_TYPE":   row["TRANSACTION_TYPE"],
                "AMOUNT":             row["AMOUNT"],
                "CURRENCY_CODE":      row["CURRENCY_CODE"],
                "CHANNEL":            row["CHANNEL"],
                "INPUT_ID":           row["INPUT_ID"],
                "AUTHOR_ID":          row["AUTHOR_ID"],
                "BRANCH_CODE":        row["BRANCH_CODE"],
                "CUSTOMER_NAME":      customer_info.get("CUSTOMER_NAME"),
                "SEGMENT":            customer_info.get("SEGMENT"),
                "REFERENCE_NO":       row["REFERENCE_NO"],
                "TRANSACTION_STATUS": row["TRANSACTION_STATUS"],
            })
            resolved_ids.append(bind_key)
        elif retry_count >= max_retry:
            logger.warning(
                f"FAILED: TXN={row['TRANSACTION_ID']} "
                f"CUSTOMER_ID='{customer_id}' retry={retry_count}/{max_retry}"
            )
            failed_ids.append(bind_key)
        else:
            next_at = _next_retry_at(retry_count + 1)
            retry_ids.append({**bind_key, "NEXT_RETRY_AT": next_at})
            logger.debug(
                f"RETRY_LATER: TXN={row['TRANSACTION_ID']} "
                f"retry={retry_count + 1}/{max_retry} next_at={next_at.isoformat()}"
            )

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
def start_stream_cdc_join(spark: SparkSession) -> StreamingQuery:
    """
    Khởi động streaming query: T24_TRANSACTIONS (Kafka) ⋈ _customer_cache (CDC-maintained)
    → T24_TXN_CUSTOMER_ENRICHED  (join thành công)
    → T24_TXN_CUSTOMER_PENDING   (DLQ: CUSTOMER_NOT_FOUND / CUSTOMER_CACHE_WARMING)

    Startup sequence:
      1. _init_customer_cache() — full load từ Oracle (~vài phút cho 10M rows)
      2. Spark streaming bắt đầu
      3. Batch đầu tiên: _poll_customer_cdc() lấy CDC events tích lũy trong thời gian warm-up
         → cache luôn fresh từ batch đầu tiên

    DLQ retry: python3 tools/retry_txn_customer_dlq.py
    """
    logger.info(
        f"[start] Initializing customer cache — topic={CUSTOMER_CDC_TOPIC} "
        f"source={CUSTOMER_SOURCE}"
    )
    _init_customer_cache()

    logger.info(f"[start] Starting TXN stream: {TXN_TOPIC}")

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
        .queryName("txn_customer_cdc_join")
        .start()
    )
