"""
retry_txn_customer_standalone.py — DLQ retry KHÔNG phụ thuộc pyspark.

Tại sao cần file này?
    retry_txn_customer_dlq.py import từ txn_customer_join.py.
    txn_customer_join.py có `from pyspark.sql import ...` ở module level.
    Python execute toàn bộ module level khi import → crash nếu không có pyspark.

    File này tự chứa hoàn toàn: chỉ dùng oracledb + stdlib.

Thay đổi so với phiên bản cũ:
    - KHÔNG dùng cache trong memory nữa.
    - Mỗi lần retry sẽ kéo dữ liệu customer trực tiếp từ T24_CUSTOMER_TARGET.
    - KHÔNG lọc theo NEXT_RETRY_AT — luôn lấy toàn bộ PENDING mỗi lần chạy.
    - Bỏ --force flag (không còn cần thiết).
    - Bỏ CDC poll / KafkaConsumer.

Usage:
    # Chạy 1 lần — retry tất cả PENDING ngay lập tức
    python3 tools/retry_txn_customer_standalone.py --once

    # Loop mỗi 5 phút
    python3 tools/retry_txn_customer_standalone.py --interval 300

    # Tăng max retry
    python3 tools/retry_txn_customer_standalone.py --once --max-retry 10

Docker:
    docker exec spark-master python3 /opt/spark/jobs/tools/retry_txn_customer_standalone.py --once
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import oracledb

# ── Config (copy từ config.py, không import để tránh phụ thuộc) ───────────────
ORACLE_HOST     = "192.168.26.180"
ORACLE_PORT     = 1521
ORACLE_SERVICE  = "dbpdb"
ORACLE_USER     = "FSS_STREAM"
ORACLE_PASSWORD = "Dapchai123"
ORACLE_DSN      = f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"
TARGET_SCHEMA   = "FSS_STREAM"

CUSTOMER_TARGET = f"{TARGET_SCHEMA}.T24_CUSTOMER_TARGET"
ENRICHED_TABLE  = f"{TARGET_SCHEMA}.T24_TXN_CUSTOMER_ENRICHED"
PENDING_TABLE   = f"{TARGET_SCHEMA}.T24_TXN_CUSTOMER_PENDING"

MAX_RETRY = 5

STATUS_PENDING  = "PENDING"
STATUS_RESOLVED = "RESOLVED"
STATUS_FAILED   = "FAILED"

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("retry_txn_customer_standalone")


# ── Helpers ───────────────────────────────────────────────────────────────────
def _get_conn() -> oracledb.Connection:
    return oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)


def _epoch_ms_to_dt(ms) -> Optional[datetime]:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).replace(tzinfo=None)
    except Exception:
        return None


# ── Fetch customer data từ T24_CUSTOMER_TARGET ────────────────────────────────
def _fetch_customers(customer_ids: List[str]) -> Dict[str, Dict]:
    """
    Query T24_CUSTOMER_TARGET cho danh sách customer_id cụ thể.
    Trả về Dict[customer_id, {CUSTOMER_NAME, SEGMENT, MANAGE_BRANCH_CODE}].
    """
    if not customer_ids:
        return {}

    conn = _get_conn()
    try:
        cur = conn.cursor()
        # Dùng IN clause với bind variables — Oracle giới hạn 1000 items/IN
        # Chia batch 999 để an toàn
        result: Dict[str, Dict] = {}
        batch_size = 999
        for i in range(0, len(customer_ids), batch_size):
            batch = customer_ids[i : i + batch_size]
            placeholders = ", ".join(f":id{j}" for j in range(len(batch)))
            binds = {f"id{j}": v for j, v in enumerate(batch)}
            cur.execute(
                f"SELECT CUSTOMER_ID, CUSTOMER_NAME, SEGMENT, MANAGE_BRANCH_CODE"
                f" FROM {CUSTOMER_TARGET}"
                f" WHERE CUSTOMER_ID IN ({placeholders})",
                binds,
            )
            for row in cur.fetchall():
                cid = str(row[0]).strip() if row[0] is not None else None
                if cid:
                    result[cid] = {
                        "CUSTOMER_NAME":      row[1],
                        "SEGMENT":            row[2],
                        "MANAGE_BRANCH_CODE": row[3],
                    }
        return result
    finally:
        conn.close()


# ── SQL ───────────────────────────────────────────────────────────────────────
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
    SET RETRY_COUNT = RETRY_COUNT + 1
    WHERE TRANSACTION_ID = :TRANSACTION_ID
"""


# ── Core retry logic ──────────────────────────────────────────────────────────
def retry_pending_once(max_retry: int = MAX_RETRY) -> Dict[str, int]:
    """
    Đọc toàn bộ PENDING rows → query customer từ T24_CUSTOMER_TARGET → retry join.

    Không dùng backoff window: mỗi lần chạy lấy hết STATUS = 'PENDING'.
    """
    # ── Đọc toàn bộ PENDING rows ──────────────────────────────────────────────
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
            ORDER BY PENDING_SINCE
        """)
        cols         = [d[0] for d in cur.description]
        pending_rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()

    if not pending_rows:
        log.info("DLQ: không có row PENDING nào.")
        return {"resolved": 0, "failed": 0, "retrying": 0}

    log.info(f"Đang retry {len(pending_rows)} PENDING rows...")

    # ── Fetch customer data từ T24_CUSTOMER_TARGET (1 query cho tất cả) ───────
    unique_customer_ids = list({
        str(r.get("CUSTOMER_ID") or "").strip()
        for r in pending_rows
        if r.get("CUSTOMER_ID")
    })
    log.info(f"Query {len(unique_customer_ids)} customer_id từ {CUSTOMER_TARGET}...")
    customer_map = _fetch_customers(unique_customer_ids)
    log.info(f"Tìm thấy {len(customer_map)}/{len(unique_customer_ids)} customers.")

    # ── Phân loại rows ────────────────────────────────────────────────────────
    enriched_binds: List[Dict] = []
    resolved_ids:   List[Dict] = []
    failed_ids:     List[Dict] = []
    retry_ids:      List[Dict] = []

    for row in pending_rows:
        retry_count   = row.get("RETRY_COUNT") or 0
        customer_id   = str(row.get("CUSTOMER_ID") or "").strip()
        customer_info = customer_map.get(customer_id)
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
            log.debug(f"RESOLVED: TXN={row['TRANSACTION_ID']} CUSTOMER_ID='{customer_id}'")
        elif retry_count >= max_retry:
            log.warning(
                f"FAILED: TXN={row['TRANSACTION_ID']} "
                f"CUSTOMER_ID='{customer_id}' retry={retry_count}/{max_retry}"
            )
            failed_ids.append(bind_key)
        else:
            retry_ids.append(bind_key)
            log.debug(
                f"RETRY_LATER: TXN={row['TRANSACTION_ID']} "
                f"CUSTOMER_ID='{customer_id}' retry={retry_count + 1}/{max_retry}"
            )

    # ── Ghi enriched ──────────────────────────────────────────────────────────
    if enriched_binds:
        conn = _get_conn()
        try:
            conn.cursor().executemany(_ENRICHED_MERGE, enriched_binds)
            conn.commit()
            log.info(f"Enriched {len(enriched_binds)} rows → {ENRICHED_TABLE}")
        except Exception:
            conn.rollback()
            log.exception("Lỗi ghi enriched")
            raise
        finally:
            conn.close()

    # ── Update DLQ status ─────────────────────────────────────────────────────
    if resolved_ids or failed_ids or retry_ids:
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
            log.exception("Lỗi update DLQ status")
            raise
        finally:
            conn2.close()

    result = {"resolved": len(resolved_ids), "failed": len(failed_ids), "retrying": len(retry_ids)}
    log.info(f"Retry done: {result}")
    return result


# ── CLI ───────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Retry T24_TXN_CUSTOMER_PENDING (no pyspark)")
    parser.add_argument("--max-retry", type=int, default=MAX_RETRY,
                        help=f"Số lần retry tối đa trước khi mark FAILED (default: {MAX_RETRY})")
    parser.add_argument("--once", action="store_true",
                        help="Chạy 1 lần rồi thoát")
    parser.add_argument("--interval", type=int, default=0, metavar="SECONDS",
                        help="Loop mode: chạy lặp mỗi N giây")
    args = parser.parse_args()

    if args.interval > 0 and not args.once:
        log.info(f"Loop mode: interval={args.interval}s, max_retry={args.max_retry}")
        while True:
            try:
                retry_pending_once(max_retry=args.max_retry)
            except Exception:
                log.exception("Retry iteration failed — sẽ thử lại sau")
            time.sleep(args.interval)
    else:
        result = retry_pending_once(max_retry=args.max_retry)
        log.info(
            f"Done — resolved={result['resolved']} "
            f"failed={result['failed']} retrying={result['retrying']}"
        )
        if result["failed"] > 0:
            log.warning(
                f"{result['failed']} record(s) exceeded max_retry={args.max_retry} → FAILED. "
                f"Query: SELECT * FROM {PENDING_TABLE} WHERE STATUS='FAILED'"
            )


if __name__ == "__main__":
    main()
