"""
bootstrap_txn_acct.py — Batch bootstrap cho T24_TXN_ACCOUNT_SNAPSHOT.

Chạy MỘT LẦN trước khi bật streaming job txn_acct_join.py.
Đọc T24_TRANSACTIONS và T24_ACCOUNT trực tiếp từ Oracle, join tĩnh,
ghi kết quả vào T24_TXN_ACCOUNT_SNAPSHOT.

Sau khi bootstrap xong, bật streaming với startingOffsets=latest —
job chỉ xử lý giao dịch mới, không replay lại lịch sử.

Production pattern (LinkedIn / Artie):
    Phase 1: bootstrap_txn_acct.py  → populate historical snapshot
    Phase 2: txn_acct_join.py       → stream delta từ latest offset

Usage:
    python3 tools/bootstrap_txn_acct.py
    python3 tools/bootstrap_txn_acct.py --batch-size 5000
    python3 tools/bootstrap_txn_acct.py --dry-run       # đếm rows, không ghi
    python3 tools/bootstrap_txn_acct.py --since 2024-01-01  # chỉ TXN từ ngày này

Chú ý:
    - Script có thể chạy lại an toàn: dùng MERGE (idempotent)
    - Nếu bị ngắt giữa chừng, chạy lại từ đầu hoặc dùng --since để giới hạn range
    - Theo dõi tiến trình qua log — mỗi batch in ra row count và thời gian
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import oracledb
from config import (
    ORACLE_DSN, ORACLE_USER, ORACLE_PASSWORD, TARGET_SCHEMA,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("bootstrap_txn_acct")

SNAPSHOT_TABLE = f"{TARGET_SCHEMA}.T24_TXN_ACCOUNT_SNAPSHOT"
TXN_SOURCE     = f"{TARGET_SCHEMA}.T24_TRANSACTIONS"
ACCT_SOURCE    = f"{TARGET_SCHEMA}.T24_ACCOUNT"

DEFAULT_BATCH_SIZE = 2000


def _get_conn() -> oracledb.Connection:
    return oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)


def _epoch_ms_to_dt(ms) -> Optional[datetime]:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).replace(tzinfo=None)
    except Exception:
        return None


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
        :TXN_TS_MS              AS TXN_TS_MS,
        :ACCT_TS_MS             AS ACCT_TS_MS
    FROM DUAL) s
    ON (t.TRANSACTION_ID = s.TRANSACTION_ID)
    WHEN MATCHED THEN UPDATE SET
        t.BALANCE_AT_TXN=s.BALANCE_AT_TXN,
        t.WORKING_BALANCE_AT_TXN=s.WORKING_BALANCE_AT_TXN,
        t.SNAPSHOT_AT=SYSTIMESTAMP
    WHEN NOT MATCHED THEN INSERT (
        TRANSACTION_ID, ACCOUNT_ID, CUSTOMER_ID,
        TRANSACTION_DATE, VALUE_DATE, TRANSACTION_TIME,
        TRANSACTION_TYPE, AMOUNT, CURRENCY_CODE, CHANNEL,
        BRANCH_CODE, REFERENCE_NO, TRANSACTION_STATUS,
        BALANCE_AT_TXN, WORKING_BALANCE_AT_TXN, ACCT_CURRENCY_CODE,
        TXN_TS_MS, ACCT_TS_MS
    ) VALUES (
        s.TRANSACTION_ID, s.ACCOUNT_ID, s.CUSTOMER_ID,
        s.TRANSACTION_DATE, s.VALUE_DATE, s.TRANSACTION_TIME,
        s.TRANSACTION_TYPE, s.AMOUNT, s.CURRENCY_CODE, s.CHANNEL,
        s.BRANCH_CODE, s.REFERENCE_NO, s.TRANSACTION_STATUS,
        s.BALANCE_AT_TXN, s.WORKING_BALANCE_AT_TXN, s.ACCT_CURRENCY_CODE,
        s.TXN_TS_MS, s.ACCT_TS_MS
    )
"""


def _load_account_map(conn: oracledb.Connection) -> Dict[str, Dict]:
    """Load toàn bộ T24_ACCOUNT vào memory — dùng để join tĩnh."""
    cur = conn.cursor()
    cur.execute(f"""
        SELECT ACCOUNT_ID, CUSTOMER_ID, WORKING_BALANCE,
               ONLINE_ACTUAL_BAL, CURRENCY_CODE
        FROM {ACCT_SOURCE}
    """)
    result = {
        r[0]: {
            "CUSTOMER_ID":       r[1],
            "WORKING_BALANCE":   r[2],
            "ONLINE_ACTUAL_BAL": r[3],
            "CURRENCY_CODE":     r[4],
        }
        for r in cur.fetchall()
    }
    log.info(f"Loaded {len(result)} accounts from {ACCT_SOURCE}")
    return result


def _count_txn(conn: oracledb.Connection, since_date: Optional[str]) -> int:
    cur = conn.cursor()
    if since_date:
        cur.execute(
            f"SELECT COUNT(*) FROM {TXN_SOURCE} WHERE TRANSACTION_DATE >= TO_DATE(:1,'YYYY-MM-DD')",
            [since_date],
        )
    else:
        cur.execute(f"SELECT COUNT(*) FROM {TXN_SOURCE}")
    return cur.fetchone()[0]


def _fetch_txn_page(
    conn: oracledb.Connection,
    offset: int,
    batch_size: int,
    since_date: Optional[str],
):
    """Lấy một page TXN theo ROWNUM pagination."""
    cur = conn.cursor()
    date_filter = (
        f"AND TRANSACTION_DATE >= TO_DATE('{since_date}','YYYY-MM-DD')"
        if since_date else ""
    )
    cur.execute(f"""
        SELECT TRANSACTION_ID, ACCOUNT_ID, CUSTOMER_ID,
               TRANSACTION_DATE, VALUE_DATE, TRANSACTION_TIME,
               TRANSACTION_TYPE, AMOUNT, CURRENCY_CODE,
               CHANNEL, BRANCH_CODE, REFERENCE_NO, TRANSACTION_STATUS
        FROM (
            SELECT t.*, ROWNUM AS RN
            FROM {TXN_SOURCE} t
            WHERE ROWNUM <= :end_row {date_filter}
            ORDER BY TRANSACTION_ID
        )
        WHERE RN > :start_row
    """, {"end_row": offset + batch_size, "start_row": offset})
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _build_bind(txn: Dict, acct: Dict) -> Dict:
    return {
        "TRANSACTION_ID":         txn["TRANSACTION_ID"],
        "ACCOUNT_ID":             txn["ACCOUNT_ID"],
        "CUSTOMER_ID":            txn.get("CUSTOMER_ID") or acct.get("CUSTOMER_ID"),
        "TRANSACTION_DATE":       txn.get("TRANSACTION_DATE"),
        "VALUE_DATE":             txn.get("VALUE_DATE"),
        "TRANSACTION_TIME":       txn.get("TRANSACTION_TIME"),
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
        "TXN_TS_MS":              None,
        "ACCT_TS_MS":             None,
    }


def run_bootstrap(batch_size: int, dry_run: bool, since_date: Optional[str]) -> None:
    t0 = time.time()
    conn = _get_conn()
    try:
        total = _count_txn(conn, since_date)
        log.info(f"Total TXN to bootstrap: {total} (since={since_date or 'ALL'})")

        if dry_run:
            log.info("Dry-run mode — không ghi vào DB.")
            return

        acct_map = _load_account_map(conn)
    finally:
        conn.close()

    offset          = 0
    total_written   = 0
    total_skipped   = 0   # TXN không có ACCOUNT tương ứng

    while offset < total:
        t_batch = time.time()
        conn2   = _get_conn()
        try:
            rows = _fetch_txn_page(conn2, offset, batch_size, since_date)
            if not rows:
                break

            binds:   List[Dict] = []
            skipped: List[str]  = []

            for txn in rows:
                acct = acct_map.get(txn["ACCOUNT_ID"])
                if acct:
                    binds.append(_build_bind(txn, acct))
                else:
                    skipped.append(txn["TRANSACTION_ID"])

            if binds:
                cur = conn2.cursor()
                cur.executemany(_SNAPSHOT_MERGE, binds)
                conn2.commit()

            total_written += len(binds)
            total_skipped += len(skipped)
            elapsed = time.time() - t_batch

            log.info(
                f"Batch offset={offset} size={len(rows)} "
                f"written={len(binds)} skipped={len(skipped)} "
                f"elapsed={elapsed:.1f}s"
            )
            if skipped:
                log.warning(f"  Skipped (no ACCOUNT): {skipped[:5]}{'...' if len(skipped)>5 else ''}")

        except Exception:
            conn2.rollback()
            log.exception(f"Lỗi tại offset={offset} — dừng bootstrap")
            raise
        finally:
            conn2.close()

        offset += batch_size

    total_elapsed = time.time() - t0
    log.info(
        f"Bootstrap hoàn thành: total={total} written={total_written} "
        f"skipped={total_skipped} elapsed={total_elapsed:.1f}s"
    )
    if total_skipped > 0:
        log.warning(
            f"{total_skipped} TXN bị skip vì ACCOUNT_ID không có trong T24_ACCOUNT. "
            f"Đây là dữ liệu orphan — kiểm tra data quality."
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bootstrap T24_TXN_ACCOUNT_SNAPSHOT từ dữ liệu lịch sử Oracle"
    )
    parser.add_argument(
        "--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
        help=f"Số rows mỗi batch MERGE (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Chỉ đếm rows, không ghi vào DB",
    )
    parser.add_argument(
        "--since", type=str, default=None, metavar="YYYY-MM-DD",
        help="Chỉ bootstrap TXN từ ngày này trở đi (giảm thời gian chạy lần đầu)",
    )
    args = parser.parse_args()
    run_bootstrap(
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        since_date=args.since,
    )


if __name__ == "__main__":
    main()
