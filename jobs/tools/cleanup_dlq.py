"""
cleanup_dlq.py — Xóa DLQ rows đã xử lý xong (RESOLVED / FAILED) cũ hơn TTL.

Chạy định kỳ qua cron hoặc Airflow để tránh table bloat.
Không xóa PENDING — những rows này cần được retry trước.

Usage:
    python3 tools/cleanup_dlq.py                       # TTL mặc định 30 ngày, dry-run OFF
    python3 tools/cleanup_dlq.py --ttl-days 7          # giữ 7 ngày gần nhất
    python3 tools/cleanup_dlq.py --dry-run             # chỉ đếm, không xóa
    python3 tools/cleanup_dlq.py --table txn_branch    # chỉ clean 1 DLQ
    python3 tools/cleanup_dlq.py --table txn_acct      # chỉ clean 1 DLQ

Cron example (mỗi ngày 2AM):
    0 2 * * * /usr/bin/python3 /opt/spark/jobs/tools/cleanup_dlq.py --ttl-days 30

DLQ tables:
    T24_TXN_PENDING_JOIN       — static_join (TXN ⋈ BRANCH)
    T24_TXN_ACCT_PENDING_JOIN  — stream_join (TXN ⋈ ACCOUNT)
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import oracledb
from config import ORACLE_DSN, ORACLE_USER, ORACLE_PASSWORD, TARGET_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("cleanup_dlq")

# ─────────────────────────────────────────────
# DLQ TABLE REGISTRY
# key → (table_name, resolved_at_col)
# ─────────────────────────────────────────────
DLQ_TABLES = {
    "txn_branch": f"{TARGET_SCHEMA}.T24_TXN_PENDING_JOIN",
    "txn_acct":   f"{TARGET_SCHEMA}.T24_TXN_ACCT_PENDING_JOIN",
}

DEFAULT_TTL_DAYS = 30

# STATUS values eligible for cleanup
CLEANUP_STATUSES = ("RESOLVED", "FAILED")


def _get_conn() -> oracledb.Connection:
    return oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)


def _count_eligible(cur, table: str, ttl_days: int) -> dict:
    """Đếm rows eligible theo từng status."""
    result = {}
    for status in CLEANUP_STATUSES:
        cur.execute(f"""
            SELECT COUNT(*) FROM {table}
            WHERE STATUS = :1
              AND RESOLVED_AT < SYSTIMESTAMP - INTERVAL :2 DAY
        """, [status, ttl_days])
        result[status] = cur.fetchone()[0]
    return result


def _delete_eligible(conn, table: str, ttl_days: int) -> dict:
    """Xóa rows RESOLVED/FAILED cũ hơn ttl_days. Returns số rows đã xóa per status."""
    deleted = {}
    cur = conn.cursor()
    for status in CLEANUP_STATUSES:
        cur.execute(f"""
            DELETE FROM {table}
            WHERE STATUS = :1
              AND RESOLVED_AT < SYSTIMESTAMP - INTERVAL :2 DAY
        """, [status, ttl_days])
        deleted[status] = cur.rowcount
    conn.commit()
    return deleted


def _report_pending_summary(cur, table: str) -> None:
    """In tóm tắt PENDING còn lại — để xác nhận không xóa nhầm."""
    cur.execute(f"""
        SELECT ERROR_CODE, COUNT(*) AS CNT
        FROM {table}
        WHERE STATUS = 'PENDING'
        GROUP BY ERROR_CODE
        ORDER BY CNT DESC
    """)
    rows = cur.fetchall()
    if rows:
        log.info(f"  {table}: PENDING còn lại:")
        for r in rows:
            log.info(f"    ERROR_CODE={r[0]}  count={r[1]:,}")
    else:
        log.info(f"  {table}: PENDING = 0 (DLQ sạch)")


def run_cleanup(ttl_days: int, dry_run: bool, table_filter: str | None) -> None:
    t0 = time.time()
    targets = (
        {table_filter: DLQ_TABLES[table_filter]}
        if table_filter
        else DLQ_TABLES
    )

    log.info(
        "DLQ Cleanup bắt đầu | TTL=%d ngày | dry_run=%s | tables=%s",
        ttl_days, dry_run, list(targets.keys()),
    )

    conn = _get_conn()
    try:
        cur = conn.cursor()
        total_deleted = 0

        for key, table in targets.items():
            counts = _count_eligible(cur, table, ttl_days)
            eligible = sum(counts.values())
            log.info(
                "[%s] %s — eligible to delete: RESOLVED=%s FAILED=%s (total=%s)",
                key, table,
                f"{counts.get('RESOLVED', 0):,}",
                f"{counts.get('FAILED', 0):,}",
                f"{eligible:,}",
            )

            if dry_run:
                log.info("[%s] Dry-run — bỏ qua xóa.", key)
            elif eligible > 0:
                deleted = _delete_eligible(conn, table, ttl_days)
                n = sum(deleted.values())
                total_deleted += n
                log.info(
                    "[%s] Đã xóa: RESOLVED=%s FAILED=%s (total=%s)",
                    key,
                    f"{deleted.get('RESOLVED', 0):,}",
                    f"{deleted.get('FAILED', 0):,}",
                    f"{n:,}",
                )
            else:
                log.info("[%s] Không có row nào cần xóa.", key)

            _report_pending_summary(cur, table)

    finally:
        conn.close()

    elapsed = time.time() - t0
    if dry_run:
        log.info("Dry-run hoàn thành (%.1fs) — không có row nào bị xóa.", elapsed)
    else:
        log.info("Cleanup hoàn thành (%.1fs) — tổng xóa: %s rows.", elapsed, f"{total_deleted:,}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Xóa DLQ rows RESOLVED/FAILED cũ hơn TTL"
    )
    parser.add_argument(
        "--ttl-days", type=int, default=DEFAULT_TTL_DAYS,
        help=f"Số ngày giữ lại rows đã xử lý (default: {DEFAULT_TTL_DAYS})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Chỉ đếm rows eligible, không xóa",
    )
    parser.add_argument(
        "--table", type=str, default=None,
        choices=list(DLQ_TABLES.keys()),
        help="Chỉ clean 1 DLQ table (default: tất cả)",
    )
    args = parser.parse_args()

    if args.ttl_days < 1:
        parser.error("--ttl-days phải >= 1")

    run_cleanup(
        ttl_days=args.ttl_days,
        dry_run=args.dry_run,
        table_filter=args.table,
    )


if __name__ == "__main__":
    main()
