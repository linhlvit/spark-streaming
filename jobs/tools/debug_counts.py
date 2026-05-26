"""
debug_counts.py — Thống kê row counts và DLQ metrics cho toàn bộ pipeline.

Chạy:
    python3 tools/debug_counts.py                  # row counts + DLQ summary
    python3 tools/debug_counts.py --detail         # thêm sample rows
    python3 tools/debug_counts.py --missing        # TXN chưa có trong snapshot
    python3 tools/debug_counts.py --dlq-detail     # top error codes + retry rate
"""

import argparse
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import oracledb
from config import ORACLE_DSN, ORACLE_USER, ORACLE_PASSWORD, TARGET_SCHEMA

S = TARGET_SCHEMA


def get_conn():
    return oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)


def section(title: str):
    print(f"\n{'═' * 65}")
    print(f"  {title}")
    print(f"{'═' * 65}")


def _scalar(cur, sql: str, params=None):
    cur.execute(sql, params or [])
    row = cur.fetchone()
    return row[0] if row else 0


# ─────────────────────────────────────────────
# SECTION 1 — ROW COUNTS (tổng quan pipeline)
# ─────────────────────────────────────────────
def print_row_counts(cur):
    section("ROW COUNTS — NGUỒN VÀ TARGET")
    tables = [
        # Nguồn (CDC mirror)
        ("T24_TRANSACTIONS",           f"SELECT COUNT(*) FROM {S}.T24_TRANSACTIONS"),
        ("T24_ACCOUNT",                f"SELECT COUNT(*) FROM {S}.T24_ACCOUNT"),
        ("T24_CUSTOMER",               f"SELECT COUNT(*) FROM {S}.T24_CUSTOMER"),
        ("T24_BRANCH",                 f"SELECT COUNT(*) FROM {S}.T24_BRANCH"),
        # Target sync
        ("T24_TRANSACTIONS_TARGET",    f"SELECT COUNT(*) FROM {S}.T24_TRANSACTIONS_TARGET"),
        ("T24_ACCOUNT_TARGET",         f"SELECT COUNT(*) FROM {S}.T24_ACCOUNT_TARGET"),
        ("T24_CUSTOMER_TARGET",        f"SELECT COUNT(*) FROM {S}.T24_CUSTOMER_TARGET"),
        ("T24_BRANCH_TARGET",          f"SELECT COUNT(*) FROM {S}.T24_BRANCH_TARGET"),
        # Enriched / Snapshot
        ("T24_TXN_ENRICHED",           f"SELECT COUNT(*) FROM {S}.T24_TXN_ENRICHED"),
        ("T24_TXN_ACCOUNT_SNAPSHOT",   f"SELECT COUNT(*) FROM {S}.T24_TXN_ACCOUNT_SNAPSHOT"),
        # DLQ
        ("T24_TXN_PENDING_JOIN",       f"SELECT COUNT(*) FROM {S}.T24_TXN_PENDING_JOIN"),
        ("T24_TXN_ACCT_PENDING_JOIN",  f"SELECT COUNT(*) FROM {S}.T24_TXN_ACCT_PENDING_JOIN"),
    ]
    print(f"  {'Bảng':<35} {'Rows':>12}")
    print(f"  {'-'*35} {'-'*12}")
    prev_group = None
    groups = {
        "T24_TRANSACTIONS": "nguồn",
        "T24_ACCOUNT": "nguồn",
        "T24_CUSTOMER": "nguồn",
        "T24_BRANCH": "nguồn",
        "T24_TRANSACTIONS_TARGET": "target",
        "T24_ACCOUNT_TARGET": "target",
        "T24_CUSTOMER_TARGET": "target",
        "T24_BRANCH_TARGET": "target",
        "T24_TXN_ENRICHED": "enriched",
        "T24_TXN_ACCOUNT_SNAPSHOT": "enriched",
        "T24_TXN_PENDING_JOIN": "dlq",
        "T24_TXN_ACCT_PENDING_JOIN": "dlq",
    }
    for name, sql in tables:
        grp = groups.get(name)
        if grp != prev_group:
            print()
            prev_group = grp
        try:
            cur.execute(sql)
            count = cur.fetchone()[0]
            print(f"  {name:<35} {count:>12,}")
        except Exception as e:
            print(f"  {name:<35} {'ERROR: ' + str(e):>12}")


# ─────────────────────────────────────────────
# SECTION 2 — DLQ SUMMARY
# ─────────────────────────────────────────────
def print_dlq_summary(cur):
    section("DLQ SUMMARY — STATIC JOIN (T24_TXN_PENDING_JOIN)")
    _print_dlq_table(cur, f"{S}.T24_TXN_PENDING_JOIN")

    section("DLQ SUMMARY — STREAM JOIN (T24_TXN_ACCT_PENDING_JOIN)")
    _print_dlq_table(cur, f"{S}.T24_TXN_ACCT_PENDING_JOIN")


def _print_dlq_table(cur, table: str):
    # Breakdown STATUS × ERROR_CODE
    cur.execute(f"""
        SELECT STATUS, ERROR_CODE, COUNT(*) AS CNT
        FROM {table}
        GROUP BY STATUS, ERROR_CODE
        ORDER BY STATUS, CNT DESC
    """)
    rows = cur.fetchall()
    if not rows:
        print("  (trống)")
        return

    print(f"  {'STATUS':<12} {'ERROR_CODE':<30} {'COUNT':>10}")
    print(f"  {'-'*12} {'-'*30} {'-'*10}")
    for r in rows:
        print(f"  {str(r[0]):<12} {str(r[1] or '-'):<30} {r[2]:>10,}")

    # Retry rate cho PENDING
    cur.execute(f"""
        SELECT
            COUNT(*)                                        AS total_pending,
            SUM(CASE WHEN RETRY_COUNT = 0 THEN 1 ELSE 0 END) AS first_attempt,
            SUM(CASE WHEN RETRY_COUNT > 0 THEN 1 ELSE 0 END) AS retried,
            MAX(RETRY_COUNT)                                AS max_retry_seen,
            ROUND(AVG(RETRY_COUNT), 1)                     AS avg_retry
        FROM {table}
        WHERE STATUS = 'PENDING'
    """)
    r = cur.fetchone()
    if r and r[0]:
        print(f"\n  PENDING retry stats:")
        print(f"    Total PENDING   : {r[0]:,}")
        print(f"    First attempt   : {r[1]:,}")
        print(f"    Already retried : {r[2]:,}")
        print(f"    Max retry_count : {r[3]}")
        print(f"    Avg retry_count : {r[4]}")

    # Resolve rate tổng thể
    cur.execute(f"""
        SELECT
            COUNT(*)                                                AS total,
            SUM(CASE WHEN STATUS = 'RESOLVED' THEN 1 ELSE 0 END)  AS resolved,
            SUM(CASE WHEN STATUS = 'FAILED'   THEN 1 ELSE 0 END)  AS failed,
            SUM(CASE WHEN STATUS = 'PENDING'  THEN 1 ELSE 0 END)  AS pending
        FROM {table}
    """)
    r = cur.fetchone()
    total = r[0] or 0
    if total > 0:
        resolved_pct = round(r[1] / total * 100, 1)
        failed_pct   = round(r[2] / total * 100, 1)
        pending_pct  = round(r[3] / total * 100, 1)
        print(f"\n  Resolve rate (all-time):")
        print(f"    Total    : {total:,}")
        print(f"    RESOLVED : {r[1]:,}  ({resolved_pct}%)")
        print(f"    FAILED   : {r[2]:,}  ({failed_pct}%)")
        print(f"    PENDING  : {r[3]:,}  ({pending_pct}%)")

    # Oldest PENDING — cảnh báo stuck records
    cur.execute(f"""
        SELECT MIN(PENDING_SINCE) FROM {table} WHERE STATUS = 'PENDING'
    """)
    oldest = cur.fetchone()[0]
    if oldest:
        age_hours = (datetime.now() - oldest).total_seconds() / 3600
        marker = "  ⚠ >24h" if age_hours > 24 else ""
        print(f"\n  Oldest PENDING  : {oldest}  ({age_hours:.1f}h ago){marker}")


# ─────────────────────────────────────────────
# SECTION 3 — DLQ DETAIL (top error codes)
# ─────────────────────────────────────────────
def print_dlq_detail(cur):
    section("DLQ DETAIL — TOP ERROR_CODE (30 ngày gần nhất)")
    for label, table in [
        ("static_join / T24_TXN_PENDING_JOIN",      f"{S}.T24_TXN_PENDING_JOIN"),
        ("stream_join / T24_TXN_ACCT_PENDING_JOIN", f"{S}.T24_TXN_ACCT_PENDING_JOIN"),
    ]:
        print(f"\n  [{label}]")
        cur.execute(f"""
            SELECT ERROR_CODE, STATUS, COUNT(*) AS CNT
            FROM {table}
            WHERE PENDING_SINCE >= SYSTIMESTAMP - INTERVAL '30' DAY
            GROUP BY ERROR_CODE, STATUS
            ORDER BY CNT DESC
            FETCH FIRST 10 ROWS ONLY
        """)
        rows = cur.fetchall()
        if not rows:
            print("    (không có row nào trong 30 ngày)")
            continue
        print(f"    {'ERROR_CODE':<30} {'STATUS':<12} {'COUNT':>8}")
        print(f"    {'-'*30} {'-'*12} {'-'*8}")
        for r in rows:
            print(f"    {str(r[0] or '-'):<30} {str(r[1]):<12} {r[2]:>8,}")


# ─────────────────────────────────────────────
# SECTION 4 — MISSING (TXN chưa có snapshot)
# ─────────────────────────────────────────────
def print_missing(cur, show_rows: bool):
    section("MISSING — TXN CHƯA CÓ TRONG T24_TXN_ACCOUNT_SNAPSHOT")
    cur.execute(f"""
        SELECT COUNT(*) FROM {S}.T24_TRANSACTIONS_TARGET t
        WHERE NOT EXISTS (
            SELECT 1 FROM {S}.T24_TXN_ACCOUNT_SNAPSHOT s
            WHERE s.TRANSACTION_ID = t.TRANSACTION_ID
        )
    """)
    missing_count = cur.fetchone()[0]
    print(f"  Transactions chưa có snapshot: {missing_count:,}")

    if show_rows and missing_count > 0:
        cur.execute(f"""
            SELECT t.TRANSACTION_ID, t.ACCOUNT_ID, t.TRANSACTION_DATE,
                   t.TRANSACTION_STATUS, t.CREATED_AT
            FROM {S}.T24_TRANSACTIONS_TARGET t
            WHERE NOT EXISTS (
                SELECT 1 FROM {S}.T24_TXN_ACCOUNT_SNAPSHOT s
                WHERE s.TRANSACTION_ID = t.TRANSACTION_ID
            )
            ORDER BY t.TRANSACTION_DATE DESC
            FETCH FIRST 20 ROWS ONLY
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        print(f"\n  {' | '.join(f'{c:<20}' for c in cols)}")
        print(f"  {'-' * (23 * len(cols))}")
        for r in rows:
            print(f"  {' | '.join(f'{str(v):<20}' for v in r)}")

    # MISSING vì đang trong DLQ
    cur.execute(f"""
        SELECT COUNT(*) FROM {S}.T24_TXN_ACCT_PENDING_JOIN
        WHERE STATUS = 'PENDING'
    """)
    in_dlq = cur.fetchone()[0]
    print(f"  Trong DLQ (PENDING, chờ retry): {in_dlq:,}")


# ─────────────────────────────────────────────
# SECTION 5 — SNAPSHOT TIMING
# ─────────────────────────────────────────────
def print_snapshot_timing(cur):
    section("SNAPSHOT — THỜI GIAN GHI (T24_TXN_ACCOUNT_SNAPSHOT)")
    cur.execute(f"""
        SELECT
            MIN(SNAPSHOT_AT)            AS first_write,
            MAX(SNAPSHOT_AT)            AS last_write,
            COUNT(DISTINCT ACCOUNT_ID)  AS distinct_accounts,
            COUNT(DISTINCT CUSTOMER_ID) AS distinct_customers
        FROM {S}.T24_TXN_ACCOUNT_SNAPSHOT
    """)
    r = cur.fetchone()
    print(f"  First write        : {r[0]}")
    print(f"  Last write         : {r[1]}")
    print(f"  Distinct accounts  : {r[2]:,}")
    print(f"  Distinct customers : {r[3]:,}")


# ─────────────────────────────────────────────
# SECTION 6 — SAMPLE ROWS
# ─────────────────────────────────────────────
def print_samples(cur):
    section("SAMPLE — T24_TXN_ACCOUNT_SNAPSHOT (10 rows mới nhất)")
    cur.execute(f"""
        SELECT TRANSACTION_ID, ACCOUNT_ID, CUSTOMER_ID, TRANSACTION_DATE,
               AMOUNT, BALANCE_AT_TXN, WORKING_BALANCE_AT_TXN, SNAPSHOT_AT
        FROM {S}.T24_TXN_ACCOUNT_SNAPSHOT
        ORDER BY SNAPSHOT_AT DESC
        FETCH FIRST 10 ROWS ONLY
    """)
    cols = [d[0] for d in cur.description]
    print("  " + " | ".join(f"{c:<22}" for c in cols))
    print("  " + "-" * (25 * len(cols)))
    for r in cur.fetchall():
        print("  " + " | ".join(f"{str(v):<22}" for v in r))

    section("SAMPLE — T24_TXN_PENDING_JOIN (10 rows mới nhất)")
    cur.execute(f"""
        SELECT TRANSACTION_ID, BRANCH_CODE, STATUS, ERROR_CODE, RETRY_COUNT, PENDING_SINCE
        FROM {S}.T24_TXN_PENDING_JOIN
        ORDER BY PENDING_SINCE DESC
        FETCH FIRST 10 ROWS ONLY
    """)
    cols = [d[0] for d in cur.description]
    print("  " + " | ".join(f"{c:<22}" for c in cols))
    print("  " + "-" * (25 * len(cols)))
    for r in cur.fetchall():
        print("  " + " | ".join(f"{str(v):<22}" for v in r))

    section("SAMPLE — T24_TXN_ACCT_PENDING_JOIN (10 rows mới nhất)")
    cur.execute(f"""
        SELECT TRANSACTION_ID, ACCOUNT_ID, STATUS, ERROR_CODE, RETRY_COUNT, PENDING_SINCE
        FROM {S}.T24_TXN_ACCT_PENDING_JOIN
        ORDER BY PENDING_SINCE DESC
        FETCH FIRST 10 ROWS ONLY
    """)
    cols = [d[0] for d in cur.description]
    print("  " + " | ".join(f"{c:<22}" for c in cols))
    print("  " + "-" * (25 * len(cols)))
    for r in cur.fetchall():
        print("  " + " | ".join(f"{str(v):<22}" for v in r))


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def run(detail: bool, missing: bool, dlq_detail: bool) -> None:
    conn = get_conn()
    cur  = conn.cursor()
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] DEBUG COUNTS — pipeline CDC Oracle → Oracle")

    print_row_counts(cur)
    print_dlq_summary(cur)
    print_snapshot_timing(cur)

    if dlq_detail:
        print_dlq_detail(cur)

    if missing:
        print_missing(cur, show_rows=True)
    else:
        print_missing(cur, show_rows=False)

    if detail:
        print_samples(cur)

    conn.close()
    print(f"\n{'═' * 65}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Debug counts cho CDC pipeline")
    parser.add_argument("--detail",     action="store_true", help="Hiển thị sample rows")
    parser.add_argument("--missing",    action="store_true", help="Hiển thị danh sách TXN chưa có snapshot")
    parser.add_argument("--dlq-detail", action="store_true", help="Hiển thị top error codes + 30-day breakdown")
    args, _ = parser.parse_known_args()
    run(detail=args.detail, missing=args.missing, dlq_detail=args.dlq_detail)
