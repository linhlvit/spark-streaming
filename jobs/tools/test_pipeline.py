"""
test_pipeline.py — Kịch bản kiểm tra end-to-end pipeline CDC Oracle → Oracle.

Mỗi kịch bản:
  1. Giả lập dữ liệu vào Oracle nguồn (INSERT/UPDATE/DELETE)
  2. Chờ Spark xử lý (polling với timeout)
  3. Assert kết quả tại Oracle target

Chạy:
    cd jobs/
    python3 tools/test_pipeline.py --scenario all            # chạy tất cả
    python3 tools/test_pipeline.py --scenario sync           # chỉ CDC sync
    python3 tools/test_pipeline.py --scenario static_join    # TXN ⋈ BRANCH
    python3 tools/test_pipeline.py --scenario stream_join    # TXN ⋈ ACCOUNT
    python3 tools/test_pipeline.py --scenario aggregation    # doanh số chi nhánh
    python3 tools/test_pipeline.py --scenario dlq            # DLQ + retry
    python3 tools/test_pipeline.py --scenario revert         # op=u revert
    python3 tools/test_pipeline.py --timeout 120             # timeout mỗi kịch bản (giây)

Yêu cầu:
    - Spark job đang chạy (main.py --jobs all)
    - Oracle có đủ bảng nguồn: T24_TRANSACTIONS, T24_ACCOUNT, T24_BRANCH, T24_CUSTOMER
    - python3 -m pip install oracledb
"""

import argparse
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timedelta
from typing import Callable, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import oracledb
from config import ORACLE_DSN, ORACLE_USER, ORACLE_PASSWORD, TARGET_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

S = TARGET_SCHEMA

# ─────────────────────────────────────────────
# DB HELPERS
# ─────────────────────────────────────────────
def get_conn() -> oracledb.Connection:
    return oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)


def scalar(conn, sql: str, params=None):
    cur = conn.cursor()
    cur.execute(sql, params or [])
    row = cur.fetchone()
    return row[0] if row else None


def execute(conn, sql: str, params=None):
    cur = conn.cursor()
    cur.execute(sql, params or [])
    conn.commit()


def poll_until(
    check_fn: Callable[[], bool],
    timeout_s: int = 90,
    interval_s: int = 5,
    desc: str = "condition",
) -> bool:
    """Poll check_fn mỗi interval_s giây cho đến khi True hoặc timeout."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if check_fn():
            return True
        logger.info(f"  Chờ {desc}... (còn {int(deadline - time.time())}s)")
        time.sleep(interval_s)
    return False


def unique_id(prefix: str = "TST") -> str:
    return f"{prefix}{uuid.uuid4().hex[:10].upper()}"


# ─────────────────────────────────────────────
# PASS / FAIL TRACKING
# ─────────────────────────────────────────────
_results: list[tuple[str, bool, str]] = []


def assert_true(condition: bool, scenario: str, message: str):
    status = "PASS" if condition else "FAIL"
    _results.append((scenario, condition, message))
    icon = "✓" if condition else "✗"
    logger.info(f"  [{icon} {status}] {message}")
    if not condition:
        logger.warning(f"  → ASSERTION FAILED: {scenario}")


def print_summary():
    passed = sum(1 for _, ok, _ in _results if ok)
    total  = len(_results)
    print(f"\n{'═'*65}")
    print(f"  KẾT QUẢ: {passed}/{total} assertions PASSED")
    print(f"{'═'*65}")
    for name, ok, msg in _results:
        icon = "✓" if ok else "✗"
        print(f"  [{icon}] [{name}] {msg}")
    print(f"{'═'*65}\n")


# ─────────────────────────────────────────────
# KỊCH BẢN 1 — CDC SYNC
# Giả lập: INSERT một TXN mới vào T24_TRANSACTIONS (nguồn)
# Kỳ vọng: row xuất hiện trong T24_TRANSACTIONS_TARGET sau ≤ 60s
#
# Spark trigger: 30s → latency kỳ vọng 35-70s
# ─────────────────────────────────────────────
def test_cdc_sync(conn, timeout_s: int):
    scenario = "CDC_SYNC"
    logger.info(f"\n{'─'*65}")
    logger.info(f"[{scenario}] Giả lập INSERT T24_TRANSACTIONS → kỳ vọng sync vào TARGET")

    txn_id = unique_id("SYNC")
    now    = datetime.now()

    # Insert vào bảng NGUỒN (Oracle, để Debezium bắt)
    execute(conn, f"""
        INSERT INTO {S}.T24_TRANSACTIONS (
            TRANSACTION_ID, ACCOUNT_ID, CUSTOMER_ID,
            TRANSACTION_DATE, VALUE_DATE, TRANSACTION_TIME,
            TRANSACTION_TYPE, AMOUNT, CURRENCY_CODE,
            CHANNEL, BRANCH_CODE, REFERENCE_NO, TRANSACTION_STATUS
        ) VALUES (
            :1, 'ACC000001', 'CUS000001',
            TRUNC(SYSDATE), TRUNC(SYSDATE), SYSTIMESTAMP,
            'TRANSFER', 1000000, 'VND',
            'WEB', 'BR001', :2, 'COMPLETED'
        )
    """, [txn_id, f"REF-{txn_id}"])

    t_insert = time.time()
    logger.info(f"  Inserted TXN={txn_id} vào T24_TRANSACTIONS lúc {datetime.now().strftime('%H:%M:%S')}")

    # Poll T24_TRANSACTIONS_TARGET
    ok = poll_until(
        lambda: scalar(conn, f"SELECT COUNT(*) FROM {S}.T24_TRANSACTIONS_TARGET WHERE TRANSACTION_ID=:1", [txn_id]) > 0,
        timeout_s=timeout_s, desc=f"TXN={txn_id} trong T24_TRANSACTIONS_TARGET",
    )
    latency = round(time.time() - t_insert, 1)
    assert_true(ok, scenario, f"TXN={txn_id} xuất hiện trong T24_TRANSACTIONS_TARGET (latency={latency}s)")

    # Kiểm tra giá trị đúng
    if ok:
        amt = scalar(conn, f"SELECT AMOUNT FROM {S}.T24_TRANSACTIONS_TARGET WHERE TRANSACTION_ID=:1", [txn_id])
        assert_true(amt == 1000000, scenario, f"AMOUNT sync đúng = 1,000,000 (actual={amt})")

    # Cleanup
    execute(conn, f"DELETE FROM {S}.T24_TRANSACTIONS WHERE TRANSACTION_ID=:1", [txn_id])
    execute(conn, f"DELETE FROM {S}.T24_TRANSACTIONS_TARGET WHERE TRANSACTION_ID=:1", [txn_id])


# ─────────────────────────────────────────────
# KỊCH BẢN 2 — CDC SYNC UPDATE
# Giả lập: INSERT rồi UPDATE TRANSACTION_STATUS
# Kỳ vọng: TARGET phản ánh giá trị mới (last-write-wins)
# ─────────────────────────────────────────────
def test_cdc_sync_update(conn, timeout_s: int):
    scenario = "CDC_SYNC_UPDATE"
    logger.info(f"\n{'─'*65}")
    logger.info(f"[{scenario}] Giả lập UPDATE → kỳ vọng TARGET cập nhật đúng")

    txn_id = unique_id("UPD")
    execute(conn, f"""
        INSERT INTO {S}.T24_TRANSACTIONS (
            TRANSACTION_ID, ACCOUNT_ID, CUSTOMER_ID,
            TRANSACTION_DATE, VALUE_DATE, TRANSACTION_TIME,
            TRANSACTION_TYPE, AMOUNT, CURRENCY_CODE,
            CHANNEL, BRANCH_CODE, REFERENCE_NO, TRANSACTION_STATUS
        ) VALUES (
            :1, 'ACC000001', 'CUS000001',
            TRUNC(SYSDATE), TRUNC(SYSDATE), SYSTIMESTAMP,
            'TRANSFER', 500000, 'VND',
            'WEB', 'BR001', :2, 'PENDING'
        )
    """, [txn_id, f"REF-{txn_id}"])

    # Chờ sync lần đầu
    poll_until(
        lambda: scalar(conn, f"SELECT COUNT(*) FROM {S}.T24_TRANSACTIONS_TARGET WHERE TRANSACTION_ID=:1", [txn_id]) > 0,
        timeout_s=timeout_s // 2, desc="initial sync",
    )

    # UPDATE status
    execute(conn, f"""
        UPDATE {S}.T24_TRANSACTIONS
        SET TRANSACTION_STATUS = 'COMPLETED',
            TRANSACTION_TIME   = SYSTIMESTAMP
        WHERE TRANSACTION_ID = :1
    """, [txn_id])
    t_update = time.time()

    ok = poll_until(
        lambda: scalar(conn, f"SELECT TRANSACTION_STATUS FROM {S}.T24_TRANSACTIONS_TARGET WHERE TRANSACTION_ID=:1", [txn_id]) == 'COMPLETED',
        timeout_s=timeout_s, desc="UPDATE sync",
    )
    latency = round(time.time() - t_update, 1)
    assert_true(ok, scenario, f"TRANSACTION_STATUS UPDATE → COMPLETED sync đúng (latency={latency}s)")

    execute(conn, f"DELETE FROM {S}.T24_TRANSACTIONS WHERE TRANSACTION_ID=:1", [txn_id])
    execute(conn, f"DELETE FROM {S}.T24_TRANSACTIONS_TARGET WHERE TRANSACTION_ID=:1", [txn_id])


# ─────────────────────────────────────────────
# KỊCH BẢN 3 — STATIC JOIN (TXN ⋈ BRANCH)
# Giả lập: INSERT TXN với BRANCH_CODE đã có trong T24_BRANCH
# Kỳ vọng: row xuất hiện trong T24_TXN_ENRICHED với BRANCH_NAME đúng
# ─────────────────────────────────────────────
def test_static_join(conn, timeout_s: int):
    scenario = "STATIC_JOIN"
    logger.info(f"\n{'─'*65}")
    logger.info(f"[{scenario}] Giả lập TXN với BRANCH hợp lệ → kỳ vọng T24_TXN_ENRICHED")

    # Lấy 1 BRANCH_CODE thực tế đang có trong T24_BRANCH
    branch_code = scalar(conn, f"SELECT BRANCH_CODE FROM {S}.T24_BRANCH FETCH FIRST 1 ROW ONLY")
    if not branch_code:
        logger.warning(f"  Bỏ qua: T24_BRANCH trống, chưa có data")
        return

    branch_name = scalar(conn, f"SELECT BRANCH_NAME FROM {S}.T24_BRANCH WHERE BRANCH_CODE=:1", [branch_code])
    txn_id = unique_id("SJ")

    execute(conn, f"""
        INSERT INTO {S}.T24_TRANSACTIONS (
            TRANSACTION_ID, ACCOUNT_ID, CUSTOMER_ID,
            TRANSACTION_DATE, VALUE_DATE, TRANSACTION_TIME,
            TRANSACTION_TYPE, AMOUNT, CURRENCY_CODE,
            CHANNEL, BRANCH_CODE, REFERENCE_NO, TRANSACTION_STATUS
        ) VALUES (
            :1, 'ACC000001', 'CUS000001',
            TRUNC(SYSDATE), TRUNC(SYSDATE), SYSTIMESTAMP,
            'DEPOSIT', 2000000, 'VND',
            'BRANCH', :2, :3, 'COMPLETED'
        )
    """, [txn_id, branch_code, f"REF-{txn_id}"])

    t_insert = time.time()
    ok = poll_until(
        lambda: scalar(conn, f"SELECT COUNT(*) FROM {S}.T24_TXN_ENRICHED WHERE TRANSACTION_ID=:1", [txn_id]) > 0,
        timeout_s=timeout_s, desc=f"TXN={txn_id} trong T24_TXN_ENRICHED",
    )
    latency = round(time.time() - t_insert, 1)
    assert_true(ok, scenario, f"TXN={txn_id} xuất hiện trong T24_TXN_ENRICHED (latency={latency}s)")

    if ok:
        actual_branch = scalar(conn, f"SELECT BRANCH_NAME FROM {S}.T24_TXN_ENRICHED WHERE TRANSACTION_ID=:1", [txn_id])
        assert_true(actual_branch == branch_name, scenario,
                    f"BRANCH_NAME='{branch_name}' được join đúng (actual='{actual_branch}')")
        in_dlq = scalar(conn, f"SELECT COUNT(*) FROM {S}.T24_TXN_PENDING_JOIN WHERE TRANSACTION_ID=:1", [txn_id])
        assert_true(in_dlq == 0, scenario, f"TXN không vào DLQ khi BRANCH hợp lệ")

    execute(conn, f"DELETE FROM {S}.T24_TRANSACTIONS WHERE TRANSACTION_ID=:1", [txn_id])
    execute(conn, f"DELETE FROM {S}.T24_TXN_ENRICHED WHERE TRANSACTION_ID=:1", [txn_id])


# ─────────────────────────────────────────────
# KỊCH BẢN 4 — DLQ: BRANCH NOT FOUND
# Giả lập: INSERT TXN với BRANCH_CODE không tồn tại
# Kỳ vọng: row vào T24_TXN_PENDING_JOIN với ERROR_CODE=BRANCH_NOT_FOUND
# ─────────────────────────────────────────────
def test_dlq_branch_not_found(conn, timeout_s: int):
    scenario = "DLQ_BRANCH_NOT_FOUND"
    logger.info(f"\n{'─'*65}")
    logger.info(f"[{scenario}] TXN với BRANCH không tồn tại → kỳ vọng vào DLQ")

    fake_branch = "BR_FAKE_999"
    txn_id = unique_id("DLQ")

    execute(conn, f"""
        INSERT INTO {S}.T24_TRANSACTIONS (
            TRANSACTION_ID, ACCOUNT_ID, CUSTOMER_ID,
            TRANSACTION_DATE, VALUE_DATE, TRANSACTION_TIME,
            TRANSACTION_TYPE, AMOUNT, CURRENCY_CODE,
            CHANNEL, BRANCH_CODE, REFERENCE_NO, TRANSACTION_STATUS
        ) VALUES (
            :1, 'ACC000001', 'CUS000001',
            TRUNC(SYSDATE), TRUNC(SYSDATE), SYSTIMESTAMP,
            'TRANSFER', 1000000, 'VND',
            'WEB', :2, :3, 'COMPLETED'
        )
    """, [txn_id, fake_branch, f"REF-{txn_id}"])

    t_insert = time.time()
    ok = poll_until(
        lambda: scalar(conn, f"SELECT COUNT(*) FROM {S}.T24_TXN_PENDING_JOIN WHERE TRANSACTION_ID=:1", [txn_id]) > 0,
        timeout_s=timeout_s, desc=f"TXN={txn_id} trong DLQ",
    )
    latency = round(time.time() - t_insert, 1)
    assert_true(ok, scenario, f"TXN={txn_id} vào T24_TXN_PENDING_JOIN (latency={latency}s)")

    if ok:
        err = scalar(conn, f"SELECT ERROR_CODE FROM {S}.T24_TXN_PENDING_JOIN WHERE TRANSACTION_ID=:1", [txn_id])
        assert_true(err == 'BRANCH_NOT_FOUND', scenario, f"ERROR_CODE='BRANCH_NOT_FOUND' (actual='{err}')")
        in_enriched = scalar(conn, f"SELECT COUNT(*) FROM {S}.T24_TXN_ENRICHED WHERE TRANSACTION_ID=:1", [txn_id])
        assert_true(in_enriched == 0, scenario, f"TXN không vào T24_TXN_ENRICHED khi BRANCH không tồn tại")

    execute(conn, f"DELETE FROM {S}.T24_TRANSACTIONS WHERE TRANSACTION_ID=:1", [txn_id])
    execute(conn, f"DELETE FROM {S}.T24_TXN_PENDING_JOIN WHERE TRANSACTION_ID=:1", [txn_id])


# ─────────────────────────────────────────────
# KỊCH BẢN 5 — DLQ RETRY: BRANCH xuất hiện sau
# Giả lập: TXN vào DLQ → INSERT BRANCH → chạy retry → TXN được resolve
# ─────────────────────────────────────────────
def test_dlq_retry_resolves(conn, timeout_s: int):
    scenario = "DLQ_RETRY_RESOLVE"
    logger.info(f"\n{'─'*65}")
    logger.info(f"[{scenario}] TXN vào DLQ → BRANCH xuất hiện → retry → RESOLVED")

    new_branch = unique_id("BRN")
    txn_id     = unique_id("RTY")

    # Bước 1: INSERT TXN với branch chưa tồn tại → vào DLQ
    execute(conn, f"""
        INSERT INTO {S}.T24_TRANSACTIONS (
            TRANSACTION_ID, ACCOUNT_ID, CUSTOMER_ID,
            TRANSACTION_DATE, VALUE_DATE, TRANSACTION_TIME,
            TRANSACTION_TYPE, AMOUNT, CURRENCY_CODE,
            CHANNEL, BRANCH_CODE, REFERENCE_NO, TRANSACTION_STATUS
        ) VALUES (
            :1, 'ACC000001', 'CUS000001',
            TRUNC(SYSDATE), TRUNC(SYSDATE), SYSTIMESTAMP,
            'DEPOSIT', 3000000, 'VND',
            'ATM', :2, :3, 'COMPLETED'
        )
    """, [txn_id, new_branch, f"REF-{txn_id}"])

    in_dlq = poll_until(
        lambda: scalar(conn, f"SELECT COUNT(*) FROM {S}.T24_TXN_PENDING_JOIN WHERE TRANSACTION_ID=:1 AND STATUS='PENDING'", [txn_id]) > 0,
        timeout_s=timeout_s, desc="TXN vào DLQ",
    )
    assert_true(in_dlq, scenario, f"Bước 1: TXN={txn_id} vào DLQ PENDING")
    if not in_dlq:
        return

    # Bước 2: INSERT BRANCH mới (mock BRANCH_TARGET)
    execute(conn, f"""
        MERGE INTO {S}.T24_BRANCH_TARGET t
        USING (SELECT :1 AS BRANCH_CODE FROM DUAL) s
        ON (t.BRANCH_CODE = s.BRANCH_CODE)
        WHEN NOT MATCHED THEN INSERT (BRANCH_CODE, BRANCH_NAME, REGION_CODE, REGION_NAME)
        VALUES (:1, 'Test Branch ' || :1, 'RG001', 'Test Region')
    """, [new_branch])
    logger.info(f"  Inserted BRANCH={new_branch} vào T24_BRANCH_TARGET")

    # Bước 3: reset NEXT_RETRY_AT để retry ngay lập tức
    execute(conn, f"""
        UPDATE {S}.T24_TXN_PENDING_JOIN
        SET NEXT_RETRY_AT = SYSTIMESTAMP - INTERVAL '1' MINUTE
        WHERE TRANSACTION_ID = :1
    """, [txn_id])

    # Bước 4: chạy retry script
    import subprocess
    logger.info("  Chạy retry_txn_branch_dlq.py --once ...")
    result = subprocess.run(
        [sys.executable, "tools/retry_txn_branch_dlq.py", "--once"],
        cwd=os.path.join(os.path.dirname(__file__), ".."),
        capture_output=True, text=True, timeout=60,
    )
    logger.info(f"  Retry stdout: {result.stdout.strip()[:200]}")

    # Bước 5: kiểm tra TXN đã RESOLVED và có trong T24_TXN_ENRICHED
    ok_enriched = poll_until(
        lambda: scalar(conn, f"SELECT COUNT(*) FROM {S}.T24_TXN_ENRICHED WHERE TRANSACTION_ID=:1", [txn_id]) > 0,
        timeout_s=30, desc="TXN RESOLVED → T24_TXN_ENRICHED",
    )
    assert_true(ok_enriched, scenario, f"Bước 5: TXN={txn_id} xuất hiện trong T24_TXN_ENRICHED sau retry")

    dlq_status = scalar(conn, f"SELECT STATUS FROM {S}.T24_TXN_PENDING_JOIN WHERE TRANSACTION_ID=:1", [txn_id])
    assert_true(dlq_status == 'RESOLVED', scenario, f"DLQ STATUS = RESOLVED (actual='{dlq_status}')")

    # Cleanup
    execute(conn, f"DELETE FROM {S}.T24_TRANSACTIONS WHERE TRANSACTION_ID=:1", [txn_id])
    execute(conn, f"DELETE FROM {S}.T24_TXN_ENRICHED WHERE TRANSACTION_ID=:1", [txn_id])
    execute(conn, f"DELETE FROM {S}.T24_TXN_PENDING_JOIN WHERE TRANSACTION_ID=:1", [txn_id])
    execute(conn, f"DELETE FROM {S}.T24_BRANCH_TARGET WHERE BRANCH_CODE=:1", [new_branch])


# ─────────────────────────────────────────────
# KỊCH BẢN 6 — STREAM JOIN (TXN ⋈ ACCOUNT)
# Giả lập: INSERT ACCOUNT rồi INSERT TXN cùng ACCOUNT_ID
# Kỳ vọng: row xuất hiện trong T24_TXN_ACCOUNT_SNAPSHOT
# Lưu ý: cần ACCOUNT có trong T24_ACCOUNT (nguồn) trước rồi Spark mới join được
# ─────────────────────────────────────────────
def test_stream_join(conn, timeout_s: int):
    scenario = "STREAM_JOIN"
    logger.info(f"\n{'─'*65}")
    logger.info(f"[{scenario}] INSERT ACCOUNT + TXN → kỳ vọng T24_TXN_ACCOUNT_SNAPSHOT")

    acct_id  = unique_id("ACC")
    cust_id  = unique_id("CUS")
    txn_id   = unique_id("SSJ")

    # Bước 1: INSERT ACCOUNT vào nguồn (để Debezium pick up)
    execute(conn, f"""
        INSERT INTO {S}.T24_ACCOUNT (
            ACCOUNT_ID, CUSTOMER_ID, WORKING_BALANCE, ONLINE_ACTUAL_BAL,
            CURRENCY_CODE, BRANCH_CODE, EVENT_TIME
        ) VALUES (
            :1, :2, 50000000, 50000000, 'VND', 'BR001', SYSTIMESTAMP
        )
    """, [acct_id, cust_id])
    logger.info(f"  Inserted ACCOUNT={acct_id}")

    # Chờ ACCOUNT_TARGET sync (cần cho DLQ retry fallback)
    poll_until(
        lambda: scalar(conn, f"SELECT COUNT(*) FROM {S}.T24_ACCOUNT_TARGET WHERE ACCOUNT_ID=:1", [acct_id]) > 0,
        timeout_s=60, desc="ACCOUNT_TARGET sync",
    )

    # Bước 2: INSERT TXN
    execute(conn, f"""
        INSERT INTO {S}.T24_TRANSACTIONS (
            TRANSACTION_ID, ACCOUNT_ID, CUSTOMER_ID,
            TRANSACTION_DATE, VALUE_DATE, TRANSACTION_TIME,
            TRANSACTION_TYPE, AMOUNT, CURRENCY_CODE,
            CHANNEL, BRANCH_CODE, REFERENCE_NO, TRANSACTION_STATUS
        ) VALUES (
            :1, :2, :3,
            TRUNC(SYSDATE), TRUNC(SYSDATE), SYSTIMESTAMP,
            'TRANSFER', 1500000, 'VND',
            'MOBILE', 'BR001', :4, 'COMPLETED'
        )
    """, [txn_id, acct_id, cust_id, f"REF-{txn_id}"])

    t_insert = time.time()
    logger.info(f"  Inserted TXN={txn_id} với ACCOUNT_ID={acct_id}")

    ok = poll_until(
        lambda: scalar(conn, f"SELECT COUNT(*) FROM {S}.T24_TXN_ACCOUNT_SNAPSHOT WHERE TRANSACTION_ID=:1", [txn_id]) > 0,
        timeout_s=timeout_s, desc=f"TXN={txn_id} trong T24_TXN_ACCOUNT_SNAPSHOT",
    )
    latency = round(time.time() - t_insert, 1)
    assert_true(ok, scenario, f"TXN={txn_id} xuất hiện trong T24_TXN_ACCOUNT_SNAPSHOT (latency={latency}s)")

    if ok:
        bal = scalar(conn, f"SELECT BALANCE_AT_TXN FROM {S}.T24_TXN_ACCOUNT_SNAPSHOT WHERE TRANSACTION_ID=:1", [txn_id])
        assert_true(bal is not None, scenario, f"BALANCE_AT_TXN được ghi (actual={bal})")
        in_dlq = scalar(conn, f"SELECT COUNT(*) FROM {S}.T24_TXN_ACCT_PENDING_JOIN WHERE TRANSACTION_ID=:1", [txn_id])
        assert_true(in_dlq == 0, scenario, f"TXN không vào DLQ acct khi ACCOUNT hợp lệ")

    # Cleanup
    for tbl, key in [
        (f"{S}.T24_TRANSACTIONS",          "TRANSACTION_ID"),
        (f"{S}.T24_ACCOUNT",               "ACCOUNT_ID"),
        (f"{S}.T24_TRANSACTIONS_TARGET",   "TRANSACTION_ID"),
        (f"{S}.T24_ACCOUNT_TARGET",        "ACCOUNT_ID"),
        (f"{S}.T24_TXN_ACCOUNT_SNAPSHOT",  "TRANSACTION_ID"),
    ]:
        val = txn_id if "TRANSACTION" in key else acct_id
        try:
            execute(conn, f"DELETE FROM {tbl} WHERE {key}=:1", [val])
        except Exception:
            pass


# ─────────────────────────────────────────────
# KỊCH BẢN 7 — AGGREGATION (doanh số chi nhánh)
# Giả lập: INSERT 3 TXN cùng BRANCH_CODE trong ngày
# Kỳ vọng: T24_BRANCH_SALES_SUMMARY có TOTAL_AMOUNT = tổng 3 TXN
# ─────────────────────────────────────────────
def test_aggregation(conn, timeout_s: int):
    scenario = "AGGREGATION"
    logger.info(f"\n{'─'*65}")
    logger.info(f"[{scenario}] INSERT 3 TXN cùng BRANCH → kỳ vọng BRANCH_SALES_SUMMARY tích lũy đúng")

    branch_code = scalar(conn, f"SELECT BRANCH_CODE FROM {S}.T24_BRANCH_TARGET FETCH FIRST 1 ROW ONLY")
    if not branch_code:
        logger.warning("  Bỏ qua: T24_BRANCH_TARGET trống")
        return

    # Ghi nhận TOTAL_AMOUNT trước khi test
    before_total = scalar(conn, f"""
        SELECT NVL(TOTAL_AMOUNT, 0) FROM {S}.T24_BRANCH_SALES_SUMMARY
        WHERE BRANCH_CODE=:1 AND RPT_DATE=TRUNC(SYSDATE) AND CURRENCY_CODE='VND'
    """, [branch_code]) or 0.0

    amounts   = [1_000_000.0, 2_500_000.0, 500_000.0]
    txn_ids   = [unique_id("AGG") for _ in amounts]

    for tid, amt in zip(txn_ids, amounts):
        execute(conn, f"""
            INSERT INTO {S}.T24_TRANSACTIONS (
                TRANSACTION_ID, ACCOUNT_ID, CUSTOMER_ID,
                TRANSACTION_DATE, VALUE_DATE, TRANSACTION_TIME,
                TRANSACTION_TYPE, AMOUNT, CURRENCY_CODE,
                CHANNEL, BRANCH_CODE, REFERENCE_NO, TRANSACTION_STATUS
            ) VALUES (
                :1, 'ACC000001', 'CUS000001',
                TRUNC(SYSDATE), TRUNC(SYSDATE), SYSTIMESTAMP,
                'DEPOSIT', :2, 'VND',
                'WEB', :3, :4, 'COMPLETED'
            )
        """, [tid, amt, branch_code, f"REF-{tid}"])

    expected_total = before_total + sum(amounts)
    logger.info(f"  Inserted 3 TXN: {amounts}. before={before_total:,.0f} expected={expected_total:,.0f}")

    ok = poll_until(
        lambda: (
            scalar(conn, f"""
                SELECT NVL(TOTAL_AMOUNT,0) FROM {S}.T24_BRANCH_SALES_SUMMARY
                WHERE BRANCH_CODE=:1 AND RPT_DATE=TRUNC(SYSDATE) AND CURRENCY_CODE='VND'
            """, [branch_code]) or 0
        ) >= expected_total - 0.01,
        timeout_s=timeout_s, desc="BRANCH_SALES_SUMMARY cập nhật",
    )
    actual = scalar(conn, f"""
        SELECT NVL(TOTAL_AMOUNT,0) FROM {S}.T24_BRANCH_SALES_SUMMARY
        WHERE BRANCH_CODE=:1 AND RPT_DATE=TRUNC(SYSDATE) AND CURRENCY_CODE='VND'
    """, [branch_code]) or 0
    assert_true(ok, scenario, f"TOTAL_AMOUNT >= {expected_total:,.0f} (actual={actual:,.0f})")

    # Cleanup
    for tid in txn_ids:
        execute(conn, f"DELETE FROM {S}.T24_TRANSACTIONS WHERE TRANSACTION_ID=:1", [tid])
        execute(conn, f"DELETE FROM {S}.T24_TRANSACTIONS_TARGET WHERE TRANSACTION_ID=:1", [tid])


# ─────────────────────────────────────────────
# KỊCH BẢN 8 — REVERT (op=u): doanh số bị trừ ngược
# Giả lập: INSERT TXN (op=c) → chờ aggregate → UPDATE status=REVERSED (op=u)
# Kỳ vọng: TOTAL_AMOUNT giảm đúng bằng AMOUNT đã cộng
# ─────────────────────────────────────────────
def test_aggregation_revert(conn, timeout_s: int):
    scenario = "AGGREGATION_REVERT"
    logger.info(f"\n{'─'*65}")
    logger.info(f"[{scenario}] INSERT TXN → aggregate → UPDATE REVERSED → TOTAL_AMOUNT giảm đúng")

    branch_code = scalar(conn, f"SELECT BRANCH_CODE FROM {S}.T24_BRANCH_TARGET FETCH FIRST 1 ROW ONLY")
    if not branch_code:
        logger.warning("  Bỏ qua: T24_BRANCH_TARGET trống")
        return

    amount = 7_777_000.0
    txn_id = unique_id("RVT")

    execute(conn, f"""
        INSERT INTO {S}.T24_TRANSACTIONS (
            TRANSACTION_ID, ACCOUNT_ID, CUSTOMER_ID,
            TRANSACTION_DATE, VALUE_DATE, TRANSACTION_TIME,
            TRANSACTION_TYPE, AMOUNT, CURRENCY_CODE,
            CHANNEL, BRANCH_CODE, REFERENCE_NO, TRANSACTION_STATUS
        ) VALUES (
            :1, 'ACC000001', 'CUS000001',
            TRUNC(SYSDATE), TRUNC(SYSDATE), SYSTIMESTAMP,
            'TRANSFER', :2, 'VND',
            'WEB', :3, :4, 'COMPLETED'
        )
    """, [txn_id, amount, branch_code, f"REF-{txn_id}"])

    # Chờ TXN được aggregate lần đầu
    time.sleep(40)
    after_insert = scalar(conn, f"""
        SELECT NVL(TOTAL_AMOUNT,0) FROM {S}.T24_BRANCH_SALES_SUMMARY
        WHERE BRANCH_CODE=:1 AND RPT_DATE=TRUNC(SYSDATE) AND CURRENCY_CODE='VND'
    """, [branch_code]) or 0.0

    # UPDATE → REVERSED (op=u): Spark sẽ nhận before.AMOUNT = amount và trừ ngược
    execute(conn, f"""
        UPDATE {S}.T24_TRANSACTIONS
        SET TRANSACTION_STATUS = 'REVERSED',
            TRANSACTION_TIME   = SYSTIMESTAMP
        WHERE TRANSACTION_ID = :1
    """, [txn_id])
    logger.info(f"  Updated TXN={txn_id} → REVERSED. before_total={after_insert:,.0f}")

    expected_after = after_insert - amount
    ok = poll_until(
        lambda: abs(
            (scalar(conn, f"""
                SELECT NVL(TOTAL_AMOUNT,0) FROM {S}.T24_BRANCH_SALES_SUMMARY
                WHERE BRANCH_CODE=:1 AND RPT_DATE=TRUNC(SYSDATE) AND CURRENCY_CODE='VND'
            """, [branch_code]) or 0) - expected_after
        ) < 0.01,
        timeout_s=timeout_s, desc="TOTAL_AMOUNT giảm sau revert",
    )
    actual = scalar(conn, f"""
        SELECT NVL(TOTAL_AMOUNT,0) FROM {S}.T24_BRANCH_SALES_SUMMARY
        WHERE BRANCH_CODE=:1 AND RPT_DATE=TRUNC(SYSDATE) AND CURRENCY_CODE='VND'
    """, [branch_code]) or 0
    assert_true(ok, scenario, f"TOTAL_AMOUNT sau revert ≈ {expected_after:,.0f} (actual={actual:,.0f})")

    # Cleanup
    execute(conn, f"DELETE FROM {S}.T24_TRANSACTIONS WHERE TRANSACTION_ID=:1", [txn_id])
    execute(conn, f"DELETE FROM {S}.T24_TRANSACTIONS_TARGET WHERE TRANSACTION_ID=:1", [txn_id])


# ─────────────────────────────────────────────
# KỊCH BẢN 9 — CDC SYNC DELETE
# Giả lập: INSERT rồi DELETE khỏi T24_BRANCH
# Kỳ vọng: row bị xóa khỏi T24_BRANCH_TARGET
# ─────────────────────────────────────────────
def test_cdc_delete(conn, timeout_s: int):
    scenario = "CDC_DELETE"
    logger.info(f"\n{'─'*65}")
    logger.info(f"[{scenario}] INSERT BRANCH rồi DELETE → kỳ vọng xóa khỏi TARGET")

    br = unique_id("BRD")
    execute(conn, f"""
        INSERT INTO {S}.T24_BRANCH (BRANCH_CODE, BRANCH_NAME, REGION_CODE, REGION_NAME)
        VALUES (:1, 'Delete Test Branch', 'RG001', 'Test Region')
    """, [br])

    # Chờ sync
    poll_until(
        lambda: scalar(conn, f"SELECT COUNT(*) FROM {S}.T24_BRANCH_TARGET WHERE BRANCH_CODE=:1", [br]) > 0,
        timeout_s=60, desc="BRANCH sync vào TARGET",
    )

    # DELETE
    execute(conn, f"DELETE FROM {S}.T24_BRANCH WHERE BRANCH_CODE=:1", [br])
    t_del = time.time()

    ok = poll_until(
        lambda: scalar(conn, f"SELECT COUNT(*) FROM {S}.T24_BRANCH_TARGET WHERE BRANCH_CODE=:1", [br]) == 0,
        timeout_s=timeout_s, desc="BRANCH bị xóa khỏi TARGET",
    )
    latency = round(time.time() - t_del, 1)
    assert_true(ok, scenario, f"BRANCH={br} bị xóa khỏi T24_BRANCH_TARGET (latency={latency}s)")


# ─────────────────────────────────────────────
# KỊCH BẢN 10 — LATENCY MEASUREMENT
# Insert 10 TXN liên tiếp, đo latency thực tế end-to-end
# ─────────────────────────────────────────────
def test_latency_measurement(conn, timeout_s: int):
    scenario = "LATENCY"
    logger.info(f"\n{'─'*65}")
    logger.info(f"[{scenario}] Đo latency thực tế: 10 TXN → T24_TXN_ENRICHED")

    branch_code = scalar(conn, f"SELECT BRANCH_CODE FROM {S}.T24_BRANCH_TARGET FETCH FIRST 1 ROW ONLY")
    if not branch_code:
        logger.warning("  Bỏ qua: T24_BRANCH_TARGET trống")
        return

    txn_ids    = [unique_id("LAT") for _ in range(10)]
    timestamps = {}

    for tid in txn_ids:
        execute(conn, f"""
            INSERT INTO {S}.T24_TRANSACTIONS (
                TRANSACTION_ID, ACCOUNT_ID, CUSTOMER_ID,
                TRANSACTION_DATE, VALUE_DATE, TRANSACTION_TIME,
                TRANSACTION_TYPE, AMOUNT, CURRENCY_CODE,
                CHANNEL, BRANCH_CODE, REFERENCE_NO, TRANSACTION_STATUS
            ) VALUES (
                :1, 'ACC000001', 'CUS000001',
                TRUNC(SYSDATE), TRUNC(SYSDATE), SYSTIMESTAMP,
                'TRANSFER', 100000, 'VND', 'WEB', :2, :3, 'COMPLETED'
            )
        """, [tid, branch_code, f"REF-{tid}"])
        timestamps[tid] = time.time()

    logger.info(f"  Inserted 10 TXN lúc {datetime.now().strftime('%H:%M:%S')}")

    # Chờ tất cả 10 TXN vào T24_TXN_ENRICHED
    deadline = time.time() + timeout_s
    latencies = {}
    while time.time() < deadline and len(latencies) < 10:
        for tid in txn_ids:
            if tid in latencies:
                continue
            found = scalar(conn, f"SELECT COUNT(*) FROM {S}.T24_TXN_ENRICHED WHERE TRANSACTION_ID=:1", [tid])
            if found:
                latencies[tid] = round(time.time() - timestamps[tid], 1)
        if len(latencies) < 10:
            time.sleep(3)

    assert_true(len(latencies) == 10, scenario, f"Tất cả 10 TXN vào T24_TXN_ENRICHED ({len(latencies)}/10)")

    if latencies:
        vals = sorted(latencies.values())
        p50  = vals[len(vals)//2]
        p95  = vals[int(len(vals)*0.95)]
        logger.info(f"  Latency kết quả:")
        logger.info(f"    min={min(vals)}s  p50={p50}s  p95={p95}s  max={max(vals)}s")
        assert_true(p95 <= 90, scenario, f"p95 latency ≤ 90s (actual={p95}s)")

    # Cleanup
    for tid in txn_ids:
        execute(conn, f"DELETE FROM {S}.T24_TRANSACTIONS WHERE TRANSACTION_ID=:1", [tid])
        execute(conn, f"DELETE FROM {S}.T24_TXN_ENRICHED WHERE TRANSACTION_ID=:1", [tid])


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
SCENARIOS = {
    "sync":          test_cdc_sync,
    "sync_update":   test_cdc_sync_update,
    "sync_delete":   test_cdc_delete,
    "static_join":   test_static_join,
    "dlq":           test_dlq_branch_not_found,
    "dlq_retry":     test_dlq_retry_resolves,
    "stream_join":   test_stream_join,
    "aggregation":   test_aggregation,
    "revert":        test_aggregation_revert,
    "latency":       test_latency_measurement,
}


def main():
    parser = argparse.ArgumentParser(description="End-to-end pipeline test")
    parser.add_argument("--scenario", default="all",
                        help=f"Kịch bản: all | {' | '.join(SCENARIOS)}")
    parser.add_argument("--timeout",  type=int, default=90,
                        help="Timeout mỗi kịch bản (giây, default=90)")
    args, _ = parser.parse_known_args()

    conn = get_conn()
    logger.info(f"Connected to Oracle: {ORACLE_DSN}")
    logger.info(f"Timeout per scenario: {args.timeout}s")

    if args.scenario == "all":
        to_run = list(SCENARIOS.keys())
    else:
        to_run = [s.strip() for s in args.scenario.split(",")]
        invalid = [s for s in to_run if s not in SCENARIOS]
        if invalid:
            logger.error(f"Kịch bản không hợp lệ: {invalid}. Hợp lệ: {list(SCENARIOS)}")
            sys.exit(1)

    logger.info(f"Chạy kịch bản: {to_run}")

    for name in to_run:
        try:
            SCENARIOS[name](conn, args.timeout)
        except Exception as e:
            logger.exception(f"[{name}] Exception: {e}")
            _results.append((name, False, f"Exception: {e}"))

    conn.close()
    print_summary()

    failed = sum(1 for _, ok, _ in _results if not ok)
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
