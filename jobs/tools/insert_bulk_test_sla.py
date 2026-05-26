"""
Bulk insert 100,000 bản ghi vào FSS_STREAM.T24_TRANSACTIONS_TEST_SLA
Dùng oracledb + executemany + multi-threading để tối đa tốc độ.

Yêu cầu:
    pip install oracledb
    Không cần Oracle Instant Client (thin mode mặc định)
"""

import oracledb
import random
import string
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
ORACLE_DSN      = "192.168.26.180:1521/dbpdb"
ORACLE_USER     = "FSS_STREAM"
ORACLE_PASSWORD = "FSS_STREAM"

TARGET_TABLE    = "FSS_STREAM.T24_TRANSACTIONS_TEST_SLA"
TOTAL_ROWS      = 100_000
START_IDX       = 200_000     # offset để tránh trùng TRANSACTION_ID với data cũ
BATCH_SIZE      = 1_000       # số row mỗi lần executemany
NUM_THREADS     = 8           # số thread song song

# ─────────────────────────────────────────────
# DATA GENERATION
# ─────────────────────────────────────────────
TRANSACTION_TYPES = ["TRANSFER", "DEPOSIT", "WITHDRAWAL", "PAYMENT", "REFUND"]
CURRENCIES        = ["VND", "USD", "EUR"]
CHANNELS          = ["MOBILE", "WEB", "ATM", "BRANCH", "POS"]
STATUSES          = ["COMPLETED", "PENDING", "FAILED", "REVERSED"]
BRANCH_CODES      = [f"BR{str(i).zfill(3)}" for i in range(1, 21)]

BASE_DATE = datetime(2024, 1, 1)


def rand_str(n=8):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=n))


def now_ms() -> int:
    """Unix timestamp hiện tại tính bằng milliseconds."""
    return int(time.time() * 1000)


def generate_batch(start_idx: int, count: int) -> list[tuple]:
    """Sinh `count` bản ghi bắt đầu từ index `start_idx`."""
    rows = []
    for i in range(count):
        idx              = start_idx + i
        txn_date         = BASE_DATE + timedelta(days=random.randint(0, 500))
        value_date       = txn_date + timedelta(days=random.randint(0, 3))
        txn_time         = txn_date.replace(
            hour=random.randint(0, 23),
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
            microsecond=random.randint(0, 999) * 1000,
        )
        rows.append((
            f"TXN{str(idx).zfill(10)}",          # TRANSACTION_ID
            f"ACC{random.randint(1, 5000):06d}",  # ACCOUNT_ID
            f"CUS{random.randint(1, 2000):06d}",  # CUSTOMER_ID
            txn_date,                              # TRANSACTION_DATE
            value_date,                            # VALUE_DATE
            txn_time,                              # TRANSACTION_TIME
            random.choice(TRANSACTION_TYPES),      # TRANSACTION_TYPE
            round(random.uniform(10_000, 500_000_000), 2),  # AMOUNT
            random.choice(CURRENCIES),             # CURRENCY_CODE
            random.choice(CHANNELS),               # CHANNEL
            f"USR{random.randint(1, 500):04d}",   # INPUT_ID
            f"AUTH{random.randint(1, 200):04d}",  # AUTHOR_ID
            random.choice(BRANCH_CODES),           # BRANCH_CODE
            f"REF{rand_str(12)}",                  # REFERENCE_NO
            random.choice(STATUSES),               # TRANSACTION_STATUS
            now_ms(),                              # CREATE_MS
        ))
    return rows


# ─────────────────────────────────────────────
# INSERT WORKER
# ─────────────────────────────────────────────
INSERT_SQL = f"""
    INSERT INTO {TARGET_TABLE} (
        TRANSACTION_ID, ACCOUNT_ID, CUSTOMER_ID,
        TRANSACTION_DATE, VALUE_DATE, TRANSACTION_TIME,
        TRANSACTION_TYPE, AMOUNT, CURRENCY_CODE,
        CHANNEL, INPUT_ID, AUTHOR_ID,
        BRANCH_CODE, REFERENCE_NO, TRANSACTION_STATUS,
        CREATE_MS
    ) VALUES (
        :1, :2, :3,
        :4, :5, :6,
        :7, :8, :9,
        :10, :11, :12,
        :13, :14, :15,
        :16
    )
"""

_lock = threading.Lock()
_total_inserted = 0


def insert_worker(worker_id: int, batches: list[list[tuple]]) -> int:
    """Mỗi thread giữ 1 connection riêng, insert nhiều batch."""
    global _total_inserted
    inserted = 0

    conn = oracledb.connect(
        user=ORACLE_USER,
        password=ORACLE_PASSWORD,
        dsn=ORACLE_DSN,
    )
    conn.autocommit = False

    try:
        cursor = conn.cursor()
        # Tắt array fetch để tiết kiệm memory
        cursor.arraysize = BATCH_SIZE

        for batch in batches:
            cursor.executemany(INSERT_SQL, batch, batcherrors=False)
            conn.commit()
            inserted += len(batch)

            with _lock:
                _total_inserted += len(batch)
                logger.info(
                    f"Worker-{worker_id}: +{len(batch)} rows | "
                    f"Total: {_total_inserted:,}/{TOTAL_ROWS:,}"
                )
    except Exception as e:
        conn.rollback()
        logger.error(f"Worker-{worker_id} lỗi: {e}")
        raise
    finally:
        conn.close()

    return inserted


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    logger.info(f"Bắt đầu insert {TOTAL_ROWS:,} bản ghi vào {TARGET_TABLE}")
    logger.info(f"Batch size: {BATCH_SIZE} | Threads: {NUM_THREADS}")

    # ── Sinh toàn bộ dữ liệu ──────────────────
    t0 = time.time()
    logger.info("Đang sinh dữ liệu...")
    all_batches: list[list[tuple]] = []
    for start in range(START_IDX, START_IDX + TOTAL_ROWS, BATCH_SIZE):
        count = min(BATCH_SIZE, START_IDX + TOTAL_ROWS - start)
        all_batches.append(generate_batch(start, count))
    logger.info(f"Sinh xong {len(all_batches)} batch trong {time.time()-t0:.1f}s")

    # ── Chia batch cho từng thread ────────────
    # Round-robin phân phối batch đều cho các thread
    thread_batches: list[list[list[tuple]]] = [[] for _ in range(NUM_THREADS)]
    for i, batch in enumerate(all_batches):
        thread_batches[i % NUM_THREADS].append(batch)

    # ── Chạy multi-thread ─────────────────────
    t1 = time.time()
    logger.info("Bắt đầu insert song song...")

    with ThreadPoolExecutor(max_workers=NUM_THREADS, thread_name_prefix="InsertWorker") as executor:
        futures = {
            executor.submit(insert_worker, wid, batches): wid
            for wid, batches in enumerate(thread_batches)
            if batches  # bỏ qua thread không có batch
        }
        total_ok = 0
        for future in as_completed(futures):
            wid = futures[future]
            try:
                total_ok += future.result()
            except Exception as e:
                logger.error(f"Worker-{wid} thất bại: {e}")

    elapsed = time.time() - t1
    logger.info("─" * 60)
    logger.info(f"Hoàn thành: {total_ok:,} bản ghi")
    logger.info(f"Thời gian insert: {elapsed:.2f}s")
    logger.info(f"Tốc độ: {total_ok / elapsed:,.0f} rows/s")


if __name__ == "__main__":
    main()
