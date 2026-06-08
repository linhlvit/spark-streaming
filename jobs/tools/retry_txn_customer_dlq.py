
"""
Standalone DLQ retry runner for T24_TXN_CUSTOMER_PENDING.

Chạy độc lập với Spark streaming job — có thể schedule qua cron, Airflow,
hoặc chạy thủ công sau khi customer data bị missing.

Usage:
    python3 tools/retry_txn_customer_dlq.py
    python3 tools/retry_txn_customer_dlq.py --max-retry 10
    python3 tools/retry_txn_customer_dlq.py --once
    python3 tools/retry_txn_customer_dlq.py --interval 300  # loop mỗi 5 phút

Cron (mỗi 10 phút):
    */10 * * * * /usr/bin/python3 /opt/spark/jobs/tools/retry_txn_customer_dlq.py --once

Airflow:
    PythonOperator(task_id="retry_customer_dlq",
                   python_callable=lambda: retry_pending_once())

Lưu ý: hàm retry_pending_once() sẽ tự load lại _customer_cache từ Oracle
nếu cache chưa được khởi tạo (ví dụ khi chạy standalone không qua main.py).
"""

import argparse
import logging
import sys
import time
import os

# Cho phép chạy từ jobs/ root hoặc từ tools/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from static_join.txn_customer_join import retry_pending_once, MAX_RETRY

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("retry_txn_customer_dlq")


def run_once(max_retry: int) -> None:
    log.info("Starting DLQ retry (max_retry=%d)", max_retry)
    result = retry_pending_once(max_retry=max_retry)
    log.info(
        "Done — resolved=%d  failed=%d  retrying=%d",
        result["resolved"],
        result["failed"],
        result["retrying"],
    )
    if result["failed"] > 0:
        log.warning(
            "%d record(s) exceeded max_retry=%d and were marked FAILED. "
            "Query T24_TXN_CUSTOMER_PENDING WHERE STATUS='FAILED' for details.",
            result["failed"],
            max_retry,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Retry T24_TXN_CUSTOMER_PENDING DLQ records")
    parser.add_argument(
        "--max-retry",
        type=int,
        default=MAX_RETRY,
        help=f"Max retry attempts before marking FAILED (default: {MAX_RETRY})",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run once and exit",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=0,
        metavar="SECONDS",
        help="Loop interval in seconds (0 = run once and exit)",
    )
    args = parser.parse_args()

    if args.interval > 0 and not args.once:
        log.info("Loop mode: interval=%ds, max_retry=%d", args.interval, args.max_retry)
        while True:
            try:
                run_once(args.max_retry)
            except Exception:
                log.exception("Retry iteration failed — will retry next cycle")
            time.sleep(args.interval)
    else:
        run_once(args.max_retry)


if __name__ == "__main__":
    main()
