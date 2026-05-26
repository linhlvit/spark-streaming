"""
Standalone DLQ retry runner cho T24_TXN_ACCT_PENDING_JOIN (stream-stream join).

Chạy độc lập, không cần SparkSession. Lên lịch qua cron hoặc Airflow.

Lifecycle: PENDING → RESOLVED (lookup T24_ACCOUNT_TARGET thành công)
                   → FAILED   (đạt MAX_RETRY)

Usage:
    python3 tools/retry_txn_acct_dlq.py --once
    python3 tools/retry_txn_acct_dlq.py --interval 600   # loop mỗi 10 phút
    python3 tools/retry_txn_acct_dlq.py --max-retry 10 --once

Cron example:
    */10 * * * * /usr/bin/python3 /opt/spark/jobs/tools/retry_txn_acct_dlq.py --once
"""

import argparse
import logging
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from stream_join.txn_acct_join import retry_pending_once, MAX_RETRY

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("retry_txn_acct_dlq")


def run_once(max_retry: int) -> None:
    log.info("Starting DLQ retry (max_retry=%d)", max_retry)
    result = retry_pending_once(max_retry=max_retry)
    log.info(
        "Done — resolved=%d  failed=%d  retrying=%d",
        result["resolved"], result["failed"], result["retrying"],
    )
    if result["failed"] > 0:
        log.warning(
            "%d record(s) exceeded max_retry=%d and were marked FAILED. "
            "Query T24_TXN_ACCT_PENDING_JOIN WHERE STATUS='FAILED' for details.",
            result["failed"], max_retry,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Retry T24_TXN_ACCT_PENDING_JOIN DLQ records")
    parser.add_argument("--max-retry", type=int, default=MAX_RETRY,
                        help=f"Max retry attempts before FAILED (default: {MAX_RETRY})")
    parser.add_argument("--once", action="store_true",
                        help="Run once and exit")
    parser.add_argument("--interval", type=int, default=0, metavar="SECONDS",
                        help="Loop interval in seconds (0 = run once)")
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
