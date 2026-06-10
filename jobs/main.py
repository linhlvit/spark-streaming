"""
main.py — Entrypoint của ứng dụng Spark Streaming CDC Oracle → Oracle.

Chạy:
    # Chạy tất cả jobs (mặc định)
    spark-submit ... main.py

    # Chỉ chạy 1 job
    spark-submit ... main.py --jobs sync
    spark-submit ... main.py --jobs static_join
    spark-submit ... main.py --jobs txn_customer_join
    spark-submit ... main.py --jobs txn_acct_join

    # Chạy nhiều job
    spark-submit ... main.py --jobs sync,static_join
    spark-submit ... main.py --jobs sync,txn_customer_join
    spark-submit ... main.py --jobs sync,txn_acct_join

Các job hợp lệ:
    sync              — CDC sync T24_ACCOUNT/CUSTOMER/BRANCH/
                         TRANSACTIONS → *_TARGET
    static_join       — T24_TRANSACTIONS ⋈ T24_BRANCH (static)
                         → T24_TXN_ENRICHED
    txn_customer_join — T24_TRANSACTIONS ⋈ T24_CUSTOMER (CDC cache)
                         → T24_TXN_CUSTOMER_ENRICHED
    txn_acct_join     — T24_TRANSACTIONS ⋈ T24_ACCOUNT
                         → T24_TXN_ACCOUNT_SNAPSHOT
    branch_sales_agg  — T24_TRANSACTIONS → T24_BRANCH_SALES_SUMMARY
                         (cộng dồn doanh số)

Lưu ý: txn_acct_join yêu cầu chạy bootstrap trước lần đầu:
    python3 tools/bootstrap_txn_acct.py
"""

import argparse
import logging
import os
import signal
import sys
import time
from contextlib import suppress
from typing import List
import threading

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from config import (
    APP_NAME,
    CHECKPOINT_BASE,
    SPARK_PACKAGES,
    SQL_FILE_PATH,
    TABLES,
)
from core.schema_parser import parse_sql_file
from aggregation.branch_sales_agg import start_branch_sales_agg
from sync.stream_processor import start_table_stream
from static_join.txn_branch_join import start_stream_static_join
from static_join.txn_customer_join import start_stream_cdc_join
from stream_join.txn_acct_join import (
    CHECKPOINT_DELETE as TXN_ACCT_DELETE_CHECKPOINT,
    CHECKPOINT_JOIN as TXN_ACCT_JOIN_CHECKPOINT,
    start_txn_acct_join,
)
# from aggregation.branch_sales_agg import start_branch_sales_agg

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VALID_JOBS = {
    "sync",
    "static_join",
    "txn_customer_join",
    "txn_acct_join",
    "branch_sales_agg",
}


def _checkpoint_paths_for_job(job_name: str) -> List[str]:
    if job_name == "sync":
        return [
            f"{CHECKPOINT_BASE}/t24_transactions",
            f"{CHECKPOINT_BASE}/t24_account",
            f"{CHECKPOINT_BASE}/t24_customer",
            f"{CHECKPOINT_BASE}/t24_branch",
        ]
    if job_name == "static_join":
        return [f"{CHECKPOINT_BASE}/txn_branch_join"]
    if job_name == "txn_customer_join":
        return [f"{CHECKPOINT_BASE}/txn_customer_join"]
    if job_name == "txn_acct_join":
        return [TXN_ACCT_JOIN_CHECKPOINT, TXN_ACCT_DELETE_CHECKPOINT]
    if job_name == "branch_sales_agg":
        return [f"{CHECKPOINT_BASE}/branch_sales_agg"]
    return []


def _process_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    with suppress(ProcessLookupError):
        os.kill(pid, 0)
        return True
    return False


def _acquire_checkpoint_lock(checkpoint_path: str, job_name: str) -> str:
    os.makedirs(checkpoint_path, exist_ok=True)
    lock_path = os.path.join(checkpoint_path, ".launch.lock")

    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            existing_pid = -1
            try:
                with open(lock_path, "r", encoding="utf-8") as handle:
                    raw_lock = handle.read().strip()
                existing_pid = int(raw_lock.split("|", 1)[0])
            except Exception:
                pass

            if existing_pid > 0 and _process_is_alive(existing_pid):
                raise RuntimeError(
                    "Checkpoint đang được dùng: "
                    f"{checkpoint_path} (pid={existing_pid}, job={job_name})"
                )

            with suppress(FileNotFoundError):
                os.remove(lock_path)
            continue

        lock_payload = f"{os.getpid()}|{int(time.time())}|{job_name}\n"
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(lock_payload)
        return lock_path


def _release_checkpoint_lock(lock_path: str) -> None:
    with suppress(FileNotFoundError):
        os.remove(lock_path)


def _stop_queries(queries: List[StreamingQuery], spark: SparkSession) -> None:
    for query in queries:
        with suppress(Exception):
            if query.isActive:
                query.stop()
    with suppress(Exception):
        spark.stop()


def _spark_context_is_stopped(spark: SparkSession) -> bool:
    with suppress(Exception):
        return bool(spark.sparkContext._jsc.sc().isStopped())
    return False


def parse_args():
    """
    Parse --jobs argument từ sys.argv.
    spark-submit truyền args sau tên file: main.py --jobs sync,static_join
    """
    parser = argparse.ArgumentParser(description="Spark Streaming CDC Oracle")
    parser.add_argument(
        "--jobs",
        type=str,
        default="all",
        help=(
            "Comma-separated list of jobs to run: sync,static_join,"
            "stream_join (default: all)"
        ),
    )
    # parse_known_args để bỏ qua các args của spark-submit
    args, _ = parser.parse_known_args()
    return args


def build_spark_session(job_names: List[str]) -> SparkSession:
    app_name = f"{APP_NAME}[{','.join(job_names)}]"
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def start_sync_job(spark: SparkSession) -> List[StreamingQuery]:
    """stream_processor: CDC sync các bảng → TARGET tables."""
    schema_map = parse_sql_file(SQL_FILE_PATH)
    queries = []
    for table_name in TABLES:
        meta = schema_map.get(table_name)
        if not meta:
            logger.error(
                "Không tìm thấy schema cho %s trong %s",
                table_name,
                SQL_FILE_PATH,
            )
            continue
        q = start_table_stream(
            spark=spark,
            table_name=table_name,
            pk=meta["pk"],
            col_types=meta["columns"],
        )
        queries.append(q)
        logger.info("[sync] Started stream for %s", table_name)
    return queries


def start_static_join_job(spark: SparkSession) -> List[StreamingQuery]:
    """stream_static_join: T24_TRANSACTIONS ⋈ T24_BRANCH → T24_TXN_ENRICHED."""
    q = start_stream_static_join(spark)
    logger.info("[static_join] Started T24_TRANSACTIONS ⋈ T24_BRANCH")
    return [q]


def start_txn_customer_join_job(spark: SparkSession) -> List[StreamingQuery]:
    """cdc_cache_join: T24_TRANSACTIONS ⋈ _customer_cache (CDC)."""
    q = start_stream_cdc_join(spark)
    logger.info("[txn_customer_join] Started T24_TRANSACTIONS ⋈ T24_CUSTOMER")
    return [q]


def start_txn_acct_join_job(spark: SparkSession) -> List[StreamingQuery]:
    """stream_stream_join: T24_TRANSACTIONS ⋈ T24_ACCOUNT."""
    qs = start_txn_acct_join(spark)
    logger.info("[txn_acct_join] Started T24_TRANSACTIONS ⋈ T24_ACCOUNT")
    return qs


def start_branch_sales_agg_job(spark: SparkSession) -> List[StreamingQuery]:
    """stateful_agg: T24_TRANSACTIONS → T24_BRANCH_SALES_SUMMARY."""
    q = start_branch_sales_agg(spark)
    logger.info(
        "[branch_sales_agg] Started T24_TRANSACTIONS → "
        "T24_BRANCH_SALES_SUMMARY"
    )
    return [q]


def main() -> None:
    args = parse_args()

    # Xác định danh sách job cần chạy
    if args.jobs.lower() == "all":
        job_names = list(VALID_JOBS)
    else:
        job_names = [j.strip() for j in args.jobs.split(",")]
        invalid = set(job_names) - VALID_JOBS
        if invalid:
            logger.error(f"Job không hợp lệ: {invalid}. Hợp lệ: {VALID_JOBS}")
            sys.exit(1)

    logger.info(f"Starting jobs: {job_names}")

    spark = build_spark_session(job_names)
    spark.sparkContext.setLogLevel("WARN")

    # Set job description cho Spark UI
    spark.sparkContext.setJobDescription(
        f"Startup — jobs={','.join(job_names)}"
    )

    all_queries: List[StreamingQuery] = []
    acquired_locks: List[str] = []
    shutdown_requested = False
    shutdown_event = threading.Event()

    try:
        for job_name in job_names:
            for checkpoint_path in _checkpoint_paths_for_job(job_name):
                acquired_locks.append(
                    _acquire_checkpoint_lock(checkpoint_path, job_name)
                )

        if "sync" in job_names:
            all_queries.extend(start_sync_job(spark))

        if "static_join" in job_names:
            all_queries.extend(start_static_join_job(spark))

        if "txn_customer_join" in job_names:
            all_queries.extend(start_txn_customer_join_job(spark))

        if "txn_acct_join" in job_names:
            all_queries.extend(start_txn_acct_join_job(spark))

        # if "branch_sales_agg" in job_names:
        #     all_queries.extend(start_branch_sales_agg_job(spark))

        if not all_queries:
            logger.error("Không có query nào được khởi động.")
            sys.exit(1)

        logger.info(f"Total streaming queries running: {len(all_queries)}")

        def monitor_spark_context() -> None:
            while not shutdown_event.is_set():
                if _spark_context_is_stopped(spark):
                    logger.info(
                        "Spark context stopped externally; shutting down."
                    )
                    shutdown_event.set()
                    _stop_queries(all_queries, spark)
                    return
                time.sleep(2)

        threading.Thread(target=monitor_spark_context, daemon=True).start()

        def handle_shutdown(signum, _frame):
            nonlocal shutdown_requested
            if shutdown_requested:
                return
            shutdown_requested = True
            shutdown_event.set()
            logger.info(
                "Received signal %s; stopping streaming queries gracefully.",
                signum,
            )
            _stop_queries(all_queries, spark)

        signal.signal(signal.SIGINT, handle_shutdown)
        signal.signal(signal.SIGTERM, handle_shutdown)

        while not shutdown_event.is_set():
            for query in all_queries:
                if shutdown_event.is_set():
                    break
                if not query.isActive:
                    exception = query.exception()
                    if exception is not None:
                        raise exception
                    shutdown_requested = True
                    shutdown_event.set()
                    break
                query.awaitTermination(1)
                exception = query.exception()
                if exception is not None:
                    raise exception
    finally:
        shutdown_event.set()
        _stop_queries(all_queries, spark)
        for lock_path in acquired_locks:
            _release_checkpoint_lock(lock_path)


if __name__ == "__main__":
    main()
