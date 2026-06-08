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
    sync              — CDC sync T24_ACCOUNT/CUSTOMER/BRANCH/TRANSACTIONS → *_TARGET
    static_join       — T24_TRANSACTIONS ⋈ T24_BRANCH (static) → T24_TXN_ENRICHED
    txn_customer_join — T24_TRANSACTIONS ⋈ T24_CUSTOMER (CDC cache) → T24_TXN_CUSTOMER_ENRICHED
    txn_acct_join     — T24_TRANSACTIONS ⋈ T24_ACCOUNT → T24_TXN_ACCOUNT_SNAPSHOT
    branch_sales_agg  — T24_TRANSACTIONS → T24_BRANCH_SALES_SUMMARY (cộng dồn doanh số)

Lưu ý: txn_acct_join yêu cầu chạy bootstrap trước lần đầu:
    python3 tools/bootstrap_txn_acct.py
"""

import argparse
import logging
import sys
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from config import APP_NAME, SPARK_PACKAGES, SQL_FILE_PATH, TABLES
from core.schema_parser import parse_sql_file
from sync.stream_processor import start_table_stream
from static_join.txn_branch_join import start_stream_static_join
from static_join.txn_customer_join import start_stream_cdc_join
from stream_join.txn_acct_join import start_txn_acct_join
# from aggregation.branch_sales_agg import start_branch_sales_agg

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VALID_JOBS = {"sync", "static_join", "txn_customer_join", "txn_acct_join", "branch_sales_agg"}


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
        help="Comma-separated list of jobs to run: sync,static_join,stream_join (default: all)",
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
            logger.error(f"Không tìm thấy schema cho {table_name} trong {SQL_FILE_PATH}")
            continue
        q = start_table_stream(
            spark=spark,
            table_name=table_name,
            pk=meta["pk"],
            col_types=meta["columns"],
        )
        queries.append(q)
        logger.info(f"[sync] Started stream for {table_name}")
    return queries


def start_static_join_job(spark: SparkSession) -> List[StreamingQuery]:
    """stream_static_join: T24_TRANSACTIONS ⋈ T24_BRANCH → T24_TXN_ENRICHED."""
    q = start_stream_static_join(spark)
    logger.info("[static_join] Started T24_TRANSACTIONS ⋈ T24_BRANCH")
    return [q]


def start_txn_customer_join_job(spark: SparkSession) -> List[StreamingQuery]:
    """cdc_cache_join: T24_TRANSACTIONS ⋈ _customer_cache (CDC) → T24_TXN_CUSTOMER_ENRICHED."""
    q = start_stream_cdc_join(spark)
    logger.info("[txn_customer_join] Started T24_TRANSACTIONS ⋈ T24_CUSTOMER (CDC cache)")
    return [q]


def start_txn_acct_join_job(spark: SparkSession) -> List[StreamingQuery]:
    """stream_stream_join: T24_TRANSACTIONS ⋈ T24_ACCOUNT → T24_TXN_ACCOUNT_SNAPSHOT."""
    qs = start_txn_acct_join(spark)
    logger.info("[txn_acct_join] Started T24_TRANSACTIONS ⋈ T24_ACCOUNT")
    return qs


def start_branch_sales_agg_job(spark: SparkSession) -> List[StreamingQuery]:
    """stateful_agg: T24_TRANSACTIONS → T24_BRANCH_SALES_SUMMARY (cộng dồn delta)."""
    q = start_branch_sales_agg(spark)
    logger.info("[branch_sales_agg] Started T24_TRANSACTIONS → T24_BRANCH_SALES_SUMMARY")
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
    spark.sparkContext.setJobDescription(f"Startup — jobs={','.join(job_names)}")

    all_queries: List[StreamingQuery] = []

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

    # Chờ tất cả queries — block mãi mãi cho đến khi có lỗi hoặc bị kill
    for q in all_queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
