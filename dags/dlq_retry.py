"""
dlq_retry.py — Retry DLQ records định kỳ, chạy song song 2 DLQ tables.

Schedule: mỗi 10 phút.

Flow:
    retry_txn_branch ──┐
                       ├─► log_retry_summary → alert_if_high_failure
    retry_txn_acct  ──┘

Lý do chạy song song:
    - 2 DLQ hoàn toàn độc lập (khác table, khác retry logic)
    - Giảm tổng thời gian từ 2x xuống 1x mỗi chu kỳ

Airflow Variables:
    EMR_CLUSTER_ID       — EMR cluster ID
    S3_JOBS_PATH         — s3://bucket/jobs
    DLQ_MAX_RETRY        — số lần retry tối đa (default: 5)
    DLQ_FAILURE_ALERT_THRESHOLD — số FAILED rows để trigger alert (default: 10)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.trigger_rule import TriggerRule

from shared.emr_config import get_cluster_id, make_python_step

default_args = {
    "owner": "data-engineering",
    "retries": 0,           # retry script tự xử lý retry count — không retry DAG
    "email_on_failure": True,
    "email_on_retry": False,
}


def _log_retry_summary(**context) -> None:
    """
    Đọc kết quả từ cả 2 retry steps qua EMR Step logs trên S3.
    Ở đây log step IDs để operator có thể xem chi tiết trên CloudWatch / S3 logs.
    """
    import logging
    log = logging.getLogger(__name__)
    ti = context["ti"]

    branch_ids  = ti.xcom_pull(task_ids="retry_txn_branch",  key="return_value")
    acct_ids    = ti.xcom_pull(task_ids="retry_txn_acct",    key="return_value")
    cluster_id  = get_cluster_id()

    log.info("=== DLQ Retry Summary ===")
    log.info("txn_branch EMR steps : %s", branch_ids)
    log.info("txn_acct   EMR steps : %s", acct_ids)
    log.info(
        "Xem log chi tiết: https://console.aws.amazon.com/elasticmapreduce/home "
        "→ cluster %s → Steps",
        cluster_id,
    )


def _alert_if_high_failure(**context) -> None:
    """
    Placeholder: kết nối Oracle để đếm FAILED rows và raise nếu vượt threshold.
    Trong production, thay bằng CloudWatch metric hoặc SNS notification.
    """
    import logging
    import os
    import sys

    log = logging.getLogger(__name__)
    threshold = int(Variable.get("DLQ_FAILURE_ALERT_THRESHOLD", default_var="10"))

    # Nếu muốn query Oracle trực tiếp từ MWAA, cần cài oracledb vào MWAA requirements.txt
    # Ví dụ kiểm tra đơn giản — mở rộng thêm nếu cần:
    log.info(
        "Alert threshold = %d. Kiểm tra FAILED rows qua debug_counts.py "
        "hoặc CloudWatch Dashboard.",
        threshold,
    )
    # Mẫu raise nếu muốn fail DAG khi FAILED cao:
    # if failed_count > threshold:
    #     raise ValueError(f"DLQ FAILED rows ({failed_count}) vượt threshold ({threshold})")


with DAG(
    dag_id="dlq_retry",
    description="Retry DLQ records mỗi 10 phút — txn_branch và txn_acct song song",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/10 * * * *",
    catchup=False,
    max_active_runs=1,      # tránh overlap: nếu run trước chưa xong thì skip run mới
    tags=["dlq", "retry", "streaming"],
) as dag:

    max_retry = Variable.get("DLQ_MAX_RETRY", default_var="5")

    # ── retry_txn_branch: T24_TXN_PENDING_JOIN (static_join DLQ) ──
    retry_txn_branch = EmrAddStepsOperator(
        task_id="retry_txn_branch",
        job_flow_id=get_cluster_id(),
        steps=[make_python_step(
            name="retry_txn_branch_dlq",
            script_path="tools/retry_txn_branch_dlq.py",
            script_args=["--once", "--max-retry", max_retry],
        )],
        aws_conn_id="aws_default",
    )

    wait_branch = EmrStepSensor(
        task_id="wait_retry_txn_branch",
        job_flow_id=get_cluster_id(),
        step_id="{{ task_instance.xcom_pull('retry_txn_branch', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=20,
        timeout=480,    # 8 phút — phải xong trước lần chạy tiếp theo (10 phút)
    )

    # ── retry_txn_acct: T24_TXN_ACCT_PENDING_JOIN (stream_join DLQ) ──
    retry_txn_acct = EmrAddStepsOperator(
        task_id="retry_txn_acct",
        job_flow_id=get_cluster_id(),
        steps=[make_python_step(
            name="retry_txn_acct_dlq",
            script_path="tools/retry_txn_acct_dlq.py",
            script_args=["--once", "--max-retry", max_retry],
        )],
        aws_conn_id="aws_default",
    )

    wait_acct = EmrStepSensor(
        task_id="wait_retry_txn_acct",
        job_flow_id=get_cluster_id(),
        step_id="{{ task_instance.xcom_pull('retry_txn_acct', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=20,
        timeout=480,
    )

    # ── Summary + Alert sau khi cả 2 xong ──
    log_summary = PythonOperator(
        task_id="log_retry_summary",
        python_callable=_log_retry_summary,
        trigger_rule=TriggerRule.ALL_DONE,   # chạy dù 1 bên fail
    )

    alert_check = PythonOperator(
        task_id="alert_if_high_failure",
        python_callable=_alert_if_high_failure,
    )

    # ─────────────────────────────────────────────
    # DEPENDENCIES — 2 retry chạy song song
    # ─────────────────────────────────────────────
    retry_txn_branch >> wait_branch
    retry_txn_acct   >> wait_acct

    [wait_branch, wait_acct] >> log_summary >> alert_check
