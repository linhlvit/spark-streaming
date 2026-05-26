"""
dlq_maintenance.py — Dọn dẹp DLQ và tạo báo cáo metrics hàng ngày.

Schedule: 2:00 AM hàng ngày (ngoài giờ cao điểm).

Flow:
    cleanup_txn_branch ──┐
                         ├─► debug_counts_report → archive_report_to_s3
    cleanup_txn_acct  ──┘

Tác dụng:
    - Xóa RESOLVED/FAILED rows cũ hơn TTL (default 30 ngày)
    - Snapshot metrics pipeline (row counts, DLQ rates, retry success rate)
    - Lưu báo cáo lên S3 để audit trail

Airflow Variables:
    EMR_CLUSTER_ID   — EMR cluster ID
    S3_JOBS_PATH     — s3://bucket/jobs
    S3_LOGS_PATH     — s3://bucket/logs/emr
    DLQ_TTL_DAYS     — số ngày giữ RESOLVED/FAILED (default: 30)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.trigger_rule import TriggerRule

from shared.emr_config import get_cluster_id, get_s3_logs_path, make_python_step

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
}


def _archive_report_to_s3(**context) -> None:
    """
    Lưu thông tin run vào S3 để audit trail.
    EMR Step logs của debug_counts.py đã được lưu tự động tại S3_LOGS_PATH.
    Hàm này log đường dẫn để operator dễ truy xuất.
    """
    import logging
    log = logging.getLogger(__name__)

    execution_date = context["ds"]        # YYYY-MM-DD
    s3_logs = get_s3_logs_path()

    log.info("=== Daily Maintenance Report ===")
    log.info("Execution date  : %s", execution_date)
    log.info("EMR Step logs   : %s/debug_counts_%s/", s3_logs, execution_date)
    log.info(
        "Xem metrics tại : https://console.aws.amazon.com/elasticmapreduce/home "
        "→ cluster %s → Steps (filter by date %s)",
        get_cluster_id(), execution_date,
    )


with DAG(
    dag_id="dlq_maintenance",
    description="Dọn DLQ RESOLVED/FAILED + snapshot metrics pipeline mỗi ngày 2AM",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["dlq", "maintenance", "cleanup"],
) as dag:

    ttl_days = Variable.get("DLQ_TTL_DAYS", default_var="30")

    # ── Cleanup: xóa rows RESOLVED/FAILED cũ hơn TTL ──
    # Chạy song song 2 tables — hoàn toàn độc lập
    cleanup_txn_branch = EmrAddStepsOperator(
        task_id="cleanup_txn_branch",
        job_flow_id=get_cluster_id(),
        steps=[make_python_step(
            name="cleanup_dlq_txn_branch",
            script_path="tools/cleanup_dlq.py",
            script_args=["--table", "txn_branch", "--ttl-days", ttl_days],
        )],
        aws_conn_id="aws_default",
    )

    wait_cleanup_branch = EmrStepSensor(
        task_id="wait_cleanup_txn_branch",
        job_flow_id=get_cluster_id(),
        step_id="{{ task_instance.xcom_pull('cleanup_txn_branch', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=1800,   # 30 phút
    )

    cleanup_txn_acct = EmrAddStepsOperator(
        task_id="cleanup_txn_acct",
        job_flow_id=get_cluster_id(),
        steps=[make_python_step(
            name="cleanup_dlq_txn_acct",
            script_path="tools/cleanup_dlq.py",
            script_args=["--table", "txn_acct", "--ttl-days", ttl_days],
        )],
        aws_conn_id="aws_default",
    )

    wait_cleanup_acct = EmrStepSensor(
        task_id="wait_cleanup_txn_acct",
        job_flow_id=get_cluster_id(),
        step_id="{{ task_instance.xcom_pull('cleanup_txn_acct', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=1800,
    )

    # ── Debug counts: snapshot metrics sau khi cleanup xong ──
    debug_counts = EmrAddStepsOperator(
        task_id="debug_counts_report",
        job_flow_id=get_cluster_id(),
        steps=[make_python_step(
            name="debug_counts_daily",
            script_path="tools/debug_counts.py",
            script_args=["--dlq-detail"],
        )],
        aws_conn_id="aws_default",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    wait_debug = EmrStepSensor(
        task_id="wait_debug_counts",
        job_flow_id=get_cluster_id(),
        step_id="{{ task_instance.xcom_pull('debug_counts_report', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=600,
    )

    # ── Archive: log đường dẫn S3 để audit ──
    archive_report = PythonOperator(
        task_id="archive_report_to_s3",
        python_callable=_archive_report_to_s3,
    )

    # ─────────────────────────────────────────────
    # DEPENDENCIES
    # ─────────────────────────────────────────────
    cleanup_txn_branch >> wait_cleanup_branch
    cleanup_txn_acct   >> wait_cleanup_acct

    [wait_cleanup_branch, wait_cleanup_acct] >> debug_counts >> wait_debug >> archive_report
