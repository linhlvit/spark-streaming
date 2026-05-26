"""
streaming_lifecycle.py — Khởi động pipeline CDC có thứ tự phụ thuộc.

Trigger: MANUAL — chạy khi deploy lần đầu hoặc sau khi EMR cluster restart.

Flow:
    bootstrap_txn_acct          ← Phase 1: populate T24_TXN_ACCOUNT_SNAPSHOT từ lịch sử
            │
            ▼
    verify_sync_job_healthy      ← Kiểm tra sync job (T24_*_TARGET) đang chạy trước
            │
            ├──────────────────────────────┐
            ▼                              ▼
    start_static_join_job        start_txn_acct_join_job
    (TXN ⋈ BRANCH → ENRICHED)   (TXN ⋈ ACCOUNT → SNAPSHOT)
            │                              │
            └──────────────┬───────────────┘
                           ▼
                  verify_streaming_started  ← check Spark UI API

Lưu ý:
    - bootstrap_txn_acct dùng idempotent MERGE — an toàn chạy lại
    - streaming jobs dùng ActionOnFailure=CONTINUE — EMR không dừng khi job crash
    - Spark streaming job KHÔNG chạy đến complete: sensor không được dùng để chờ
    - Dùng --dry-run để kiểm tra trước khi chạy bootstrap thật

Airflow Variables cần thiết:
    EMR_CLUSTER_ID, S3_JOBS_PATH, S3_LOGS_PATH, S3_CHECKPOINTS_PATH
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.trigger_rule import TriggerRule

from shared.emr_config import (
    get_cluster_id,
    make_python_step,
    make_spark_submit_step,
)

# ─────────────────────────────────────────────
# DAG DEFAULT ARGS
# ─────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": True,
    "email_on_retry": False,
}

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def _check_bootstrap_needed(**context) -> str:
    """
    Kiểm tra T24_TXN_ACCOUNT_SNAPSHOT đã có data chưa.
    Nếu có → skip bootstrap, đi thẳng sang verify_sync.
    Nếu chưa → chạy bootstrap.

    Logic: đọc Airflow Variable 'BOOTSTRAP_COMPLETED' (set thủ công sau lần đầu).
    Hoặc có thể query Oracle để đếm rows — đơn giản hóa bằng Variable.
    """
    bootstrap_done = Variable.get("BOOTSTRAP_COMPLETED", default_var="false").lower()
    if bootstrap_done == "true":
        return "skip_bootstrap"
    return "bootstrap_dry_run"


def _set_bootstrap_completed(**context) -> None:
    Variable.set("BOOTSTRAP_COMPLETED", "true")


def _verify_streaming_started(**context) -> None:
    """
    Kiểm tra các EMR Step đã được submit thành công.
    Với streaming job, chỉ cần xác nhận step đã được PENDING/RUNNING — không chờ complete.
    Log step IDs để dễ track trên EMR console.
    """
    ti = context["ti"]
    static_step_ids  = ti.xcom_pull(task_ids="start_static_join", key="return_value")
    txn_acct_step_ids = ti.xcom_pull(task_ids="start_txn_acct_join", key="return_value")

    import logging
    log = logging.getLogger(__name__)
    log.info("Static join EMR steps: %s", static_step_ids)
    log.info("TxnAcct join EMR steps: %s", txn_acct_step_ids)
    log.info(
        "Streaming jobs submitted. Monitor tại: "
        "https://console.aws.amazon.com/elasticmapreduce/home → cluster %s → Steps",
        get_cluster_id(),
    )


# ─────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────
with DAG(
    dag_id="streaming_lifecycle",
    description="Khởi động pipeline CDC: bootstrap → sync check → streaming jobs",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,       # manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "cdc", "lifecycle"],
) as dag:

    # ── 1. Kiểm tra có cần bootstrap không ──
    check_bootstrap = BranchPythonOperator(
        task_id="check_bootstrap_needed",
        python_callable=_check_bootstrap_needed,
    )

    # ── 2a. Bootstrap dry-run trước (kiểm tra, không ghi) ──
    bootstrap_dry_run = EmrAddStepsOperator(
        task_id="bootstrap_dry_run",
        job_flow_id=get_cluster_id(),
        steps=[make_python_step(
            name="bootstrap_txn_acct_dry_run",
            script_path="tools/bootstrap_txn_acct.py",
            script_args=["--dry-run"],
        )],
        aws_conn_id="aws_default",
    )

    bootstrap_dry_run_sensor = EmrStepSensor(
        task_id="wait_bootstrap_dry_run",
        job_flow_id=get_cluster_id(),
        step_id="{{ task_instance.xcom_pull('bootstrap_dry_run', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=600,
    )

    # ── 2b. Bootstrap thật (MERGE vào T24_TXN_ACCOUNT_SNAPSHOT) ──
    bootstrap_run = EmrAddStepsOperator(
        task_id="bootstrap_txn_acct",
        job_flow_id=get_cluster_id(),
        steps=[make_python_step(
            name="bootstrap_txn_acct",
            script_path="tools/bootstrap_txn_acct.py",
            script_args=["--batch-size", "2000"],
        )],
        aws_conn_id="aws_default",
    )

    bootstrap_run_sensor = EmrStepSensor(
        task_id="wait_bootstrap_txn_acct",
        job_flow_id=get_cluster_id(),
        step_id="{{ task_instance.xcom_pull('bootstrap_txn_acct', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=7200,   # 2 giờ — bootstrap lớn có thể lâu
    )

    mark_bootstrap_done = PythonOperator(
        task_id="mark_bootstrap_completed",
        python_callable=_set_bootstrap_completed,
    )

    # ── 2c. Skip nếu đã bootstrap ──
    from airflow.operators.empty import EmptyOperator
    skip_bootstrap = EmptyOperator(task_id="skip_bootstrap")

    # ── 3. Verify sync job healthy (T24_*_TARGET đang được sync) ──
    verify_sync = EmrAddStepsOperator(
        task_id="verify_sync_job_healthy",
        job_flow_id=get_cluster_id(),
        steps=[make_python_step(
            name="debug_counts_pre_check",
            script_path="tools/debug_counts.py",
            script_args=[],
        )],
        aws_conn_id="aws_default",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    verify_sync_sensor = EmrStepSensor(
        task_id="wait_verify_sync",
        job_flow_id=get_cluster_id(),
        step_id="{{ task_instance.xcom_pull('verify_sync_job_healthy', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=300,
    )

    # ── 4a. Start static_join (TXN ⋈ BRANCH) ──
    # Fire-and-forget: streaming job chạy mãi → không dùng Sensor chờ complete
    start_static_join = EmrAddStepsOperator(
        task_id="start_static_join",
        job_flow_id=get_cluster_id(),
        steps=[make_spark_submit_step(
            name="spark_static_join",
            job_args=["--jobs", "static_join"],
        )],
        aws_conn_id="aws_default",
    )

    # ── 4b. Start txn_acct_join (TXN ⋈ ACCOUNT, startingOffsets=latest) ──
    start_txn_acct_join = EmrAddStepsOperator(
        task_id="start_txn_acct_join",
        job_flow_id=get_cluster_id(),
        steps=[make_spark_submit_step(
            name="spark_txn_acct_join",
            job_args=["--jobs", "txn_acct_join"],
        )],
        aws_conn_id="aws_default",
    )

    # ── 5. Xác nhận đã submit ──
    verify_started = PythonOperator(
        task_id="verify_streaming_started",
        python_callable=_verify_streaming_started,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ─────────────────────────────────────────────
    # DEPENDENCIES
    # ─────────────────────────────────────────────
    check_bootstrap >> bootstrap_dry_run >> bootstrap_dry_run_sensor
    bootstrap_dry_run_sensor >> bootstrap_run >> bootstrap_run_sensor >> mark_bootstrap_done
    check_bootstrap >> skip_bootstrap

    [mark_bootstrap_done, skip_bootstrap] >> verify_sync >> verify_sync_sensor

    verify_sync_sensor >> [start_static_join, start_txn_acct_join]
    [start_static_join, start_txn_acct_join] >> verify_started
