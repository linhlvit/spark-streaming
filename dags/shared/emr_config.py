"""
shared/emr_config.py — Cấu hình EMR dùng chung cho tất cả DAG.

Tất cả giá trị hardcode ở đây nên được override bằng Airflow Variables
hoặc AWS Parameter Store trong môi trường production.

Airflow Variables cần thiết (set qua MWAA UI hoặc Terraform):
    EMR_CLUSTER_ID          — ID của EMR cluster đang chạy (e.g. j-XXXXXXXXXXXX)
    S3_JOBS_PATH            — s3://bucket/jobs  (không có trailing slash)
    S3_LOGS_PATH            — s3://bucket/logs/emr
    S3_CHECKPOINTS_PATH     — s3://bucket/checkpoints
    SPARK_PACKAGES          — org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5
"""

from airflow.models import Variable

# ─────────────────────────────────────────────
# EMR
# ─────────────────────────────────────────────
def get_cluster_id() -> str:
    return Variable.get("EMR_CLUSTER_ID")

# ─────────────────────────────────────────────
# S3 paths — sync từ repo này lên S3 trước khi chạy DAG
# ─────────────────────────────────────────────
def get_s3_jobs_path() -> str:
    return Variable.get("S3_JOBS_PATH", default_var="s3://lpb-poc-bucket/jobs")

def get_s3_logs_path() -> str:
    return Variable.get("S3_LOGS_PATH", default_var="s3://lpb-poc-bucket/logs/emr")

def get_s3_checkpoints_path() -> str:
    return Variable.get("S3_CHECKPOINTS_PATH", default_var="s3://lpb-poc-bucket/checkpoints")

def get_spark_packages() -> str:
    return Variable.get(
        "SPARK_PACKAGES",
        default_var="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
    )

# ─────────────────────────────────────────────
# EMR Step defaults
# ─────────────────────────────────────────────
EMR_STEP_ACTION_ON_FAILURE = "CONTINUE"  # không dừng cluster khi 1 step fail

def make_spark_submit_step(name: str, job_args: list[str]) -> dict:
    """
    Tạo EMR Step config cho spark-submit.
    job_args: danh sách args sau tên file, ví dụ ["--jobs", "sync,static_join"]
    """
    s3_jobs    = get_s3_jobs_path()
    s3_ckpt    = get_s3_checkpoints_path()
    packages   = get_spark_packages()
    logs_path  = get_s3_logs_path()

    return {
        "Name": name,
        "ActionOnFailure": EMR_STEP_ACTION_ON_FAILURE,
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--master", "yarn",
                "--conf", f"spark.jars.packages={packages}",
                "--conf", f"spark.checkpoint.dir={s3_ckpt}",
                "--conf", "spark.sql.streaming.multipleWatermarkPolicy=min",
                "--py-files", f"{s3_jobs}/packages.zip",
                f"{s3_jobs}/main.py",
                *job_args,
            ],
        },
    }


def make_python_step(name: str, script_path: str, script_args: list[str] | None = None) -> dict:
    """
    Tạo EMR Step config cho python script (DLQ retry, cleanup, debug).
    script_path: đường dẫn tương đối từ S3_JOBS_PATH, e.g. "tools/retry_txn_branch_dlq.py"
    """
    s3_jobs = get_s3_jobs_path()
    args = script_args or []

    return {
        "Name": name,
        "ActionOnFailure": EMR_STEP_ACTION_ON_FAILURE,
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "bash", "-c",
                (
                    f"aws s3 sync {s3_jobs}/ /tmp/spark_jobs/ --quiet && "
                    f"cd /tmp/spark_jobs && "
                    f"python3 {script_path} {' '.join(args)}"
                ),
            ],
        },
    }
