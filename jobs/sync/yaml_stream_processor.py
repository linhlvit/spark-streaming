"""
yaml_stream_processor.py — Spark Structured Streaming pipeline được cấu hình qua YAML.

Thay vì hardcode danh sách bảng trong config.py, job này đọc file YAML để biết:
  - Tên topic Kafka nguồn
  - Tên bảng đích Oracle
  - Schema nguồn / schema đích
  - Primary key và time_column (last-write-wins guard)
  - Mapping cột: nguồn → đích (hỗ trợ expression như UPPER(col), ABS(col), ...)

Cách dùng:
    spark-submit ... main.py --jobs yaml_sync
    spark-submit ... main.py --jobs yaml_sync --sync-config /opt/spark/jobs/sync/table_sync_configs.yml

File YAML mẫu: jobs/sync/table_sync_configs.yml
"""

import json
import logging
import os
import time
from typing import Any, Dict, List

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, expr, from_json, get_json_object, max_by, when
from pyspark.sql.streaming import StreamingQuery

from core.oracle_writer import upsert_rows, delete_rows

from config import (
    CHECKPOINT_BASE,
    DEBEZIUM_ENVELOPE_SCHEMA,
    KAFKA_BOOTSTRAP_SERVERS,
    MAX_OFFSETS_PER_TRIGGER,
    TOPIC_PREFIX,
    TRIGGER_INTERVAL,
)
logger = logging.getLogger(__name__)

# Path mặc định — override bằng --sync-config khi spark-submit
DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "table_sync_configs.yml")


# ─────────────────────────────────────────────
# CONFIG LOADER
# ─────────────────────────────────────────────

def load_yaml_config(config_path: str) -> Dict[str, Any]:
    """
    Đọc và validate file YAML config.

    Args:
        config_path: Đường dẫn tuyệt đối hoặc tương đối tới file YAML.

    Returns:
        Dict chứa 'defaults' và 'tables'.

    Raises:
        FileNotFoundError: Nếu file không tồn tại.
        ValueError: Nếu thiếu trường bắt buộc trong config.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Không tìm thấy file config: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if not config or "tables" not in config:
        raise ValueError(f"File config thiếu key 'tables': {config_path}")

    return config


def _resolve_table_config(table_cfg: Dict, defaults: Dict) -> Dict:
    """
    Merge defaults vào table_cfg — table_cfg có độ ưu tiên cao hơn.
    Trả về dict đầy đủ các trường cần thiết.
    """
    merged = {**defaults, **table_cfg}

    # Validate các trường bắt buộc
    required = ["job_name", "target_table", "key", "source_schema", "mapping"]
    missing = [k for k in required if not merged.get(k)]
    if missing:
        raise ValueError(
            f"Config bảng '{merged.get('job_name', '?')}' thiếu trường bắt buộc: {missing}"
        )

    # Resolve topic: dùng trực tiếp nếu có, ngược lại ghép prefix + suffix
    if not merged.get("topic"):
        suffix = merged.get("topic_suffix") or merged.get("job_name", "").upper()
        prefix = merged.get("topic_prefix", TOPIC_PREFIX)
        merged["topic"] = f"{prefix}.{suffix}"

    # Resolve checkpoint directory
    if not merged.get("checkpoint_dir"):
        merged["checkpoint_dir"] = (
            f"{merged.get('checkpoint_base', CHECKPOINT_BASE)}/{merged['job_name']}"
        )

    # Resolve trigger / offsets — fallback về giá trị global trong config.py
    merged.setdefault("trigger_interval", TRIGGER_INTERVAL)
    merged.setdefault("max_offsets_per_trigger", MAX_OFFSETS_PER_TRIGGER)
    merged.setdefault("starting_offsets", "earliest")
    merged.setdefault("fail_on_data_loss", False)
    merged.setdefault("time_column", None)

    return merged


# ─────────────────────────────────────────────
# KAFKA READER
# ─────────────────────────────────────────────

def _read_kafka_stream(spark: SparkSession, table_cfg: Dict) -> DataFrame:
    """Đọc raw Kafka stream theo cấu hình từ YAML."""
    topic              = table_cfg["topic"]
    bootstrap_servers  = table_cfg.get("kafka_bootstrap_servers", KAFKA_BOOTSTRAP_SERVERS)
    starting_offsets   = table_cfg["starting_offsets"]
    fail_on_data_loss  = str(table_cfg["fail_on_data_loss"]).lower()
    max_offsets        = table_cfg["max_offsets_per_trigger"]

    logger.info(f"[{table_cfg['job_name']}] Kafka subscribe: {topic}")

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", fail_on_data_loss)
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )


# ─────────────────────────────────────────────
# DEBEZIUM PARSER
# ─────────────────────────────────────────────

def _parse_debezium(raw_df: DataFrame) -> DataFrame:
    """
    Parse Debezium CDC envelope từ Kafka value (JSON string).
    Output schema: _op STRING, _kafka_offset LONG, row_data STRING
    """
    envelope_df = (
        raw_df
        .select(
            from_json(col("value").cast("string"), DEBEZIUM_ENVELOPE_SCHEMA).alias("msg"),
            col("offset").alias("_kafka_offset"),
        )
        .select("msg.*", "_kafka_offset")
        .filter(col("op").isNotNull())
    )

    return (
        envelope_df
        .withColumn(
            "row_data",
            when(col("op").isin("r", "c", "u"), col("after"))
            .otherwise(col("before"))
        )
        .withColumn("_op", col("op"))
        .select("_op", "_kafka_offset", "row_data")
        .filter(col("row_data").isNotNull())
    )


# ─────────────────────────────────────────────
# MAPPING RESOLVER
# ─────────────────────────────────────────────

def _apply_mapping(row_dict: Dict, mapping: Dict, source_schema: Dict) -> Dict:
    """
    Áp dụng mapping cột từ YAML lên một row dict từ Debezium payload.

    Hỗ trợ 2 dạng mapping:
      1. tên_cột_nguồn          → copy giá trị trực tiếp
      2. "FUNC(col)"            → expression đơn giản (UPPER, LOWER, ABS, COALESCE, ...)

    Args:
        row_dict      : Dict raw từ JSON payload Debezium.
        mapping       : Dict {target_col: source_col_or_expr} từ YAML.
        source_schema : Dict {col_name: oracle_type} để biết kiểu dữ liệu.

    Returns:
        Dict đã được map sang cột đích.
    """
    result: Dict = {}
    for target_col, source_expr in mapping.items():
        if source_expr is None:
            continue  # null trong YAML → bỏ qua cột này

        source_expr_str = str(source_expr).strip()

        # Kiểm tra xem có phải expression không (có dấu ngoặc hoặc hàm SQL đơn giản)
        is_expression = (
            "(" in source_expr_str
            or source_expr_str.upper().startswith("SELECT ")
        )

        if not is_expression:
            # Mapping đơn giản: lấy giá trị từ nguồn theo tên cột
            result[target_col] = row_dict.get(source_expr_str)
        else:
            # Expression đơn giản: hỗ trợ UPPER, LOWER, ABS, COALESCE
            # Chỉ áp dụng ở collect() time — dùng Python thuần
            value = _eval_simple_expression(source_expr_str, row_dict)
            result[target_col] = value

    return result


def _eval_simple_expression(expression: str, row_dict: Dict) -> Any:
    """
    Evaluate biểu thức mapping đơn giản trên một row dict.

    Hỗ trợ:
      - UPPER(col)
      - LOWER(col)
      - ABS(col)
      - COALESCE(col1, col2)
      - Fallback: coi expression là tên cột trực tiếp

    Args:
        expression : Chuỗi biểu thức từ YAML mapping.
        row_dict   : Dict giá trị raw của row.

    Returns:
        Giá trị đã xử lý, hoặc None nếu không resolve được.
    """
    import re

    upper = expression.upper().strip()

    # UPPER(col)
    m = re.fullmatch(r"UPPER\((\w+)\)", upper)
    if m:
        val = row_dict.get(m.group(1))
        return str(val).upper() if val is not None else None

    # LOWER(col)
    m = re.fullmatch(r"LOWER\((\w+)\)", upper)
    if m:
        val = row_dict.get(m.group(1))
        return str(val).lower() if val is not None else None

    # ABS(col)
    m = re.fullmatch(r"ABS\((\w+)\)", upper)
    if m:
        val = row_dict.get(m.group(1))
        if val is not None:
            try:
                return abs(float(val))
            except (ValueError, TypeError):
                return val
        return None

    # COALESCE(col1, col2, ...)
    m = re.fullmatch(r"COALESCE\((.+)\)", upper)
    if m:
        cols = [c.strip() for c in m.group(1).split(",")]
        for c in cols:
            val = row_dict.get(c)
            if val is not None:
                return val
        return None

    # Fallback: coi expression là tên cột
    return row_dict.get(expression)


# ─────────────────────────────────────────────
# BATCH WRITER (foreachBatch callback)
# ─────────────────────────────────────────────

def _make_batch_writer(table_cfg: Dict):
    """
    Factory trả về hàm write_batch dùng cho foreachBatch.
    Đóng gói toàn bộ cấu hình từ YAML vào closure.

    Args:
        table_cfg : Dict cấu hình đã resolved của một bảng.
    """
    job_name      = table_cfg["job_name"]
    target_table  = table_cfg["target_table"]
    pk            = table_cfg["key"]
    time_column   = table_cfg.get("time_column")  # None nếu không có
    source_schema = table_cfg["source_schema"]     # {col: type}
    mapping       = table_cfg["mapping"]           # {target_col: src_col_or_expr}

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        t_start = time.time()
        print(f"[batch={batch_id}] START: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t_start))}")
        
        if batch_df.isEmpty():
            print(f"[batch={batch_id}] END (empty): {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t_end))} | elapsed={t_end - t_start:.3f}s")
            return

        spark = batch_df.sparkSession
        spark.sparkContext.setJobDescription(
            f"[yaml_sync:{job_name}] batch={batch_id} — dedup & collect"
        )

        # Dedup: mỗi PK chỉ giữ event mới nhất trong batch theo Kafka offset.
        # Bảo đảm sequence INSERT→DELETE→INSERT kết thúc bằng INSERT đúng.
        batch_deduped = (
            batch_df
            .withColumn("_pk_val", get_json_object(col("row_data"), f"$.{pk}"))
            .groupBy("_pk_val")
            .agg(
                max_by(
                    expr(
                        "named_struct('_op', _op, "
                        "'_kafka_offset', _kafka_offset, 'row_data', row_data)"
                    ),
                    col("_kafka_offset"),
                ).alias("latest")
            )
            .select(
                col("latest._op").alias("_op"),
                col("latest._kafka_offset").alias("_kafka_offset"),
                col("latest.row_data").alias("row_data"),
            )
        )

        spark.sparkContext.setJobDescription(
            f"[yaml_sync:{job_name}] batch={batch_id} — collect rows"
        )
        
        upsert_data: List[Dict] = []
        delete_data: List[Dict] = []

        for row in batch_deduped.collect():
            op       = row["_op"]
            raw_dict = json.loads(row["row_data"])

            # Áp dụng mapping YAML: nguồn → đích (kể cả expression)
            mapped_row = _apply_mapping(raw_dict, mapping, source_schema)

            if op in ("r", "c", "u"):
                upsert_data.append(mapped_row)
            elif op == "d":
                delete_data.append(mapped_row)

        # col_types chỉ chứa base type (VARCHAR2, NUMBER, TIMESTAMP, ...)
        # dùng source_schema vì kiểu dữ liệu logic giống nhau nguồn/đích
        col_types = {k: v.split("(")[0].strip() for k, v in source_schema.items()}

        if upsert_data:
            upsert_rows(upsert_data, target_table, pk, col_types, time_column)
            logger.info(
                f"[yaml_sync:{job_name}] batch={batch_id} upserted {len(upsert_data)} rows"
            )

        if delete_data:
            delete_rows(delete_data, target_table, pk)
            logger.info(
                f"[yaml_sync:{job_name}] batch={batch_id} deleted {len(delete_data)} rows"
            )
        t_end = time.time()
        print(f"[batch={batch_id}] END: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t_end))} | elapsed={t_end - t_start:.3f}s")

    return write_batch


# ─────────────────────────────────────────────
# PUBLIC: START ONE TABLE STREAM
# ─────────────────────────────────────────────

def start_yaml_table_stream(spark: SparkSession, table_cfg: Dict) -> StreamingQuery:
    """
    Khởi động một streaming query từ cấu hình YAML của một bảng.

    Args:
        spark      : SparkSession đang chạy.
        table_cfg  : Dict cấu hình đã resolved (từ _resolve_table_config).

    Returns:
        StreamingQuery — có thể gọi .awaitTermination() hoặc .stop().
    """
    job_name       = table_cfg["job_name"]
    checkpoint_dir = table_cfg["checkpoint_dir"]
    trigger        = table_cfg["trigger_interval"]

    logger.info(
        f"[yaml_sync] Starting: topic={table_cfg['topic']} "
        f"→ {table_cfg['target_table']} (pk={table_cfg['key']}, "
        f"time_col={table_cfg.get('time_column')}, checkpoint={checkpoint_dir})"
    )

    raw_df    = _read_kafka_stream(spark, table_cfg)
    parsed_df = _parse_debezium(raw_df)

    return (
        parsed_df.writeStream
        .foreachBatch(_make_batch_writer(table_cfg))
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=trigger)
        .start()
    )


# ─────────────────────────────────────────────
# PUBLIC: START ALL ENABLED TABLE STREAMS
# ─────────────────────────────────────────────

def start_all_yaml_streams(
    spark: SparkSession,
    config_path: str = DEFAULT_CONFIG_PATH,
) -> List[StreamingQuery]:
    """
    Đọc file YAML, khởi động streaming query cho tất cả bảng `enabled: true`.

    Args:
        spark       : SparkSession đang chạy.
        config_path : Đường dẫn tới file YAML config.

    Returns:
        List[StreamingQuery] — tất cả queries đang chạy.
    """
    config   = load_yaml_config(config_path)
    defaults = config.get("defaults", {})
    tables   = config.get("tables", [])

    if not tables:
        logger.warning(f"[yaml_sync] Không có bảng nào trong config: {config_path}")
        return []

    queries: List[StreamingQuery] = []
    for raw_cfg in tables:
        if not raw_cfg.get("enabled", True):
            logger.info(f"[yaml_sync] Bỏ qua '{raw_cfg.get('job_name')}' (enabled: false)")
            continue

        try:
            table_cfg = _resolve_table_config(raw_cfg, defaults)
        except ValueError as e:
            logger.error(f"[yaml_sync] Config lỗi, bỏ qua bảng: {e}")
            continue

        q = start_yaml_table_stream(spark, table_cfg)
        queries.append(q)
        logger.info(f"[yaml_sync] Started '{table_cfg['job_name']}'")

    logger.info(f"[yaml_sync] Tổng {len(queries)} stream(s) đang chạy từ {config_path}")
    return queries


def get_yaml_checkpoint_paths(config_path: str = DEFAULT_CONFIG_PATH) -> List[str]:
    """
    Trả về danh sách checkpoint directories của tất cả bảng enabled.
    Dùng trong main.py để acquire checkpoint lock.

    Args:
        config_path: Đường dẫn tới file YAML.

    Returns:
        List[str] các đường dẫn checkpoint.
    """
    try:
        config   = load_yaml_config(config_path)
        defaults = config.get("defaults", {})
        paths    = []
        for raw_cfg in config.get("tables", []):
            if not raw_cfg.get("enabled", True):
                continue
            resolved = _resolve_table_config(raw_cfg, defaults)
            paths.append(resolved["checkpoint_dir"])
        return paths
    except Exception as e:
        logger.warning(f"[yaml_sync] Không đọc được checkpoint paths: {e}")
        return []
