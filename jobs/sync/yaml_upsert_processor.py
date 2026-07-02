"""
yaml_upsert_processor.py — Spark Structured Streaming pipeline (upsert-only).

Khác với yaml_stream_processor.py:
  - Không xử lý op=d (delete) — chỉ ghi r/c/u
  - Không dedup theo max_by offset — ghi tất cả rows trong batch theo thứ tự Kafka
  - Schema Debezium envelope được cấu hình per-topic trong YAML

Cách dùng:
    spark-submit ... main.py --jobs yaml_upsert
    spark-submit ... main.py --jobs yaml_upsert --upsert-config /opt/spark/jobs/sync/table_sync_configs_upsert.yml

File YAML mẫu: jobs/sync/table_sync_configs_upsert.yml
"""

import json
import logging
import os
import time
from typing import Any, Dict, List

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from config import (
    CHECKPOINT_BASE,
    KAFKA_BOOTSTRAP_SERVERS,
    MAX_OFFSETS_PER_TRIGGER,
    TOPIC_PREFIX,
    TRIGGER_INTERVAL,
)
from core.oracle_writer import upsert_rows

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "table_sync_configs_upsert.yml")

_DEFAULT_ENVELOPE_SCHEMA = {
    "op":     "STRING",
    "before": "STRING",
    "after":  "STRING",
    "ts_ms":  "STRING",
}

_SPARK_TYPE_MAP = {
    "STRING":  StringType(),
    "LONG":    LongType(),
    "INTEGER": IntegerType(),
    "BOOLEAN": BooleanType(),
}


# ─────────────────────────────────────────────
# ENVELOPE SCHEMA BUILDER
# ─────────────────────────────────────────────

def _build_envelope_schema(schema_cfg: Dict) -> StructType:
    """Build StructType từ dict {field_name: type_string} trong YAML."""
    return StructType([
        StructField(name, _SPARK_TYPE_MAP.get(type_str.upper(), StringType()), True)
        for name, type_str in schema_cfg.items()
    ])


# ─────────────────────────────────────────────
# CONFIG LOADER
# ─────────────────────────────────────────────

def load_yaml_config(config_path: str) -> Dict[str, Any]:
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Không tìm thấy file config: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    if not config or "tables" not in config:
        raise ValueError(f"File config thiếu key 'tables': {config_path}")
    return config


def _resolve_table_config(table_cfg: Dict, defaults: Dict) -> Dict:
    merged = {**defaults, **table_cfg}

    required = ["job_name", "target_table", "key", "source_schema", "mapping"]
    missing = [k for k in required if not merged.get(k)]
    if missing:
        raise ValueError(
            f"Config bảng '{merged.get('job_name', '?')}' thiếu trường bắt buộc: {missing}"
        )

    if not merged.get("topic"):
        suffix = merged.get("topic_suffix") or merged.get("job_name", "").upper()
        prefix = merged.get("topic_prefix", TOPIC_PREFIX)
        merged["topic"] = f"{prefix}.{suffix}"

    if not merged.get("checkpoint_dir"):
        merged["checkpoint_dir"] = (
            f"{merged.get('checkpoint_base', CHECKPOINT_BASE)}/{merged['job_name']}"
        )

    merged.setdefault("trigger_interval", TRIGGER_INTERVAL)
    merged.setdefault("max_offsets_per_trigger", MAX_OFFSETS_PER_TRIGGER)
    merged.setdefault("starting_offsets", "earliest")
    merged.setdefault("fail_on_data_loss", False)
    merged.setdefault("time_column", None)
    merged.setdefault("debezium_envelope_schema", _DEFAULT_ENVELOPE_SCHEMA)

    return merged


# ─────────────────────────────────────────────
# KAFKA READER
# ─────────────────────────────────────────────

def _read_kafka_stream(spark: SparkSession, table_cfg: Dict) -> DataFrame:
    topic             = table_cfg["topic"]
    bootstrap_servers = table_cfg.get("kafka_bootstrap_servers", KAFKA_BOOTSTRAP_SERVERS)
    starting_offsets  = table_cfg["starting_offsets"]
    fail_on_data_loss = str(table_cfg["fail_on_data_loss"]).lower()
    max_offsets       = table_cfg["max_offsets_per_trigger"]

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

def _parse_debezium(raw_df: DataFrame, envelope_schema: StructType) -> DataFrame:
    """
    Parse Debezium CDC envelope, chỉ giữ lại op r/c/u (upsert).
    Output schema: _op STRING, _kafka_offset LONG, row_data STRING
    """
    return (
        raw_df
        .select(
            from_json(col("value").cast("string"), envelope_schema).alias("msg"),
            col("offset").alias("_kafka_offset"),
        )
        .select("msg.*", "_kafka_offset")
        .filter(col("op").isin("r", "c", "u"))
        .select(
            col("op").alias("_op"),
            col("_kafka_offset"),
            col("after").alias("row_data"),
        )
        .filter(col("row_data").isNotNull())
    )


# ─────────────────────────────────────────────
# MAPPING RESOLVER
# ─────────────────────────────────────────────

def _apply_mapping(row_dict: Dict, mapping: Dict, source_schema: Dict) -> Dict:
    result: Dict = {}
    for target_col, source_expr in mapping.items():
        if source_expr is None:
            continue
        source_expr_str = str(source_expr).strip()
        is_expression = (
            "(" in source_expr_str
            or source_expr_str.upper().startswith("SELECT ")
        )
        if not is_expression:
            result[target_col] = row_dict.get(source_expr_str)
        else:
            result[target_col] = _eval_simple_expression(source_expr_str, row_dict)
    return result


def _eval_simple_expression(expression: str, row_dict: Dict) -> Any:
    import re

    upper = expression.upper().strip()

    m = re.fullmatch(r"UPPER\((\w+)\)", upper)
    if m:
        val = row_dict.get(m.group(1))
        return str(val).upper() if val is not None else None

    m = re.fullmatch(r"LOWER\((\w+)\)", upper)
    if m:
        val = row_dict.get(m.group(1))
        return str(val).lower() if val is not None else None

    m = re.fullmatch(r"ABS\((\w+)\)", upper)
    if m:
        val = row_dict.get(m.group(1))
        if val is not None:
            try:
                return abs(float(val))
            except (ValueError, TypeError):
                return val
        return None

    m = re.fullmatch(r"COALESCE\((.+)\)", upper)
    if m:
        cols = [c.strip() for c in m.group(1).split(",")]
        for c in cols:
            val = row_dict.get(c)
            if val is not None:
                return val
        return None

    return row_dict.get(expression)


# ─────────────────────────────────────────────
# BATCH WRITER (foreachBatch callback)
# ─────────────────────────────────────────────

def _make_batch_writer(table_cfg: Dict):
    """
    Factory trả về hàm write_batch dùng cho foreachBatch.
    Không dedup — ghi tất cả rows upsert trong batch theo thứ tự Kafka offset.
    """
    job_name      = table_cfg["job_name"]
    target_table  = table_cfg["target_table"]
    pk            = table_cfg["key"]
    time_column   = table_cfg.get("time_column")
    source_schema = table_cfg["source_schema"]
    mapping       = table_cfg["mapping"]
    col_types     = {k: v.split("(")[0].strip() for k, v in source_schema.items()}

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        t_start = time.time()
        print(f"[batch={batch_id}] START: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t_start))}")

        if batch_df.isEmpty():
            t_end = time.time()
            print(f"[batch={batch_id}] END (empty): elapsed={t_end - t_start:.3f}s")
            return

        spark = batch_df.sparkSession
        spark.sparkContext.setJobDescription(
            f"[yaml_upsert:{job_name}] batch={batch_id} — collect rows"
        )

        upsert_data: List[Dict] = []

        for row in batch_df.collect():
            raw_dict   = json.loads(row["row_data"])
            mapped_row = _apply_mapping(raw_dict, mapping, source_schema)
            upsert_data.append(mapped_row)

        if upsert_data:
            upsert_rows(upsert_data, target_table, pk, col_types, time_column)
            logger.info(
                f"[yaml_upsert:{job_name}] batch={batch_id} upserted {len(upsert_data)} rows"
            )

        t_end = time.time()
        print(f"[batch={batch_id}] END: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t_end))} | elapsed={t_end - t_start:.3f}s")

    return write_batch


# ─────────────────────────────────────────────
# PUBLIC: START ONE TABLE STREAM
# ─────────────────────────────────────────────

def start_yaml_upsert_table_stream(spark: SparkSession, table_cfg: Dict) -> StreamingQuery:
    job_name        = table_cfg["job_name"]
    checkpoint_dir  = table_cfg["checkpoint_dir"]
    trigger         = table_cfg["trigger_interval"]
    envelope_schema = _build_envelope_schema(table_cfg["debezium_envelope_schema"])

    logger.info(
        f"[yaml_upsert] Starting: topic={table_cfg['topic']} "
        f"→ {table_cfg['target_table']} (pk={table_cfg['key']}, "
        f"time_col={table_cfg.get('time_column')}, checkpoint={checkpoint_dir})"
    )

    raw_df    = _read_kafka_stream(spark, table_cfg)
    parsed_df = _parse_debezium(raw_df, envelope_schema)

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

def start_all_yaml_upsert_streams(
    spark: SparkSession,
    config_path: str = DEFAULT_CONFIG_PATH,
) -> List[StreamingQuery]:
    config   = load_yaml_config(config_path)
    defaults = config.get("defaults", {})
    tables   = config.get("tables", [])

    if not tables:
        logger.warning(f"[yaml_upsert] Không có bảng nào trong config: {config_path}")
        return []

    queries: List[StreamingQuery] = []
    for raw_cfg in tables:
        if not raw_cfg.get("enabled", True):
            logger.info(f"[yaml_upsert] Bỏ qua '{raw_cfg.get('job_name')}' (enabled: false)")
            continue

        try:
            table_cfg = _resolve_table_config(raw_cfg, defaults)
        except ValueError as e:
            logger.error(f"[yaml_upsert] Config lỗi, bỏ qua bảng: {e}")
            continue

        q = start_yaml_upsert_table_stream(spark, table_cfg)
        queries.append(q)
        logger.info(f"[yaml_upsert] Started '{table_cfg['job_name']}'")

    logger.info(f"[yaml_upsert] Tổng {len(queries)} stream(s) đang chạy từ {config_path}")
    return queries


def get_yaml_upsert_checkpoint_paths(config_path: str = DEFAULT_CONFIG_PATH) -> List[str]:
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
        logger.warning(f"[yaml_upsert] Không đọc được checkpoint paths: {e}")
        return []
