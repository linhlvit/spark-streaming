"""
oracle_writer.py — Ghi dữ liệu vào Oracle Target qua python-oracledb.

Public API:
    upsert_rows(rows, target_table, pk, col_types)
    delete_rows(rows, target_table, pk)
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import oracledb

from config import (
    ORACLE_DSN,
    ORACLE_USER,
    ORACLE_PASSWORD,
    TARGET_SCHEMA,
    DATE_TYPES,
    NUMBER_TYPES,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONNECTION POOL (tái dùng connection, tránh overhead tạo mới mỗi batch)
# ─────────────────────────────────────────────
_pool: Optional[oracledb.ConnectionPool] = None


def _get_pool() -> oracledb.ConnectionPool:
    global _pool
    if _pool is None:
        _pool = oracledb.create_pool(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=ORACLE_DSN,
            min=2,
            max=10,
            increment=1,
            ping_interval=60,       # ping connection mỗi 60s để giữ alive
            ping_timeout=5,
        )
        logger.info("Oracle connection pool created.")
    return _pool


def get_connection() -> oracledb.Connection:
    """Lấy connection từ pool, tự reset pool nếu bị drop (network/timeout).
    Không retry nếu lỗi là authentication — tránh vòng lặp vô ích.
    """
    global _pool
    try:
        conn = _get_pool().acquire()
        conn.ping()
        return conn
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        # ORA-01017: sai credentials — không retry, raise ngay để fail fast
        if hasattr(error_obj, "code") and error_obj.code == 1017:
            logger.error(
                "Oracle authentication failed (ORA-01017). "
                "Kiểm tra ORACLE_USER / ORACLE_PASSWORD trong config.py."
            )
            raise
        # Lỗi khác (network drop, pool timeout) → recreate pool và thử lại
        logger.warning(f"Connection pool error: {e}. Recreating pool...")
        try:
            if _pool:
                _pool.close(force=True)
        except Exception:
            pass
        _pool = None
        return _get_pool().acquire()
    except oracledb.InterfaceError as e:
        # DPY-1002: pool not open → recreate
        logger.warning(f"Connection pool error: {e}. Recreating pool...")
        try:
            if _pool:
                _pool.close(force=True)
        except Exception:
            pass
        _pool = None
        return _get_pool().acquire()


# ─────────────────────────────────────────────
# TYPE CONVERSION
# ─────────────────────────────────────────────
def _convert_value(value, col_type: str):
    """
    Convert giá trị Debezium (string/number) sang Python type phù hợp
    với Oracle column type.

    - DATE / TIMESTAMP : Debezium gửi epoch microseconds → datetime
    - NUMBER / FLOAT   : float hoặc int
    - Còn lại          : str
    """
    if value is None:
        return None

    base_type = col_type.upper()

    if base_type in DATE_TYPES:
        try:
            micros = int(float(value))
            return datetime.fromtimestamp(
                micros / 1_000_000, tz=timezone.utc
            ).replace(tzinfo=None)
        except (ValueError, OSError):
            logger.warning(f"Không convert được DATE value={value!r}, giữ nguyên.")
            return value

    if base_type in NUMBER_TYPES:
        try:
            f = float(value)
            return int(f) if f == int(f) else f
        except (ValueError, TypeError):
            logger.warning(f"Không convert được NUMBER value={value!r}, giữ nguyên.")
            return value

    return str(value)


# ─────────────────────────────────────────────
# SQL BUILDERS
# ─────────────────────────────────────────────
def _build_merge_sql(target_table: str, columns: List[str], pk: str,
                     event_ts_col: Optional[str] = None) -> str:
    """
    Tạo câu MERGE INTO ... USING DUAL để upsert một row.
    Dùng named bind variables (:COL_NAME).

    event_ts_col: nếu được cung cấp, thêm điều kiện last-write-wins vào WHEN MATCHED —
        chỉ UPDATE nếu event mới hơn row đang có trong target.
        Dùng để chống out-of-order CDC event ghi đè state mới bằng state cũ.
    """
    full_table    = f"{TARGET_SCHEMA}.{target_table}"
    update_cols   = [c for c in columns if c != pk]
    select_clause = ", ".join([f":{c} AS {c}" for c in columns])
    update_clause = ", ".join([f"t.{c} = s.{c}" for c in update_cols])
    insert_cols   = ", ".join(columns)
    insert_vals   = ", ".join([f"s.{c}" for c in columns])

    # last-write-wins: chỉ UPDATE nếu event_ts mới hơn row hiện tại
    lww_condition = ""
    if event_ts_col and event_ts_col in columns:
        lww_condition = f" AND t.{event_ts_col} < s.{event_ts_col}"

    return f"""
        MERGE INTO {full_table} t
        USING (SELECT {select_clause} FROM DUAL) s
        ON (t.{pk} = s.{pk})
        WHEN MATCHED THEN
            UPDATE SET {update_clause}
            WHERE 1=1{lww_condition}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
    """


def _build_delete_sql(target_table: str, pk: str) -> str:
    full_table = f"{TARGET_SCHEMA}.{target_table}"
    return f"DELETE FROM {full_table} WHERE {pk} = :{pk}"


# ─────────────────────────────────────────────
# PUBLIC WRITE FUNCTIONS
# ─────────────────────────────────────────────
def upsert_rows(
    rows: List[Dict],
    target_table: str,
    pk: str,
    col_types: Dict[str, str],
    event_ts_col: Optional[str] = None,
) -> None:
    """
    Upsert (MERGE) danh sách rows vào target_table.

    Args:
        rows         : List dict {col_name: raw_value} từ Debezium after payload.
        target_table : Tên bảng đích (không có schema prefix).
        pk           : Tên cột primary key.
        col_types    : Dict {col_name: oracle_type} từ schema_parser.
        event_ts_col : Tên cột timestamp dùng làm last-write-wins guard (optional).
                       Nếu cung cấp, MERGE chỉ UPDATE khi event mới hơn row hiện tại —
                       chống out-of-order CDC ghi đè state mới bằng state cũ.
    """
    if not rows:
        return

    columns   = list(rows[0].keys())
    merge_sql = _build_merge_sql(target_table, columns, pk, event_ts_col)

    # Convert type cho toàn bộ batch
    batch = [
        {k: _convert_value(v, col_types.get(k, "VARCHAR2")) for k, v in row.items()}
        for row in rows
    ]

    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.executemany(merge_sql, batch)
        conn.commit()
        logger.debug(f"Upserted {len(batch)} rows → {TARGET_SCHEMA}.{target_table}")
    except Exception:
        conn.rollback()
        logger.exception(f"Lỗi upsert vào {target_table}")
        raise
    finally:
        conn.close()


def delete_rows(
    rows: List[Dict],
    target_table: str,
    pk: str,
) -> None:
    """
    Xóa các row theo PK khỏi target_table.

    Args:
        rows        : List dict chứa ít nhất trường pk (từ Debezium before payload).
        target_table: Tên bảng đích.
        pk          : Tên cột primary key.
    """
    if not rows:
        return

    delete_sql = _build_delete_sql(target_table, pk)
    batch      = [{pk: str(row[pk])} for row in rows if row.get(pk)]

    if not batch:
        return

    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.executemany(delete_sql, batch)
        conn.commit()
        logger.debug(f"Deleted {len(batch)} rows ← {TARGET_SCHEMA}.{target_table}")
    except Exception:
        conn.rollback()
        logger.exception(f"Lỗi delete khỏi {target_table}")
        raise
    finally:
        conn.close()
