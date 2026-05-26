"""
schema_parser.py — Parse file create_target_tables.sql để lấy metadata bảng.

Trả về dict:
    {
        "TABLE_NAME": {
            "pk": "PK_COLUMN",
            "columns": {"COL_NAME": "ORACLE_TYPE", ...}
        },
        ...
    }
"""

import re
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# Keyword SQL không phải tên cột
_SQL_KEYWORDS = {"CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "FOREIGN"}


def parse_sql_file(sql_file_path: str) -> dict:
    """
    Đọc file DDL SQL và trả về metadata (pk + column types) cho từng bảng.

    Args:
        sql_file_path: Đường dẫn tới file create_target_tables.sql

    Returns:
        dict { source_table_name: {"pk": str, "columns": {col: type}} }

    Raises:
        FileNotFoundError: Nếu file không tồn tại.
    """
    with open(sql_file_path, "r") as f:
        content = f.read()

    result: dict = {}

    table_blocks = re.findall(
        r"CREATE\s+TABLE\s+\w+\.(\w+)\s*\((.*?)\);",
        content,
        re.IGNORECASE | re.DOTALL,
    )

    if not table_blocks:
        logger.warning(f"Không tìm thấy CREATE TABLE nào trong {sql_file_path}")
        return result

    for table_name, body in table_blocks:
        # Bỏ _TARGET suffix để map với tên bảng nguồn Kafka
        source_name = table_name.upper().replace("_TARGET", "")
        columns: Dict = {}
        pk: Optional[str] = None

        for line in body.split("\n"):
            line = line.strip().rstrip(",")
            if not line or line.startswith("--"):
                continue

            # PRIMARY KEY constraint
            pk_match = re.search(
                r"CONSTRAINT\s+\w+\s+PRIMARY\s+KEY\s*\((\w+)\)",
                line,
                re.IGNORECASE,
            )
            if pk_match:
                pk = pk_match.group(1).upper()
                continue

            # Column definition: COL_NAME TYPE[(precision)] [NOT NULL] ...
            col_match = re.match(r"(\w+)\s+(\w+)(\s*\([^)]*\))?", line)
            if col_match:
                col_name = col_match.group(1).upper()
                col_type = col_match.group(2).upper()
                if col_name in _SQL_KEYWORDS:
                    continue
                columns[col_name] = col_type

        result[source_name] = {"pk": pk, "columns": columns}
        logger.info(f"Parsed {source_name}: pk={pk}, cols={list(columns.keys())}")

    return result
