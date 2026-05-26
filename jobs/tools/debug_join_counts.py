"""
debug_join_counts.py — Debug join T24_ACCOUNT x T24_CUSTOMER (không dùng Spark)

Dùng confluent-kafka để đọc toàn bộ message từ 2 topic,
parse Debezium envelope, tạo pandas DataFrame,
sau đó thực hiện 2 loại FULL JOIN và in thống kê.

Cài đặt:
    pip install confluent-kafka pandas

Cách chạy:
    python jobs/debug/debug_join_counts.py
"""

import json
from datetime import datetime, timedelta

import pandas as pd
from confluent_kafka import Consumer, KafkaError, TopicPartition

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
ACCT_TOPIC = "oracle.FSS_STREAM.T24_ACCOUNT"
CUST_TOPIC = "oracle.FSS_STREAM.T24_CUSTOMER"

# Timeout chờ message mới (giây) — dừng đọc khi không có message mới trong khoảng này
IDLE_TIMEOUT_SEC = 5


# ─────────────────────────────────────────────
# ĐỌC TOÀN BỘ MESSAGE TỪ KAFKA TOPIC
# ─────────────────────────────────────────────
def read_all_messages(topic: str) -> list[dict]:
    """
    Đọc toàn bộ message từ một Kafka topic (từ offset 0 đến cuối).
    Trả về list các dict đã parse JSON từ value của message.
    """
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id":          f"debug_join_counts_{topic}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    # Lấy danh sách partition của topic
    metadata = consumer.list_topics(topic, timeout=10)
    partitions = [
        TopicPartition(topic, p, 0)
        for p in metadata.topics[topic].partitions.keys()
    ]

    # Seek về offset 0 cho tất cả partition
    consumer.assign(partitions)
    for tp in partitions:
        consumer.seek(tp)

    # Lấy high watermark (offset cuối) của từng partition
    end_offsets = {}
    for tp in partitions:
        _, high = consumer.get_watermark_offsets(tp, timeout=5)
        end_offsets[tp.partition] = high

    print(f"  Topic '{topic}': {len(partitions)} partition(s), "
          f"tổng message ≈ {sum(end_offsets.values()):,}")

    messages = []
    current_offsets = {tp.partition: 0 for tp in partitions}

    while True:
        # Kiểm tra đã đọc hết tất cả partition chưa
        all_done = all(
            current_offsets[p] >= end_offsets[p]
            for p in current_offsets
        )
        if all_done:
            break

        msg = consumer.poll(timeout=IDLE_TIMEOUT_SEC)

        if msg is None:
            # Không có message mới trong IDLE_TIMEOUT_SEC → coi như đã đọc xong
            break

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                current_offsets[msg.partition()] = end_offsets[msg.partition()]
                continue
            print(f"  [WARN] Kafka error: {msg.error()}")
            continue

        current_offsets[msg.partition()] = msg.offset() + 1

        value = msg.value()
        if value is None:
            continue  # tombstone message (Kafka compaction)

        try:
            parsed = json.loads(value.decode("utf-8"))
            messages.append(parsed)
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass  # bỏ qua message không parse được

    consumer.close()
    return messages


# ─────────────────────────────────────────────
# PARSE DEBEZIUM ENVELOPE → pandas DataFrame
# ─────────────────────────────────────────────
def parse_account_messages(messages: list[dict]) -> pd.DataFrame:
    """
    Parse Debezium envelope từ T24_ACCOUNT.
    Chỉ lấy op = r, c, u (bỏ delete).
    Thêm cột acct_event_ts (datetime) từ ts_ms.
    """
    rows = []
    for msg in messages:
        op = msg.get("op")
        if op not in ("r", "c", "u"):
            continue

        after_raw = msg.get("after")
        if not after_raw:
            continue

        # after có thể là string JSON hoặc dict tuỳ cấu hình Debezium
        after = json.loads(after_raw) if isinstance(after_raw, str) else after_raw

        account_id = after.get("ACCOUNT_ID")
        if not account_id:
            continue

        ts_ms = msg.get("ts_ms")
        try:
            acct_event_ts = datetime.utcfromtimestamp(int(ts_ms) / 1000) if ts_ms else None
        except (ValueError, TypeError):
            acct_event_ts = None

        rows.append({
            "ACCOUNT_ID":        account_id,
            "CUSTOMER_ID":       after.get("CUSTOMER_ID"),
            "WORKING_BALANCE":   after.get("WORKING_BALANCE"),
            "ONLINE_ACTUAL_BAL": after.get("ONLINE_ACTUAL_BAL"),
            "CURRENCY_CODE":     after.get("CURRENCY_CODE"),
            "BRANCH_CODE":       after.get("BRANCH_CODE"),
            "LAST_TX_TIME":      after.get("LAST_TX_TIME"),
            "EVENT_TIME":        after.get("EVENT_TIME"),
            "acct_event_ts":     acct_event_ts,
        })

    return pd.DataFrame(rows)


def parse_customer_messages(messages: list[dict]) -> pd.DataFrame:
    """
    Parse Debezium envelope từ T24_CUSTOMER.
    Chỉ lấy op = r, c, u (bỏ delete).
    Thêm cột cust_event_ts (datetime) từ ts_ms.
    """
    rows = []
    for msg in messages:
        op = msg.get("op")
        if op not in ("r", "c", "u"):
            continue

        after_raw = msg.get("after")
        if not after_raw:
            continue

        after = json.loads(after_raw) if isinstance(after_raw, str) else after_raw

        customer_id = after.get("CUSTOMER_ID")
        if not customer_id:
            continue

        ts_ms = msg.get("ts_ms")
        try:
            cust_event_ts = datetime.utcfromtimestamp(int(ts_ms) / 1000) if ts_ms else None
        except (ValueError, TypeError):
            cust_event_ts = None

        rows.append({
            "CUSTOMER_ID":   customer_id,
            "CUSTOMER_NAME": after.get("CUSTOMER_NAME"),
            "CUSTOMER_TYPE": after.get("CUSTOMER_TYPE"),
            "SEGMENT":       after.get("SEGMENT"),
            "STATUS":        after.get("STATUS"),
            "cust_event_ts": cust_event_ts,
        })

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# IN THỐNG KÊ JOIN
# ─────────────────────────────────────────────
def print_duplicate_customer_ids(label: str, matched: pd.DataFrame) -> None:
    """
    Trong matched DataFrame, tìm và in các CUSTOMER_ID xuất hiện nhiều hơn 1 lần.
    Dùng CUS_CUSTOMER_ID làm key (là CUSTOMER_ID phía customer sau khi rename).
    """
    # Dùng CUS_CUSTOMER_ID nếu có, fallback về CUSTOMER_ID
    id_col = "CUS_CUSTOMER_ID" if "CUS_CUSTOMER_ID" in matched.columns else "CUSTOMER_ID"

    counts = matched[id_col].value_counts()
    dupes  = counts[counts > 1]

    print(f"\n  [DUPLICATE CUSTOMER_ID trong matched — {label}]")
    if dupes.empty:
        print(f"    → Không có CUSTOMER_ID nào bị lặp.")
        return

    print(f"    → {len(dupes):,} CUSTOMER_ID bị lặp (tổng {dupes.sum():,} rows):\n")

    # In từng nhóm duplicate
    dup_ids = dupes.index.tolist()
    dup_rows = matched[matched[id_col].isin(dup_ids)].sort_values(
        by=[id_col, "acct_event_ts"] if "acct_event_ts" in matched.columns else [id_col]
    )

    # Chọn cột hiển thị gọn
    display_cols = [c for c in [
        id_col, "ACCOUNT_ID", "CUSTOMER_ID",
        "acct_event_ts", "cust_event_ts",
        "WORKING_BALANCE", "CUSTOMER_TYPE", "STATUS",
    ] if c in dup_rows.columns]

    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    pd.set_option("display.float_format", "{:,.2f}".format)

    print(dup_rows[display_cols].to_string(index=False))
    print()


def print_join_stats(label: str, joined: pd.DataFrame, acct_count: int, cust_count: int) -> None:
    """
    In thống kê chi tiết của một kết quả FULL JOIN.

    Phân loại dựa trên cột ACCOUNT_ID (từ acct) và CUS_CUSTOMER_ID (từ cust):
      - matched   : cả 2 cột đều not null  → join thành công
      - acct_only : ACCOUNT_ID not null, CUS_CUSTOMER_ID null  → acct không có cust
      - cust_only : ACCOUNT_ID null, CUS_CUSTOMER_ID not null  → cust không có acct
    """
    total_joined = len(joined)

    matched   = joined[joined["ACCOUNT_ID"].notna()   & joined["CUS_CUSTOMER_ID"].notna()]
    acct_only = joined[joined["ACCOUNT_ID"].notna()   & joined["CUS_CUSTOMER_ID"].isna()]
    cust_only = joined[joined["ACCOUNT_ID"].isna()    & joined["CUS_CUSTOMER_ID"].notna()]

    print(f"\n{'='*62}")
    print(f"  {label}")
    print(f"{'='*62}")
    print(f"  DataFrame gốc:")
    print(f"    Số row T24_ACCOUNT  (acct_df) : {acct_count:>10,}")
    print(f"    Số row T24_CUSTOMER (cust_df) : {cust_count:>10,}")
    print()
    print(f"  Kết quả FULL JOIN:")
    print(f"    Tổng số row sau join           : {total_joined:>10,}")
    print(f"    Số row MATCH (cả 2 bên)        : {len(matched):>10,}")
    print(f"    Số row ACCT có, CUST không có  : {len(acct_only):>10,}")
    print(f"    Số row CUST có, ACCT không có  : {len(cust_only):>10,}")
    print(f"{'='*62}")

    # In các CUSTOMER_ID bị lặp trong matched
    print_duplicate_customer_ids(label, matched)


# ─────────────────────────────────────────────
# JOIN 1: CUSTOMER_ID + time window ±30 phút
# ─────────────────────────────────────────────
def join_with_time_window(acct_df: pd.DataFrame, cust_df: pd.DataFrame) -> pd.DataFrame:
    """
    FULL OUTER JOIN:
      ON  a.CUSTOMER_ID = c.CUSTOMER_ID
      AND a.acct_event_ts >= c.cust_event_ts - 30 phút
      AND a.acct_event_ts <= c.cust_event_ts + 30 phút

    pandas không hỗ trợ inequality join trực tiếp nên dùng merge + filter.
    """
    window = timedelta(minutes=30)

    # Đổi tên cột CUSTOMER_ID của cust để tránh xung đột
    cust_renamed = cust_df.rename(columns={"CUSTOMER_ID": "CUS_CUSTOMER_ID"})

    # Bước 1: inner join theo CUSTOMER_ID để lấy các cặp match
    inner = acct_df.merge(
        cust_renamed,
        left_on="CUSTOMER_ID",
        right_on="CUS_CUSTOMER_ID",
        how="inner",
    )

    # Bước 2: lọc theo time window
    if "acct_event_ts" in inner.columns and "cust_event_ts" in inner.columns:
        mask = (
            inner["acct_event_ts"].notna() & inner["cust_event_ts"].notna() &
            (inner["acct_event_ts"] >= inner["cust_event_ts"] - window) &
            (inner["acct_event_ts"] <= inner["cust_event_ts"] + window)
        )
        matched_inner = inner[mask]
    else:
        matched_inner = inner

    # Bước 3: tìm acct_only — account không match bất kỳ customer nào trong window
    matched_acct_ids = set(matched_inner["ACCOUNT_ID"].dropna())
    acct_only = acct_df[~acct_df["ACCOUNT_ID"].isin(matched_acct_ids)].copy()
    acct_only["CUS_CUSTOMER_ID"] = None

    # Bước 4: tìm cust_only — customer không match bất kỳ account nào trong window
    matched_cust_ids = set(matched_inner["CUS_CUSTOMER_ID"].dropna())
    cust_only = cust_renamed[~cust_renamed["CUS_CUSTOMER_ID"].isin(matched_cust_ids)].copy()
    cust_only["ACCOUNT_ID"] = None

    # Bước 5: ghép lại thành full join
    joined = pd.concat([matched_inner, acct_only, cust_only], ignore_index=True)
    return joined


# ─────────────────────────────────────────────
# JOIN 2: Chỉ theo CUSTOMER_ID
# ─────────────────────────────────────────────
def join_by_customer_id_only(acct_df: pd.DataFrame, cust_df: pd.DataFrame) -> pd.DataFrame:
    """
    FULL OUTER JOIN:
      ON a.CUSTOMER_ID = c.CUSTOMER_ID
    """
    cust_renamed = cust_df.rename(columns={"CUSTOMER_ID": "CUS_CUSTOMER_ID"})

    joined = acct_df.merge(
        cust_renamed,
        left_on="CUSTOMER_ID",
        right_on="CUS_CUSTOMER_ID",
        how="outer",
    )
    return joined


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print("\n" + "="*62)
    print("  DEBUG JOIN COUNTS — T24_ACCOUNT x T24_CUSTOMER")
    print("="*62)

    # ── Đọc Kafka ────────────────────────────────────────
    print(f"\n[1/4] Đọc topic '{ACCT_TOPIC}'...")
    acct_messages = read_all_messages(ACCT_TOPIC)
    print(f"      → {len(acct_messages):,} message thô")

    print(f"\n[2/4] Đọc topic '{CUST_TOPIC}'...")
    cust_messages = read_all_messages(CUST_TOPIC)
    print(f"      → {len(cust_messages):,} message thô")

    # ── Parse thành DataFrame ─────────────────────────────
    acct_df = parse_account_messages(acct_messages)
    cust_df = parse_customer_messages(cust_messages)

    acct_count = len(acct_df)
    cust_count = len(cust_df)

    print(f"\n  Sau khi parse (chỉ op r/c/u, bỏ null ACCOUNT_ID/CUSTOMER_ID):")
    print(f"    acct_df : {acct_count:,} rows")
    print(f"    cust_df : {cust_count:,} rows")

    if acct_count == 0 or cust_count == 0:
        print("\n[WARN] Một trong 2 DataFrame rỗng — kiểm tra lại topic hoặc Debezium config.")

    # ── JOIN 1: time window ±30 phút ─────────────────────
    print("\n[3/4] Thực hiện JOIN 1 (CUSTOMER_ID + time window ±30 phút)...")
    joined_tw = join_with_time_window(acct_df, cust_df)
    print_join_stats(
        label="JOIN 1: ON CUSTOMER_ID + acct_event_ts WITHIN ±30 MIN of cust_event_ts",
        joined=joined_tw,
        acct_count=acct_count,
        cust_count=cust_count,
    )

    # ── JOIN 2: chỉ CUSTOMER_ID ───────────────────────────
    print("\n[4/4] Thực hiện JOIN 2 (chỉ CUSTOMER_ID)...")
    joined_id = join_by_customer_id_only(acct_df, cust_df)
    print_join_stats(
        label="JOIN 2: ON CUSTOMER_ID only",
        joined=joined_id,
        acct_count=acct_count,
        cust_count=cust_count,
    )

    # ── So sánh 2 phương pháp ─────────────────────────────
    tw_match = len(joined_tw[joined_tw["ACCOUNT_ID"].notna() & joined_tw["CUS_CUSTOMER_ID"].notna()])
    id_match = len(joined_id[joined_id["ACCOUNT_ID"].notna() & joined_id["CUS_CUSTOMER_ID"].notna()])

    print(f"\n{'='*62}")
    print(f"  SO SÁNH 2 PHƯƠNG PHÁP JOIN")
    print(f"{'='*62}")
    print(f"  Tổng row join (time window) : {len(joined_tw):>10,}")
    print(f"  Tổng row join (id only)     : {len(joined_id):>10,}")
    print(f"  Match (time window)         : {tw_match:>10,}")
    print(f"  Match (id only)             : {id_match:>10,}")
    print(f"  Chênh lệch match            : {id_match - tw_match:>+10,}  (id_only - time_window)")
    print(f"{'='*62}\n")


if __name__ == "__main__":
    main()
