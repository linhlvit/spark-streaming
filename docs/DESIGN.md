# Thiết kế giải pháp — Spark Streaming CDC Oracle → Oracle

> File này mô tả **thiết kế kỹ thuật** của toàn bộ pipeline.
> Cập nhật file này mỗi khi có thay đổi về giải pháp hoặc bổ sung yêu cầu mới.
> README.md giữ vai trò hướng dẫn vận hành (cách chạy, cấu hình, deploy).

---

## 1. Bối cảnh & mục tiêu

**Bài toán:** Đồng bộ và làm giàu dữ liệu từ Oracle core banking (T24) sang Oracle ODS theo thời gian thực, dùng Debezium CDC + Kafka + Spark Structured Streaming.

**Yêu cầu chính:**
- Không mất sự kiện (at-least-once delivery, idempotent sink)
- Enriched data sẵn sàng phục vụ downstream (báo cáo, analytics)
- Giao dịch hoặc account chưa join được do data ordering → không drop, retry sau
- Mỗi bảng nguồn có một mirror target để downstream có thể tra cứu trực tiếp

---

## 2. Kiến trúc tổng thể

```
Oracle Source (T24)
    │
    ▼  Debezium LogMiner CDC
Kafka Topics  oracle.FSS_STREAM.*
    │
    ├─► sync job              → T24_*_TARGET              (mirror 1:1)
    ├─► static_join job       → T24_TXN_ENRICHED           (TXN enriched với BRANCH)
    │                         + T24_TXN_PENDING_JOIN        (DLQ)
    ├─► txn_acct_join job     → T24_TXN_ACCOUNT_SNAPSHOT   (TXN + số dư tại thời điểm GD)
    │                         + T24_TXN_ACCT_PENDING_JOIN   (DLQ)
    └─► branch_sales_agg job  → T24_BRANCH_SALES_SUMMARY   (doanh số tích lũy theo chi nhánh)
    │
    ▼
Oracle Target — schema FSS_STREAM
```

**Stack:**
- Debezium 2.x — LogMiner connector, Oracle 19c
- Kafka (topic per bảng, prefix `oracle.FSS_STREAM.`)
- Spark Structured Streaming 3.5, PySpark
- Oracle target: `oracledb` thin mode (không dùng JDBC driver)

---

## 3. Các pattern kỹ thuật

### 3.1 `foreachBatch` — sink duy nhất cho mọi job

Tất cả job ghi Oracle qua `foreachBatch`. Lý do:

- Spark Structured Streaming không có Oracle sink native
- `foreachBatch` cho phép ghi idempotent bằng Oracle MERGE
- Mỗi batch có `batch_id` — Spark đảm bảo không retry cùng batch_id 2 lần (checkpoint)
- Logic phân loại (enriched vs pending) chạy trong batch, không cần join state ngoài

### 3.2 Oracle MERGE — UPSERT idempotent

Tất cả ghi vào Oracle dùng `MERGE ... ON (PK) WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT`.

- An toàn khi batch replay (Spark có thể chạy lại batch nếu executor chết giữa chừng)
- Named bind variables (`:COLUMN_NAME`) — tránh SQL injection, tái dùng execution plan

### 3.3 `oracledb` thuần thay vì `spark.read.jdbc`

Load data tĩnh (BRANCH, CUSTOMER_TARGET, ACCOUNT_TARGET) bên trong `foreachBatch` dùng `oracledb.connect()` trực tiếp, **không** dùng `spark.read.jdbc`.

**Lý do:** `spark.read.jdbc` bên trong `foreachBatch` gây `ClassCastException` do JAR serialization conflict giữa Oracle JDBC driver và Spark executor classloader.

### 3.4 Watermark dùng `kafka.timestamp` (stream_join)

Stream-stream join dùng `kafka.timestamp` (broker timestamp) làm watermark, **không** dùng `ts_ms` (CDC timestamp trong envelope Debezium).

**Lý do:** Khi replay từ `earliest`, `ts_ms` là thời gian cũ (hàng ngày/hàng tuần trước) → watermark evict ngay toàn bộ event trước khi join kịp. `kafka.timestamp` tăng dần theo thứ tự xử lý, tránh evict sớm.

### 3.5 Out-of-order CDC events — tác động và cách xử lý

Debezium đọc Oracle Redo Log theo SCN. Trong điều kiện bình thường (single-partition topic), event luôn theo thứ tự. Tuy nhiên out-of-order **có xảy ra** trong một số tình huống:

| Tình huống | Tần suất |
|---|---|
| Debezium restart — connector re-read từ last committed offset | Không thường xuyên |
| Kafka topic multi-partition — consumer đọc round-robin | Xảy ra khi partition > 1 |
| Oracle RAC multi-node — SCN không tuyến tính giữa các node | Đặc thù Oracle RAC |
| Consumer lag recovery — consume batch lớn từ quá khứ | Sau spike traffic |

**Tác động theo pattern:**

| Pattern | Tác động | Severity | Cách xử lý |
|---|---|---|---|
| CDC Sync | Event cũ ghi đè state mới (last-write-wins sai) | Trung bình | Last-write-wins guard trong MERGE (xem §3.5.1) |
| Stream-Static Join | Không ảnh hưởng — mỗi TXN xử lý độc lập | Thấp | N/A |
| Stream-Stream Join | TXN out-of-order bị watermark evict → DLQ | Thấp | DLQ retry qua T24_ACCOUNT_TARGET (xem §7) |
| Stateful Aggregation | op=u (revert) phải trừ ngược — không phải cộng thêm | Trung bình | SIGNED_AMOUNT = -before.AMOUNT (xem §3.5.2) |

#### 3.5.1 Last-write-wins guard — CDC Sync

`oracle_writer._build_merge_sql()` nhận tham số `event_ts_col`. Khi được cung cấp, MERGE thêm điều kiện:

```sql
WHEN MATCHED THEN UPDATE SET ...
    WHERE t.{event_ts_col} < s.{event_ts_col}   -- chỉ UPDATE nếu event mới hơn
```

Mapping per-table trong `sync/stream_processor.py`:

```python
_EVENT_TS_COL = {
    "T24_TRANSACTIONS": "TRANSACTION_TIME",  # timestamp giao dịch
    "T24_ACCOUNT":      "EVENT_TIME",        # timestamp cập nhật số dư
    "T24_CUSTOMER":     "UPDATED_AT",        # timestamp cập nhật KH
    "T24_BRANCH":       None,                # dimension ổn định, không cần guard
}
```

Event cũ đến sau sẽ bị MERGE bỏ qua (`0 rows updated`) thay vì ghi đè — Oracle target luôn giữ state mới nhất.

#### 3.5.2 Revert aggregation — Stateful Aggregation

**Quan điểm nghiệp vụ:** `T24_TRANSACTIONS` chỉ có INSERT (`op=c`). `op=u` xảy ra khi giao dịch bị revert/hủy trong ngày — `AMOUNT` không thay đổi, chỉ `TRANSACTION_STATUS` thay đổi.

Hệ quả: khi `op=u`, AMOUNT trong `before` là giá trị đã được cộng vào doanh số lúc `op=c`. Cần trừ ngược để hoàn tác:

```
op=c: SIGNED_AMOUNT = +after.AMOUNT   → TOTAL += AMOUNT   (giao dịch phát sinh)
op=u: SIGNED_AMOUNT = -before.AMOUNT  → TOTAL -= AMOUNT   (giao dịch bị revert)
```

`aggregation/branch_sales_agg.py` tách `insert_df` (op=c) và `revert_df` (op=u) trước khi `unionByName` và groupBy — Oracle MERGE nhận DELTA_AMOUNT có thể âm.

---

## 4. Pattern 1 — CDC Sync

**File:** `sync/stream_processor.py`

**Mục đích:** Mirror dữ liệu từ 4 bảng nguồn sang target 1:1, xử lý đủ 4 loại op Debezium.

```
Kafka (oracle.FSS_STREAM.T24_*)
    └─► parse Debezium envelope (op, before, after)
            ├── op = r/c/u → MERGE vào *_TARGET (upsert)
            └── op = d     → DELETE khỏi *_TARGET (theo PK)
```

**Bảng:**

| Kafka Topic | Target Table |
|---|---|
| `oracle.FSS_STREAM.T24_TRANSACTIONS` | `T24_TRANSACTIONS_TARGET` |
| `oracle.FSS_STREAM.T24_ACCOUNT` | `T24_ACCOUNT_TARGET` |
| `oracle.FSS_STREAM.T24_CUSTOMER` | `T24_CUSTOMER_TARGET` |
| `oracle.FSS_STREAM.T24_BRANCH` | `T24_BRANCH_TARGET` |

**Thiết kế quyết định:**
- Schema tự động từ DDL file (`sql/create_target_tables.sql`) qua `core/schema_parser.py` — không hardcode column list
- Mỗi bảng là 1 StreamingQuery riêng → lỗi 1 bảng không chặn bảng khác

---

## 5. Pattern 2 — Stream-Static Join

**File:** `static_join/txn_branch_join.py`

**Mục đích:** Enrich giao dịch với thông tin chi nhánh (BRANCH_NAME, REGION_CODE, REGION_NAME) để ODS có đủ chiều dữ liệu cho báo cáo.

```
T24_TRANSACTIONS (Kafka)
    └─► foreachBatch
            ├── load T24_BRANCH từ Oracle (fresh mỗi batch)
            ├── join BRANCH_CODE
            ├── [FOUND]     → MERGE vào T24_TXN_ENRICHED
            └── [NOT FOUND] → INSERT vào T24_TXN_PENDING_JOIN (DLQ)
                                  STATUS = PENDING
                                  ERROR_REASON = BRANCH_NOT_FOUND
                                              | BRANCH_TABLE_EMPTY
```

**Khi nào dùng pattern này:**
- Reference table thay đổi chậm (BRANCH ít khi thêm/sửa)
- Không cần join state phức tạp
- Phù hợp khi dimension table nhỏ (load toàn bộ vào memory mỗi batch)

**Thiết kế quyết định: Flat schema cho DLQ**

`T24_TXN_PENDING_JOIN` lưu toàn bộ payload dạng flat (không dùng CLOB JSON). Lý do:
- Retry logic cần `BRANCH_CODE` trực tiếp để lookup — không cần deserialize JSON
- Schema `T24_TRANSACTIONS` ổn định → không cần generic storage
- Dễ query, alert, debug trực tiếp bằng SQL

**Thiết kế quyết định: Mỗi job có DLQ riêng**

Dù nhiều static_join job có thể cùng lookup `T24_BRANCH`, mỗi job có DLQ riêng. Lý do:
- Payload schema khác nhau theo từng job (TXN vs LOAN vs...)
- Điều kiện retry khác nhau
- Spike DLQ của 1 job không ảnh hưởng job khác

---

## 6. Pattern 3 — Stream-Stream Join

### Nguyên tắc chọn bảng cho stream-stream join

Stream-stream join có ý nghĩa khi **cả hai bên đều là event stream** — tức là cả hai thay đổi liên tục và độc lập. Nếu một bên là dimension (ít thay đổi, 1 row/key), nên dùng stream-static join thay thế.

| Bảng | Loại | Pattern phù hợp |
|---|---|---|
| T24_TRANSACTIONS | Event stream (mỗi giao dịch = 1 event) | ✅ stream-stream |
| T24_ACCOUNT | Event stream (cập nhật sau mỗi giao dịch) | ✅ stream-stream |
| T24_CUSTOMER | Dimension (ít thay đổi, 1 row/customer) | ❌ nên dùng stream-static |
| T24_BRANCH | Dimension (rất ít thay đổi) | ❌ nên dùng stream-static |

**Job minh họa:** `T24_TRANSACTIONS ⋈ T24_ACCOUNT` — cả hai thay đổi cùng lúc khi có giao dịch, thể hiện đúng bản chất stream-stream join.

> **Lý do không dùng T24_CUSTOMER:** T24_CUSTOMER là dimension — một khách hàng hiếm khi thay đổi thông tin. Nếu dùng stream-stream join với T24_CUSTOMER, watermark window phải rất rộng để chờ customer event, gây tốn memory. Stream-static join với T24_CUSTOMER_TARGET (mirror luôn cập nhật) phù hợp hơn nhiều.

---

### 6.1 Job: T24_TRANSACTIONS ⋈ T24_ACCOUNT → T24_TXN_ACCOUNT_SNAPSHOT

**File:** `stream_join/txn_acct_join.py`

**Mục đích:** Ghi nhận trạng thái số dư tài khoản tại thời điểm xảy ra giao dịch — audit trail giao dịch + số dư tức thời.

```
T24_TRANSACTIONS (Kafka) ──┐
                             ├─► LEFT OUTER JOIN ON ACCOUNT_ID (watermark ±30')
T24_ACCOUNT      (Kafka) ──┘
    │
    ├── [MATCHED]   → MERGE vào T24_TXN_ACCOUNT_SNAPSHOT
    │                   (giao dịch + BALANCE_AT_TXN + WORKING_BALANCE_AT_TXN)
    └── [TXN_ONLY]  → INSERT vào T24_TXN_ACCT_PENDING_JOIN (DLQ)
                         ERROR_REASON = TXN_NO_ACCOUNT_MATCH
```

**Thiết kế quyết định: Tại sao LEFT OUTER JOIN thay vì FULL OUTER JOIN?**

T24_TRANSACTIONS là bên chủ đạo (driving stream) — mỗi giao dịch phải được ghi nhận dù account chưa sync. T24_ACCOUNT là bên lookup. LEFT JOIN đảm bảo:
- TXN không có ACCOUNT trong window → TXN_ONLY → DLQ (không mất giao dịch)
- ACCOUNT không có TXN → không emit row thừa (không cần xử lý ACCT_ONLY)

**Xử lý DELETE:** T24_TRANSACTIONS op=d → DELETE T24_TXN_ACCOUNT_SNAPSHOT theo TRANSACTION_ID (streaming query riêng).

---

### 6.2 Xác định độ dài watermark trong thực tế

Watermark `±30 minutes` trong POC này là ước lượng ban đầu. Khi triển khai production, cần đo thực tế để tránh 2 nguy cơ trái chiều:

- **Watermark quá ngắn** → TXN bị evict trước khi ACCOUNT tương ứng đến → DLQ flood
- **Watermark quá dài** → Spark giữ join state quá lớn trong memory → OOM hoặc GC pressure

#### Bước 1 — Đo `event_lag` của từng stream

`event_lag` là khoảng thời gian từ khi một sự kiện xảy ra trong Oracle đến khi nó xuất hiện trong Kafka topic (Debezium → Kafka commit latency).

```python
# tools/measure_watermark.py — chạy độc lập, không cần Spark
# Đọc trực tiếp Kafka bằng confluent_kafka, tính lag giữa ts_ms (Oracle commit time)
# và kafka.timestamp (broker receive time).

from confluent_kafka import Consumer
import json, statistics, time

def measure_lag(topic: str, bootstrap: str, sample_size: int = 1000) -> dict:
    conf = {
        "bootstrap.servers": bootstrap,
        "group.id":          f"lag-measure-{topic}",
        "auto.offset.reset": "latest",
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    lags = []
    while len(lags) < sample_size:
        msg = consumer.poll(timeout=5.0)
        if msg is None or msg.error():
            continue
        payload = json.loads(msg.value())
        ts_ms       = payload.get("ts_ms")          # Oracle commit timestamp (epoch ms)
        kafka_ts_ms = msg.timestamp()[1]             # Kafka broker timestamp (epoch ms)
        if ts_ms:
            lags.append(kafka_ts_ms - ts_ms)        # dương = Kafka nhận sau Oracle commit
    consumer.close()
    lags_s = [l / 1000 for l in lags]               # chuyển sang giây
    return {
        "topic":   topic,
        "samples": len(lags_s),
        "p50_s":   statistics.median(lags_s),
        "p95_s":   statistics.quantiles(lags_s, n=100)[94],
        "p99_s":   statistics.quantiles(lags_s, n=100)[98],
        "max_s":   max(lags_s),
    }
```

Chạy trên cả hai topic để có số liệu riêng:

```bash
python3 tools/measure_watermark.py \
  --topics oracle.FSS_STREAM.T24_TRANSACTIONS,oracle.FSS_STREAM.T24_ACCOUNT \
  --samples 5000
```

Kết quả mẫu (số liệu giả định để minh họa):

| Topic | p50 | p95 | p99 | max |
|---|---|---|---|---|
| T24_TRANSACTIONS | 1.2s | 4.5s | 12s | 38s |
| T24_ACCOUNT | 0.8s | 3.1s | 9s | 27s |

#### Bước 2 — Tính `skew` giữa 2 stream

Trong stream-stream join, điều quan trọng hơn lag tuyệt đối là **độ lệch tương đối** giữa TXN và ACCOUNT cùng `ACCOUNT_ID`:

```
skew = |kafka_ts(TXN) − kafka_ts(ACCOUNT)| cho cùng một giao dịch
```

Cùng một giao dịch Oracle sẽ tạo ra 1 CDC event trên `T24_TRANSACTIONS` và 1 CDC event trên `T24_ACCOUNT` (số dư thay đổi). Hai event này không arrive đồng thời trên Kafka — khoảng cách đó là `skew`.

```sql
-- Đo skew sau khi đã có T24_TXN_ACCOUNT_SNAPSHOT (chạy sau một thời gian streaming):
SELECT
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ABS(TXN_TS_MS - ACCT_TS_MS)) / 1000.0 AS p95_skew_s,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY ABS(TXN_TS_MS - ACCT_TS_MS)) / 1000.0 AS p99_skew_s,
    MAX(ABS(TXN_TS_MS - ACCT_TS_MS)) / 1000.0                                           AS max_skew_s
FROM FSS_STREAM.T24_TXN_ACCOUNT_SNAPSHOT
WHERE SNAPSHOT_AT >= SYSTIMESTAMP - INTERVAL '7' DAY;
```

#### Bước 3 — Công thức chọn watermark

```
watermark = max(p99_skew, max_observed_skew_peak) × safety_factor
```

| Thành phần | Ý nghĩa | Giá trị khuyến nghị |
|---|---|---|
| `p99_skew` | Bao phủ 99% trường hợp thông thường | Tính từ SNAPSHOT_AT query trên |
| `max_observed_skew_peak` | Peak trong giờ cao điểm / Debezium restart | Quan sát ít nhất 7 ngày |
| `safety_factor` | Biên an toàn cho spike bất thường | 1.5× – 2× |

**Ví dụ cụ thể** (từ số liệu mẫu trên):
- p99 skew đo được = 45 giây
- Max peak (Debezium restart, lag spike) = 8 phút
- Safety factor = 2×
- → `watermark = 8 min × 2 = 16 phút` → **làm tròn lên 20 phút**

#### Bước 4 — Theo dõi DLQ rate để validate watermark

Sau khi chọn watermark, theo dõi `DLQ insertion rate` liên tục. Nếu tỷ lệ vào DLQ tăng → watermark quá ngắn:

```sql
-- Monitor DLQ rate theo giờ:
SELECT
    TRUNC(PENDING_SINCE, 'HH') AS hour_bucket,
    COUNT(*)                   AS dlq_inserts,
    COUNT(*) / 60.0            AS per_minute
FROM FSS_STREAM.T24_TXN_ACCT_PENDING_JOIN
WHERE PENDING_SINCE >= SYSTIMESTAMP - INTERVAL '24' HOUR
GROUP BY TRUNC(PENDING_SINCE, 'HH')
ORDER BY 1;
```

Ngưỡng cảnh báo đề xuất:
- DLQ rate > 1% tổng TXN trong giờ → investigate
- DLQ rate > 5% → tăng watermark ngay

#### Bước 5 — Đánh đổi memory vs completeness

Khi tăng watermark, Spark giữ state lâu hơn. Ước tính bộ nhớ join state:

```
state_memory ≈ TPS × watermark_seconds × row_size_bytes × 2 (cả 2 stream)
```

Ví dụ với `TPS = 1000 tx/s`, `watermark = 30 phút`, `row_size ≈ 500 bytes`:
```
state_memory ≈ 1000 × 1800 × 500 × 2 = ~1.8 GB per executor
```

Nếu không đủ memory → giảm watermark + chấp nhận DLQ rate cao hơn + tăng DLQ retry capacity.

**Tổng kết quy trình:**

```
1. Measure event_lag (tools/measure_watermark.py) — 1 ngày
2. Collect skew từ SNAPSHOT (SQL trên) — sau 7 ngày streaming
3. Tính watermark = p99_skew × 2, làm tròn lên 5' gần nhất
4. Deploy, monitor DLQ rate — 3–7 ngày
5. Điều chỉnh nếu DLQ > 1% hoặc executor OOM
```

> **Giá trị `±30 minutes` trong POC này** là conservative estimate phù hợp cho môi trường lab. Production nên chạy qua quy trình đo trên — kết quả thực tế thường nằm trong khoảng **10–20 phút** cho hệ thống core banking có Debezium ổn định.

---

## 7. DLQ — Dead Letter Queue

### 7.1 Vấn đề với naive retry

Retry vòng lặp trong streaming job (daemon thread) gây:
- Chiếm tài nguyên executor của Spark
- Không được Spark checkpoint quản lý → mất trạng thái khi restart
- Không thể restart riêng lẻ khi DLQ gặp sự cố
- Khó theo dõi qua log/metric riêng

*Tham khảo: "Building Reliable Reprocessing and Dead Letter Queues with Apache Kafka" — Ning Xia, Uber (2018)*

### 7.2 Giải pháp: Retry tách độc lập

DLQ retry là standalone script, **không** chạy trong streaming job:

```
Streaming Job (Spark)                 DLQ Retry Script (standalone)
──────────────────────────────        ──────────────────────────────
foreachBatch                          tools/retry_txn_branch_dlq.py
  ├── [FOUND]  → enriched table         import retry_pending_once()
  └── [NOT FOUND] → DLQ (PENDING)       from static_join.txn_branch_join
                                          ↓
                                        load branch / target tables
                                          ↓
                                        MERGE → enriched
                                          ↓
                                        UPDATE DLQ status
```

Script có thể lên lịch qua cron hoặc Airflow, log riêng, restart độc lập.

### 7.3 DLQ lifecycle

**`T24_TXN_PENDING_JOIN`** (static_join):
```
PENDING → RESOLVED   (lookup branch thành công lần retry)
        → FAILED     (đạt MAX_RETRY=5, branch vĩnh viễn không tồn tại)
```

**`T24_TXN_ACCT_PENDING_JOIN`** (stream_join):
```
PENDING → RESOLVED   (lookup T24_ACCOUNT_TARGET thành công)
        → FAILED     (đạt MAX_RETRY=5)
```

### 7.4 Error codes chuẩn hóa

Mỗi DLQ row có 2 trường phân biệt:
- **`ERROR_CODE`** — mã cố định dùng để GROUP BY, alert, filter. Không thay đổi theo thời gian.
- **`ERROR_REASON`** — free-text chi tiết (key, context). Dùng để debug.

| ERROR_CODE | Job | Ý nghĩa |
|---|---|---|
| `BRANCH_NOT_FOUND` | static_join | TXN có `BRANCH_CODE` nhưng không tìm thấy trong `T24_BRANCH` |
| `BRANCH_TABLE_EMPTY` | static_join | `T24_BRANCH` rỗng hoàn toàn tại thời điểm batch |
| `TXN_NO_ACCOUNT_MATCH` | stream_join | TXN không có ACCOUNT event trong window ±30' |

Cách alert theo error code:
```sql
-- Đếm DLQ mới trong 1 giờ qua theo loại lỗi
SELECT ERROR_CODE, COUNT(*) AS cnt
FROM FSS_STREAM.T24_TXN_PENDING_JOIN
WHERE PENDING_SINCE >= SYSTIMESTAMP - INTERVAL '1' HOUR
  AND STATUS = 'PENDING'
GROUP BY ERROR_CODE;
```

### 7.5 Retry scripts

| Script | Import từ | DLQ table |
|---|---|---|
| `tools/retry_txn_branch_dlq.py` | `static_join.txn_branch_join.retry_pending_once` | `T24_TXN_PENDING_JOIN` |
| `tools/retry_txn_acct_dlq.py` | `stream_join.txn_acct_join.retry_pending_once` | `T24_TXN_ACCT_PENDING_JOIN` |

### 7.6 DLQ cleanup (TTL)

Rows RESOLVED/FAILED tích lũy theo thời gian. Chạy cleanup định kỳ để tránh table bloat:

```bash
# Dry-run: xem có bao nhiêu rows eligible
python3 tools/cleanup_dlq.py --dry-run

# Xóa rows cũ hơn 30 ngày (default)
python3 tools/cleanup_dlq.py

# Tùy chỉnh TTL
python3 tools/cleanup_dlq.py --ttl-days 7

# Chỉ clean 1 table
python3 tools/cleanup_dlq.py --table txn_branch
python3 tools/cleanup_dlq.py --table txn_acct
```

Lên lịch qua cron (mỗi ngày 2AM):
```
0 2 * * * /usr/bin/python3 /opt/spark/jobs/tools/cleanup_dlq.py --ttl-days 30
```

### 7.7 Exponential backoff per pattern

Thay vì retry cố định mỗi N phút, mỗi DLQ row lưu `NEXT_RETRY_AT` — thời điểm sớm nhất được phép retry. Retry script chỉ lấy rows `WHERE NEXT_RETRY_AT <= SYSTIMESTAMP`, tránh query thừa và giảm noise trong log.

**Lý do chọn base interval khác nhau theo pattern:**

| Pattern | Base interval | Lý do |
|---|---|---|
| `static_join` | 5 phút | Root cause là BRANCH CDC lag (thường vài giây–vài phút) → giải quyết nhanh |
| `stream_join` | 10 phút | TXN đã chờ ≥ 30' trong watermark window trước khi vào DLQ → không cần retry sớm hơn |

**Backoff schedule:**

| Retry lần | static_join (+phút) | stream_join (+phút) |
|---|---|---|
| 0 (lần đầu) | +5 | +10 |
| 1 | +15 | +30 |
| 2 | +60 | +120 |
| 3 | +240 | +360 |
| 4 (cuối) | +1440 (24h) | +1440 (24h) |

Sau `MAX_RETRY = 5` lần thất bại, row được mark `FAILED` và không retry thêm. Alert dựa trên `ERROR_CODE` (xem §7.4).

**Implementation:**
- `BACKOFF_MINUTES_STATIC = [5, 15, 60, 240, 1440]` — `static_join/txn_branch_join.py`
- `BACKOFF_MINUTES_STREAM = [10, 30, 120, 360, 1440]` — `stream_join/txn_acct_join.py`
- Hàm `_next_retry_at(retry_count)` tính `datetime.utcnow() + timedelta(minutes=schedule[min(retry_count, len-1)])`
- `_PENDING_RETRY_INC` SQL cập nhật cả `RETRY_COUNT` và `NEXT_RETRY_AT` trong một lần UPDATE

---

## 8. Cold Start — Batch Bootstrap

### 8.1 Vấn đề

Khi bật stream-stream join lần đầu, Debezium snapshot (`snapshot.mode=initial`) đổ toàn bộ dữ liệu lịch sử vào Kafka. Hai snapshot của 2 topic có thể hoàn thành lệch nhau > 30 phút → watermark evict bên đến trước → **DLQ flood hàng triệu rows**.

Đây là vấn đề đã biết ở production (Netflix DBLog, LinkedIn, Artie). Spark watermark được thiết kế cho steady-state CDC lag, không cho bulk snapshot.

### 8.2 Giải pháp: 2-phase bootstrap (LinkedIn / Artie pattern)

```
Phase 1 — Batch Bootstrap (one-time, trước khi bật stream)
    tools/bootstrap_txn_acct.py
        ├── Đọc T24_TRANSACTIONS từ Oracle (oracledb, phân trang)
        ├── Load T24_ACCOUNT vào memory
        ├── JOIN tĩnh theo ACCOUNT_ID
        └── MERGE → T24_TXN_ACCOUNT_SNAPSHOT
                   (idempotent, có thể chạy lại an toàn)

Phase 2 — Streaming Delta (ongoing)
    txn_acct_join.py (startingOffsets=latest)
        ├── Chỉ xử lý giao dịch MỚI từ đây về sau
        └── DLQ chỉ handle genuine late arrival (< vài phút lag)
```

### 8.3 Cấu hình Debezium cho production

```json
// Phase 1: snapshot một lần, dùng cho batch bootstrap
{ "snapshot.mode": "initial_only" }

// Phase 2: streaming delta, không snapshot lại
{ "snapshot.mode": "no_data" }
```

Khi cần re-snapshot một bảng cụ thể (không restart connector):
```sql
INSERT INTO debezium_signal (id, type, data)
VALUES ('reснap-1', 'execute-snapshot',
        '{"data-collections": ["FSS_STREAM.T24_ACCOUNT"], "type": "incremental"}');
```
Incremental snapshot đọc theo chunk, không lock bảng, không gây ORA-01555.

### 8.4 Chạy bootstrap

```bash
# Dry-run: đếm rows, không ghi
python3 tools/bootstrap_txn_acct.py --dry-run

# Bootstrap toàn bộ lịch sử
python3 tools/bootstrap_txn_acct.py --batch-size 2000

# Chỉ bootstrap từ ngày cụ thể (giảm thời gian chạy)
python3 tools/bootstrap_txn_acct.py --since 2024-01-01

# Sau khi bootstrap xong → bật streaming
spark-submit ... main.py --jobs stream_join
```

### 8.5 Watermark policy

- Giữ `spark.sql.streaming.multipleWatermarkPolicy=min` (default Spark) — global watermark = min của 2 stream, cho cả 2 bên thời gian tối đa để join trước khi evict.
- **Không** dùng `max` — stream nhanh hơn sẽ evict stream chậm ngay lập tức.
- Watermark 30' phù hợp cho steady-state Oracle LogMiner lag (thường < vài phút). Tăng lên 60' nếu môi trường production có lag cao hơn.

---

## 9. Quyết định thiết kế — tóm tắt

| Quyết định | Lựa chọn | Lý do |
|---|---|---|
| Sink Oracle | `oracledb` + MERGE | JDBC gây ClassCastException trong foreachBatch |
| Chọn bảng stream-stream | T24_TRANSACTIONS ⋈ T24_ACCOUNT | Cả 2 là event stream — thay đổi cùng lúc khi có giao dịch |
| Join type (txn_acct) | LEFT OUTER JOIN | TXN là driving stream — không muốn emit ACCT_ONLY thừa |
| Watermark | `kafka.timestamp` | `ts_ms` quá cũ khi replay, evict ngay |
| Watermark policy | `min` (Spark default) | Cho cả 2 stream thời gian tối đa trước khi evict |
| Cold start | Batch bootstrap trước, `latest` offset | Snapshot bulk không tương thích với watermark window |
| Debezium snapshot | `no_data` cho streaming | `initial` gây DLQ flood khi 2 snapshot lệch > window |
| DLQ storage | Flat schema | Retry cần field trực tiếp, schema ổn định |
| DLQ scope | Per-job | Payload khác nhau, isolation tốt hơn |
| DLQ retry | Standalone script | Daemon thread không được Spark quản lý |
| DLQ error classification | `ERROR_CODE` (chuẩn) + `ERROR_REASON` (detail) | `ERROR_CODE` dùng để alert/GROUP BY, `ERROR_REASON` để debug |
| DLQ cleanup | TTL-based (`cleanup_dlq.py`, default 30 ngày) | Tránh table bloat — chỉ xóa RESOLVED/FAILED, không xóa PENDING |
| DLQ retry backoff | Exponential per pattern (`NEXT_RETRY_AT`) | static base 5', stream base 10' — tránh retry thừa, giảm log noise |
| Out-of-order CDC Sync | Last-write-wins guard (`event_ts_col` trong MERGE WHERE) | Event cũ đến sau không được ghi đè state mới hơn trong target |
| Out-of-order Aggregation | `op=u` = revert → `SIGNED_AMOUNT = -before.AMOUNT` | Giao dịch chỉ có INSERT; UPDATE chỉ là revert — không cộng thêm mà trừ ngược |
| Aggregation state | Oracle MERGE += delta (không dùng Spark window) | State trong Oracle → restart an toàn, không mất tổng tích lũy |
| Aggregation idempotency | `T24_STREAM_BATCH_LOG` (batch_id guard) | Spark có thể replay batch khi executor lỗi — cộng double nếu không guard |
| Schema nguồn | Parse từ DDL file | Không hardcode, dễ thêm bảng mới |

---

## 10. Orchestration — Airflow (MWAA) + EMR

### 10.1 Phân tách trách nhiệm

Streaming job chạy liên tục (`awaitTermination`) — Airflow **không** quản lý vòng đời process dài hạn. Phân tách rõ:

| Tầng | Công cụ | Trách nhiệm |
|---|---|---|
| **Process management** | EMR (YARN) / supervisor | Giữ Spark job sống, restart khi crash |
| **Orchestration** | MWAA (Airflow) | Khởi động có thứ tự, DLQ retry định kỳ, cleanup, alert |

### 10.2 Kiến trúc MWAA + EMR

```
MWAA DAG                          EMR Cluster (YARN)
─────────────────────             ────────────────────────────────
streaming_lifecycle (manual)  ──► spark-submit main.py --jobs ...
dlq_retry (*/10 * * * *)      ──► python3 tools/retry_*.py
dlq_maintenance (0 2 * * *)   ──► python3 tools/cleanup_dlq.py
                                  python3 tools/debug_counts.py

                    S3 Bucket
                    ──────────────────────
                    s3://bucket/jobs/          ← sync từ repo (packages.zip + *.py)
                    s3://bucket/checkpoints/   ← Spark checkpoint
                    s3://bucket/logs/emr/      ← EMR Step logs
```

Code DAG nằm tại `dags/` trong repo này — MWAA remote mount S3 bucket chứa DAG files.

### 10.3 DAG 1 — `streaming_lifecycle` (manual trigger)

Khởi động pipeline lần đầu hoặc sau cluster restart, đảm bảo đúng thứ tự:

```
check_bootstrap_needed
    ├── [chưa chạy] → bootstrap_dry_run → wait → bootstrap_txn_acct → wait → mark_done
    └── [đã chạy]  → skip_bootstrap
            │
            ▼
    verify_sync_job_healthy   ← debug_counts để xác nhận sync đang chạy
            │
    ┌───────┴───────┐
    ▼               ▼
start_static_join  start_txn_acct_join   ← fire-and-forget, không dùng Sensor chờ
    └───────┬───────┘
            ▼
    verify_streaming_started   ← log EMR Step IDs để monitor
```

**Điểm quan trọng:** Streaming job submit xong là DAG kết thúc — Spark tiếp tục chạy trên YARN không phụ thuộc vào Airflow. Dùng `ActionOnFailure=CONTINUE` để EMR cluster không dừng khi job crash.

Airflow Variable kiểm soát: `BOOTSTRAP_COMPLETED=true` (set tự động sau bootstrap thành công) — tránh chạy bootstrap lại khi trigger lifecycle lần 2.

### 10.4 DAG 2 — `dlq_retry` (mỗi 10 phút)

Chạy 2 DLQ retry song song, timeout 8 phút mỗi bên (phải xong trước lần chạy tiếp):

```
retry_txn_branch ──┐
                   ├─► log_retry_summary → alert_if_high_failure
retry_txn_acct  ──┘
```

`max_active_runs=1` tránh overlap — nếu run trước chưa xong, run mới bị skip.

### 10.5 DAG 3 — `dlq_maintenance` (2AM hàng ngày)

```
cleanup_txn_branch ──┐
                     ├─► debug_counts_report (--dlq-detail) → archive_report_to_s3
cleanup_txn_acct  ──┘
```

Xóa RESOLVED/FAILED rows theo TTL (Variable `DLQ_TTL_DAYS`, default 30 ngày), sau đó snapshot metrics pipeline để audit trail.

### 10.6 Airflow Variables cần thiết

| Variable | Mô tả | Ví dụ |
|---|---|---|
| `EMR_CLUSTER_ID` | ID EMR cluster đang chạy | `j-XXXXXXXXXXXX` |
| `S3_JOBS_PATH` | Path chứa jobs code | `s3://fss-stream/jobs` |
| `S3_LOGS_PATH` | Path lưu EMR Step logs | `s3://fss-stream/logs/emr` |
| `S3_CHECKPOINTS_PATH` | Spark checkpoint | `s3://fss-stream/checkpoints` |
| `SPARK_PACKAGES` | Maven packages | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5` |
| `BOOTSTRAP_COMPLETED` | Flag tránh re-bootstrap | `true` / `false` |
| `DLQ_MAX_RETRY` | Số retry tối đa | `5` |
| `DLQ_TTL_DAYS` | Số ngày giữ RESOLVED/FAILED | `30` |
| `DLQ_FAILURE_ALERT_THRESHOLD` | Ngưỡng FAILED rows để alert | `10` |

### 10.7 Deploy DAG lên MWAA

```bash
# Sync code lên S3 (chạy sau mỗi lần update)
aws s3 sync jobs/ s3://fss-stream-bucket/jobs/ --exclude "*.pyc" --exclude "__pycache__/*"
aws s3 sync dags/ s3://fss-stream-bucket/dags/

# Build packages.zip trước khi sync
pip install --target jobs/packages oracledb
cd jobs/packages && zip -r ../packages.zip . && cd ..
zip -r jobs/packages.zip jobs/config.py jobs/core/ jobs/sync/ jobs/static_join/ jobs/stream_join/
aws s3 cp jobs/packages.zip s3://fss-stream-bucket/jobs/packages.zip
```

---

## 11. Pattern 4 — Stateful Aggregation

**File:** `aggregation/branch_sales_agg.py`

**Mục đích:** Tính tổng doanh số theo chi nhánh near-realtime — dashboard báo cáo nội bộ cập nhật mỗi ~1 phút mà không cần aggregation lại trên toàn bộ bảng giao dịch.

```
T24_TRANSACTIONS (Kafka)
    └─► foreachBatch
            ├── tính DELTA: GROUP BY (BRANCH_CODE, RPT_DATE, CURRENCY_CODE)
            │       SUM(AMOUNT), COUNT(*), SUM(CREDIT), SUM(DEBIT)
            └── MERGE vào T24_BRANCH_SALES_SUMMARY
                    WHEN MATCHED    → TOTAL_AMOUNT += DELTA, TXN_COUNT += DELTA_COUNT
                    WHEN NOT MATCHED → INSERT row mới với giá trị ban đầu = delta
```

### 11.1 Tại sao không dùng Spark window function?

Spark `groupBy(window(...))` tích lũy join state trong bộ nhớ executor. Với tumbling window 1 ngày + 1000 TPS, state có thể lên tới vài GB — tốn memory và rủi ro OOM.

Thay vào đó, **state được lưu trong Oracle** (`T24_BRANCH_SALES_SUMMARY`):
- Spark chỉ tính delta trong batch hiện tại → rất nhỏ (vài nghìn rows)
- Oracle MERGE cộng delta vào tổng → O(1) theo số branch-date, không theo số TXN
- Restart Spark không mất tổng tích lũy — Oracle là source of truth

### 11.2 Latency

| Lớp | Thời gian |
|---|---|
| Oracle commit → Kafka (Debezium) | 1–5s |
| Spark trigger interval | 30s |
| Spark aggregate + Oracle MERGE | 1–3s |
| **Tổng end-to-end** | **~35–70s** |

Dashboard refresh mỗi 1 phút là trải nghiệm phù hợp cho báo cáo near-realtime tại ngân hàng.

### 11.3 Idempotency — batch_id guard

Spark có thể chạy lại cùng `batch_id` khi executor lỗi giữa chừng. Với MERGE UPSERT (pattern 1–3), replay an toàn vì ghi đè. Nhưng với `TOTAL += delta` — **replay sẽ cộng double → sai số liệu**.

Giải pháp: `T24_STREAM_BATCH_LOG` ghi `batch_id` đã commit **trong cùng Oracle transaction** với MERGE:

```
BEGIN TRANSACTION
  → MERGE delta vào T24_BRANCH_SALES_SUMMARY
  → INSERT INTO T24_STREAM_BATCH_LOG (job_name, batch_id)
COMMIT
```

Khi batch replay: check batch_id đã có trong log → skip toàn bộ batch. Đảm bảo exactly-once tại Oracle layer dù Spark ở at-least-once.

### 11.4 Schema bảng output

```sql
T24_BRANCH_SALES_SUMMARY
    PK: (BRANCH_CODE, RPT_DATE, CURRENCY_CODE)
    TOTAL_AMOUNT   — tổng doanh số tích lũy trong ngày
    TXN_COUNT      — số giao dịch
    CREDIT_AMOUNT  — tổng giao dịch có loại CREDIT
    DEBIT_AMOUNT   — tổng giao dịch không phải CREDIT
    CREATED_AT / UPDATED_AT
```

### 11.5 Query dashboard mẫu

```sql
-- Doanh số hôm nay theo chi nhánh (near-realtime, latency ~1 phút)
SELECT b.BRANCH_NAME,
       s.TOTAL_AMOUNT,
       s.TXN_COUNT,
       s.UPDATED_AT
FROM FSS_STREAM.T24_BRANCH_SALES_SUMMARY s
JOIN FSS_STREAM.T24_BRANCH_TARGET b ON b.BRANCH_CODE = s.BRANCH_CODE
WHERE s.RPT_DATE  = TRUNC(SYSDATE)
  AND s.CURRENCY_CODE = 'VND'
ORDER BY s.TOTAL_AMOUNT DESC;

-- Doanh số 7 ngày gần nhất theo vùng
SELECT b.REGION_NAME,
       s.RPT_DATE,
       SUM(s.TOTAL_AMOUNT) AS REGION_TOTAL,
       SUM(s.TXN_COUNT)    AS REGION_TXN_COUNT
FROM FSS_STREAM.T24_BRANCH_SALES_SUMMARY s
JOIN FSS_STREAM.T24_BRANCH_TARGET b ON b.BRANCH_CODE = s.BRANCH_CODE
WHERE s.RPT_DATE >= TRUNC(SYSDATE) - 6
  AND s.CURRENCY_CODE = 'VND'
GROUP BY b.REGION_NAME, s.RPT_DATE
ORDER BY s.RPT_DATE DESC, REGION_TOTAL DESC;
```

---

## 12. Hướng mở rộng

### Thêm aggregation job mới (ví dụ: doanh số theo sản phẩm)

1. Copy `aggregation/branch_sales_agg.py` → `aggregation/product_sales_agg.py`
2. Đổi `GROUP BY` key, bảng output, `JOB_NAME` (để batch_id guard không xung đột)
3. Tạo bảng summary tương ứng trong `sql/create_target_tables.sql`
4. Đăng ký trong `main.py`

`T24_STREAM_BATCH_LOG` dùng chung cho tất cả aggregation jobs — `JOB_NAME` phân biệt.

### Thêm job static_join mới (ví dụ: LOAN ⋈ BRANCH)

1. Copy `static_join/txn_branch_join.py` → `static_join/loan_branch_join.py`
2. Đổi topic, bảng enriched, bảng DLQ, payload schema
3. Tạo bảng DLQ tương ứng trong `sql/create_target_tables.sql`
4. Tạo `tools/retry_loan_branch_dlq.py` — copy `retry_txn_branch_dlq.py`, đổi import
5. Đăng ký trong `main.py`

### Thêm job stream_join mới (ví dụ: LOAN ⋈ ACCOUNT)

1. Copy `stream_join/txn_acct_join.py` → `stream_join/loan_acct_join.py`
2. Đổi topic, schema, bảng output/DLQ
3. Tạo bảng DLQ trong `sql/create_target_tables.sql`
4. Tạo `tools/retry_loan_acct_dlq.py`
5. Đăng ký trong `main.py`

### Nâng cấp DLQ lên multi-level (Uber pattern)

Hiện tại: 1 cấp retry, polling Oracle.
Nâng cấp: Thêm Kafka retry topic (`txn_retry_1`, `txn_retry_2`) với delay tăng dần.
Phù hợp khi: volume DLQ cao, cần observability theo cấp, hoặc có nhiều loại lỗi transient.
