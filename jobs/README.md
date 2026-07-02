# jobs/ — Spark Streaming CDC Oracle → Oracle

POC pipeline xử lý CDC từ Oracle qua Debezium/Kafka, dùng Spark Structured Streaming để đồng bộ và làm giàu dữ liệu sang Oracle target.

> Thiết kế chi tiết: [docs/DESIGN.md](../docs/DESIGN.md)

---

## Cấu trúc thư mục

```
jobs/
├── main.py              # Entrypoint — khởi động 1 hoặc nhiều job
├── config.py            # Tất cả cấu hình tập trung (Kafka, Oracle, Spark)
│
├── sync/                # Pattern: CDC sync (1 file per bảng)
│   ├── stream_processor.py      # Sync 4 bảng T24_* → *_TARGET (hardcode)
│   ├── yaml_stream_processor.py # Sync N bảng được cấu hình qua YAML
│   └── table_sync_configs.yml   # Cấu hình bảng cho yaml_stream_processor
│
├── static_join/         # Pattern: Stream ⋈ Static (1 file per job)
│   └── txn_branch_join.py       # T24_TRANSACTIONS ⋈ T24_BRANCH → T24_TXN_ENRICHED
│
├── stream_join/         # Pattern: Stream ⋈ Stream (1 file per job)
│   └── txn_acct_join.py         # T24_TRANSACTIONS ⋈ T24_ACCOUNT → T24_TXN_ACCOUNT_SNAPSHOT
│
├── aggregation/         # Pattern: Stateful Aggregation (1 file per job)
│   └── branch_sales_agg.py      # T24_TRANSACTIONS → T24_BRANCH_SALES_SUMMARY (cộng dồn delta)
│
├── core/                # Shared utilities
│   ├── oracle_writer.py         # MERGE/DELETE vào Oracle
│   └── schema_parser.py         # Parse DDL SQL → metadata
│
├── sql/
│   └── create_target_tables.sql # DDL tạo bảng target và DLQ
│
├── tools/               # DLQ scripts & debug (không deploy lên production)
│   ├── retry_txn_branch_dlq.py  # Retry DLQ T24_TXN_PENDING_JOIN       (static_join)
│   ├── retry_txn_acct_dlq.py    # Retry DLQ T24_TXN_ACCT_PENDING_JOIN  (stream_join: txn_acct)
│   ├── cleanup_dlq.py           # Xóa RESOLVED/FAILED rows cũ theo TTL (tất cả DLQ)
│   ├── bootstrap_txn_acct.py    # Batch bootstrap T24_TXN_ACCOUNT_SNAPSHOT (chạy 1 lần trước stream)
│   ├── debug_counts.py          # Thống kê row counts + DLQ metrics
│   ├── debug_branch.py          # Kiểm tra T24_BRANCH lookup
│   ├── debug_join_counts.py     # Đọc Kafka trực tiếp, test join logic
│   └── insert_bulk_test_sla.py  # Sinh 100K giao dịch test
│
└── examples/            # Prototypes / code tham khảo
```

### Thêm job mới

```
# Thêm bảng sync mới qua YAML (cách nhanh nhất — không cần viết code)
# → Mở sync/table_sync_configs.yml, thêm entry mới vào danh sách `tables`
# → Khởi động lại: spark-submit ... main.py --jobs yaml_sync

# Thêm static_join mới (ví dụ: LOAN ⋈ BRANCH)
static_join/loan_branch_join.py   # copy txn_branch_join.py, đổi bảng
tools/retry_loan_branch_dlq.py    # copy retry_txn_branch_dlq.py, đổi import

# Thêm stream_join mới (ví dụ: LOAN ⋈ ACCOUNT)
stream_join/loan_acct_join.py     # copy txn_acct_join.py, đổi bảng
tools/retry_loan_acct_dlq.py      # copy retry_txn_acct_dlq.py, đổi import
```

---

## Chạy

```bash
# Chạy tất cả jobs
spark-submit \
  --master spark://spark-master:7077 \
  --py-files packages.zip \
  main.py

# Chỉ chạy 1 job
spark-submit ... main.py --jobs sync
spark-submit ... main.py --jobs static_join
spark-submit ... main.py --jobs sync,static_join
spark-submit ... main.py --jobs txn_acct_join
spark-submit ... main.py --jobs branch_sales_agg

# yaml_sync — đọc cấu hình bảng từ file YAML (mặc định: sync/table_sync_configs.yml)
spark-submit ... main.py --jobs yaml_sync
spark-submit ... main.py --jobs yaml_sync --sync-config /opt/spark/jobs/sync/my_tables.yml
spark-submit ... main.py --jobs yaml_sync,static_join

# Các job hợp lệ: sync | yaml_sync | static_join | txn_acct_join | branch_sales_agg

# Lưu ý: txn_acct_join cần bootstrap trước lần đầu
python3 tools/bootstrap_txn_acct.py --dry-run   # kiểm tra trước
python3 tools/bootstrap_txn_acct.py             # chạy bootstrap
# Sau đó mới bật stream
```

---

## Kiểm tra pipeline (test end-to-end)

Yêu cầu: Spark job đang chạy (`main.py --jobs all`).

```bash
# Chạy tất cả kịch bản
python3 tools/test_pipeline.py --scenario all

# Chạy từng kịch bản riêng lẻ
python3 tools/test_pipeline.py --scenario sync           # CDC sync INSERT
python3 tools/test_pipeline.py --scenario sync_update    # CDC sync UPDATE (last-write-wins)
python3 tools/test_pipeline.py --scenario sync_delete    # CDC sync DELETE
python3 tools/test_pipeline.py --scenario static_join    # TXN ⋈ BRANCH → T24_TXN_ENRICHED
python3 tools/test_pipeline.py --scenario dlq            # TXN với BRANCH không tồn tại → DLQ
python3 tools/test_pipeline.py --scenario dlq_retry      # DLQ → BRANCH xuất hiện → RESOLVED
python3 tools/test_pipeline.py --scenario stream_join    # TXN ⋈ ACCOUNT → SNAPSHOT
python3 tools/test_pipeline.py --scenario aggregation    # 3 TXN → BRANCH_SALES_SUMMARY tích lũy
python3 tools/test_pipeline.py --scenario revert         # UPDATE REVERSED → TOTAL_AMOUNT giảm
python3 tools/test_pipeline.py --scenario latency        # Đo latency thực tế 10 TXN end-to-end

# Tùy chỉnh timeout (default 90s)
python3 tools/test_pipeline.py --scenario all --timeout 120
```

## Monitoring & thống kê SQL

```bash
# Script thống kê nhanh (terminal)
python3 tools/debug_counts.py                # row counts + DLQ summary
python3 tools/debug_counts.py --detail       # thêm sample rows
python3 tools/debug_counts.py --missing      # TXN chưa có snapshot
python3 tools/debug_counts.py --dlq-detail   # top error codes + retry rate (30 ngày)
```

SQL đo latency và health check: [`sql/monitoring.sql`](sql/monitoring.sql)

| Block | Nội dung |
|---|---|
| 1 | Row counts tổng quan toàn pipeline |
| 2b | Latency static_join (p50/p95/max) — đo từ Oracle commit → ENRICHED_AT |
| 3 | Latency stream_join (p50/p95/max) — đo từ TXN_TS_MS → SNAPSHOT_AT |
| 4 | Độ tươi aggregation — SYSDATE - UPDATED_AT |
| 5 | Latency theo giờ (phát hiện giờ cao điểm) |
| 6 | Throughput (TXN/phút trong 30 phút gần nhất) |
| 7 | DLQ rate so với tổng TXN (ngưỡng >1% → investigate) |
| 9 | Coverage: TXN hôm nay đã có enriched + snapshot chưa |
| 10 | Accuracy: so sánh SUM raw vs BRANCH_SALES_SUMMARY |
| 11 | DLQ age distribution (phát hiện stuck records) |

---

## Airflow / MWAA Orchestration

DAG files nằm tại `dags/` trong repo:

| DAG | Schedule | Mô tả |
|---|---|---|
| `streaming_lifecycle` | Manual | Bootstrap → verify sync → start streaming jobs |
| `dlq_retry` | `*/10 * * * *` | Retry DLQ records mỗi 10 phút (song song 2 tables) |
| `dlq_maintenance` | `0 2 * * *` | Cleanup TTL + snapshot metrics hàng ngày 2AM |

Xem chi tiết thiết kế: [docs/DESIGN.md §10](../docs/DESIGN.md)

---

## Cấu hình

Tất cả cấu hình tập trung tại [config.py](config.py):

| Nhóm | Biến chính |
|---|---|
| Kafka | `KAFKA_BOOTSTRAP_SERVERS`, `TOPIC_PREFIX` |
| Oracle | `ORACLE_HOST`, `ORACLE_PORT`, `ORACLE_SERVICE`, `ORACLE_USER`, `ORACLE_PASSWORD` |
| Spark | `CHECKPOINT_BASE`, `TRIGGER_INTERVAL`, `MAX_OFFSETS_PER_TRIGGER` |
| Schema | `SQL_FILE_PATH` (path tới `sql/create_target_tables.sql`) |

---

## Chuẩn bị môi trường

### 1. Tạo bảng target trong Oracle
```sql
@sql/create_target_tables.sql
```

### 2. Build packages
```bash
pip install --target ./packages oracledb pyyaml
cd packages && zip -r ../packages.zip . && cd ..
zip packages.zip config.py core/*.py sync/*.py static_join/*.py stream_join/*.py
# Đính kèm file YAML config cùng với packages
zip packages.zip sync/table_sync_configs.yml
```

### 3a. Deploy lên Spark cluster (local/Docker)
```bash
docker cp jobs/ spark-master:/opt/spark/jobs/
```

### 3b. Deploy lên EMR + MWAA (AWS)
```bash
# Sync jobs code lên S3
aws s3 sync jobs/ s3://fss-stream-bucket/jobs/ --exclude "*.pyc" --exclude "__pycache__/*"
aws s3 cp jobs/packages.zip s3://fss-stream-bucket/jobs/packages.zip

# Sync DAGs lên S3 (MWAA tự pick up sau vài phút)
aws s3 sync dags/ s3://fss-stream-bucket/dags/

# Set Airflow Variables (MWAA UI hoặc CLI)
# EMR_CLUSTER_ID, S3_JOBS_PATH, S3_LOGS_PATH, S3_CHECKPOINTS_PATH
# SPARK_PACKAGES, DLQ_MAX_RETRY, DLQ_TTL_DAYS
```

---

## DLQ retry scripts

Chạy độc lập (không cần Spark), có thể lên lịch qua cron hoặc Airflow:

```bash
# static_join DLQ
python3 tools/retry_txn_branch_dlq.py --once
python3 tools/retry_txn_branch_dlq.py --interval 300  # loop mỗi 5 phút
python3 tools/retry_txn_branch_dlq.py --max-retry 10

# stream_join DLQ (txn_acct)
python3 tools/retry_txn_acct_dlq.py --once
python3 tools/retry_txn_acct_dlq.py --interval 600
```

---

## DLQ cleanup

Chạy định kỳ để tránh DLQ table bloat. Script chỉ xóa rows RESOLVED/FAILED, không xóa PENDING:

```bash
# Dry-run: xem có bao nhiêu rows eligible (không xóa)
python3 tools/cleanup_dlq.py --dry-run

# Xóa rows cũ hơn 30 ngày (default)
python3 tools/cleanup_dlq.py

# Tùy chỉnh TTL hoặc chỉ clean 1 table
python3 tools/cleanup_dlq.py --ttl-days 7
python3 tools/cleanup_dlq.py --table txn_branch    # T24_TXN_PENDING_JOIN
python3 tools/cleanup_dlq.py --table txn_acct      # T24_TXN_ACCT_PENDING_JOIN
```

Cron (mỗi ngày 2AM):
```
0 2 * * * /usr/bin/python3 /opt/spark/jobs/tools/cleanup_dlq.py --ttl-days 30
```

---

## Debug tools

```bash
python3 tools/debug_counts.py                 # row counts + DLQ summary
python3 tools/debug_counts.py --detail        # kèm sample rows
python3 tools/debug_counts.py --missing       # TXN chưa có trong snapshot
python3 tools/debug_counts.py --dlq-detail    # top error codes + retry rate (30 ngày)

python3 tools/debug_branch.py
python3 tools/debug_join_counts.py

python3 tools/insert_bulk_test_sla.py    # sinh 100K giao dịch test
```
