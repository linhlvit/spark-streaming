# poc-spark_streaming

POC pipeline xử lý CDC từ Oracle core banking (T24) theo thời gian thực,
dùng Debezium → Kafka → Spark Structured Streaming → Oracle ODS.

> Thiết kế kỹ thuật chi tiết: [docs/DESIGN.md](docs/DESIGN.md)

---

## Cấu trúc repo

```
poc-spark_streaming/
├── jobs/                    # Spark Streaming jobs (entrypoint: jobs/main.py)
│   ├── main.py
│   ├── config.py
│   ├── sync/                # Pattern 1 — CDC Sync → *_TARGET
│   ├── static_join/         # Pattern 2 — Stream ⋈ Static (TXN ⋈ BRANCH)
│   ├── stream_join/         # Pattern 3 — Stream ⋈ Stream (TXN ⋈ ACCOUNT)
│   ├── aggregation/         # Pattern 4 — Stateful Aggregation (doanh số chi nhánh)
│   ├── core/                # Shared: oracle_writer, schema_parser
│   ├── tools/               # DLQ retry, cleanup, debug scripts
│   ├── sql/                 # DDL tạo bảng target + DLQ + summary
│   └── examples/            # Prototype tham khảo (không deploy)
│
├── dags/                    # Airflow DAGs (MWAA + EMR)
│   ├── streaming_lifecycle.py
│   ├── dlq_retry.py
│   ├── dlq_maintenance.py
│   └── shared/emr_config.py
│
├── docs/
│   ├── DESIGN.md            # Thiết kế kỹ thuật đầy đủ
│   ├── kafka_streaming_context.md
│   ├── solution-overview.html
│   └── config/              # Debezium connector configs + hướng dẫn Oracle setup
│
└── docker-compose.yml       # Môi trường dev local
```

---

## Chạy nhanh

```bash
# 1. Tạo bảng Oracle target
sqlplus FSS_STREAM/FSS_STREAM@//host:1521/dbpdb @jobs/sql/create_target_tables.sql

# 2. Build packages
pip install --target jobs/packages oracledb
cd jobs/packages && zip -r ../packages.zip . && cd ..
zip packages.zip jobs/config.py jobs/core/*.py jobs/sync/*.py \
    jobs/static_join/*.py jobs/stream_join/*.py jobs/aggregation/*.py

# 3. Chạy tất cả jobs
spark-submit \
  --master spark://spark-master:7077 \
  --py-files jobs/packages.zip \
  jobs/main.py

# Chỉ chạy 1 job
spark-submit ... jobs/main.py --jobs sync
spark-submit ... jobs/main.py --jobs static_join
spark-submit ... jobs/main.py --jobs txn_acct_join
spark-submit ... jobs/main.py --jobs branch_sales_agg
```

Xem hướng dẫn đầy đủ tại [jobs/README.md](jobs/README.md).

---

## Stack

- Oracle 19c (T24 core banking) — nguồn CDC
- Debezium 2.x — LogMiner connector
- Kafka — event bus (topic per bảng, prefix `oracle.FSS_STREAM.`)
- Spark Structured Streaming 3.5, PySpark
- Oracle ODS — target (schema `FSS_STREAM`), ghi qua `oracledb` thin mode
- Airflow MWAA + EMR — orchestration (optional, xem `dags/`)

---

## Debezium connector

Config mẫu nằm tại [docs/config/](docs/config/):

| File | Mô tả |
|---|---|
| `oracle-fss-connector-origin.json` | Config gốc, không có SMT |
| `oracle-fss-connector.json` | Dùng `ExtractNewRecordState` SMT (envelope unwrap) |
| `oracle-fss-connector-v2.json` | Dùng `ReplaceField` SMT (bỏ source/ts_us/ts_ns fields) |
| `oracle-debezium-kafka-test-sla-v*.json` | Config test SLA với bảng `T24_TRANSACTIONS_TEST_SLA` |
| `Cau hinh debezium vao kafka Oracle.txt` | Hướng dẫn bật ARCHIVELOG + supplemental logging Oracle |
