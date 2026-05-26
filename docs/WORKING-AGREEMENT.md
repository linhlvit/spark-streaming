# Working Agreement — Spark Streaming CDC Oracle → Oracle

> **Mục đích:** Tài liệu này ghi lại những gì nhóm đã thống nhất về yêu cầu nghiệp vụ,
> quyết định kỹ thuật, kế hoạch kiểm thử, và phân công Production readiness.
>
> **Cách dùng:** Đọc trước buổi Alignment Session. Điền/cập nhật trong và sau buổi họp.
> Khi có thay đổi, cập nhật file này — không tạo tài liệu mới song song.
>
> **Trạng thái:** `DRAFT` — chưa qua Alignment Session

---

## Thành viên nhóm

| Vai trò | Tên | Trách nhiệm chính |
|---|---|---|
| Tech Lead | _(điền tên)_ | Thiết kế giải pháp, review code, guideline vận hành cho khách hàng |
| Developer | _(điền tên)_ | Implement, unit test, DLQ scripts |
| PO / BA | _(điền tên)_ | Xác nhận yêu cầu nghiệp vụ, acceptance criteria, bàn giao |

---

## Phần A — Yêu cầu nghiệp vụ

*Mục đích: PO/BA xác nhận đây là đúng nghiệp vụ. Tech Lead và Developer không giải thích kỹ thuật ở phần này.*

### A.1 Định nghĩa đầu ra từng pattern

#### Pattern 1 — CDC Sync (Mirror 1:1)

Đồng bộ dữ liệu từ T24 core banking sang ODS để downstream có thể tra cứu trực tiếp.

| Bảng nguồn | Bảng target | Mục đích nghiệp vụ |
|---|---|---|
| T24_TRANSACTIONS | T24_TRANSACTIONS_TARGET | Mirror giao dịch — dùng cho tra cứu, reconciliation |
| T24_ACCOUNT | T24_ACCOUNT_TARGET | Mirror số dư tài khoản |
| T24_CUSTOMER | T24_CUSTOMER_TARGET | Mirror thông tin khách hàng |
| T24_BRANCH | T24_BRANCH_TARGET | Mirror danh mục chi nhánh |

**Câu hỏi cần xác nhận với PO/BA:**
- [ ] Downstream sẽ dùng *_TARGET vào mục đích cụ thể nào? (báo cáo? API lookup? reconciliation?)
- [ ] Có field nào trong T24 nguồn cần ẩn/mask trước khi đưa vào ODS không?
- [ ] Yêu cầu giữ lịch sử (history) hay chỉ cần trạng thái hiện tại?

**Đã thống nhất:** _(điền sau buổi họp)_

---

#### Pattern 2 — Static Join: TXN ⋈ BRANCH → T24_TXN_ENRICHED

Enrich giao dịch với thông tin chi nhánh để báo cáo không phải JOIN thêm.

**Các field được thêm vào so với T24_TRANSACTIONS:**

| Field | Nguồn | Ý nghĩa |
|---|---|---|
| BRANCH_NAME | T24_BRANCH | Tên chi nhánh đầy đủ |
| REGION_CODE | T24_BRANCH | Mã vùng/khu vực |
| REGION_NAME | T24_BRANCH | Tên vùng/khu vực |
| ENRICHED_AT | Spark (SYSTIMESTAMP) | Thời điểm enrich — dùng đo latency |

**Câu hỏi cần xác nhận với PO/BA:**
- [ ] Báo cáo sẽ nhóm theo REGION_CODE hay REGION_NAME?
- [ ] Nếu chi nhánh bị xóa khỏi T24, TXN_ENRICHED cũ có cần giữ nguyên BRANCH_NAME cũ không?
- [ ] SLA: giao dịch phải xuất hiện trong T24_TXN_ENRICHED trong bao lâu sau khi commit T24?

**Đã thống nhất:** _(điền sau buổi họp)_

---

#### Pattern 3 — Stream Join: TXN ⋈ ACCOUNT → T24_TXN_ACCOUNT_SNAPSHOT

Ghi lại snapshot số dư tài khoản tại thời điểm giao dịch xảy ra.

**Các field chính:**

| Field | Ý nghĩa nghiệp vụ |
|---|---|
| BALANCE_AT_TXN | Số dư thực tế (ONLINE_ACTUAL_BAL) ngay sau giao dịch |
| WORKING_BALANCE_AT_TXN | Số dư khả dụng tại thời điểm giao dịch |
| ACCT_CURRENCY_CODE | Đơn vị tiền tệ của tài khoản |
| SNAPSHOT_AT | Thời điểm Spark ghi — dùng đo latency |

**Câu hỏi cần xác nhận với PO/BA:**
- [ ] "Số dư tại thời điểm giao dịch" — đây là BEFORE hay AFTER giao dịch? (hiện tại là AFTER — ONLINE_ACTUAL_BAL từ T24_ACCOUNT event ngay sau TXN)
- [ ] Snapshot này phục vụ use case cụ thể nào? (phát hiện gian lận? reconciliation? báo cáo?)
- [ ] Nếu TXN và ACCOUNT event cách nhau > 30 phút (timeout watermark) → TXN vào DLQ — điều này có chấp nhận được không hay cần mở rộng window?

**Đã thống nhất:** _(điền sau buổi họp)_

---

#### Pattern 4 — Aggregation: TXN → T24_BRANCH_SALES_SUMMARY

Tính doanh số tích lũy theo chi nhánh, ngày, và loại tiền tệ — cập nhật near-realtime.

**Định nghĩa nghiệp vụ:**

| Field | Định nghĩa |
|---|---|
| TOTAL_AMOUNT | Tổng giá trị giao dịch (cộng dồn, có thể giảm khi revert) |
| TXN_COUNT | Số lượng giao dịch (giảm 1 khi revert) |
| CREDIT_AMOUNT | Tổng giao dịch có TRANSACTION_TYPE = 'CREDIT' |
| DEBIT_AMOUNT | Tổng giao dịch có TRANSACTION_TYPE = 'DEBIT' |

**Quy tắc nghiệp vụ đã xác định:**
- Giao dịch chỉ có INSERT (`op=c`). `op=u` chỉ xảy ra khi giao dịch bị **revert/hủy trong ngày** — AMOUNT không thay đổi, chỉ TRANSACTION_STATUS đổi sang REVERSED.
- Khi revert: TOTAL_AMOUNT giảm đúng bằng AMOUNT của giao dịch gốc.

**Câu hỏi cần xác nhận với PO/BA:**
- [ ] TRANSACTION_TYPE trong T24 có những giá trị nào? Cần liệt kê để phân loại đúng CREDIT/DEBIT.
- [ ] RPT_DATE là ngày giao dịch (TRANSACTION_DATE từ T24) hay ngày xử lý Spark? (hiện tại: TRANSACTION_DATE từ T24)
- [ ] Dashboard/báo cáo sẽ query theo granularity nào? (theo ngày? theo tháng? có cần rollup không?)
- [ ] Nếu giao dịch revert vào ngày hôm sau (khác RPT_DATE gốc) — cần xử lý thế nào?

**Đã thống nhất:** _(điền sau buổi họp)_

---

### A.2 SLA và ngưỡng chấp nhận

| Chỉ số | Giá trị hiện tại (POC) | Yêu cầu Production | Đã thống nhất |
|---|---|---|---|
| Latency static_join (p95) | ≤ 30s | _(điền)_ | ☐ |
| Latency stream_join (p95) | ≤ 90s | _(điền)_ | ☐ |
| Latency aggregation | ≤ 70s | _(điền)_ | ☐ |
| DLQ rate tối đa | < 1% TXN | _(điền)_ | ☐ |
| Thời gian DLQ retry tối đa | 1440 phút (24h) | _(điền)_ | ☐ |
| Uptime yêu cầu | _(chưa định nghĩa)_ | _(điền)_ | ☐ |

---

## Phần B — Quyết định kỹ thuật

*Mục đích: Cả nhóm hiểu tại sao chọn cách này. PO/BA hiểu trade-off ảnh hưởng đến nghiệp vụ.*

### B.1 Bảng quyết định đã thống nhất

| # | Quyết định | Lựa chọn | Lý do | Đã confirm |
|---|---|---|---|---|
| 1 | Ghi Oracle | `oracledb` + MERGE (không dùng JDBC) | JDBC gây ClassCastException trong foreachBatch executor | ☑ |
| 2 | Watermark stream-join | `kafka.timestamp` (broker), không dùng `ts_ms` (CDC) | `ts_ms` quá cũ khi replay, evict toàn bộ event trước khi join | ☑ |
| 3 | Out-of-order CDC Sync | Last-write-wins guard: `WHERE t.ts < s.ts` trong MERGE | Event cũ không được ghi đè state mới — Oracle target luôn giữ bản ghi mới nhất | ☑ |
| 4 | Out-of-order Aggregation | Revert semantics: `op=u → SIGNED = -before.AMOUNT` | AMOUNT không đổi khi revert, chỉ STATUS đổi — trừ ngược là đúng nghiệp vụ | ☑ |
| 5 | Aggregation state | Lưu trong Oracle (không dùng Spark window state) | Spark state mất khi restart, cần seed lại — Oracle persistent hơn | ☑ |
| 6 | Aggregation idempotency | `T24_STREAM_BATCH_LOG` check trước khi MERGE delta | Spark có thể replay batch_id cũ khi executor lỗi — không có guard sẽ double-count | ☑ |
| 7 | DLQ schema | Flat schema (không dùng CLOB JSON) | Retry cần field trực tiếp; SQL debug dễ hơn; schema T24 ổn định | ☑ |
| 8 | DLQ scope | Per-job riêng biệt (không shared) | Payload khác nhau; isolation — spike 1 job không ảnh hưởng job khác | ☑ |
| 9 | DLQ retry | Standalone script + exponential backoff | Daemon thread không được Spark checkpoint quản lý; không thể restart riêng lẻ | ☑ |
| 10 | DLQ backoff | static_join: 5→15→60→240→1440 phút<br>stream_join: 10→30→120→360→1440 phút | Base stream_join dài hơn vì phụ thuộc vào ACCOUNT sync (thường chậm hơn BRANCH) | ☑ |
| 11 | Schema parsing | Tự động từ DDL file, không hardcode | Thêm bảng mới không cần sửa code stream_processor.py | ☑ |

### B.2 Quyết định còn mở — cần thống nhất trong buổi họp

| # | Câu hỏi | Options | Owner quyết định |
|---|---|---|---|
| ? | Production Kafka: số partition cho mỗi topic? | 1 partition (no reorder) vs multi (throughput cao hơn, cần xử lý out-of-order) | Tech Lead + khách hàng |
| ? | Checkpoint storage: local disk hay HDFS/S3? | Local (đơn giản) vs S3/HDFS (HA, survive node restart) | Tech Lead |
| ? | DLQ MAX_RETRY có thể cấu hình hay hard-code? | Hard-code (đơn giản) vs config file (linh hoạt cho từng table) | Tech Lead + PO/BA |
| ? | Khi nào xóa DLQ FAILED records? | TTL 30 ngày (default) vs giữ vĩnh viễn để audit | PO/BA |
| ? | Alert khi DLQ rate > 1%: channel nào? | Email? Slack? Oracle alert? | PO/BA + khách hàng |

---

## Phần C — Kế hoạch kiểm thử

*Mục đích: PO/BA xác nhận kịch bản đúng nghiệp vụ. Developer biết phải test gì. Tech Lead biết acceptance criteria.*

### C.1 Môi trường kiểm thử

| Bước | Môi trường | Ai thực hiện | Mục tiêu |
|---|---|---|---|
| 1 | Local Docker (`docker-compose.yml`) | Developer | Xác nhận code chạy đúng, unit-level |
| 2 | Môi trường công ty (trước khi bàn giao) | Cả nhóm | Integration test, SLA, load test nhẹ |
| 3 | Môi trường khách hàng | Tech Lead guideline | UAT, acceptance |

**Trạng thái môi trường công ty:** _(điền: đã có Oracle + Kafka chưa? Debezium đã cấu hình chưa?)_

---

### C.2 Danh sách kịch bản kiểm thử

Chạy bằng: `python3 tools/test_pipeline.py --scenario all --timeout 120`

Chi tiết từng kịch bản: xem `docs/poc-demo-scenarios.html`

| # | Kịch bản | Pattern | Acceptance criteria | Owner verify | Kết quả |
|---|---|---|---|---|---|
| S1 | CDC Sync INSERT | Pattern 1 | Row trong *_TARGET ≤ 30s, AMOUNT đúng | Developer | ☐ |
| S2 | CDC Sync UPDATE (last-write-wins) | Pattern 1 | STATUS = COMPLETED; out-of-order event không ghi đè | Developer | ☐ |
| S3 | CDC Sync DELETE | Pattern 1 | Row biến mất khỏi *_TARGET sau DELETE nguồn | Developer | ☐ |
| S4 | Static Join thành công | Pattern 2 | BRANCH_NAME, REGION_CODE, REGION_NAME đúng trong ENRICHED | PO/BA verify field | ☐ |
| S5 | DLQ — Branch không tồn tại | Pattern 2 | ERROR_CODE = BRANCH_NOT_FOUND; không vào ENRICHED | Developer | ☐ |
| S6 | DLQ Retry → RESOLVED | Pattern 2 | Sau khi BRANCH xuất hiện, STATUS = RESOLVED + vào ENRICHED | Developer | ☐ |
| S7 | Stream Join Snapshot | Pattern 3 | BALANCE_AT_TXN = số dư đúng; SNAPSHOT_AT ≤ 90s | PO/BA xác nhận số dư | ☐ |
| S8 | Aggregation tích lũy | Pattern 4 | TOTAL_AMOUNT cộng đúng sau 3 TXN | PO/BA verify số | ☐ |
| S9 | Aggregation revert | Pattern 4 | TOTAL_AMOUNT giảm đúng sau UPDATE REVERSED | PO/BA verify số | ☐ |
| S10 | Latency end-to-end | Tất cả | p95 ≤ 90s trên 10 TXN | Tech Lead | ☐ |

### C.3 Kiểm thử bổ sung trước Production (chưa có script tự động)

| # | Kịch bản | Mô tả | Ai thực hiện |
|---|---|---|---|
| E1 | Restart Debezium connector giữa chừng | Connector restart → event replay → không duplicate, không mất | Tech Lead |
| E2 | Restart Spark job giữa chừng | Job crash → restart từ checkpoint → không double-count SUMMARY | Developer |
| E3 | Kafka topic multi-partition out-of-order | Tạo 2 partition, gửi event cũ sau event mới → LWW guard hoạt động | Developer |
| E4 | Load test 10K TXN/giờ | Đo throughput thực tế, DLQ rate, latency p99 | Tech Lead |
| E5 | DLQ backlog lớn (1000 PENDING records) | Retry script xử lý được, không timeout, không OOM | Developer |
| E6 | Oracle target down 5 phút rồi recover | Spark không crash, event không mất, ghi lại khi Oracle lên | Tech Lead |

---

## Phần D — Production Readiness Gaps

*Mục đích: Liệt kê đầy đủ gap giữa POC và Production. Assign owner và deadline rõ ràng.*

### D.1 Gap checklist

| # | Hạng mục | Mô tả | Ưu tiên | Owner | Deadline | Trạng thái |
|---|---|---|---|---|---|---|
| G1 | Cấu hình Production | `config.py` dùng hardcode — cần đọc từ env var hoặc secrets manager | Cao | Developer | _(điền)_ | ☐ |
| G2 | Logging chuẩn hóa | Hiện dùng `print()` — cần chuyển sang structured logging (log level, job name, batch_id) | Cao | Developer | _(điền)_ | ☐ |
| G3 | Health check endpoint | Không có cách kiểm tra job đang chạy hay đã chết từ bên ngoài | Cao | Tech Lead | _(điền)_ | ☐ |
| G4 | Alert DLQ rate | Khi DLQ rate > 1% — cần cơ chế thông báo | Trung bình | Tech Lead | _(điền)_ | ☐ |
| G5 | Secrets management | Oracle password, Kafka credentials không nên trong config.py | Cao | Tech Lead | _(điền)_ | ☐ |
| G6 | Checkpoint storage HA | Checkpoint local disk mất khi node restart — cần S3/NFS | Cao | Tech Lead | _(điền)_ | ☐ |
| G7 | Airflow DAG deploy | DAG files viết xong nhưng chưa test trên môi trường thực | Trung bình | Developer | _(điền)_ | ☐ |
| G8 | Bootstrap txn_acct | Cần chạy `bootstrap_txn_acct.py` trước khi bật stream_join lần đầu | Cao | Developer | _(điền)_ | ☐ |
| G9 | Runbook vận hành | Tài liệu hướng dẫn vận hành cho khách hàng (khởi động, dừng, xử lý lỗi) | Trung bình | Tech Lead | _(điền)_ | ☐ |
| G10 | Monitoring SQL | `sql/monitoring.sql` đã có — cần tích hợp vào dashboard thực tế | Thấp | PO/BA + Tech Lead | _(điền)_ | ☐ |
| G11 | Oracle performance | Index trên target tables đủ chưa? MERGE trên bảng lớn cần review execution plan | Trung bình | Tech Lead + DBA KH | _(điền)_ | ☐ |
| G12 | Debezium connector Production config | Config mẫu có, cần review với Oracle DBA khách hàng (supplemental logging, LogMiner permissions) | Cao | Tech Lead | _(điền)_ | ☐ |

### D.2 Định nghĩa "Done" cho Production

Giải pháp sẵn sàng bàn giao khi:

- [ ] Tất cả S1–S10 PASS trên môi trường công ty
- [ ] Tất cả E1–E6 đã kiểm tra, documented kết quả
- [ ] Tất cả G1–G12 ở trạng thái ☑ hoặc có quyết định rõ ràng (defer với lý do)
- [ ] Runbook vận hành (`docs/RUNBOOK.md`) đã review bởi Tech Lead
- [ ] PO/BA sign-off trên acceptance criteria từng pattern

---

## Phần E — Ghi chú buổi Alignment Session

> Điền sau buổi họp. Ghi những điểm **thay đổi so với mặc định** hoặc **quyết định mới**.

**Ngày họp:** _(điền)_

**Có mặt:** _(điền)_

### Nghiệp vụ — thay đổi / làm rõ

_(điền)_

### Kỹ thuật — quyết định mới hoặc thay đổi

_(điền)_

### Kế hoạch kiểm thử — cập nhật

_(điền)_

### Phân công — cập nhật deadline

_(điền)_

### Action items

| # | Nội dung | Owner | Deadline |
|---|---|---|---|
| | | | |
