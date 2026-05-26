-- ============================================================
-- Tạo 3 bảng TARGET trong schema LPB_POC
-- Cấu trúc 1:1 với bảng nguồn, không có constraint CHECK/FK
-- ============================================================

-- ── T24_ACCOUNT_TARGET ──────────────────────────────────────
CREATE TABLE LPB_POC.T24_ACCOUNT_TARGET (
    ACCOUNT_ID          VARCHAR2(50),
    CUSTOMER_ID         VARCHAR2(50)        NOT NULL,
    WORKING_BALANCE     NUMBER(18,2),
    ONLINE_ACTUAL_BAL   NUMBER(18,2),
    CURRENCY_CODE       VARCHAR2(10),
    BRANCH_CODE         VARCHAR2(20),
    LAST_TX_TIME        TIMESTAMP,
    EVENT_TIME          TIMESTAMP,
    CREATED_AT          TIMESTAMP,
    UPDATED_AT          TIMESTAMP,
    CONSTRAINT PK_T24_ACCOUNT_TARGET PRIMARY KEY (ACCOUNT_ID)
);

-- ── T24_CUSTOMER_TARGET ─────────────────────────────────────
CREATE TABLE LPB_POC.T24_CUSTOMER_TARGET (
    CUSTOMER_ID         VARCHAR2(50),
    CUSTOMER_CODE_BIZ   VARCHAR2(50),
    CUSTOMER_TYPE       VARCHAR2(10),
    CUSTOMER_NAME       VARCHAR2(255),
    BIRTH_DATE          DATE,
    GENDER              VARCHAR2(10),
    SEGMENT             VARCHAR2(20),
    MANAGE_BRANCH_CODE  VARCHAR2(20),
    STATUS              VARCHAR2(10),
    OPEN_DATE           DATE,
    CLOSE_DATE          DATE,
    CREATED_AT          TIMESTAMP(6),
    UPDATED_AT          TIMESTAMP(6),
    BANK_KHOI           VARCHAR2(50),
    CUSTOMER_SEGMENT    VARCHAR2(50),
    CONSTRAINT PK_T24_CUSTOMER_TARGET PRIMARY KEY (CUSTOMER_ID)
);

-- ── T24_TRANSACTIONS_TARGET ─────────────────────────────────
CREATE TABLE LPB_POC.T24_TRANSACTIONS_TARGET (
    TRANSACTION_ID      VARCHAR2(50),
    ACCOUNT_ID          VARCHAR2(50)        NOT NULL,
    CUSTOMER_ID         VARCHAR2(50)        NOT NULL,
    TRANSACTION_DATE    DATE                NOT NULL,
    VALUE_DATE          DATE                NOT NULL,
    TRANSACTION_TIME    TIMESTAMP(3)        NOT NULL,
    TRANSACTION_TYPE    VARCHAR2(50)        NOT NULL,
    AMOUNT              NUMBER(18,2)        NOT NULL,
    CURRENCY_CODE       VARCHAR2(10)        NOT NULL,
    CHANNEL             VARCHAR2(50),
    INPUT_ID            VARCHAR2(50),
    AUTHOR_ID           VARCHAR2(50),
    BRANCH_CODE         VARCHAR2(20)        NOT NULL,
    REFERENCE_NO        VARCHAR2(100),
    TRANSACTION_STATUS  VARCHAR2(20),
    CONSTRAINT PK_T24_TRANSACTIONS_TARGET PRIMARY KEY (TRANSACTION_ID)
);

-- ── T24_BRANCH_TARGET ───────────────────────────────────────
CREATE TABLE LPB_POC.T24_BRANCH_TARGET (
    BRANCH_CODE         VARCHAR2(20),
    BRANCH_NAME         VARCHAR2(255)       NOT NULL,
    REGION_CODE         VARCHAR2(20)        NOT NULL,
    REGION_NAME         VARCHAR2(255)       NOT NULL,
    CONSTRAINT PK_T24_BRANCH_TARGET PRIMARY KEY (BRANCH_CODE)
);

-- ── T24_TXN_PENDING_JOIN (DLQ) ──────────────────────────────
-- Flat schema: lưu toàn bộ payload để retry không cần đọc lại Kafka.
-- STATUS    lifecycle: PENDING → RESOLVED (join thành công) | FAILED (hết MAX_RETRY)
-- ERROR_CODE: mã lỗi chuẩn hóa để filter/alert (BRANCH_NOT_FOUND | BRANCH_TABLE_EMPTY)
-- ERROR_REASON: free-text chi tiết (branch_code, context)
-- RESOLVED_AT : timestamp khi retry thành công hoặc mark FAILED
CREATE TABLE LPB_POC.T24_TXN_PENDING_JOIN (
    TRANSACTION_ID      VARCHAR2(50),
    ACCOUNT_ID          VARCHAR2(50)        NOT NULL,
    CUSTOMER_ID         VARCHAR2(50)        NOT NULL,
    TRANSACTION_DATE    DATE                NOT NULL,
    VALUE_DATE          DATE                NOT NULL,
    TRANSACTION_TIME    TIMESTAMP(3)        NOT NULL,
    TRANSACTION_TYPE    VARCHAR2(50)        NOT NULL,
    AMOUNT              NUMBER(18,2)        NOT NULL,
    CURRENCY_CODE       VARCHAR2(10)        NOT NULL,
    CHANNEL             VARCHAR2(50),
    INPUT_ID            VARCHAR2(50),
    AUTHOR_ID           VARCHAR2(50),
    BRANCH_CODE         VARCHAR2(20)        NOT NULL,
    REFERENCE_NO        VARCHAR2(100),
    TRANSACTION_STATUS  VARCHAR2(20),
    CREATE_MS           NUMBER(15),
    PENDING_SINCE       TIMESTAMP           DEFAULT SYSTIMESTAMP,
    RETRY_COUNT         NUMBER(3)           DEFAULT 0,
    NEXT_RETRY_AT       TIMESTAMP           DEFAULT SYSTIMESTAMP,
    STATUS              VARCHAR2(20)        DEFAULT 'PENDING',
    ERROR_CODE          VARCHAR2(50),
    ERROR_REASON        VARCHAR2(200),
    RESOLVED_AT         TIMESTAMP,
    CONSTRAINT PK_TXN_PENDING_JOIN PRIMARY KEY (TRANSACTION_ID)
);

-- Migration cho DB đang chạy (chạy nếu bảng đã tồn tại):
-- ALTER TABLE LPB_POC.T24_TXN_PENDING_JOIN ADD (
--     STATUS        VARCHAR2(20)  DEFAULT 'PENDING',
--     ERROR_CODE    VARCHAR2(50),
--     ERROR_REASON  VARCHAR2(200),
--     NEXT_RETRY_AT TIMESTAMP     DEFAULT SYSTIMESTAMP,
--     RESOLVED_AT   TIMESTAMP
-- );

-- ── T24_TXN_ENRICHED ────────────────────────────────────────
CREATE TABLE LPB_POC.T24_TXN_ENRICHED (
    TRANSACTION_ID      VARCHAR2(50),
    ACCOUNT_ID          VARCHAR2(50)        NOT NULL,
    CUSTOMER_ID         VARCHAR2(50)        NOT NULL,
    TRANSACTION_DATE    DATE                NOT NULL,
    VALUE_DATE          DATE                NOT NULL,
    TRANSACTION_TIME    TIMESTAMP(3)        NOT NULL,
    TRANSACTION_TYPE    VARCHAR2(50)        NOT NULL,
    AMOUNT              NUMBER(18,2)        NOT NULL,
    CURRENCY_CODE       VARCHAR2(10)        NOT NULL,
    CHANNEL             VARCHAR2(50),
    INPUT_ID            VARCHAR2(50),
    AUTHOR_ID           VARCHAR2(50),
    BRANCH_CODE         VARCHAR2(20)        NOT NULL,
    BRANCH_NAME         VARCHAR2(255),
    REGION_CODE         VARCHAR2(20),
    REGION_NAME         VARCHAR2(255),
    REFERENCE_NO        VARCHAR2(100),
    TRANSACTION_STATUS  VARCHAR2(20),
    CREATE_MS           NUMBER(15),
    ENRICHED_AT         TIMESTAMP           DEFAULT SYSTIMESTAMP,
    CONSTRAINT PK_TXN_ENRICHED PRIMARY KEY (TRANSACTION_ID)
);

-- ── T24_TXN_ACCOUNT_SNAPSHOT ────────────────────────────────
-- Stream-Stream Join: T24_TRANSACTIONS ⋈ T24_ACCOUNT ON ACCOUNT_ID
-- Mỗi TRANSACTION_ID = 1 bản ghi, kèm snapshot số dư tại thời điểm giao dịch
-- BALANCE_AT_TXN: số dư thực tế ngay sau giao dịch (ONLINE_ACTUAL_BAL)
-- WORKING_BALANCE_AT_TXN: số dư khả dụng tại thời điểm giao dịch
CREATE TABLE LPB_POC.T24_TXN_ACCOUNT_SNAPSHOT (
    TRANSACTION_ID          VARCHAR2(50),
    ACCOUNT_ID              VARCHAR2(50)        NOT NULL,
    CUSTOMER_ID             VARCHAR2(50)        NOT NULL,
    TRANSACTION_DATE        DATE                NOT NULL,
    VALUE_DATE              DATE                NOT NULL,
    TRANSACTION_TIME        TIMESTAMP(3)        NOT NULL,
    TRANSACTION_TYPE        VARCHAR2(50)        NOT NULL,
    AMOUNT                  NUMBER(18,2)        NOT NULL,
    CURRENCY_CODE           VARCHAR2(10)        NOT NULL,
    CHANNEL                 VARCHAR2(50),
    BRANCH_CODE             VARCHAR2(20),
    REFERENCE_NO            VARCHAR2(100),
    TRANSACTION_STATUS      VARCHAR2(20),
    BALANCE_AT_TXN          NUMBER(18,2),
    WORKING_BALANCE_AT_TXN  NUMBER(18,2),
    ACCT_CURRENCY_CODE      VARCHAR2(10),
    TXN_TS_MS               NUMBER(15),
    ACCT_TS_MS              NUMBER(15),
    SNAPSHOT_AT             TIMESTAMP           DEFAULT SYSTIMESTAMP,
    CONSTRAINT PK_TXN_ACCOUNT_SNAPSHOT PRIMARY KEY (TRANSACTION_ID)
);

-- Migration nếu bảng đã tồn tại — TXN_TS_MS đã có, dùng cho latency query BLOCK 3

-- ── T24_TXN_ACCT_PENDING_JOIN (DLQ) ─────────────────────────
-- Flat schema: lưu toàn bộ TXN payload để retry không cần đọc lại Kafka
-- ERROR_CODE: mã lỗi chuẩn hóa (TXN_NO_ACCOUNT_MATCH)
-- ERROR_REASON: free-text chi tiết (account_id, context)
-- STATUS lifecycle: PENDING → RESOLVED | FAILED (hết MAX_RETRY)
CREATE TABLE LPB_POC.T24_TXN_ACCT_PENDING_JOIN (
    TRANSACTION_ID      VARCHAR2(50),
    ACCOUNT_ID          VARCHAR2(50)        NOT NULL,
    CUSTOMER_ID         VARCHAR2(50)        NOT NULL,
    TRANSACTION_DATE    DATE                NOT NULL,
    VALUE_DATE          DATE                NOT NULL,
    TRANSACTION_TIME    TIMESTAMP(3)        NOT NULL,
    TRANSACTION_TYPE    VARCHAR2(50)        NOT NULL,
    AMOUNT              NUMBER(18,2)        NOT NULL,
    CURRENCY_CODE       VARCHAR2(10)        NOT NULL,
    CHANNEL             VARCHAR2(50),
    BRANCH_CODE         VARCHAR2(20),
    REFERENCE_NO        VARCHAR2(100),
    TRANSACTION_STATUS  VARCHAR2(20),
    TXN_TS_MS           NUMBER(15),
    PENDING_SINCE       TIMESTAMP           DEFAULT SYSTIMESTAMP,
    RETRY_COUNT         NUMBER(3)           DEFAULT 0,
    NEXT_RETRY_AT       TIMESTAMP           DEFAULT SYSTIMESTAMP,
    STATUS              VARCHAR2(20)        DEFAULT 'PENDING',
    ERROR_CODE          VARCHAR2(50),
    ERROR_REASON        VARCHAR2(200),
    RESOLVED_AT         TIMESTAMP,
    CONSTRAINT PK_TXN_ACCT_PENDING_JOIN PRIMARY KEY (TRANSACTION_ID)
);

-- Migration cho DB đang chạy (chạy nếu bảng đã tồn tại):
-- ALTER TABLE LPB_POC.T24_TXN_ACCT_PENDING_JOIN ADD (
--     ERROR_CODE    VARCHAR2(50),
--     NEXT_RETRY_AT TIMESTAMP DEFAULT SYSTIMESTAMP
-- );


-- ── T24_BRANCH_SALES_SUMMARY ─────────────────────────────────
-- Pattern 4 — Stateful Aggregation: cộng dồn doanh số theo chi nhánh + ngày
-- PK: (BRANCH_CODE, RPT_DATE, CURRENCY_CODE)
-- TOTAL_AMOUNT / TXN_COUNT: tổng tích lũy — cập nhật bằng MERGE += delta
-- CREDIT_AMOUNT / DEBIT_AMOUNT: phân loại theo TRANSACTION_TYPE
CREATE TABLE LPB_POC.T24_BRANCH_SALES_SUMMARY (
    BRANCH_CODE     VARCHAR2(20)        NOT NULL,
    RPT_DATE        DATE                NOT NULL,
    CURRENCY_CODE   VARCHAR2(10)        NOT NULL,
    TOTAL_AMOUNT    NUMBER(20,2)        DEFAULT 0,
    TXN_COUNT       NUMBER(10)          DEFAULT 0,
    CREDIT_AMOUNT   NUMBER(20,2)        DEFAULT 0,
    DEBIT_AMOUNT    NUMBER(20,2)        DEFAULT 0,
    CREATED_AT      TIMESTAMP           DEFAULT SYSTIMESTAMP,
    UPDATED_AT      TIMESTAMP           DEFAULT SYSTIMESTAMP,
    CONSTRAINT PK_BRANCH_SALES_SUMMARY PRIMARY KEY (BRANCH_CODE, RPT_DATE, CURRENCY_CODE)
);

-- Index hỗ trợ query dashboard theo ngày
CREATE INDEX IDX_BRANCH_SALES_RPT_DATE ON LPB_POC.T24_BRANCH_SALES_SUMMARY (RPT_DATE, BRANCH_CODE);


-- ── T24_STREAM_BATCH_LOG ──────────────────────────────────────
-- Idempotency guard cho stateful aggregation jobs.
-- Ghi batch_id đã xử lý trong cùng Oracle transaction với MERGE.
-- Spark có thể replay batch_id cũ khi executor lỗi — guard này ngăn cộng double.
-- TTL: cleanup rows cũ hơn 7 ngày (chạy trong foreachBatch sau mỗi commit).
CREATE TABLE LPB_POC.T24_STREAM_BATCH_LOG (
    JOB_NAME        VARCHAR2(50)        NOT NULL,
    BATCH_ID        NUMBER              NOT NULL,
    PROCESSED_AT    TIMESTAMP           DEFAULT SYSTIMESTAMP,
    CONSTRAINT PK_STREAM_BATCH_LOG PRIMARY KEY (JOB_NAME, BATCH_ID)
);

