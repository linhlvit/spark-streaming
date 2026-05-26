-- ============================================================
-- monitoring.sql — SQL thống kê pipeline + đo latency
--
-- Cách đo latency end-to-end:
--   CREATE_MS  = Debezium ts_ms = thời điểm Oracle commit giao dịch (epoch ms)
--   ENRICHED_AT / SNAPSHOT_AT / UPDATED_AT = thời điểm Spark ghi vào Oracle target
--   Latency = cột ghi target - to_timestamp(CREATE_MS / 1000)
--
-- Chạy từng block bằng cách bôi đen và F5 trong SQL Developer,
-- hoặc @monitoring.sql trong sqlplus.
-- ============================================================


-- ============================================================
-- BLOCK 1 — TỔNG QUAN PIPELINE (row counts)
-- ============================================================
SELECT 'T24_TRANSACTIONS_TARGET'   AS tbl, COUNT(*) AS rows FROM FSS_STREAM.T24_TRANSACTIONS_TARGET
UNION ALL
SELECT 'T24_ACCOUNT_TARGET',        COUNT(*) FROM FSS_STREAM.T24_ACCOUNT_TARGET
UNION ALL
SELECT 'T24_CUSTOMER_TARGET',       COUNT(*) FROM FSS_STREAM.T24_CUSTOMER_TARGET
UNION ALL
SELECT 'T24_BRANCH_TARGET',         COUNT(*) FROM FSS_STREAM.T24_BRANCH_TARGET
UNION ALL
SELECT '--- enriched ---',          0        FROM DUAL
UNION ALL
SELECT 'T24_TXN_ENRICHED',          COUNT(*) FROM FSS_STREAM.T24_TXN_ENRICHED
UNION ALL
SELECT 'T24_TXN_ACCOUNT_SNAPSHOT',  COUNT(*) FROM FSS_STREAM.T24_TXN_ACCOUNT_SNAPSHOT
UNION ALL
SELECT 'T24_BRANCH_SALES_SUMMARY',  COUNT(*) FROM FSS_STREAM.T24_BRANCH_SALES_SUMMARY
UNION ALL
SELECT '--- dlq ---',               0        FROM DUAL
UNION ALL
SELECT 'DLQ_TXN_PENDING_JOIN',      COUNT(*) FROM FSS_STREAM.T24_TXN_PENDING_JOIN       WHERE STATUS = 'PENDING'
UNION ALL
SELECT 'DLQ_ACCT_PENDING_JOIN',     COUNT(*) FROM FSS_STREAM.T24_TXN_ACCT_PENDING_JOIN  WHERE STATUS = 'PENDING'
ORDER BY 1;


-- ============================================================
-- BLOCK 2 — LATENCY: static_join (TXN → T24_TXN_ENRICHED)
--
-- CREATE_MS = ts_ms Debezium = Oracle commit time (epoch ms)
-- ENRICHED_AT = Spark ghi vào Oracle
-- Latency = ENRICHED_AT - Oracle commit time
-- ============================================================
SELECT
    ROUND(AVG (
        (ENRICHED_AT - CAST(TO_TIMESTAMP('1970-01-01','YYYY-MM-DD')
            + NUMTODSINTERVAL(CREATE_MS / 1000, 'SECOND') AS TIMESTAMP WITH TIME ZONE AT TIME ZONE 'UTC'
            ) CAST (TIMESTAMP)
        ) * 86400
    ), 1)  AS avg_latency_s,
    ROUND(
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY
            (ENRICHED_AT - CAST(TO_TIMESTAMP('1970-01-01','YYYY-MM-DD')
                + NUMTODSINTERVAL(CREATE_MS / 1000, 'SECOND') AS TIMESTAMP WITH TIME ZONE AT TIME ZONE 'UTC'
                ) CAST (TIMESTAMP)
            ) * 86400
        ), 1
    ) AS p50_latency_s,
    ROUND(
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY
            (ENRICHED_AT - CAST(TO_TIMESTAMP('1970-01-01','YYYY-MM-DD')
                + NUMTODSINTERVAL(CREATE_MS / 1000, 'SECOND') AS TIMESTAMP WITH TIME ZONE AT TIME ZONE 'UTC'
                ) CAST (TIMESTAMP)
            ) * 86400
        ), 1
    ) AS p95_latency_s,
    MAX(
        (ENRICHED_AT - CAST(TO_TIMESTAMP('1970-01-01','YYYY-MM-DD')
            + NUMTODSINTERVAL(CREATE_MS / 1000, 'SECOND') AS TIMESTAMP WITH TIME ZONE AT TIME ZONE 'UTC'
            ) CAST (TIMESTAMP)
        ) * 86400
    ) AS max_latency_s,
    COUNT(*) AS sample_rows
FROM FSS_STREAM.T24_TXN_ENRICHED
WHERE CREATE_MS IS NOT NULL
  AND ENRICHED_AT >= SYSTIMESTAMP - INTERVAL '1' HOUR;


-- ============================================================
-- BLOCK 2b — LATENCY dạng đơn giản hơn (Oracle-friendly)
-- Dùng khi BLOCK 2 gặp lỗi timezone casting
-- ============================================================
SELECT
    ROUND(AVG(
        (ENRICHED_AT - (DATE '1970-01-01' + CREATE_MS/1000/86400)) * 86400
    ), 1)  AS avg_latency_s,
    ROUND(
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY
            (ENRICHED_AT - (DATE '1970-01-01' + CREATE_MS/1000/86400)) * 86400
        ), 1
    ) AS p50_latency_s,
    ROUND(
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY
            (ENRICHED_AT - (DATE '1970-01-01' + CREATE_MS/1000/86400)) * 86400
        ), 1
    ) AS p95_latency_s,
    MAX(
        (ENRICHED_AT - (DATE '1970-01-01' + CREATE_MS/1000/86400)) * 86400
    ) AS max_latency_s,
    COUNT(*) AS sample_rows
FROM FSS_STREAM.T24_TXN_ENRICHED
WHERE CREATE_MS IS NOT NULL
  AND ENRICHED_AT >= SYSTIMESTAMP - INTERVAL '1' HOUR;


-- ============================================================
-- BLOCK 3 — LATENCY: stream_join (TXN → T24_TXN_ACCOUNT_SNAPSHOT)
--
-- TXN_TS_MS = ts_ms của TXN event (Debezium)
-- SNAPSHOT_AT = Spark ghi vào Oracle
-- Latency = SNAPSHOT_AT - Oracle commit time của TXN
-- ============================================================
SELECT
    ROUND(AVG(
        (SNAPSHOT_AT - (DATE '1970-01-01' + TXN_TS_MS/1000/86400)) * 86400
    ), 1)  AS avg_latency_s,
    ROUND(
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY
            (SNAPSHOT_AT - (DATE '1970-01-01' + TXN_TS_MS/1000/86400)) * 86400
        ), 1
    ) AS p50_latency_s,
    ROUND(
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY
            (SNAPSHOT_AT - (DATE '1970-01-01' + TXN_TS_MS/1000/86400)) * 86400
        ), 1
    ) AS p95_latency_s,
    MAX(
        (SNAPSHOT_AT - (DATE '1970-01-01' + TXN_TS_MS/1000/86400)) * 86400
    ) AS max_latency_s,
    COUNT(*) AS sample_rows
FROM FSS_STREAM.T24_TXN_ACCOUNT_SNAPSHOT
WHERE TXN_TS_MS IS NOT NULL
  AND SNAPSHOT_AT >= SYSTIMESTAMP - INTERVAL '1' HOUR;


-- ============================================================
-- BLOCK 4 — LATENCY: aggregation (TXN → T24_BRANCH_SALES_SUMMARY)
--
-- UPDATED_AT = lần cuối MERGE cập nhật row summary
-- Không đo per-TXN vì aggregation gộp nhiều TXN vào 1 row.
-- Dùng UPDATED_AT để xem độ tươi của data: SYSDATE - UPDATED_AT
-- ============================================================
SELECT
    BRANCH_CODE,
    RPT_DATE,
    TOTAL_AMOUNT,
    TXN_COUNT,
    ROUND((SYSDATE - CAST(UPDATED_AT AS DATE)) * 86400, 0) AS data_age_s,
    UPDATED_AT
FROM FSS_STREAM.T24_BRANCH_SALES_SUMMARY
WHERE RPT_DATE = TRUNC(SYSDATE)
  AND CURRENCY_CODE = 'VND'
ORDER BY UPDATED_AT DESC
FETCH FIRST 20 ROWS ONLY;


-- ============================================================
-- BLOCK 5 — LATENCY theo giờ (trend trong ngày)
-- Phát hiện giờ cao điểm latency tăng
-- ============================================================
SELECT
    TO_CHAR(ENRICHED_AT, 'HH24') AS hour_of_day,
    COUNT(*)                      AS txn_count,
    ROUND(AVG(
        (ENRICHED_AT - (DATE '1970-01-01' + CREATE_MS/1000/86400)) * 86400
    ), 1) AS avg_latency_s,
    ROUND(
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY
            (ENRICHED_AT - (DATE '1970-01-01' + CREATE_MS/1000/86400)) * 86400
        ), 1
    ) AS p95_latency_s
FROM FSS_STREAM.T24_TXN_ENRICHED
WHERE CREATE_MS IS NOT NULL
  AND ENRICHED_AT >= TRUNC(SYSDATE)
GROUP BY TO_CHAR(ENRICHED_AT, 'HH24')
ORDER BY 1;


-- ============================================================
-- BLOCK 6 — THROUGHPUT: số TXN xử lý được mỗi phút
-- ============================================================
SELECT
    TO_CHAR(ENRICHED_AT, 'YYYY-MM-DD HH24:MI') AS minute_bucket,
    COUNT(*)                                    AS txn_per_minute
FROM FSS_STREAM.T24_TXN_ENRICHED
WHERE ENRICHED_AT >= SYSTIMESTAMP - INTERVAL '30' MINUTE
GROUP BY TO_CHAR(ENRICHED_AT, 'YYYY-MM-DD HH24:MI')
ORDER BY 1;


-- ============================================================
-- BLOCK 7 — DLQ HEALTH: tỷ lệ vào DLQ so với tổng TXN
-- Ngưỡng cảnh báo: >1% → investigate, >5% → tăng watermark
-- ============================================================
SELECT
    total_txn,
    enriched_ok,
    snapshot_ok,
    dlq_branch,
    dlq_acct,
    ROUND(dlq_branch / NULLIF(total_txn, 0) * 100, 2) AS dlq_branch_pct,
    ROUND(dlq_acct  / NULLIF(total_txn, 0) * 100, 2) AS dlq_acct_pct
FROM (
    SELECT
        (SELECT COUNT(*) FROM FSS_STREAM.T24_TRANSACTIONS_TARGET
         WHERE TRANSACTION_DATE >= TRUNC(SYSDATE))           AS total_txn,
        (SELECT COUNT(*) FROM FSS_STREAM.T24_TXN_ENRICHED
         WHERE ENRICHED_AT >= TRUNC(SYSDATE))                AS enriched_ok,
        (SELECT COUNT(*) FROM FSS_STREAM.T24_TXN_ACCOUNT_SNAPSHOT
         WHERE SNAPSHOT_AT >= TRUNC(SYSDATE))                AS snapshot_ok,
        (SELECT COUNT(*) FROM FSS_STREAM.T24_TXN_PENDING_JOIN
         WHERE STATUS = 'PENDING'
           AND PENDING_SINCE >= TRUNC(SYSDATE))              AS dlq_branch,
        (SELECT COUNT(*) FROM FSS_STREAM.T24_TXN_ACCT_PENDING_JOIN
         WHERE STATUS = 'PENDING'
           AND PENDING_SINCE >= TRUNC(SYSDATE))              AS dlq_acct
    FROM DUAL
);


-- ============================================================
-- BLOCK 8 — SYNC LAG: độ trễ giữa source và target (CDC Sync)
-- TXN có trong SOURCE nhưng chưa có trong TARGET → sync chậm
-- ============================================================
SELECT
    COUNT(*) AS not_yet_synced,
    MIN(TRANSACTION_DATE) AS oldest_unsynced_date
FROM FSS_STREAM.T24_TRANSACTIONS_TARGET src
WHERE NOT EXISTS (
    SELECT 1 FROM FSS_STREAM.T24_TRANSACTIONS_TARGET tgt
    WHERE tgt.TRANSACTION_ID = src.TRANSACTION_ID
);
-- Lưu ý: nếu source và target cùng schema thì dùng bảng nguồn Oracle thực tế
-- SELECT COUNT(*) FROM FSS_STREAM.T24_TRANSACTIONS src
-- WHERE NOT EXISTS (SELECT 1 FROM FSS_STREAM.T24_TRANSACTIONS_TARGET tgt
--                   WHERE tgt.TRANSACTION_ID = src.TRANSACTION_ID);


-- ============================================================
-- BLOCK 9 — COVERAGE: TXN hôm nay đã có đủ enriched + snapshot chưa
-- ============================================================
SELECT
    t.TRANSACTION_ID,
    t.BRANCH_CODE,
    t.ACCOUNT_ID,
    CASE WHEN e.TRANSACTION_ID IS NOT NULL THEN 'OK' ELSE 'MISSING' END AS enriched_status,
    CASE WHEN s.TRANSACTION_ID IS NOT NULL THEN 'OK' ELSE 'MISSING' END AS snapshot_status,
    CASE WHEN p1.TRANSACTION_ID IS NOT NULL THEN p1.STATUS END          AS branch_dlq,
    CASE WHEN p2.TRANSACTION_ID IS NOT NULL THEN p2.STATUS END          AS acct_dlq
FROM FSS_STREAM.T24_TRANSACTIONS_TARGET t
LEFT JOIN FSS_STREAM.T24_TXN_ENRICHED          e  ON e.TRANSACTION_ID = t.TRANSACTION_ID
LEFT JOIN FSS_STREAM.T24_TXN_ACCOUNT_SNAPSHOT  s  ON s.TRANSACTION_ID = t.TRANSACTION_ID
LEFT JOIN FSS_STREAM.T24_TXN_PENDING_JOIN      p1 ON p1.TRANSACTION_ID = t.TRANSACTION_ID
LEFT JOIN FSS_STREAM.T24_TXN_ACCT_PENDING_JOIN p2 ON p2.TRANSACTION_ID = t.TRANSACTION_ID
WHERE t.TRANSACTION_DATE >= TRUNC(SYSDATE)
  AND (e.TRANSACTION_ID IS NULL OR s.TRANSACTION_ID IS NULL)
ORDER BY t.TRANSACTION_DATE DESC
FETCH FIRST 50 ROWS ONLY;


-- ============================================================
-- BLOCK 10 — AGGREGATION ACCURACY: kiểm tra tổng doanh số khớp
-- So sánh SUM từ T24_TRANSACTIONS_TARGET với T24_BRANCH_SALES_SUMMARY
-- ============================================================
SELECT
    src.BRANCH_CODE,
    src.CURRENCY_CODE,
    src.rpt_date,
    src.raw_sum,
    NVL(agg.TOTAL_AMOUNT, 0)                          AS agg_sum,
    src.raw_count,
    NVL(agg.TXN_COUNT, 0)                             AS agg_count,
    src.raw_sum - NVL(agg.TOTAL_AMOUNT, 0)            AS amount_diff,
    CASE
        WHEN ABS(src.raw_sum - NVL(agg.TOTAL_AMOUNT, 0)) < 0.01 THEN 'OK'
        ELSE 'MISMATCH'
    END AS check_result
FROM (
    SELECT
        BRANCH_CODE,
        CURRENCY_CODE,
        TRUNC(TRANSACTION_DATE) AS rpt_date,
        SUM(CASE WHEN TRANSACTION_STATUS != 'REVERSED'
                 THEN AMOUNT ELSE -AMOUNT END)         AS raw_sum,
        COUNT(*)                                        AS raw_count
    FROM FSS_STREAM.T24_TRANSACTIONS_TARGET
    WHERE TRANSACTION_DATE >= TRUNC(SYSDATE) - 7
    GROUP BY BRANCH_CODE, CURRENCY_CODE, TRUNC(TRANSACTION_DATE)
) src
LEFT JOIN FSS_STREAM.T24_BRANCH_SALES_SUMMARY agg
    ON  agg.BRANCH_CODE   = src.BRANCH_CODE
    AND agg.CURRENCY_CODE = src.CURRENCY_CODE
    AND agg.RPT_DATE      = src.rpt_date
ORDER BY check_result DESC, src.rpt_date DESC;


-- ============================================================
-- BLOCK 11 — DLQ AGE DISTRIBUTION: phân phối thời gian PENDING
-- Phát hiện stuck records (PENDING quá lâu)
-- ============================================================
SELECT
    table_name,
    age_bucket,
    COUNT(*) AS cnt
FROM (
    SELECT
        'T24_TXN_PENDING_JOIN' AS table_name,
        CASE
            WHEN (SYSDATE - CAST(PENDING_SINCE AS DATE)) * 24 < 1    THEN '< 1h'
            WHEN (SYSDATE - CAST(PENDING_SINCE AS DATE)) * 24 < 6    THEN '1-6h'
            WHEN (SYSDATE - CAST(PENDING_SINCE AS DATE)) * 24 < 24   THEN '6-24h'
            WHEN (SYSDATE - CAST(PENDING_SINCE AS DATE)) * 24 < 72   THEN '1-3 days'
            ELSE '> 3 days'
        END AS age_bucket
    FROM FSS_STREAM.T24_TXN_PENDING_JOIN
    WHERE STATUS = 'PENDING'
    UNION ALL
    SELECT
        'T24_TXN_ACCT_PENDING_JOIN',
        CASE
            WHEN (SYSDATE - CAST(PENDING_SINCE AS DATE)) * 24 < 1    THEN '< 1h'
            WHEN (SYSDATE - CAST(PENDING_SINCE AS DATE)) * 24 < 6    THEN '1-6h'
            WHEN (SYSDATE - CAST(PENDING_SINCE AS DATE)) * 24 < 24   THEN '6-24h'
            WHEN (SYSDATE - CAST(PENDING_SINCE AS DATE)) * 24 < 72   THEN '1-3 days'
            ELSE '> 3 days'
        END
    FROM FSS_STREAM.T24_TXN_ACCT_PENDING_JOIN
    WHERE STATUS = 'PENDING'
)
GROUP BY table_name, age_bucket
ORDER BY table_name, age_bucket;
