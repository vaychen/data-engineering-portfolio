-- sql/dml/dqs_report_row_count_check.sql
--
-- Data quality check: assert that a report / DWS table has non-zero rows
-- for yesterday's business date, and that the day-over-day row count ratio
-- is within an acceptable range (0.5 – 2.0 × prior day).
--
-- This SQL is executed via RedshiftSQLOperator with Jinja rendering.
-- The operator raises an exception if any row is returned by the final
-- SELECT (non-empty result = assertion failure).
--
-- Parameters
-- ----------
-- {{ params.report_table }}      : target table name (no schema prefix)
-- {{ params.backfill_scan_date }} : window width in days (drives prior-day lookup)

WITH today_count AS (
    SELECT COUNT(*) AS row_count
    FROM analytics_dw.{{ params.report_table }}
    WHERE partition_date = DATEADD(day, -1, CURRENT_DATE)
),
yesterday_count AS (
    SELECT COUNT(*) AS row_count
    FROM analytics_dw.{{ params.report_table }}
    WHERE partition_date = DATEADD(day, -2, CURRENT_DATE)
),
check_result AS (
    SELECT
        '{{ params.report_table }}'                     AS table_name,
        DATEADD(day, -1, CURRENT_DATE)                  AS business_date,
        t.row_count                                     AS today_rows,
        y.row_count                                     AS yesterday_rows,
        CASE
            WHEN t.row_count = 0
                THEN 'FAIL: zero rows for business date'
            WHEN y.row_count > 0
             AND (t.row_count::FLOAT / y.row_count) NOT BETWEEN 0.5 AND 2.0
                THEN 'FAIL: day-over-day ratio out of bounds ('
                     || ROUND(t.row_count::FLOAT / y.row_count, 2)::VARCHAR || 'x)'
            ELSE 'PASS'
        END                                             AS status
    FROM today_count t
    CROSS JOIN yesterday_count y
)
SELECT
    table_name,
    business_date,
    today_rows,
    yesterday_rows,
    status
FROM check_result
WHERE status <> 'PASS';
