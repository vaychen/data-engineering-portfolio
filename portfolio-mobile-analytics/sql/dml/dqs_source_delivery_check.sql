-- =============================================================================
-- DML: dqs_source_delivery_check.sql
-- Layer  : Data Quality (DQS)
-- Purpose: Assert that a source ODS table received data today and that the
--          day-over-day row count change is within an acceptable band.
--
-- Jinja params
--   {{ ds }}                         -- Airflow logical date (YYYY-MM-DD)
--   {{ params.source_table }}        -- ODS table name (e.g. ods_nova_app_events)
--   {{ params.backfill_scan_date }}  -- days back (informational, not used in checks)
--
-- Failure modes
--   Assertion 1: Division by zero if today's count = 0.
--                Redshift raises: ERROR: division by zero
--   Assertion 2: CASE WHEN raises a cast error if ratio is outside [0.5, 2.0].
--                Pattern: CAST( 'DQ FAILURE: ...' AS INTEGER ) forces an error
--                that surfaces in Airflow logs with a descriptive message.
-- =============================================================================

-- -------------------------------------------------------------------------
-- Assertion 1: Non-zero delivery check.
--
-- SELECT 1 / COUNT(*) evaluates to:
--   - 1 if at least one row exists  (1 / n where n >= 1 truncates to 1)
--   - division by zero error if no rows exist for today
--
-- The outer SELECT wraps the result so Redshift reports a clean assertion
-- row rather than a bare scalar when the check passes.
-- -------------------------------------------------------------------------
SELECT
    'non_zero_delivery'                         AS assertion_name,
    '{{ params.source_table }}'                 AS source_table,
    '{{ ds }}'                                  AS check_date,
    1 / COUNT(*)                                AS assertion_result   -- fails on 0 rows

FROM analytics_dw.{{ params.source_table }}

WHERE partition_date = '{{ ds }}'::DATE;

-- -------------------------------------------------------------------------
-- Assertion 2: Day-over-day volume ratio check.
--
-- Computes today_count / yesterday_count and asserts the ratio falls within
-- [0.5, 2.0] (i.e. no more than 50 % drop or 100 % spike).
--
-- If the ratio is outside the band, CAST( '...' AS INTEGER ) is evaluated,
-- which raises:  ERROR: invalid input syntax for integer
-- with a message that clearly describes the violation.
-- -------------------------------------------------------------------------
WITH daily_counts AS (
    SELECT
        partition_date,
        COUNT(*) AS row_count
    FROM analytics_dw.{{ params.source_table }}
    WHERE partition_date IN (
        '{{ ds }}'::DATE,
        DATEADD(DAY, -1, '{{ ds }}'::DATE)
    )
    GROUP BY partition_date
),

today    AS (SELECT row_count FROM daily_counts WHERE partition_date = '{{ ds }}'::DATE),
yesterday AS (SELECT row_count FROM daily_counts WHERE partition_date = DATEADD(DAY, -1, '{{ ds }}'::DATE))

SELECT
    'dod_volume_ratio'                          AS assertion_name,
    '{{ params.source_table }}'                 AS source_table,
    '{{ ds }}'                                  AS check_date,
    t.row_count                                 AS today_count,
    y.row_count                                 AS yesterday_count,
    ROUND(t.row_count::DECIMAL / NULLIF(y.row_count, 0), 4)
                                                AS dod_ratio,
    CASE
        WHEN y.row_count = 0
            -- No baseline to compare against; pass through (cold-start or backfill).
            THEN 1
        WHEN t.row_count::DECIMAL / y.row_count BETWEEN 0.5 AND 2.0
            THEN 1
        ELSE
            -- Force an error with a descriptive message surfaced in Airflow logs.
            CAST(
                'DQ FAILURE [dod_ratio]: table={{ params.source_table }} '
                || 'check_date={{ ds }} '
                || 'ratio=' || ROUND(t.row_count::DECIMAL / y.row_count, 4)::VARCHAR
                || ' is outside [0.50, 2.00]'
                AS INTEGER
            )
    END                                         AS assertion_result

FROM today t
CROSS JOIN yesterday y;
