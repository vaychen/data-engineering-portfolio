-- =============================================================================
-- DML: dws_user_retention.sql
-- Layer  : DWS
-- Table  : analytics_dw.dws_user_retention
-- Pattern: Idempotent DELETE + INSERT over backfill window
--
-- Grain  : One row per (cohort_date, partition_date, client_name, product_id)
--
-- Retention definition:
--   cohort_date = first day a client_id was active (MIN partition_date in DWD)
--   A user is "retained" on partition_date if they appear in dwd_user_active_daily
--   for that date, having first appeared on cohort_date.
--
-- This script recomputes retention for any cohort_date whose cohort members
-- had activity within the backfill window. It re-evaluates all partition_dates
-- for those cohorts to keep retention curves consistent.
--
-- Jinja params
--   {{ ds }}                        -- Airflow logical date (YYYY-MM-DD)
--   {{ params.backfill_scan_date }} -- Days back to scan for affected cohort members
-- =============================================================================

-- Step 1: Identify cohort_dates affected by the backfill window.
--         Then delete all retention rows for those cohorts.
DELETE FROM analytics_dw.dws_user_retention
WHERE cohort_date IN (
    SELECT DISTINCT c.cohort_date
    FROM analytics_dw.dws_user_retention AS c
    INNER JOIN (
        -- cohort_dates of users active within the backfill window
        SELECT DISTINCT uad.client_id, uad.client_name
        FROM analytics_dw.dwd_user_active_daily AS uad
        WHERE uad.partition_date
              BETWEEN DATEADD(DAY, -{{ params.backfill_scan_date }}, '{{ ds }}'::DATE)
                  AND '{{ ds }}'::DATE
    ) AS active ON c.client_name = active.client_name
);

-- Step 2: Recompute retention for affected cohorts.
INSERT INTO analytics_dw.dws_user_retention
(
    cohort_date,
    partition_date,
    days_since_cohort,
    client_name,
    product_id,
    cohort_size,
    retained_users,
    retention_rate,
    pipeline_load_timestamp
)

WITH

-- Derive each user's cohort date: earliest partition_date in the DWD table.
cohort_map AS (
    SELECT
        client_id,
        client_name,
        product_id,
        MIN(partition_date)     AS cohort_date
    FROM analytics_dw.dwd_user_active_daily
    GROUP BY client_id, client_name, product_id
),

-- Filter to cohorts affected by the backfill window.
affected_cohorts AS (
    SELECT DISTINCT cm.cohort_date, cm.client_name, cm.product_id
    FROM cohort_map AS cm
    INNER JOIN analytics_dw.dwd_user_active_daily AS uad
        ON  cm.client_id   = uad.client_id
        AND cm.client_name = uad.client_name
    WHERE uad.partition_date
          BETWEEN DATEADD(DAY, -{{ params.backfill_scan_date }}, '{{ ds }}'::DATE)
              AND '{{ ds }}'::DATE
),

-- Cohort sizes.
cohort_sizes AS (
    SELECT
        cm.cohort_date,
        cm.client_name,
        cm.product_id,
        COUNT(DISTINCT cm.client_id)    AS cohort_size
    FROM cohort_map AS cm
    INNER JOIN affected_cohorts AS ac
        ON  cm.cohort_date  = ac.cohort_date
        AND cm.client_name  = ac.client_name
        AND cm.product_id   = ac.product_id
    GROUP BY cm.cohort_date, cm.client_name, cm.product_id
),

-- All (cohort, measurement_date) pairs to evaluate — up to ds.
measurement_dates AS (
    SELECT DISTINCT
        ac.cohort_date,
        ac.client_name,
        ac.product_id,
        uad.partition_date              AS measurement_date
    FROM affected_cohorts AS ac
    CROSS JOIN analytics_dw.dwd_user_active_daily AS uad
    WHERE uad.partition_date >= ac.cohort_date
      AND uad.partition_date <= '{{ ds }}'::DATE
),

-- Count retained users per (cohort, measurement_date).
retention_counts AS (
    SELECT
        cm.cohort_date,
        cm.client_name,
        cm.product_id,
        uad.partition_date              AS partition_date,
        COUNT(DISTINCT uad.client_id)   AS retained_users
    FROM cohort_map AS cm
    INNER JOIN analytics_dw.dwd_user_active_daily AS uad
        ON  cm.client_id   = uad.client_id
        AND cm.client_name = uad.client_name
    INNER JOIN affected_cohorts AS ac
        ON  cm.cohort_date  = ac.cohort_date
        AND cm.client_name  = ac.client_name
        AND cm.product_id   = ac.product_id
    WHERE uad.partition_date >= cm.cohort_date
      AND uad.partition_date <= '{{ ds }}'::DATE
    GROUP BY cm.cohort_date, cm.client_name, cm.product_id, uad.partition_date
)

SELECT
    rc.cohort_date,
    rc.partition_date,
    DATEDIFF(DAY, rc.cohort_date, rc.partition_date)    AS days_since_cohort,
    rc.client_name,
    rc.product_id,
    cs.cohort_size,
    rc.retained_users,
    ROUND(rc.retained_users::DECIMAL / NULLIF(cs.cohort_size, 0), 4)
                                                        AS retention_rate,
    GETDATE()                                           AS pipeline_load_timestamp

FROM retention_counts AS rc
INNER JOIN cohort_sizes AS cs
    ON  rc.cohort_date  = cs.cohort_date
    AND rc.client_name  = cs.client_name
    AND rc.product_id   = cs.product_id;
