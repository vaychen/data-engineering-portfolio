-- =============================================================================
-- DML: dwd_user_active_daily.sql
-- Layer  : DWD (Data Warehouse Detail)
-- Table  : analytics_dw.dwd_user_active_daily
-- Pattern: Idempotent DELETE + INSERT over the backfill window
--
-- Grain  : One row per (partition_date, client_id, client_name,
--                        client_variant, client_version)
--
-- Jinja params
--   {{ ds }}                        -- Airflow logical date (YYYY-MM-DD)
--   {{ params.backfill_scan_date }} -- days back to include in window
-- =============================================================================

-- -------------------------------------------------------------------------
-- Step 1: Delete the target window to make this run idempotent.
--
-- Lower bound = ds - backfill_scan_date days
-- Upper bound = ds (inclusive)
-- -------------------------------------------------------------------------
DELETE FROM analytics_dw.dwd_user_active_daily
WHERE partition_date
      BETWEEN DATEADD(DAY, -{{ params.backfill_scan_date }}, '{{ ds }}'::DATE)
          AND '{{ ds }}'::DATE;

-- -------------------------------------------------------------------------
-- Step 2: Insert deduplicated daily active user records.
--
-- Source: analytics_dw.ods_nova_app_events (ODS layer)
--
-- For each (partition_date, client) tuple we compute:
--   - first_event_ts / last_event_ts    : wall-clock timestamps (ms epoch)
--   - first_event_mono / last_event_mono: monotonic counters for ordering
--   - event_count                        : total events fired that day
--   - session_count                      : distinct sessions
-- -------------------------------------------------------------------------
INSERT INTO analytics_dw.dwd_user_active_daily
    (partition_date,
     client_id,
     client_name,
     client_variant,
     client_version,
     client_os_name,
     client_os_version,
     product_id,
     product_name,
     first_event_ts,
     last_event_ts,
     first_event_mono,
     last_event_mono,
     event_count,
     session_count,
     pipeline_load_timestamp)

SELECT
    e.partition_date,
    e.client_id,
    e.client_name,
    -- Normalise NULL variants to a sentinel so GROUP BY is deterministic.
    COALESCE(e.client_variant, '__none__')          AS client_variant,
    e.client_version,
    e.client_os_name,
    e.client_os_version,
    e.product_id,
    e.product_name,
    MIN(e.event_timestamp)                          AS first_event_ts,
    MAX(e.event_timestamp)                          AS last_event_ts,
    MIN(e.client_mono_time)                         AS first_event_mono,
    MAX(e.client_mono_time)                         AS last_event_mono,
    COUNT(*)                                        AS event_count,
    COUNT(DISTINCT e.session_id)                    AS session_count,
    GETDATE()                                       AS pipeline_load_timestamp

FROM analytics_dw.ods_nova_app_events AS e

WHERE
    -- Narrow to the backfill window.
    e.partition_date BETWEEN DATEADD(DAY, -{{ params.backfill_scan_date }}, '{{ ds }}'::DATE)
                         AND '{{ ds }}'::DATE

    -- Exclude rows with no client identity.
    AND e.client_id IS NOT NULL
    AND e.client_id <> ''

    -- Exclude known test / internal client identifiers.
    AND e.client_name NOT LIKE '%_test%'
    AND e.client_name NOT LIKE '%_internal%'

GROUP BY
    e.partition_date,
    e.client_id,
    e.client_name,
    COALESCE(e.client_variant, '__none__'),
    e.client_version,
    e.client_os_name,
    e.client_os_version,
    e.product_id,
    e.product_name;
