-- =============================================================================
-- DML: dwd_product_active_daily.sql
-- Layer  : DWD (Data Warehouse Detail)
-- Table  : analytics_dw.dwd_product_active_daily
-- Pattern: Idempotent DELETE + INSERT over the backfill window
--
-- Grain  : One row per (event_local_date, product_guid)
--
-- Source : analytics_dw.ods_nova_app_events — rows where product_guid IS NOT
--          NULL and product_firmware_version IS NOT NULL represent events
--          emitted while a sentinel device was connected to the app.
--
-- Firmware resolution:
--   LAST_VALUE() with ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
--   selects the most recent firmware version seen for a given device on a given
--   day.  This handles mid-day OTA updates where multiple firmware strings may
--   appear for the same product_guid.
--
-- Jinja params
--   {{ ds }}                        -- Airflow logical date (YYYY-MM-DD)
--   {{ params.backfill_scan_date }} -- Days back included in the load window
-- =============================================================================

-- -------------------------------------------------------------------------
-- Step 1: Delete the target window to make this run idempotent.
-- -------------------------------------------------------------------------
DELETE FROM analytics_dw.dwd_product_active_daily
WHERE partition_date
      BETWEEN DATEADD(DAY, -{{ params.backfill_scan_date }}, '{{ ds }}'::DATE)
          AND '{{ ds }}'::DATE;

-- -------------------------------------------------------------------------
-- Step 2: Insert deduplicated daily active device records.
-- -------------------------------------------------------------------------
INSERT INTO analytics_dw.dwd_product_active_daily
    (event_local_date,
     product_guid,
     product_id,
     product_name,
     firmware_version,
     client_variant,
     first_seen_ts,
     last_seen_ts,
     active_user_count,
     event_count,
     partition_date,
     pipeline_load_timestamp)

WITH

-- Narrow to the backfill window and filter for rows carrying device context.
raw_events AS (
    SELECT
        event_local_date,
        product_guid,
        product_id,
        product_name,
        product_firmware_version    AS firmware_version,
        COALESCE(client_variant, '__none__') AS client_variant,
        event_timestamp,
        client_id,
        partition_date
    FROM analytics_dw.ods_nova_app_events
    WHERE
        partition_date
            BETWEEN DATEADD(DAY, -{{ params.backfill_scan_date }}, '{{ ds }}'::DATE)
                AND '{{ ds }}'::DATE

        -- Only rows with a linked sentinel device.
        AND product_guid IS NOT NULL
        AND TRIM(product_guid) <> ''

        -- Require firmware context — rows without it are app-only events.
        AND product_firmware_version IS NOT NULL
        AND TRIM(product_firmware_version) <> ''

        -- Exclude known test / internal traffic.
        AND client_name NOT LIKE '%_test%'
        AND client_name NOT LIKE '%_internal%'
),

-- Resolve firmware_version to the last value seen per (device, day).
-- LAST_VALUE with ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
-- scans the full partition so every row receives the true daily-last value.
firmware_resolved AS (
    SELECT
        event_local_date,
        product_guid,
        product_id,
        product_name,
        client_variant,
        event_timestamp,
        client_id,
        partition_date,
        LAST_VALUE(firmware_version) OVER (
            PARTITION BY event_local_date, product_guid
            ORDER BY     event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS latest_firmware_version
    FROM raw_events
)

SELECT
    event_local_date,
    product_guid,
    MAX(product_id)              AS product_id,
    MAX(product_name)            AS product_name,
    MAX(latest_firmware_version) AS firmware_version,
    MAX(client_variant)          AS client_variant,
    MIN(event_timestamp)         AS first_seen_ts,
    MAX(event_timestamp)         AS last_seen_ts,
    COUNT(DISTINCT client_id)    AS active_user_count,
    COUNT(*)                     AS event_count,
    MAX(partition_date)          AS partition_date,
    GETDATE()                    AS pipeline_load_timestamp

FROM firmware_resolved

GROUP BY
    event_local_date,
    product_guid;
