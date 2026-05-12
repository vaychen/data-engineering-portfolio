-- =============================================================================
-- DML: ods_nova_events_dedupe.sql
-- Layer  : ODS
-- Tables : analytics_dw.ods_nova_events_staging  →  analytics_dw.ods_nova_app_events
-- Pattern: Idempotent DELETE (target window) + INSERT (deduplicated rows from staging)
--
-- Deduplication key: event_guid
--   Multiple Firehose deliveries or Lambda retries can produce duplicate
--   event_guid values in the staging table. ROW_NUMBER() over event_guid
--   keeps only the earliest-received copy.
--
-- Jinja params
--   {{ ds }}                        -- Airflow logical date (YYYY-MM-DD)
--   {{ params.backfill_scan_date }} -- Days back included in the load window
-- =============================================================================

-- -------------------------------------------------------------------------
-- Step 1: Delete the target window from the production ODS table.
--         Ensures the upcoming INSERT is idempotent on retry.
-- -------------------------------------------------------------------------
DELETE FROM analytics_dw.ods_nova_app_events
WHERE partition_date
      BETWEEN DATEADD(DAY, -{{ params.backfill_scan_date }}, '{{ ds }}'::DATE)
          AND '{{ ds }}'::DATE;

-- -------------------------------------------------------------------------
-- Step 2: Insert deduplicated rows from staging into the production table.
--
-- ROW_NUMBER() partitioned by event_guid and ordered by event_received_at
-- retains the first-received copy when duplicates exist.
-- -------------------------------------------------------------------------
INSERT INTO analytics_dw.ods_nova_app_events
(
    event_name,
    event_schema_version,
    event_guid,
    event_timestamp,
    event_local_date,
    event_received_at,
    client_name,
    client_variant,
    client_version,
    client_build,
    client_id,
    client_mono_time,
    client_is_background,
    client_os_name,
    client_os_version,
    product_name,
    product_id,
    product_variant,
    product_guid,
    product_firmware_version,
    session_id,
    payload,
    partition_date
)

WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_guid
            ORDER BY     event_received_at ASC
        ) AS rn
    FROM analytics_dw.ods_nova_events_staging
)

SELECT
    event_name,
    event_schema_version,
    event_guid,
    event_timestamp,
    event_local_date,
    event_received_at,
    client_name,
    client_variant,
    client_version,
    client_build,
    client_id,
    client_mono_time,
    client_is_background,
    client_os_name,
    client_os_version,
    product_name,
    product_id,
    product_variant,
    product_guid,
    product_firmware_version,
    session_id,
    payload,
    partition_date

FROM deduped
WHERE rn = 1;
