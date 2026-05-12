{{
  config(
    materialized         = 'incremental',
    incremental_strategy = 'append',
    full_refresh         = false,
    database             = var('analytics_database'),
    schema               = var('analytics_schema_etl'),
    alias                = 'dwd_product_active_daily',
    on_schema_change     = 'sync_all_columns',

    column_types = {
      "event_local_date":    "DATE",
      "product_guid":        "VARCHAR(36)",
      "firmware_version":    "VARCHAR(64)",
      "client_variant":      "VARCHAR(64)",
      "first_seen_ts":       "TIMESTAMP",
      "last_seen_ts":        "TIMESTAMP",
      "active_user_count":   "INTEGER",
      "event_count":         "INTEGER",
      "partition_date":      "DATE",
      "ingest_timestamp":    "TIMESTAMP"
    },

    pre_hook = [
      """
      DELETE FROM {{ this }}
      WHERE partition_date >= {{ analytics_backfill_lower_bound('analytics_backfill_days') }}
        AND partition_date <= {{ analytics_airflow_ds() }}
      """
    ],

    tags = ['etl_data', 'daily_active', 'product', 'sentinel']
  )
}}

{# --------------------------------------------------------------------------
   dwd_product_active_daily

   One row per (event_local_date, product_guid, client_variant) recording
   product / sentinel device activity.

   The firmware_version column presents a common challenge: multiple firmware
   versions can be reported for the same product_guid on the same day if a
   device updates mid-day.  We resolve this with LAST_VALUE() over an
   ordered window to capture the most recent firmware seen that day — this is
   the "last known state" pattern common in IoT pipelines.

   Filters applied
   ───────────────
   • product_guid IS NOT NULL and LEN = 36  → discard malformed GUIDs
   • firmware_version IS NOT NULL           → only rows with device context
   • is_test_event = FALSE                  → exclude QA / dev devices
   -------------------------------------------------------------------------- #}

WITH

raw_events AS (

  SELECT
    event_local_date,
    product_guid,
    client_variant,
    firmware_version,
    event_timestamp,
    client_id,
    partition_date
  FROM {{ ref('ods_nova_app_events') }}
  WHERE
    partition_date >= {{ analytics_backfill_lower_bound('analytics_backfill_days') }}
    AND partition_date <= {{ analytics_airflow_ds() }}
    AND product_guid IS NOT NULL
    AND LEN(product_guid) = 36
    AND firmware_version IS NOT NULL
    AND TRIM(firmware_version) <> ''
    AND COALESCE(is_test_event, FALSE) = FALSE

),

-- Resolve firmware_version to the last value seen per device per day.
-- LAST_VALUE with ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
-- scans the full partition so every row receives the true daily-last value,
-- not just the last row encountered before the current one.
firmware_resolved AS (

  SELECT
    event_local_date,
    product_guid,
    client_variant,
    firmware_version,
    event_timestamp,
    client_id,
    partition_date,
    LAST_VALUE(firmware_version) OVER (
      PARTITION BY event_local_date, product_guid
      ORDER BY event_timestamp
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )                                                   AS latest_firmware_version
  FROM raw_events

),

aggregated AS (

  SELECT
    event_local_date,
    product_guid,
    MAX(latest_firmware_version)                        AS firmware_version,
    MAX(client_variant)                                 AS client_variant,

    MIN(event_timestamp)                                AS first_seen_ts,
    MAX(event_timestamp)                                AS last_seen_ts,

    COUNT(DISTINCT client_id)                           AS active_user_count,
    COUNT(*)                                            AS event_count,

    MAX(partition_date)                                 AS partition_date

  FROM firmware_resolved
  GROUP BY
    event_local_date,
    product_guid

)

SELECT
  event_local_date,
  product_guid,
  firmware_version,
  client_variant,
  first_seen_ts,
  last_seen_ts,
  active_user_count,
  event_count,
  partition_date,
  GETDATE()                                             AS ingest_timestamp
FROM aggregated
