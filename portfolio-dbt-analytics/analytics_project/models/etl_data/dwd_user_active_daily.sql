{{
  config(
    materialized         = 'incremental',
    incremental_strategy = 'append',
    full_refresh         = false,
    database             = var('analytics_database'),
    schema               = var('analytics_schema_etl'),
    alias                = 'dwd_user_active_daily',
    on_schema_change     = 'sync_all_columns',

    column_types = {
      "event_local_date":        "DATE",
      "client_id":               "VARCHAR(64)",
      "client_name":             "VARCHAR(120)",
      "client_variant":          "VARCHAR(64)",
      "client_version":          "VARCHAR(32)",
      "client_build":            "VARCHAR(32)",
      "client_platform":         "VARCHAR(32)",
      "first_event_timestamp":   "TIMESTAMP",
      "last_event_timestamp":    "TIMESTAMP",
      "first_mono_time":         "BIGINT",
      "last_mono_time":          "BIGINT",
      "event_count":             "INTEGER",
      "session_count":           "INTEGER",
      "partition_date":          "DATE",
      "ingest_timestamp":        "TIMESTAMP"
    },

    -- Delete the exact date window being reprocessed to ensure idempotency.
    -- The window uses the same macro as the ODS source so that a backfill
    -- of N days in the DAG propagates consistently through all layers.
    pre_hook = [
      """
      DELETE FROM {{ this }}
      WHERE partition_date >= {{ analytics_backfill_lower_bound('analytics_backfill_days') }}
        AND partition_date <= {{ analytics_airflow_ds() }}
      """
    ],

    tags = ['etl_data', 'daily_active', 'user']
  )
}}

{# --------------------------------------------------------------------------
   dwd_user_active_daily

   One row per (event_local_date, client_id, client_name, client_variant,
   client_version, client_build, client_platform).

   "Active" is defined as: at least one event recorded for that client on
   that local calendar date.

   Key design choices
   ──────────────────
   • Uses event_local_date (device-reported local date) rather than
     partition_date (server ingestion date) so that timezone-shifted users
     are bucketed to the day they actually used the app.
   • MIN/MAX of both wall-clock timestamps and mono_time (monotonic clock)
     are retained to allow session-gap detection in downstream models.
   • client_id NULL rows are excluded — they represent unauthenticated
     pre-login events that cannot be attributed to a user.
   -------------------------------------------------------------------------- #}

WITH

backfill_data AS (

  SELECT
    event_local_date,
    client_id,
    client_name,
    client_variant,
    client_version,
    client_build,
    client_platform,
    event_timestamp,
    client_mono_time,
    session_id,
    partition_date
  FROM {{ ref('ods_nova_app_events') }}
  WHERE
    partition_date >= {{ analytics_backfill_lower_bound('analytics_backfill_days') }}
    AND partition_date <= {{ analytics_airflow_ds() }}
    AND client_id IS NOT NULL
    AND TRIM(client_id) <> ''
    -- Exclude test / internal events from user-facing metrics
    AND COALESCE(is_test_event, FALSE) = FALSE

),

cleaned_data AS (

  SELECT
    event_local_date,
    client_id,
    client_name,
    client_variant,
    client_version,
    client_build,
    client_platform,

    MIN(event_timestamp)                                AS first_event_timestamp,
    MAX(event_timestamp)                                AS last_event_timestamp,
    MIN(client_mono_time)                               AS first_mono_time,
    MAX(client_mono_time)                               AS last_mono_time,

    COUNT(*)                                            AS event_count,
    COUNT(DISTINCT session_id)                          AS session_count,

    -- Carry the server-side partition_date for incremental management;
    -- use MAX so that late-arriving events don't produce multiple rows.
    MAX(partition_date)                                 AS partition_date

  FROM backfill_data
  GROUP BY
    event_local_date,
    client_id,
    client_name,
    client_variant,
    client_version,
    client_build,
    client_platform

)

SELECT
  event_local_date,
  client_id,
  client_name,
  client_variant,
  client_version,
  client_build,
  client_platform,
  first_event_timestamp,
  last_event_timestamp,
  first_mono_time,
  last_mono_time,
  event_count,
  session_count,
  partition_date,
  GETDATE()                                             AS ingest_timestamp
FROM cleaned_data
