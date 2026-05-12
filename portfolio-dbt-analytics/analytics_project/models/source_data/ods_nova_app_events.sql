{{
  config(
    materialized         = 'incremental',
    incremental_strategy = 'append',
    full_refresh         = false,
    database             = var('analytics_database'),
    schema               = var('analytics_schema_source'),
    alias                = 'ods_nova_app_events',
    on_schema_change     = 'sync_all_columns',

    -- Explicit column_types ensures Redshift picks the right encoding
    -- and that schema drift is caught at load time rather than silently
    -- widening a VARCHAR column.
    column_types = {
      "event_name":              "VARCHAR(120)",
      "event_guid":              "VARCHAR(36)",
      "event_timestamp":         "TIMESTAMP",
      "event_local_timestamp":   "TIMESTAMP",
      "event_local_date":        "DATE",
      "client_id":               "VARCHAR(64)",
      "client_name":             "VARCHAR(120)",
      "client_variant":          "VARCHAR(64)",
      "client_version":          "VARCHAR(32)",
      "client_build":            "VARCHAR(32)",
      "client_platform":         "VARCHAR(32)",
      "product_guid":            "VARCHAR(36)",
      "firmware_version":        "VARCHAR(64)",
      "payload":                 "SUPER",
      "partition_date":          "DATE",
      "source_schema":           "VARCHAR(64)",
      "source_table":            "VARCHAR(120)",
      "record_version":          "SMALLINT",
      "ingest_timestamp":        "TIMESTAMP",
      "client_mono_time":        "BIGINT",
      "event_sequence_id":       "BIGINT",
      "session_id":              "VARCHAR(64)",
      "app_instance_id":         "VARCHAR(64)",
      "user_id":                 "VARCHAR(64)",
      "is_test_event":           "BOOLEAN"
    },

    -- Idempotent delete: wipe the target partition window before appending
    -- so every run is safe to retry or re-trigger as a backfill.
    pre_hook = [
      """
      DELETE FROM {{ this }}
      WHERE partition_date >= {{ analytics_partition_lower_bound('nova') }}
        AND partition_date <= {{ analytics_airflow_ds() }}
        {% if var('nova_event_name', none) is not none %}
          AND event_name = '{{ var('nova_event_name') }}'
        {% endif %}
      """
    ],

    tags = ['source_data', 'nova', 'events']
  )
}}

{# --------------------------------------------------------------------------
   ods_nova_app_events

   Unified Operational Data Store for all Nova mobile app events.

   Design decisions
   ────────────────
   • All event types are unioned into a single wide table to simplify
     downstream joins — each event type is identified by event_name.
   • The payload SUPER column carries event-specific fields so the envelope
     schema never needs to change when new events are added.
   • v4 events (legacy schema) arrive from a separate landing table with a
     different column layout; they are mapped to the v5 envelope schema in
     the v4_events CTE and appended via the final UNION ALL.
   • var('nova_event_name') allows single-event reruns without touching
     other partitions — used by the Airflow partial-backfill task.

   Partition window
   ────────────────
   The pre_hook DELETE + this SELECT both use the same window expression
   [analytics_partition_lower_bound('nova'), analytics_airflow_ds()]
   so the model is fully idempotent: running it twice for the same date
   produces the same result.
   -------------------------------------------------------------------------- #}

WITH

-- ── v5 events: generated union across all registered event definitions ──────
v5_events AS (

  {% set event_defs = analytics_event_definitions() %}

  {# Optionally filter to a single event type for targeted reruns #}
  {% set target_event = var('nova_event_name', none) %}

  {% set ns = namespace(first_written=false) %}

  {% for event_def in event_defs %}
    {% if target_event is none or target_event == event_def.event_name %}

      {% if ns.first_written %}
      UNION ALL
      {% endif %}

      -- ── {{ event_def.event_name }} ──────────────────────────────────────
      SELECT
        event_name,
        event_guid,
        event_timestamp,
        event_local_timestamp,
        event_local_date,
        client_id,
        client_name,
        client_variant,
        client_version,
        client_build,
        client_platform,
        product_guid,
        firmware_version,
        payload,
        partition_date,
        '{{ var("analytics_schema_source") }}'         AS source_schema,
        '{{ event_def.table_name }}'                   AS source_table,
        5                                              AS record_version,
        GETDATE()                                      AS ingest_timestamp,
        client_mono_time,
        event_sequence_id,
        session_id,
        app_instance_id,
        user_id,
        COALESCE(is_test_event, FALSE)                 AS is_test_event
      FROM (
        {{ analytics_event_union(event_def) }}
      ) AS _unioned_{{ event_def.event_name | replace('.','_') }}

      {% set ns.first_written = true %}
    {% endif %}
  {% endfor %}

),

-- ── v4 events: legacy schema with different column names ────────────────────
-- v4 tables use underscore-prefixed GUID columns and do not carry session /
-- app_instance context.  They are mapped to the v5 envelope here so that
-- consumers never need to handle both schemas.
v4_events AS (

  SELECT
    event_type_name                                    AS event_name,
    event_uuid                                         AS event_guid,
    CAST(server_received_at AS TIMESTAMP)              AS event_timestamp,
    CAST(client_local_time  AS TIMESTAMP)              AS event_local_timestamp,
    CAST(client_local_date  AS DATE)                   AS event_local_date,
    app_user_id                                        AS client_id,
    app_name                                           AS client_name,
    app_variant                                        AS client_variant,
    app_version                                        AS client_version,
    app_build                                          AS client_build,
    device_platform                                    AS client_platform,
    NULL::VARCHAR                                      AS product_guid,
    NULL::VARCHAR                                      AS firmware_version,
    object(
      'raw_payload', raw_payload_json
    )                                                  AS payload,
    CAST(log_date AS DATE)                             AS partition_date,
    '{{ var("analytics_schema_source") }}_v4'          AS source_schema,
    'nova_v4_event_log'                                AS source_table,
    4                                                  AS record_version,
    GETDATE()                                          AS ingest_timestamp,
    NULL::BIGINT                                       AS client_mono_time,
    row_id                                             AS event_sequence_id,
    NULL::VARCHAR                                      AS session_id,
    NULL::VARCHAR                                      AS app_instance_id,
    app_user_id                                        AS user_id,
    COALESCE(is_internal, FALSE)                       AS is_test_event

  FROM {{ source('analytics_source_prod', 'nova_v4_event_log') }}

  WHERE
    log_date >= {{ analytics_partition_lower_bound('nova') }}
    AND log_date <= {{ analytics_airflow_ds() }}
    AND event_uuid IS NOT NULL
    {% if var('nova_event_name', none) is not none %}
      AND event_type_name = '{{ var('nova_event_name') }}'
    {% endif %}

)

-- ── Final union ─────────────────────────────────────────────────────────────
SELECT * FROM v5_events

UNION ALL

SELECT * FROM v4_events
