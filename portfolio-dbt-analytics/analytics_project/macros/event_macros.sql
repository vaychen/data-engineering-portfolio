{#
  ============================================================================
  event_macros.sql — Event definition registry and SQL union generators.

  This file is the core of the analytics_pipeline source layer.  All Nova
  mobile app events land in separate Redshift Spectrum external tables (one
  table per event type, partitioned by day).  Column names in those tables
  reflect the output of flatten_nova_record() in the telemetry Lambda:

    Envelope (top level)          : event_name, event_guid, event_timestamp
    ClientContext  (prefix client_): client_name, client_variant,
                                     client_version, client_build, client_id,
                                     client_mono_time, client_is_background,
                                     client_session_id,
                                     client_time_since_last_event
    NovaMobilePayload (prefix nova_): nova_event_name, nova_sdk_version,
                                      nova_device_os, nova_oob_bt_connect, …
                                      (field set varies per event type;
                                       see DeviceSessionPayload / DevicePairingPayload /
                                       UserAuthPayload in portfolio-telemetry-schema)
    ProductContext (prefix product_): product_name, product_id,
                                      product_guid, product_firmware_version

  The macros here:
    1. Declare every tracked event in a single registry
       (analytics_event_definitions).
    2. Generate a normalised SELECT for one event table
       (analytics_event_select).
    3. Union dev + prod source tables for one event
       (analytics_event_union).
    4. Build the Redshift OBJECT() payload fragment
       (analytics_payload_object).

  Adding a new event requires only a one-line entry in
  analytics_event_definitions() — all SQL is generated automatically.
  ============================================================================
#}


{# --------------------------------------------------------------------------
   analytics_event_definitions()

   Returns a list of event definition dicts.  Each dict has:

     table_name      – physical Redshift Spectrum table name in the source
                       schema; matches the event_name value in the flat JSON
                       written by flatten_nova_record().
     event_name      – canonical event name stored in the ODS column.
     has_product_ctx – True when the event carries product context columns
                       (product_guid, product_firmware_version).
     ctx_columns     – extra payload column names (from the flatten output)
                       to include in the OBJECT() payload.

   Events are listed high-volume first for faster short-circuit evaluation
   in filtered queries.

   Event table → portfolio schema mapping
   ──────────────────────────────────────
   device_session_record     ← DeviceSessionPayload  (device_session.py)
   device_pairing_record     ← DevicePairingPayload  (device_pair.py)
   user_auth_record          ← UserAuthPayload        (user_auth.py)
   nova_bt_connect_attempt   ← (removed from portfolio schema)
   nova_bt_incomplete_result ← (removed from portfolio schema)
   nova_bt_error_result      ← (removed from portfolio schema)
   nova_login_attempt        ← (removed from portfolio schema)
   nova_login_incomplete     ← (removed from portfolio schema)
   nova_login_failure        ← (removed from portfolio schema)
   nova_onboarding_first_flow ← (removed from portfolio schema)
   nova_onboarding_last_flow  ← (removed from portfolio schema)

   Note: the registry below retains all 11 production event types because this
   dbt project reflects the real pipeline.  The portfolio-telemetry-schema
   Python schemas expose only the 3 canonical record types above.
   -------------------------------------------------------------------------- #}
{% macro analytics_event_definitions() %}
  {% set events = [

    {
      "table_name":      "nova_session_start",
      "event_name":      "nova_session_start",
      "has_product_ctx": false,
      "ctx_columns":     ["nova_sdk_version", "nova_device_os",
                          "nova_device_os_version", "nova_device_brand",
                          "nova_device_model"]
    },

    {
      "table_name":      "nova_bt_connect_attempt",
      "event_name":      "nova_bt_connect_attempt",
      "has_product_ctx": true,
      "ctx_columns":     []
    },

    {
      "table_name":      "nova_bt_connect_result",
      "event_name":      "nova_bt_connect_result",
      "has_product_ctx": true,
      "ctx_columns":     ["nova_oob_bt_connect"]
    },

    {
      "table_name":      "nova_bt_incomplete_result",
      "event_name":      "nova_bt_incomplete_result",
      "has_product_ctx": true,
      "ctx_columns":     ["nova_oob_bt_connect"]
    },

    {
      "table_name":      "nova_bt_error_result",
      "event_name":      "nova_bt_error_result",
      "has_product_ctx": true,
      "ctx_columns":     ["nova_oob_bt_connect", "nova_error_details"]
    },

    {
      "table_name":      "nova_login_attempt",
      "event_name":      "nova_login_attempt",
      "has_product_ctx": false,
      "ctx_columns":     ["nova_identity_provider"]
    },

    {
      "table_name":      "nova_login_incomplete",
      "event_name":      "nova_login_incomplete",
      "has_product_ctx": false,
      "ctx_columns":     ["nova_identity_provider", "nova_error_description"]
    },

    {
      "table_name":      "nova_login_failure",
      "event_name":      "nova_login_failure",
      "has_product_ctx": false,
      "ctx_columns":     ["nova_identity_provider", "nova_error_description",
                          "nova_error_detail"]
    },

    {
      "table_name":      "nova_login_successful",
      "event_name":      "nova_login_successful",
      "has_product_ctx": false,
      "ctx_columns":     ["nova_identity_provider", "nova_user_id"]
    },

    {
      "table_name":      "nova_onboarding_first_flow",
      "event_name":      "nova_onboarding_first_flow",
      "has_product_ctx": false,
      "ctx_columns":     ["nova_screen_name", "nova_screen_rank",
                          "nova_subtask_grouping", "nova_subtask_rank",
                          "nova_onboarding_duration"]
    },

    {
      "table_name":      "nova_onboarding_last_flow",
      "event_name":      "nova_onboarding_last_flow",
      "has_product_ctx": true,
      "ctx_columns":     ["nova_screen_name", "nova_screen_rank",
                          "nova_subtask_grouping", "nova_subtask_rank",
                          "nova_onboarding_duration"]
    }

  ] %}
  {{ return(events) }}
{% endmacro %}


{# --------------------------------------------------------------------------
   analytics_payload_object(ctx_columns)

   Builds a Redshift OBJECT() expression from a list of column names:
       object('col1', col1, 'col2', col2, ...)

   Returns object() when ctx_columns is empty so that callers can
   still reference a payload column (it will be an empty SUPER object).
   -------------------------------------------------------------------------- #}
{% macro analytics_payload_object(ctx_columns) %}
  {%- if ctx_columns | length > 0 -%}
    object(
      {%- for col in ctx_columns %}
        '{{ col }}', {{ col }}{% if not loop.last %},{% endif %}
      {%- endfor %}
    )
  {%- else -%}
    object()
  {%- endif -%}
{% endmacro %}


{# --------------------------------------------------------------------------
   analytics_event_select(source_name, table_name, event_slug,
                          ctx_columns, has_product_ctx)

   Generates a single SELECT statement that reads from one raw event table
   and normalises it to the standard ODS schema.

   Column name conventions in source tables
   ─────────────────────────────────────────
   Source tables expose the flat JSON keys written by flatten_nova_record():
      • Envelope top-level : event_name, event_guid, event_timestamp
      • ClientContext       : client_name, client_variant, client_version,
                              client_build, client_id, client_mono_time,
                              client_is_background, client_session_id
      • ProductContext      : product_guid, product_firmware_version
                              (present only when has_product_ctx = true)
      • Event payload       : nova_<field> columns (varies per event type)
      • Partition key       : dt  (Hive-style YYYY-MM-DD from Firehose)

   Standard envelope columns emitted
   ──────────────────────────────────
     event_name, event_guid,
      event_timestamp      ← CAST to TIMESTAMP (UTC)
      event_local_timestamp ← event_timestamp shifted to UTC+8 (Asia/Shanghai)
      event_local_date     ← DATE derived from event_local_timestamp (UTC+8)
     client_id, client_name, client_variant, client_version,
     client_build,        ← from ClientContext.build
     client_platform,     ← derived from client_name (nova-ios → iOS, …)
     client_mono_time,
     product_guid         ← NULL when has_product_ctx = false
     firmware_version     ← product_firmware_version aliased; NULL when no ctx
     payload              ← SUPER OBJECT of ctx_columns
     partition_date,
     session_id           ← client_session_id aliased
     user_id              ← NULL for all v5 events (attribution in a separate layer)
     app_instance_id      ← NULL (not captured in v2 schema)
     event_sequence_id    ← NULL (not captured in v2 schema)
     is_test_event        ← TRUE when client_variant = 'dev'
     source_schema, source_table, record_version, ingest_timestamp

   Arguments
   ─────────
     source_name     – dbt source name as declared in sources.yml
     table_name      – physical table name within the source schema
     event_slug      – event_name string to hard-code into the output
     ctx_columns     – list of extra payload columns (forwarded to
                       analytics_payload_object)
     has_product_ctx – when True, pulls product_guid and
                       product_firmware_version; otherwise emits NULL
   -------------------------------------------------------------------------- #}
{% macro analytics_event_select(source_name, table_name, event_slug,
                                ctx_columns, has_product_ctx) %}

  SELECT
    -- ── Event envelope ───────────────────────────────────────────────────────
    '{{ event_slug }}'                                         AS event_name,
    event_guid                                                 AS event_guid,
    CAST(event_timestamp AS TIMESTAMP)                         AS event_timestamp,

    -- event_local_timestamp / event_local_date: shift the UTC envelope
    -- timestamp to UTC+8 (Asia/Shanghai) — the primary market timezone.
    -- ClientContext v2 does not carry a device-side tz offset, so UTC+8
    -- is applied uniformly as a business convention.
    CONVERT_TIMEZONE('UTC', 'Asia/Shanghai',
      CAST(event_timestamp AS TIMESTAMPTZ))                        AS event_local_timestamp,

    CAST(
      CONVERT_TIMEZONE('UTC', 'Asia/Shanghai',
        CAST(event_timestamp AS TIMESTAMPTZ))
    AS DATE)                                                       AS event_local_date,

    -- ── Client / app context ─────────────────────────────────────────────────
    -- Column names match flatten_nova_record() output with prefix "client_".
    client_id                                                  AS client_id,
    client_name                                                AS client_name,
    client_variant                                             AS client_variant,
    client_version                                             AS client_version,
    client_build                                               AS client_build,

    -- client_platform is derived from client_name (nova-ios / nova-android /
    -- nova-ohos) because the raw ClientContext carries a full app identifier,
    -- not a bare OS platform string.
    CASE client_name
      WHEN 'nova-ios'     THEN 'iOS'
      WHEN 'nova-android' THEN 'Android'
      WHEN 'nova-ohos'    THEN 'HarmonyOS'
      ELSE NULL
    END                                                        AS client_platform,

    client_mono_time                                           AS client_mono_time,

    -- ── Product / device context ─────────────────────────────────────────────
    {% if has_product_ctx %}
    product_guid                                               AS product_guid,
    product_firmware_version                                   AS firmware_version,
    {% else %}
    NULL::VARCHAR                                              AS product_guid,
    NULL::VARCHAR                                              AS firmware_version,
    {% endif %}

    -- ── Event-specific payload ───────────────────────────────────────────────
    {{ analytics_payload_object(ctx_columns) }}                AS payload,

    -- ── Partition key ────────────────────────────────────────────────────────
    -- dt is the Hive-style partition column injected by Firehose (YYYY-MM-DD).
    -- Cast to DATE to match the ODS partition_date column type.
    CAST(dt AS DATE)                                           AS partition_date,

    -- ── Session ──────────────────────────────────────────────────────────────
    client_session_id                                          AS session_id,

    -- ── Fields not captured in v2 envelope (populated from v4 legacy data) ──
    -- user_id: login-time user attribution is handled in a separate layer.
    NULL::VARCHAR                                              AS user_id,
    -- app_instance_id: not present in v2 ClientContext schema.
    NULL::VARCHAR                                              AS app_instance_id,
    -- event_sequence_id: row-level sequence not available in v2.
    NULL::BIGINT                                               AS event_sequence_id,

    -- ── Quality flag ─────────────────────────────────────────────────────────
    -- Mark dev-variant events as test events so they can be excluded from
    -- user-facing metrics without dropping the rows entirely.
    CASE WHEN client_variant = 'dev' THEN TRUE ELSE FALSE END  AS is_test_event,

    -- ── Pipeline metadata ────────────────────────────────────────────────────
    '{{ var("analytics_schema_source") }}'                     AS source_schema,
    '{{ table_name }}'                                         AS source_table,
    5                                                          AS record_version,
    GETDATE()                                                  AS ingest_timestamp

  FROM {{ source(source_name, table_name) }}

  WHERE
    dt >= {{ analytics_partition_lower_bound(event_slug) }}
    AND dt <= {{ analytics_airflow_ds() }}
    AND event_guid IS NOT NULL

{% endmacro %}


{# --------------------------------------------------------------------------
   analytics_event_union(event_definition)

   For a single event definition dict (from analytics_event_definitions()),
   generates a UNION ALL between:
     • The production source  (analytics_source_prod schema)
     • The development source (analytics_source_dev schema)

   Both legs use analytics_event_select() so the column list is always
   identical.  The dev leg is excluded in production runs by checking the
   dbt target name — only included when target.name in ('dev', 'ci').

   Usage (in ods_nova_app_events.sql):
       {% for event_def in analytics_event_definitions() %}
         {{ analytics_event_union(event_def) }}
         {% if not loop.last %} UNION ALL {% endif %}
       {% endfor %}
   -------------------------------------------------------------------------- #}
{% macro analytics_event_union(event_definition) %}

  {%- set tbl      = event_definition.table_name -%}
  {%- set slug     = event_definition.event_name -%}
  {%- set ctx_cols = event_definition.ctx_columns -%}
  {%- set has_prd  = event_definition.has_product_ctx -%}

  -- ── Production events ─────────────────────────────────────────────────────
  {{ analytics_event_select(
      source_name     = "analytics_source_prod",
      table_name      = tbl,
      event_slug      = slug,
      ctx_columns     = ctx_cols,
      has_product_ctx = has_prd
  ) }}

  {% if target.name in ('dev', 'ci') %}
  UNION ALL

  -- ── Development events (dev / CI only) ────────────────────────────────────
  {{ analytics_event_select(
      source_name     = "analytics_source_dev",
      table_name      = tbl,
      event_slug      = slug,
      ctx_columns     = ctx_cols,
      has_product_ctx = has_prd
  ) }}
  {% endif %}

{% endmacro %}
