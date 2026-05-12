-- sql/dml/ods_nova_events_load.sql
--
-- Load nova mobile app events from the S3-backed Redshift Spectrum external
-- table into the internal ODS staging table.
--
-- Source  : analytics_source.nova_app_events
--           Redshift Spectrum external table over the Firehose delivery bucket.
--           Partitioned by dt=YYYY-MM-DD (Hive-style, injected by Firehose).
--
--           Column names reflect the output of flatten_nova_record() in the
--           telemetry Lambda (see schemas/data-domains/ for per-event schemas):
--
--             Envelope      (top level)      : event_name, event_schema_version,
--                                              event_guid, event_timestamp
--             ClientContext (prefix client_) : client_name, client_variant,
--                                              client_version, client_build,
--                                              client_id, client_mono_time,
--                                              client_is_background,
--                                              client_session_id
--             NovaMobilePayload (prefix nova_): nova_event_name,
--                                              nova_device_os, nova_device_os_version,
--                                              nova_sdk_version, nova_device_brand,
--                                              nova_device_model,
--                                              nova_oob_bt_connect, nova_error_details,
--                                              nova_identity_provider, ...
--             ProductContext (prefix product_): product_name, product_id,
--                                              product_variant, product_guid,
--                                              product_firmware_version
--             Partition key                  : dt
--
--           Event-specific payload fields are present only for the event types
--           that carry them; Spectrum returns NULL for absent columns in records
--           of other event types.
--
-- Target  : analytics_dw.ods_nova_events_staging
--           Transient landing table; truncated before this INSERT runs
--           (ods_nova_events_staging_truncate.sql).  A subsequent dedupe step
--           (ods_nova_events_dedupe.sql) promotes unique rows into
--           ods_nova_app_events.
--
-- Parameters
-- ----------
-- {{ params.backfill_scan_date }}  : integer — number of partition days to scan
--                                    (1 = yesterday only, 7 = 7-day window)
-- =============================================================================

INSERT INTO analytics_dw.ods_nova_events_staging
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
SELECT

    -- ── Envelope fields ──────────────────────────────────────────────────────
    event_name,
    CAST(event_schema_version   AS VARCHAR(16))                     AS event_schema_version,
    event_guid,

    -- event_timestamp is an ISO-8601 AwareDatetime string in the JSON record.
    -- Convert to Unix epoch milliseconds (BIGINT) for downstream arithmetic.
    (EXTRACT(EPOCH FROM CAST(event_timestamp AS TIMESTAMPTZ))::BIGINT) * 1000
                                                                    AS event_timestamp,

    -- Derive the local calendar date in UTC+8 (Asia/Shanghai).
    -- All nova events originate from a UTC+8 primary market; bucketing by
    -- UTC+8 date aligns metrics with the business day experienced by the user.
    CAST(
        CONVERT_TIMEZONE('UTC', 'Asia/Shanghai', CAST(event_timestamp AS TIMESTAMPTZ))
    AS DATE)                                                        AS event_local_date,

    -- event_received_at: pipeline load time used as a proxy for ingest time
    -- (Firehose does not embed an arrival timestamp in the flat JSON payload).
    SYSDATE                                                         AS event_received_at,

    -- ── ClientContext (flatten_nova_record prefix: "client_") ────────────────
    client_name                                                     AS client_name,
    client_variant                                                  AS client_variant,
    client_version                                                  AS client_version,
    client_build                                                    AS client_build,
    CAST(client_id              AS VARCHAR(64))                     AS client_id,
    client_mono_time                                                AS client_mono_time,
    client_is_background                                            AS client_is_background,

    -- ── OS context (nova_session_start only; NULL for all other event types) ─
    -- nova_device_os and nova_device_os_version are promoted to named ODS
    -- columns so downstream DWD queries can filter by OS without parsing the
    -- payload SUPER.
    nova_device_os                                                  AS client_os_name,
    nova_device_os_version                                          AS client_os_version,

    -- ── ProductContext (flatten prefix: "product_") ───────────────────────────
    -- Present only on BT-connect and onboarding events; NULL for login/session.
    product_name                                                    AS product_name,
    product_id                                                      AS product_id,
    product_variant                                                 AS product_variant,
    CAST(product_guid           AS VARCHAR(64))                     AS product_guid,
    product_firmware_version                                        AS product_firmware_version,

    -- ── Session ──────────────────────────────────────────────────────────────
    CAST(client_session_id      AS VARCHAR(64))                     AS session_id,

    -- ── Event-specific payload (all nova_* fields not individually mapped) ───
    -- Captures every event-specific column in a single SUPER value so the
    -- core ODS schema never needs to change when new event types are added.
    -- Columns not present for a given event type are NULL in the SUPER object.
    object(
        'nova_sdk_version',            nova_sdk_version,
        'nova_device_brand',           nova_device_brand,
        'nova_device_model',           nova_device_model,
        'nova_oob_bt_connect',         nova_oob_bt_connect,
        'nova_error_details',          nova_error_details,
        'nova_identity_provider',      nova_identity_provider,
        'nova_error_description',      nova_error_description,
        'nova_error_detail',           nova_error_detail,
        'nova_user_id',                nova_user_id,
        'nova_user_metadata',          nova_user_metadata,
        'nova_screen_name',            nova_screen_name,
        'nova_screen_rank',            nova_screen_rank,
        'nova_subtask_grouping',       nova_subtask_grouping,
        'nova_subtask_rank',           nova_subtask_rank,
        'nova_onboarding_duration',    nova_onboarding_duration,
        'nova_client_timestamp_enter', nova_client_timestamp_enter,
        'nova_client_timestamp_exit',  nova_client_timestamp_exit
    )                                                               AS payload,

    CAST(dt AS DATE)                                                AS partition_date

FROM analytics_source.nova_app_events
WHERE
    dt BETWEEN
        TO_CHAR(
            DATEADD(day, -{{ params.backfill_scan_date }}, CURRENT_DATE),
            'YYYY-MM-DD'
        )
        AND TO_CHAR(DATEADD(day, -1, CURRENT_DATE), 'YYYY-MM-DD')
    AND event_guid IS NOT NULL
    AND TRIM(event_guid) <> '';
