-- =============================================================================
-- DDL: ods_nova_events_staging
-- Layer   : ODS staging (transient)
-- Purpose : Temporary landing table for nova app events loaded from Redshift
--           Spectrum / S3 before deduplication into ods_nova_app_events.
--           Truncated at the start of each pipeline run; never queried by
--           downstream layers directly.
--
-- Column layout mirrors the output of flatten_nova_record() in the telemetry
-- Lambda (see portfolio-telemetry-schema/schemas/data-domains/):
--
--   Envelope      : event_name, event_schema_version, event_guid,
--                   event_timestamp (mapped to epoch ms BIGINT)
--   ClientContext : client_name, client_variant, client_version, client_build,
--                   client_id, client_mono_time, client_is_background
--   Derived       : client_os_name, client_os_version
--                   (mapped from nova_device_os / nova_device_os_version,
--                    present only on nova_session_start rows)
--   ProductContext: product_name, product_id, product_variant, product_guid,
--                   product_firmware_version
--                   (present only on BT-connect and onboarding rows)
--   Session       : session_id  (← client_session_id)
--   Payload       : payload SUPER — all event-specific nova_* fields not
--                   individually mapped above
--   Partition     : partition_date  (← dt cast to DATE)
--
-- Distribution: EVEN — staging table has no stable join key; even distribution
--               avoids skew while rows are being inserted from Spectrum.
-- Sort key  : None — the table is always fully scanned then dropped each run.
-- =============================================================================

CREATE TABLE IF NOT EXISTS analytics_dw.ods_nova_events_staging
(
    -- -------------------------------------------------------------------------
    -- Event identity
    -- -------------------------------------------------------------------------
    event_name              VARCHAR(128)        NOT NULL,
    event_schema_version    VARCHAR(16)         NOT NULL,
    event_guid              VARCHAR(64)         NOT NULL,

    -- -------------------------------------------------------------------------
    -- Event timing
    -- -------------------------------------------------------------------------
    event_timestamp         BIGINT              NOT NULL,   -- Unix epoch milliseconds (UTC)
    event_local_date        DATE                NOT NULL,   -- Calendar date in UTC+8 (Asia/Shanghai) derived from event_timestamp
    event_received_at       TIMESTAMP           NOT NULL,   -- Pipeline load time (proxy for ingest time)

    -- -------------------------------------------------------------------------
    -- Client / app metadata  (ClientContext — prefix client_)
    -- -------------------------------------------------------------------------
    client_name             VARCHAR(64)         NOT NULL,   -- App identifier: nova-ios | nova-android | nova-ohos
    client_variant          VARCHAR(64),                    -- Release variant: dev | staging | prod
    client_version          VARCHAR(32)         NOT NULL,   -- Semantic version string
    client_build            VARCHAR(32)         NOT NULL,   -- Build identifier, e.g. "2024.01.15.1234"
    client_id               VARCHAR(64)         NOT NULL,   -- Anonymous device-level UUID
    client_mono_time        BIGINT,                         -- Milliseconds since app start (monotonic clock)
    client_is_background    BOOLEAN,                        -- TRUE when event fired while app in background

    -- -------------------------------------------------------------------------
    -- OS context  (derived from nova_session_start payload; NULL on other events)
    -- -------------------------------------------------------------------------
    client_os_name          VARCHAR(32),                    -- Android | iOS | HarmonyOS
    client_os_version       VARCHAR(32),                    -- OS version string, e.g. "17.4.1"

    -- -------------------------------------------------------------------------
    -- Product / device metadata  (ProductContext — prefix product_)
    -- Present only on BT-connect and onboarding events
    -- -------------------------------------------------------------------------
    product_name            VARCHAR(128),                   -- Human-readable product name
    product_id              VARCHAR(32),                    -- Hex product identifier, e.g. "0x1A2B"
    product_variant         INTEGER,                        -- Hardware variant integer (0–19)
    product_guid            VARCHAR(64),                    -- Globally unique device unit UUID
    product_firmware_version VARCHAR(32),                   -- Firmware semver string

    -- -------------------------------------------------------------------------
    -- Session
    -- -------------------------------------------------------------------------
    session_id              VARCHAR(64),                    -- Monotonic session counter (client_session_id)

    -- -------------------------------------------------------------------------
    -- Event-specific payload (all nova_* fields not individually mapped above)
    -- -------------------------------------------------------------------------
    payload                 SUPER,

    -- -------------------------------------------------------------------------
    -- Pipeline metadata
    -- -------------------------------------------------------------------------
    pipeline_load_timestamp TIMESTAMP           NOT NULL DEFAULT GETDATE(),

    -- -------------------------------------------------------------------------
    -- Partition column
    -- -------------------------------------------------------------------------
    partition_date          DATE                NOT NULL    -- YYYY-MM-DD; derived from Firehose dt partition
)
DISTSTYLE EVEN;

COMMENT ON TABLE analytics_dw.ods_nova_events_staging IS
    'Transient staging for nova app events. Truncated each pipeline run. '
    'Do not reference from downstream layers.';
