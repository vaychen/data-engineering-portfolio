-- =============================================================================
-- DDL: dwd_product_active_daily
-- Layer   : DWD (Data Warehouse Detail)
-- Purpose : One row per (event_local_date, product_guid) — daily active device
--           fact derived from nova app events that carry product/sentinel context.
-- Refresh : Idempotent DELETE + INSERT; see dml/dwd_product_active_daily.sql
--
-- A row exists when a paired sentinel device emitted at least one event that
-- day.  firmware_version is resolved to the last version seen per device per
-- day, accommodating mid-day OTA updates via the LAST_VALUE() window pattern.
--
-- Note: In the v2 dbt migration (portfolio-dbt-analytics) this table is
--       re-implemented as the dwd_product_active_daily dbt model, sourcing
--       the same ods_nova_app_events with an identical firmware resolution
--       strategy.
--
-- Distribution strategy:
--   DISTKEY  partition_date — consistent with other DWD tables for date joins.
--   SORTKEY  (partition_date, product_guid) — efficient per-device range scans.
-- =============================================================================

CREATE TABLE IF NOT EXISTS analytics_dw.dwd_product_active_daily
(
    -- -------------------------------------------------------------------------
    -- Grain
    -- -------------------------------------------------------------------------
    event_local_date        DATE                NOT NULL,   -- Device-local calendar date
    product_guid            VARCHAR(64)         NOT NULL,   -- UUID v4 device identifier

    -- -------------------------------------------------------------------------
    -- Product metadata (carried from ODS for convenience)
    -- -------------------------------------------------------------------------
    product_id              VARCHAR(32),                    -- Short opaque SKU identifier
    product_name            VARCHAR(128),                   -- Human-readable product name
    firmware_version        VARCHAR(32),                    -- Last firmware version seen that day

    -- -------------------------------------------------------------------------
    -- App context
    -- -------------------------------------------------------------------------
    client_variant          VARCHAR(64)         NOT NULL DEFAULT '__none__',

    -- -------------------------------------------------------------------------
    -- Activity metrics
    -- -------------------------------------------------------------------------
    first_seen_ts           BIGINT,                         -- Epoch ms of first event (UTC)
    last_seen_ts            BIGINT,                         -- Epoch ms of last event (UTC)
    active_user_count       INTEGER             NOT NULL DEFAULT 0,  -- Distinct paired users
    event_count             INTEGER             NOT NULL DEFAULT 0,  -- Total device events

    -- -------------------------------------------------------------------------
    -- Pipeline metadata
    -- -------------------------------------------------------------------------
    partition_date          DATE                NOT NULL,
    pipeline_load_timestamp TIMESTAMP           NOT NULL DEFAULT GETDATE()
)
DISTKEY  (partition_date)
SORTKEY  (partition_date, product_guid);

COMMENT ON TABLE analytics_dw.dwd_product_active_daily IS
    'Daily active device/product fact. One row per (event_local_date, product_guid). '
    'firmware_version is the last known version seen that day (LAST_VALUE window). '
    'Idempotent DELETE + INSERT; safe for backfill.';
