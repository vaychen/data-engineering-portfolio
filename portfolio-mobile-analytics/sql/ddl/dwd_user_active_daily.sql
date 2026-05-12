-- =============================================================================
-- DDL: dwd_user_active_daily
-- Layer   : DWD (Data Warehouse Detail)
-- Purpose : One row per (partition_date, client_id, client_name,
--           client_variant, client_version) — daily active app user fact.
-- Refresh : Idempotent DELETE + INSERT; see dml/dwd_user_active_daily.sql
--
-- DISTKEY  partition_date — co-locates with other DWD tables for date joins.
-- SORTKEY  (partition_date, client_name, client_id) — supports per-app and
--          per-user range scans efficiently.
-- =============================================================================

CREATE TABLE IF NOT EXISTS analytics_dw.dwd_user_active_daily
(
    partition_date          DATE                NOT NULL,
    client_id               VARCHAR(64)         NOT NULL,
    client_name             VARCHAR(64)         NOT NULL,
    client_variant          VARCHAR(64)         NOT NULL DEFAULT '__none__',
    client_version          VARCHAR(32)         NOT NULL,
    client_os_name          VARCHAR(32),
    client_os_version       VARCHAR(32),
    product_id              VARCHAR(32),
    product_name            VARCHAR(128),

    first_event_ts          BIGINT,                         -- Epoch ms of first event
    last_event_ts           BIGINT,                         -- Epoch ms of last event
    first_event_mono        BIGINT,                         -- client_mono_time min (ms since app start)
    last_event_mono         BIGINT,                         -- client_mono_time max (ms since app start)
    event_count             BIGINT              NOT NULL DEFAULT 0,
    session_count           INTEGER             NOT NULL DEFAULT 0,

    pipeline_load_timestamp TIMESTAMP           NOT NULL DEFAULT GETDATE()
)
DISTKEY  (partition_date)
SORTKEY  (partition_date, client_name, client_id);

COMMENT ON TABLE analytics_dw.dwd_user_active_daily IS
    'Daily active app user fact. One row per (partition_date, client_id, client_name, '
    'client_variant, client_version). Idempotent DELETE + INSERT; safe for backfill.';
