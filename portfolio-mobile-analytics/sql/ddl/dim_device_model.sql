-- sql/ddl/dim_device_model.sql
--
-- Mobile client device model dimension.
-- Tracks the hardware models (e.g. iPhone 14 Pro, Samsung Galaxy S23) seen
-- in nova app events, derived from the client_os_name / firmware metadata
-- fields carried in ods_nova_app_events.
-- Full-refresh nightly via dim_device_model_refresh.sql.

CREATE TABLE IF NOT EXISTS analytics_dw.dim_device_model (
    device_model_key    INTEGER       NOT NULL ENCODE az64,   -- surrogate key (IDENTITY)
    device_model_name   VARCHAR(128)  NOT NULL ENCODE zstd,   -- e.g. 'iPhone 14 Pro'
    manufacturer        VARCHAR(64)   NOT NULL ENCODE zstd,   -- e.g. 'Apple', 'Samsung'
    platform            VARCHAR(16)   NOT NULL ENCODE zstd,   -- 'iOS' | 'Android'
    os_version_min      VARCHAR(16)       NULL ENCODE zstd,   -- earliest OS version seen
    os_version_max      VARCHAR(16)       NULL ENCODE zstd,   -- latest OS version seen
    first_seen_date     DATE          NOT NULL ENCODE az64,
    last_seen_date      DATE          NOT NULL ENCODE az64,
    is_active           BOOLEAN       NOT NULL ENCODE raw DEFAULT TRUE
)
DISTSTYLE ALL
SORTKEY (platform, manufacturer, device_model_name);
