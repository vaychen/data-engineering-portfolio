-- =============================================================================
-- DDL: dws_user_retention
-- Layer   : DWS (Data Warehouse Summary)
-- Purpose : Daily user retention aggregation — one row per
--           (cohort_date, partition_date, client_name, product_id).
--           Supports D1/D7/D30 retention reporting in the ADS layer.
-- Refresh : Idempotent DELETE + INSERT; see dml/dws_user_retention.sql
--
-- DISTKEY  cohort_date — retention queries always filter and group on cohort.
-- SORTKEY  (cohort_date, partition_date, client_name)
-- =============================================================================

CREATE TABLE IF NOT EXISTS analytics_dw.dws_user_retention
(
    cohort_date             DATE                NOT NULL,   -- Date of first activity (D0)
    partition_date          DATE                NOT NULL,   -- Date of this retention measurement
    days_since_cohort       INTEGER             NOT NULL,   -- partition_date - cohort_date
    client_name             VARCHAR(64)         NOT NULL,
    product_id              VARCHAR(32),

    cohort_size             INTEGER             NOT NULL DEFAULT 0,   -- Users active on cohort_date
    retained_users          INTEGER             NOT NULL DEFAULT 0,   -- Users active on partition_date
    retention_rate          DECIMAL(7, 4),                            -- retained / cohort_size

    pipeline_load_timestamp TIMESTAMP           NOT NULL DEFAULT GETDATE()
)
DISTKEY  (cohort_date)
SORTKEY  (cohort_date, partition_date, client_name);

COMMENT ON TABLE analytics_dw.dws_user_retention IS
    'Daily user retention summary. One row per (cohort_date, partition_date, '
    'client_name, product_id). Supports D1/D7/D30 retention curves.';
