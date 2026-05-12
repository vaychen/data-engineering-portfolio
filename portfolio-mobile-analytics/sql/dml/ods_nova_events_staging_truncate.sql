-- =============================================================================
-- DML: ods_nova_events_staging_truncate.sql
-- Layer  : ODS staging
-- Table  : analytics_dw.ods_nova_events_staging
-- Pattern: Full truncate at the start of the nova source chain.
--
-- This runs as the first task in the load_nova_events TaskGroup.
-- Ensures the staging table is empty before Spectrum rows are inserted,
-- preventing duplicate accumulation across retried or restarted DAG runs.
--
-- Jinja params
--   {{ ds }}                        -- Airflow logical date (informational; logged)
--   {{ params.backfill_scan_date }} -- Informational; not used in this statement
-- =============================================================================

TRUNCATE TABLE analytics_dw.ods_nova_events_staging;
