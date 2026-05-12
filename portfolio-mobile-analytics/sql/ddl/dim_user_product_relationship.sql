-- =============================================================================
-- DDL: dim_user_product_relationship
-- Layer   : DIM (Dimension)
-- Purpose : Rolling latest-pairing snapshot — one row per
--           (client_id, client_variant, product_name, product_guid).
--           Records the most recent date on which each user was seen
--           paired with each product.
-- Refresh : Full DELETE + INSERT (latest-state snapshot, no history retained);
--           see dml/dim_user_product_relationship.sql
--
-- In v1 this dimension is derived from dwd_user_active_daily, which carries
-- product_id and product_name directly from the ODS.  dim_product is joined
-- to resolve product_guid for cross-project consistency.
--
-- In the v2 dbt migration (portfolio-dbt-analytics) this table is
-- re-implemented as the dim_user_product_relationship dbt model, which uses a
-- temp-table snapshot trick to maintain rolling state without a full rebuild.
--
-- DISTSTYLE ALL ensures every Redshift node has a local copy so that joins
--   from DWD/DWS fact tables incur no network shuffle.
-- SORTKEY  (client_id, client_variant) — fast point lookups per user.
-- =============================================================================

CREATE TABLE IF NOT EXISTS analytics_dw.dim_user_product_relationship
(
    client_variant          VARCHAR(64)         NOT NULL DEFAULT '__none__',
    client_id               VARCHAR(64)         NOT NULL,
    product_name            VARCHAR(128),                   -- Resolved from dim_product
    product_guid            VARCHAR(64),                    -- UUID, matches ods_nova_app_events
    last_active_dt          DATE,                           -- Most recent pairing date
    pipeline_load_timestamp TIMESTAMP           NOT NULL DEFAULT GETDATE()
)
DISTSTYLE ALL
SORTKEY  (client_id, client_variant);

COMMENT ON TABLE analytics_dw.dim_user_product_relationship IS
    'Rolling latest-pairing dimension. One row per (client_id, client_variant, '
    'product_name, product_guid). Full-refresh daily; no SCD history retained.';
