-- =============================================================================
-- DDL: dim_product
-- Layer   : DIM (Dimension)
-- Purpose : Product catalogue — one row per product SKU.
-- Refresh : Full DELETE + INSERT (SCD Type 1); see dml/dim_product_refresh.sql
--
-- DISTSTYLE ALL ensures every Redshift node has a local copy of this small
-- table, eliminating broadcast joins when DWD fact tables join on product_id.
-- =============================================================================

CREATE TABLE IF NOT EXISTS analytics_dw.dim_product
(
    product_id          VARCHAR(32)         NOT NULL,       -- Short opaque hex identifier
    product_guid        VARCHAR(64)         NOT NULL,       -- UUID v4
    product_name        VARCHAR(128)        NOT NULL,       -- Sanitised display name
    product_category    VARCHAR(64),                        -- e.g. "wireless_headphones"
    product_line        VARCHAR(64),                        -- e.g. "nova_audio"
    launch_date         DATE,
    is_active           BOOLEAN             NOT NULL DEFAULT TRUE,
    updated_at          TIMESTAMP           NOT NULL DEFAULT GETDATE()
)
DISTSTYLE ALL
SORTKEY (product_id);

COMMENT ON TABLE analytics_dw.dim_product IS
    'Product dimension. SCD Type 1 (full refresh). Replicated to all nodes via DISTSTYLE ALL.';
