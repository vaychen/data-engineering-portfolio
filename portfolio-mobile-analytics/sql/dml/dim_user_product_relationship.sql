-- =============================================================================
-- DML: dim_user_product_relationship.sql
-- Layer  : DIM (Dimension)
-- Table  : analytics_dw.dim_user_product_relationship
-- Pattern: Full DELETE + INSERT (latest-state snapshot refresh)
--
-- Approach:
--   dwd_user_active_daily carries product_id and product_name directly from
--   the ODS (ods_nova_app_events).  We join to dim_product on product_id to
--   resolve product_guid, then aggregate to keep one row per
--   (client_id, client_variant, product_name, product_guid) with the most
--   recent pairing date (MAX partition_date = last_active_dt).
--
--   This full-refresh approach is the v1 equivalent of the dbt temp-table
--   snapshot trick used in portfolio-dbt-analytics.  Both produce the same
--   grain and semantics; the dbt version avoids a full rebuild by carrying
--   forward the previous day's snapshot.
--
-- Jinja params
--   {{ ds }}                        -- Airflow logical date (YYYY-MM-DD)
--   {{ params.backfill_scan_date }} -- Informational; not used (full refresh)
-- =============================================================================

-- Step 1: Full refresh — wipe existing rows.
--         This dimension is small; a full rebuild is cheaper than row-level
--         merge and avoids stale pairings from users who changed products.
DELETE FROM analytics_dw.dim_user_product_relationship
WHERE 1 = 1;

-- Step 2: Rebuild from dwd_user_active_daily joined to dim_product.
INSERT INTO analytics_dw.dim_user_product_relationship
    (client_variant,
     client_id,
     product_name,
     product_guid,
     last_active_dt,
     pipeline_load_timestamp)

WITH

latest_pairings AS (
    SELECT
        uad.client_variant,
        uad.client_id,
        COALESCE(uad.product_name, 'Unknown')   AS product_name,
        dp.product_guid,
        MAX(uad.partition_date)                 AS last_active_dt

    FROM analytics_dw.dwd_user_active_daily AS uad

    -- Resolve product_guid from the product dimension using the SKU identifier.
    LEFT JOIN analytics_dw.dim_product AS dp
        ON  uad.product_id = dp.product_id

    WHERE
        uad.product_name IS NOT NULL
        AND TRIM(uad.product_name) <> ''

    GROUP BY
        uad.client_variant,
        uad.client_id,
        uad.product_name,
        dp.product_guid
)

SELECT
    client_variant,
    client_id,
    product_name,
    product_guid,
    last_active_dt,
    GETDATE()   AS pipeline_load_timestamp

FROM latest_pairings;
