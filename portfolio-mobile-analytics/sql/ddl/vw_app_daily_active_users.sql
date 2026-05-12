-- =============================================================================
-- DDL: vw_app_daily_active_users
-- Layer   : ADS (Application Data Store) — Redshift BI view
-- Purpose : BI-facing view combining daily active users, product metadata,
--           and a cumulative accumulated-users metric.
--           Consumed directly by Power BI / QuickSight.
-- Created : Once at schema setup; no daily Airflow task required.
--
-- Upstream dependencies (must exist before creating this view):
--   analytics_dw.dwd_user_active_daily
--   analytics_dw.dwd_product_active_daily
--   analytics_dw.dim_user_product_relationship
--   analytics_dw.dim_product
--
-- Columns
--   report_date           – partition_date from dwd_user_active_daily (UTC)
--   product_name          – resolved via dim_user_product_relationship
--   product_category      – from dim_product (e.g. wireless_headphones, earbuds)
--   lifecycle_status      – 'Active' or 'End of Life' (derived from is_active)
--   client_variant        – app A/B variant tag
--   dau_count             – distinct active client_ids on report_date
--   new_users             – client_ids whose first-ever active date = report_date
--   new_products_active   – distinct product_guids active on report_date
--   accumulated_users     – cumulative distinct users ever active up to report_date
--
-- Design notes
--   • This is a VIEW (no storage cost).  For high-cardinality date ranges,
--     materialise as an incremental table if query latency becomes an issue.
--   • accumulated_users uses window SUM over new_users because Redshift does
--     not support COUNT(DISTINCT) inside window functions.
--   • report_date uses partition_date (server ingestion date) for v1 alignment.
--     In the v2 dbt migration (vw_app_daily_active_users model) this becomes
--     event_local_date (device-reported local date).
--
-- v2 equivalent: analytics_project/models/report_data/vw_app_daily_active_users.sql
--   in portfolio-dbt-analytics.
-- =============================================================================

CREATE OR REPLACE VIEW analytics_dw.vw_app_daily_active_users AS

WITH

-- ── Join user-daily facts with product relationship dimension ─────────────────
user_daily AS (

    SELECT
        u.partition_date                        AS report_date,
        COALESCE(upr.product_name, 'Unknown')   AS product_name,
        u.client_variant,
        u.client_id
    FROM analytics_dw.dwd_user_active_daily AS u
    LEFT JOIN analytics_dw.dim_user_product_relationship AS upr
        ON  u.client_id      = upr.client_id
        AND u.client_variant = upr.client_variant

),

-- ── Product dimension metadata ───────────────────────────────────────────────
product_meta AS (

    SELECT
        product_name,
        product_category,
        CASE
            WHEN is_active THEN 'Active'
            ELSE 'End of Life'
        END AS lifecycle_status
    FROM analytics_dw.dim_product

),

-- ── Count distinct active devices per (partition_date, client_variant) ────────
product_daily AS (

    SELECT
        partition_date                  AS report_date,
        client_variant,
        COUNT(DISTINCT product_guid)    AS new_products_active
    FROM analytics_dw.dwd_product_active_daily
    GROUP BY
        partition_date,
        client_variant

),

-- ── Daily active user aggregation ────────────────────────────────────────────
dau_base AS (

    SELECT
        report_date,
        product_name,
        client_variant,
        COUNT(DISTINCT client_id)   AS dau_count
    FROM user_daily
    GROUP BY
        report_date,
        product_name,
        client_variant

),

-- ── First-active date per (client_id, product, variant) ──────────────────────
-- Used as the denominator for the accumulated_users window calculation.
client_first_active AS (

    SELECT
        product_name,
        client_variant,
        client_id,
        MIN(report_date)    AS first_active_date
    FROM user_daily
    GROUP BY
        product_name,
        client_variant,
        client_id

),

-- ── Count of new (first-ever) users per (date, product, variant) ─────────────
new_users_daily AS (

    SELECT
        first_active_date       AS report_date,
        product_name,
        client_variant,
        COUNT(DISTINCT client_id)   AS new_users
    FROM client_first_active
    GROUP BY
        first_active_date,
        product_name,
        client_variant

),

-- ── Cumulative unique users (window SUM over daily new_user counts) ───────────
accumulated AS (

    SELECT
        report_date,
        product_name,
        client_variant,
        new_users,
        SUM(new_users) OVER (
            PARTITION BY product_name, client_variant
            ORDER BY report_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )   AS accumulated_users
    FROM new_users_daily

)

SELECT
    d.report_date,
    d.product_name,
    COALESCE(pm.product_category,   'Unknown')  AS product_category,
    COALESCE(pm.lifecycle_status,   'Unknown')  AS lifecycle_status,
    d.client_variant,
    d.dau_count,
    COALESCE(nu.new_users,          0)          AS new_users,
    COALESCE(pd.new_products_active, 0)         AS new_products_active,
    COALESCE(acc.accumulated_users,  0)         AS accumulated_users

FROM dau_base               AS d
LEFT JOIN product_meta      AS pm
    ON  d.product_name   = pm.product_name
LEFT JOIN new_users_daily   AS nu
    ON  d.report_date    = nu.report_date
    AND d.product_name   = nu.product_name
    AND d.client_variant = nu.client_variant
LEFT JOIN product_daily     AS pd
    ON  d.report_date    = pd.report_date
    AND d.client_variant = pd.client_variant
LEFT JOIN accumulated       AS acc
    ON  d.report_date    = acc.report_date
    AND d.product_name   = acc.product_name
    AND d.client_variant = acc.client_variant

ORDER BY
    d.report_date DESC,
    d.product_name,
    d.client_variant;
