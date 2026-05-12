{{
  config(
    materialized = 'view',
    database     = var('analytics_database'),
    schema       = var('analytics_schema_report'),
    alias        = 'vw_app_daily_active_users',
    tags         = ['report_data', 'dau', 'bi']
  )
}}

{# --------------------------------------------------------------------------
   vw_app_daily_active_users

   BI-facing view providing daily active user counts enriched with product
   metadata.  Designed for direct connection by Tableau / QuickSight / Superset.

   Columns
   ───────
   report_date         – calendar date (client local date)
   product_name        – human-readable product name (from dim_product)
   product_category    – product grouping (from dim_product)
   lifecycle_status    – "Active" / "End of Life" (from dim_product)
   client_variant      – app variant / build flavour
   dau_count           – distinct active users on this date
   new_products_active – distinct products active on this date
   accumulated_users   – running total of ever-active users up to report_date
                         (cumulative unique client_ids, product-scoped)

   Design notes
   ────────────
   • This is a VIEW (no storage cost) because it joins two small fact tables.
     For high-cardinality date ranges, promote to incremental if query latency
     becomes an issue.
   • accumulated_users is computed with a window SUM of COUNT(DISTINCT …).
     Redshift does not support COUNT(DISTINCT) inside window functions, so
     the accumulated count uses a dense-rank trick to assign a sequence number
     to each client's first-active date per product, then sums the "new user"
     flags cumulatively.
   -------------------------------------------------------------------------- #}

WITH

-- ── Daily active users per product ─────────────────────────────────────────
user_daily AS (

  SELECT
    uad.event_local_date                                 AS report_date,
    upr.product_name,
    uad.client_variant,
    uad.client_id,
    uad.partition_date
  FROM {{ ref('dwd_user_active_daily') }}          AS uad
  LEFT JOIN {{ ref('dim_user_product_relationship') }} AS upr
    ON  uad.client_id      = upr.client_id
    AND uad.client_variant = upr.client_variant

),

-- ── Product metadata ────────────────────────────────────────────────────────
product_meta AS (

  SELECT
    product_name,
    product_category,
    lifecycle_status
  FROM {{ ref('dim_product') }}

),

-- ── Daily product activity ──────────────────────────────────────────────────
product_daily AS (

  SELECT
    event_local_date                                     AS report_date,
    client_variant,
    COUNT(DISTINCT product_guid)                         AS new_products_active
  FROM {{ ref('dwd_product_active_daily') }}
  GROUP BY
    event_local_date,
    client_variant

),

-- ── DAU aggregation ─────────────────────────────────────────────────────────
dau_base AS (

  SELECT
    ud.report_date,
    COALESCE(ud.product_name, 'Unknown')                 AS product_name,
    ud.client_variant,
    COUNT(DISTINCT ud.client_id)                         AS dau_count
  FROM user_daily AS ud
  GROUP BY
    ud.report_date,
    ud.product_name,
    ud.client_variant

),

-- ── First-active date per client per product (for accumulated users) ─────────
client_first_active AS (

  SELECT
    COALESCE(product_name, 'Unknown')                    AS product_name,
    client_variant,
    client_id,
    MIN(report_date)                                     AS first_active_date
  FROM user_daily
  GROUP BY
    product_name,
    client_variant,
    client_id

),

-- ── Count of "new" unique users per (date, product, variant) ────────────────
new_users_daily AS (

  SELECT
    first_active_date                                    AS report_date,
    product_name,
    client_variant,
    COUNT(DISTINCT client_id)                            AS new_users
  FROM client_first_active
  GROUP BY
    first_active_date,
    product_name,
    client_variant

),

-- ── Cumulative unique users (window SUM over new_users) ─────────────────────
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
    )                                                    AS accumulated_users
  FROM new_users_daily

),

-- ── Final join ──────────────────────────────────────────────────────────────
final AS (

  SELECT
    d.report_date,
    d.product_name,
    COALESCE(pm.product_category, 'Unknown')             AS product_category,
    COALESCE(pm.lifecycle_status, 'Unknown')             AS lifecycle_status,
    d.client_variant,
    d.dau_count,
    COALESCE(nu.new_users, 0)                            AS new_users,
    COALESCE(pd.new_products_active, 0)                  AS new_products_active,
    COALESCE(acc.accumulated_users, 0)                   AS accumulated_users
  FROM dau_base              AS d
  LEFT JOIN product_meta     AS pm  ON d.product_name   = pm.product_name
  LEFT JOIN new_users_daily  AS nu  ON d.report_date    = nu.report_date
                                    AND d.product_name  = nu.product_name
                                    AND d.client_variant = nu.client_variant
  LEFT JOIN product_daily    AS pd  ON d.report_date    = pd.report_date
                                    AND d.client_variant = pd.client_variant
  LEFT JOIN accumulated      AS acc ON d.report_date    = acc.report_date
                                    AND d.product_name  = acc.product_name
                                    AND d.client_variant = acc.client_variant

)

SELECT
  report_date,
  product_name,
  product_category,
  lifecycle_status,
  client_variant,
  dau_count,
  new_users,
  new_products_active,
  accumulated_users
FROM final
ORDER BY
  report_date DESC,
  product_name,
  client_variant
