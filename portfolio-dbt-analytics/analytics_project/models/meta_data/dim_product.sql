{{
  config(
    materialized = 'view',
    database     = var('analytics_database'),
    schema       = var('analytics_schema_metadata'),
    alias        = 'dim_product',
    tags         = ['meta_data', 'dimension']
  )
}}

{# --------------------------------------------------------------------------
   dim_product

   Static product dimension.  In production this view sits atop a dbt seed
   table (analytics_project/seeds/dim_product.csv) so product metadata is
   version-controlled alongside the pipeline code and updated via
   `dbt seed --select dim_product`.

    Columns
    ───────
    product_id        – surrogate integer key
    product_code      – business-facing SKU / product code
    product_guid      – UUID matching ods_nova_app_events.product_guid (join key)
    product_name      – human-readable product name
    product_category  – hardware-specific category (e.g. wireless_headphones, earbuds)
    launch_date       – commercial launch date (used in cohort date-range gates)
    is_active         – FALSE once a product reaches end-of-life (synced with v1)
    months_since_launch – derived; months from launch_date to today
    lifecycle_status  – derived; 'Active' or 'End of Life'
   -------------------------------------------------------------------------- #}

WITH source_products AS (

  -- Static values CTE mirrors the seed schema; swap to
  --   FROM {{ ref('dim_product_seed') }}
  -- once seeds are loaded.
  --
  -- product_guid values match the physical UUIDs used in ods_nova_app_events
  -- so that dwd_product_active_daily can join on this column.
  -- active / inactive flags and launch dates are kept in sync with the v1
  -- dim_product_refresh.sql in the portfolio-mobile-analytics project.
  SELECT
    1                                            AS product_id,
    'PRODUCT_A'                                  AS product_code,
    'Nova Alpha'                                 AS product_name,
    'wireless_headphones'                        AS product_category,
    CAST('2021-03-15' AS DATE)                   AS launch_date,
    TRUE                                         AS is_active,
    'f47ac10b-58cc-4372-a567-0e02b2c3d479'      AS product_guid
  UNION ALL
  SELECT
    2,
    'PRODUCT_B',
    'Nova Beta',
    'earbuds',
    CAST('2022-06-01' AS DATE),
    TRUE,
    '550e8400-e29b-41d4-a716-446655440001'
  UNION ALL
  SELECT
    3,
    'PRODUCT_C',
    'Nova Compact',
    'wireless_headphones',
    CAST('2023-01-10' AS DATE),
    TRUE,
    '6ba7b810-9dad-11d1-80b4-00c04fd430c9'
  UNION ALL
  SELECT
    4,
    'PRODUCT_D',
    'Nova Pro',
    'over_ear_headphones',
    CAST('2020-09-22' AS DATE),
    FALSE,                                       -- end-of-life; matches v1 dim_product_refresh
    '6ba7b811-9dad-11d1-80b4-00c04fd430ca'
  UNION ALL
  SELECT
    5,
    'PRODUCT_E',
    'Nova Legacy',
    'soundbar',
    CAST('2023-09-05' AS DATE),
    TRUE,
    '6ba7b812-9dad-11d1-80b4-00c04fd430cb'

),

final AS (

  SELECT
    product_id,
    product_code,
    product_guid,
    product_name,
    product_category,
    launch_date,
    is_active,
    -- Derived convenience columns
    DATEDIFF('month', launch_date, CURRENT_DATE) AS months_since_launch,
    CASE
      WHEN is_active THEN 'Active'
      ELSE 'End of Life'
    END                                          AS lifecycle_status
  FROM source_products

)

SELECT * FROM final
