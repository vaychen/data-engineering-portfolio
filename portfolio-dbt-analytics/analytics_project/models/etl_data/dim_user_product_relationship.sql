{{
  config(
    materialized         = 'incremental',
    incremental_strategy = 'append',
    full_refresh         = false,
    database             = var('analytics_database'),
    schema               = var('analytics_schema_etl'),
    alias                = 'dim_user_product_relationship',
    on_schema_change     = 'sync_all_columns',

    column_types = {
      "client_variant":    "VARCHAR(64)",
      "client_id":         "VARCHAR(64)",
      "product_name":      "VARCHAR(120)",
      "product_guid":      "VARCHAR(36)",
      "last_active_dt":    "DATE",
      "airflow_ds":        "DATE",
      "ingest_timestamp":  "TIMESTAMP"
    },

    -- ── Temp-table snapshot trick ──────────────────────────────────────────
    -- Redshift Serverless has no native incremental snapshot / SCD support
    -- in dbt.  We implement a rolling "latest state" dimension by:
    --   1. (pre_hook step 1)  Copy the current table into a temp table.
    --   2. (pre_hook step 2)  Delete today's rows from the live table
    --                         (we are about to re-derive them).
    --   3. (model body)       UNION today's fresh data with the temp snapshot.
    --   4. (post_hook)        Purge historical rows from the live table that
    --                         pre-date today's airflow_ds — the temp table
    --                         served as the bridge; we no longer need them.
    --
    -- Net result: the live table always holds exactly ONE row per
    -- (client_variant, client_id, product_name, product_guid) — the most
    -- recent pairing — with no external state store required.
    pre_hook = [
      "CREATE TEMP TABLE dim_upr_snapshot AS SELECT * FROM {{ this }}",
      """
      DELETE FROM {{ this }}
      WHERE airflow_ds = {{ analytics_airflow_ds() }}
      """
    ],

    post_hook = [
      """
      DELETE FROM {{ this }}
      WHERE airflow_ds < {{ analytics_airflow_ds() }}
      """
    ],

    tags = ['etl_data', 'dimension', 'user_product']
  )
}}

{# --------------------------------------------------------------------------
   dim_user_product_relationship

   Rolling latest-pairing dimension: for every (client_variant, client_id,
   product_name, product_guid) combination, stores the most recent date on
   which that user was seen paired with that product.

   This enables downstream queries like:
     "How many unique users are currently paired with Product A?"
   without a full historical scan.

   Refresh cadence
   ───────────────
   Runs daily after dwd_user_active_daily and dwd_product_active_daily.
   Each run:
     • Derives today's active pairings from the two upstream fact tables.
     • Merges them with the snapshot from the previous run.
     • The post_hook prunes the now-redundant previous-day rows so the table
       stays small (single-row-per-pairing, not append-only historical).
   -------------------------------------------------------------------------- #}

WITH

-- ── Today's active pairings ─────────────────────────────────────────────────
-- Join the two daily-active tables to link each client to the products they
-- interacted with today.  product_guid is the join key; product_name is
-- resolved via dim_product for a stable display name.
today_pairings AS (

  SELECT
    u.client_variant,
    u.client_id,
    COALESCE(p.product_name, 'Unknown')                 AS product_name,
    d.product_guid,
    u.event_local_date                                  AS last_active_dt

  FROM {{ ref('dwd_user_active_daily') }}        AS u
  INNER JOIN {{ ref('dwd_product_active_daily') }} AS d
    ON  u.partition_date = d.partition_date
    AND u.client_variant = d.client_variant

  -- Bring in the canonical product name from the dimension
  LEFT JOIN {{ ref('dim_product') }}             AS p
    ON  d.product_guid = p.product_guid

  WHERE
    u.partition_date = {{ analytics_airflow_ds() }}
    AND d.product_guid IS NOT NULL

),

-- ── Previous snapshot (from temp table created by pre_hook) ─────────────────
previous_snapshot AS (

  SELECT
    client_variant,
    client_id,
    product_name,
    product_guid,
    last_active_dt
  FROM dim_upr_snapshot

),

-- ── Union and deduplicate ───────────────────────────────────────────────────
-- UNION ALL then GROUP BY, taking MAX(last_active_dt), so that if a pairing
-- appears in both today's data and the snapshot, we keep the more recent date.
combined AS (

  SELECT client_variant, client_id, product_name, product_guid, last_active_dt
  FROM today_pairings

  UNION ALL

  SELECT client_variant, client_id, product_name, product_guid, last_active_dt
  FROM previous_snapshot

),

deduped AS (

  SELECT
    client_variant,
    client_id,
    product_name,
    product_guid,
    MAX(last_active_dt)                                 AS last_active_dt
  FROM combined
  GROUP BY
    client_variant,
    client_id,
    product_name,
    product_guid

)

SELECT
  client_variant,
  client_id,
  product_name,
  product_guid,
  last_active_dt,
  {{ analytics_airflow_ds() }}                          AS airflow_ds,
  GETDATE()                                             AS ingest_timestamp
FROM deduped
