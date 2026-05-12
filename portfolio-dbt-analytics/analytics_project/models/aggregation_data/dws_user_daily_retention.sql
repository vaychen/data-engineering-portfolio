{{
  config(
    materialized         = 'incremental',
    incremental_strategy = 'append',
    full_refresh         = false,
    database             = var('analytics_database'),
    schema               = var('analytics_schema_aggregation'),
    alias                = 'dws_user_daily_retention',
    on_schema_change     = 'sync_all_columns',

    column_types = {
      "cohort_date":          "DATE",
      "product_name":         "VARCHAR(120)",
      "client_variant":       "VARCHAR(64)",
      "d0_users":             "INTEGER",
      "d1_users":             "INTEGER",
      "d2_users":             "INTEGER",
      "d3_users":             "INTEGER",
      "d7_users":             "INTEGER",
      "d1_retention_rate":    "FLOAT4",
      "d7_retention_rate":    "FLOAT4",
      "partition_date":       "DATE",
      "ingest_timestamp":     "TIMESTAMP"
    },

    -- Delete the cohort window being recomputed.  The retention window for a
    -- cohort date extends 7 days forward, so a backfill of N days must also
    -- cover cohort dates up to 7 days prior.
    pre_hook = [
      """
      DELETE FROM {{ this }}
      WHERE cohort_date >= DATEADD(day, -7, {{ analytics_backfill_lower_bound('analytics_backfill_days') }})
        AND cohort_date <= {{ analytics_airflow_ds() }}
      """
    ],

    tags = ['aggregation_data', 'retention', 'cohort']
  )
}}

{# --------------------------------------------------------------------------
   dws_user_daily_retention

   Classic D0–D7 retention cohort model.

   Methodology
   ───────────
   • Cohort date = the first day a client_id was active (D0).
     In practice, for a daily refresh we define D0 as the event_local_date
     from dwd_user_active_daily; new vs. returning users are not split here —
     this is a "daily cohort active" model, not a "new user cohort" model.
   • For each cohort date, we LEFT JOIN the daily-active table N days later
     to count how many of the D0 users were also active on D+N.
   • Rates are computed as ROUND(Dn / D0, 4) so that 0–1 floats are returned
     (multiply by 100 for percentages in BI tooling).

   Granularity
   ───────────
   (cohort_date, product_name, client_variant) — one row per cohort-slice.
   product_name is resolved via dim_user_product_relationship so that each
   user is attributed to the product they were paired with on D0.

   Backfill note
   ─────────────
   Because D7 retention for cohort_date C requires data through C+7, the DAG
   re-runs this model with analytics_backfill_days ≥ 8 after any source
   reprocessing to ensure D7 figures settle correctly.
   -------------------------------------------------------------------------- #}

WITH

-- ── Base daily active table for the relevant window ─────────────────────────
daily_active AS (

  SELECT
    event_local_date,
    client_id,
    client_variant
  FROM {{ ref('dwd_user_active_daily') }}
  WHERE
    -- We need data from (lower_bound - 7) through airflow_ds to evaluate
    -- D7 retention for cohorts within the backfill window.
    partition_date >= DATEADD(
      day,
      -7,
      {{ analytics_backfill_lower_bound('analytics_backfill_days') }}
    )
    AND partition_date <= {{ analytics_airflow_ds() }}
    AND COALESCE(client_id, '') <> ''

),

-- ── Resolve each client to a product_name on their cohort date ───────────────
client_product AS (

  SELECT
    client_id,
    client_variant,
    product_name
  FROM {{ ref('dim_user_product_relationship') }}

),

-- ── D0 cohort: all (client_id, client_variant, product_name) active on each date
cohort_d0 AS (

  SELECT
    da.event_local_date                                  AS cohort_date,
    da.client_id,
    da.client_variant,
    COALESCE(cp.product_name, 'Unknown')                 AS product_name
  FROM daily_active AS da
  LEFT JOIN client_product AS cp
    ON  da.client_id      = cp.client_id
    AND da.client_variant = cp.client_variant
  WHERE
    da.event_local_date >= {{ analytics_backfill_lower_bound('analytics_backfill_days') }}
    AND da.event_local_date <= {{ analytics_airflow_ds() }}

),

-- ── Self-joins for D1 through D7 ────────────────────────────────────────────
retention_joined AS (

  SELECT
    c0.cohort_date,
    c0.product_name,
    c0.client_variant,
    c0.client_id                                         AS d0_client_id,

    d1.client_id                                         AS d1_client_id,
    d2.client_id                                         AS d2_client_id,
    d3.client_id                                         AS d3_client_id,
    d7.client_id                                         AS d7_client_id

  FROM cohort_d0 AS c0

  LEFT JOIN daily_active AS d1
    ON  c0.client_id      = d1.client_id
    AND d1.event_local_date = DATEADD(day, 1, c0.cohort_date)

  LEFT JOIN daily_active AS d2
    ON  c0.client_id      = d2.client_id
    AND d2.event_local_date = DATEADD(day, 2, c0.cohort_date)

  LEFT JOIN daily_active AS d3
    ON  c0.client_id      = d3.client_id
    AND d3.event_local_date = DATEADD(day, 3, c0.cohort_date)

  LEFT JOIN daily_active AS d7
    ON  c0.client_id      = d7.client_id
    AND d7.event_local_date = DATEADD(day, 7, c0.cohort_date)

),

-- ── Aggregate to cohort-slice granularity ───────────────────────────────────
cohort_aggregated AS (

  SELECT
    cohort_date,
    product_name,
    client_variant,

    COUNT(DISTINCT d0_client_id)                         AS d0_users,
    COUNT(DISTINCT d1_client_id)                         AS d1_users,
    COUNT(DISTINCT d2_client_id)                         AS d2_users,
    COUNT(DISTINCT d3_client_id)                         AS d3_users,
    COUNT(DISTINCT d7_client_id)                         AS d7_users

  FROM retention_joined
  GROUP BY
    cohort_date,
    product_name,
    client_variant

)

SELECT
  cohort_date,
  product_name,
  client_variant,
  d0_users,
  d1_users,
  d2_users,
  d3_users,
  d7_users,

  -- Retention rates: NULL-safe division; NULL when d0 = 0
  CASE
    WHEN d0_users > 0
    THEN ROUND(CAST(d1_users AS FLOAT) / d0_users, 4)
  END                                                    AS d1_retention_rate,

  CASE
    WHEN d0_users > 0
    THEN ROUND(CAST(d7_users AS FLOAT) / d0_users, 4)
  END                                                    AS d7_retention_rate,

  {{ analytics_airflow_ds() }}                           AS partition_date,
  GETDATE()                                              AS ingest_timestamp

FROM cohort_aggregated
