{#
  ============================================================================
  utils.sql — Core utility macros for the analytics_pipeline dbt project.

  These macros standardise how every model resolves:
    • The Airflow logical execution date / timestamp
    • Incremental delete-window lower bounds (global + per-event overrides)

  All date arithmetic targets Amazon Redshift SQL dialect.
  ============================================================================
#}


{# --------------------------------------------------------------------------
   analytics_airflow_ds()

   Returns the Airflow logical execution date as a DATE value.

   Priority:
     1. var('airflow_ds') — injected by the MWAA DAG as --vars '{"airflow_ds":"…"}'
     2. run_started_at   — dbt's built-in context variable (UTC, as timestamp)

   Usage in SQL:
       WHERE partition_date = {{ analytics_airflow_ds() }}
   -------------------------------------------------------------------------- #}
{% macro analytics_airflow_ds() %}
  {%- if var('airflow_ds', none) is not none -%}
    CAST('{{ var("airflow_ds") }}' AS DATE)
  {%- else -%}
    CAST('{{ run_started_at.strftime("%Y-%m-%d") }}' AS DATE)
  {%- endif -%}
{% endmacro %}


{# --------------------------------------------------------------------------
   analytics_airflow_ts()

   Returns the Airflow logical execution timestamp as a TIMESTAMP value.
   Same priority logic as analytics_airflow_ds().

   Usage in SQL:
       WHERE event_timestamp < {{ analytics_airflow_ts() }}
   -------------------------------------------------------------------------- #}
{% macro analytics_airflow_ts() %}
  {%- if var('airflow_ds', none) is not none -%}
    CAST('{{ var("airflow_ds") }} 00:00:00' AS TIMESTAMP)
  {%- else -%}
    CAST('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' AS TIMESTAMP)
  {%- endif -%}
{% endmacro %}


{# --------------------------------------------------------------------------
   analytics_backfill_lower_bound(backfill_var, default_days=1)

   Returns the lower-bound DATE for an incremental delete / select window.

   Arguments:
     backfill_var  – name of the dbt var that holds the number of days to
                     look back (e.g. 'analytics_backfill_days').
     default_days  – fallback when the var is not set (default = 1).

   The computed lower bound is:
       DATEADD(day, -N, analytics_airflow_ds())

   It is also floor-clamped to var('analytics_initial_partition_date') so that
   runaway backfill values cannot accidentally reach data before the pipeline
   was launched.

   Usage in pre_hook DELETE:
       DELETE FROM {{ this }}
       WHERE partition_date >= {{ analytics_backfill_lower_bound('analytics_backfill_days') }}
         AND partition_date <= {{ analytics_airflow_ds() }}
   -------------------------------------------------------------------------- #}
{% macro analytics_backfill_lower_bound(backfill_var, default_days=1) %}
  {%- set n_days = var(backfill_var, default_days) | int -%}
  GREATEST(
    DATEADD(day, -{{ n_days }}, {{ analytics_airflow_ds() }}),
    CAST('{{ var("analytics_initial_partition_date", "2023-01-01") }}' AS DATE)
  )
{% endmacro %}


{# --------------------------------------------------------------------------
   analytics_partition_lower_bound(event_slug, override=None)

   Per-event backfill lower bound.  Checks var('backfill_days_by_event') for
   an event-specific override before falling back to the global
   analytics_backfill_days var.

   Arguments:
     event_slug – the event identifier key used in backfill_days_by_event dict
                  (e.g. 'session_start', 'login_attempt').
     override   – optional hard-coded day count; skips var lookup when set.

   Priority resolution:
     1. override argument (if not None)
     2. var('backfill_days_by_event')[event_slug]
     3. var('analytics_backfill_days', 1)

   Usage:
       WHERE partition_date >= {{ analytics_partition_lower_bound('session_start') }}
         AND partition_date <= {{ analytics_airflow_ds() }}
   -------------------------------------------------------------------------- #}
{% macro analytics_partition_lower_bound(event_slug, override=None) %}
  {%- if override is not none -%}
    {%- set n_days = override | int -%}
  {%- else -%}
    {%- set per_event_map = var('backfill_days_by_event', {}) -%}
    {%- set n_days = per_event_map.get(event_slug, var('analytics_backfill_days', 1)) | int -%}
  {%- endif -%}
  GREATEST(
    DATEADD(day, -{{ n_days }}, {{ analytics_airflow_ds() }}),
    CAST('{{ var("analytics_initial_partition_date", "2023-01-01") }}' AS DATE)
  )
{% endmacro %}


{# --------------------------------------------------------------------------
   generate_schema_name(custom_schema_name, node)

   dbt default behaviour appends the custom schema to the target schema:
       <target_schema>_<custom_schema>

   This override returns the custom schema verbatim so that models end up in
   exactly the schema specified in their config() block (e.g. analytics_source,
   analytics_dw, analytics_report, …) regardless of the profile's target schema.

   Reference: https://docs.getdbt.com/docs/build/custom-schemas
   -------------------------------------------------------------------------- #}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}
    {{ target.schema }}
  {%- else -%}
    {{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
