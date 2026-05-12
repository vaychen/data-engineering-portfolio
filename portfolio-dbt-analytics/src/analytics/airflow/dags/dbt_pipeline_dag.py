"""
dbt_pipeline_dag.py
═══════════════════
Airflow DAG that orchestrates the analytics_pipeline dbt project on AWS MWAA.

Design decisions
────────────────
• schedule=None — this DAG is triggered exclusively by an upstream source-load
  DAG (via TriggerDagRunOperator) once raw event data is confirmed to have
  landed in Redshift.  Running dbt before source data is ready would produce
  silently empty increments.

• Layer-by-layer task decomposition — each dbt layer is a separate
  BashOperator task so that:
    - Failed layers surface clearly in the MWAA UI.
    - Individual layers can be cleared and re-run without re-running the
      full pipeline.
    - The DAG structure documents the dependency order for new engineers.

• Credentials via Secrets Manager + env_var() — no credentials appear in
  code, environment variables baked at deploy time, or Airflow Connections.
  Each BashOperator task calls redshift_env_snippet() to fetch the secret at
  runtime and export DBT_ENV_SECRET_REDSHIFT_* into the shell.  dbt resolves
  those values through env_var() calls in the profiles.yml written by
  dbt_install.sh.  Secret rotation requires no DAG redeploy.

• profiles.yml is provided by dbt_install.sh — the install script writes
  ~/.dbt/profiles.yml (or $DBT_PROFILES_DIR/profiles.yml) using dbt's
  env_var() helper.  The DAG does not write its own profiles file; it only
  populates the environment variables that profiles.yml references.

• Backfill control via Airflow Variables — operators can trigger backfills by
  setting the `analytics_backfill_days` Airflow Variable without touching code.
  The DAG reads the variable and forwards it as a dbt --vars argument.
"""

from __future__ import annotations

import json
import textwrap
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from analytics.airflow.plugins.function.dbt_helpers import profiles_dir_snippet, redshift_env_snippet

# ──────────────────────────────────────────────────────────────────────────────
# Constants  (overridable via Airflow Variables in the MWAA environment)
# ──────────────────────────────────────────────────────────────────────────────

# Absolute path to the dbt project on the MWAA worker.
DBT_PROJECT_DIR = Variable.get(
    "dbt_project_dir",
    default_var="/usr/local/airflow/dags/analytics_project",
)

# Directory that contains profiles.yml — written by dbt_install.sh.
# Tilde is expanded at task runtime by profiles_dir_snippet().
DBT_PROFILES_DIR = Variable.get("dbt_profiles_dir", default_var="~/.dbt")

# dbt target (profile output) to use.
DBT_TARGET = Variable.get("dbt_target", default_var="prod")

# dbt-core executable installed by dbt_install.sh into a virtualenv.
# Path matches $DBT_CORE_VENV_PATH/bin/dbt (default path in dbt_install.sh).
DBT_BIN = Variable.get(
    "dbt_core_executable",
    default_var="/usr/local/airflow/.local/dbt_core_venv/bin/dbt",
)

# Name of the dbt profile block in profiles.yml (must match dbt_project.yml).
DBT_PROFILE = "analytics_pipeline"

# Secrets Manager secret ID for Redshift credentials.
# Secret must be a JSON blob with keys: host, port, database, user, password.
REDSHIFT_SECRET_ID = "analytics/redshift/pipeline-credentials"

# Airflow Variable keys for backfill control.
VAR_BACKFILL_DAYS = "analytics_backfill_days"
VAR_BACKFILL_DAYS_BY_EVENT = "analytics_backfill_days_by_event"  # JSON dict


# ──────────────────────────────────────────────────────────────────────────────
# Shared bash building blocks
# ──────────────────────────────────────────────────────────────────────────────

# Preamble prepended to every BashOperator command:
#   1. profiles_dir_snippet — resolves PROFILES_DIR (handles ~ expansion)
#   2. redshift_env_snippet — fetches Secrets Manager and exports
#      DBT_ENV_SECRET_REDSHIFT_{HOST,USER,PASSWORD} so that dbt's env_var()
#      calls in profiles.yml resolve correctly.
_PREAMBLE = (
    "set -euo pipefail\n"
    + profiles_dir_snippet(DBT_PROFILES_DIR)
    + redshift_env_snippet(REDSHIFT_SECRET_ID)
)

# Base dbt invocation shared by all run/test tasks.
# --profiles-dir uses the shell variable set by profiles_dir_snippet.
_DBT_BASE = (
    f"{DBT_BIN} "
    f"--profiles-dir \"$PROFILES_DIR\" "
    f"--project-dir {DBT_PROJECT_DIR} "
    f"--profile {DBT_PROFILE} "
    f"--target {DBT_TARGET} "
)

# Build the --vars JSON at task execution time (not DAG parse time) so that
# Airflow Variable reads reflect the current values.
_VARS_CMD = textwrap.dedent("""\
    DBT_VARS=$(python3 -c "
    import json
    from airflow.models import Variable
    backfill_days = int(Variable.get('analytics_backfill_days', default_var=1))
    try:
        by_event = json.loads(Variable.get('analytics_backfill_days_by_event', default_var='{}'))
    except Exception:
        by_event = {}
    v = {
        'airflow_ds': '{{ ds }}',
        'analytics_backfill_days': backfill_days,
        'backfill_days_by_event': by_event,
    }
    print(json.dumps(v))
    ")
""")


# ──────────────────────────────────────────────────────────────────────────────
# DAG definition
# ──────────────────────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "analytics-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # Give each dbt layer up to 90 minutes before Airflow kills it.
    "execution_timeout": timedelta(minutes=90),
}

with DAG(
    dag_id="dbt_analytics_pipeline",
    description=(
        "Runs the analytics_pipeline dbt project: source → meta → etl → "
        "aggregation → report layers on Redshift Serverless."
    ),
    default_args=DEFAULT_ARGS,
    # Triggered by upstream source-load DAG; no cron schedule.
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "analytics", "redshift"],
    render_template_as_native_obj=False,
    doc_md=__doc__,
) as dag:

    # ── 1. dbt seed ─────────────────────────────────────────────────────────
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            _PREAMBLE
            + _VARS_CMD
            + f'{_DBT_BASE} seed --full-refresh --vars "$DBT_VARS"\n'
        ),
        doc_md=(
            "Loads seed CSV files (dim_product, etc.) into the metadata schema. "
            "Runs with --full-refresh to replace seeds on every pipeline execution."
        ),
    )

    # ── 2. meta_data layer (dimensions / lookups) ───────────────────────────
    dbt_run_meta = BashOperator(
        task_id="dbt_run_meta",
        bash_command=(
            _PREAMBLE
            + _VARS_CMD
            + f'{_DBT_BASE} run --select meta_data --vars "$DBT_VARS"\n'
        ),
        doc_md=(
            "Materialises meta_data layer views (dim_product, etc.). "
            "Views are cheap to refresh and always reflect the latest seed data."
        ),
    )

    # ── 3. source_data layer (ODS event union) ──────────────────────────────
    dbt_run_source = BashOperator(
        task_id="dbt_run_source",
        bash_command=(
            _PREAMBLE
            + _VARS_CMD
            + f'{_DBT_BASE} run --select source_data --vars "$DBT_VARS"\n'
        ),
        doc_md=(
            "Runs the ODS incremental model (ods_nova_app_events). "
            "Unions all event types from raw Redshift tables into a single "
            "normalised event stream. Idempotent via pre_hook DELETE."
        ),
    )

    # ── 4. etl_data layer (cleaned facts + dimensions) ──────────────────────
    dbt_run_etl = BashOperator(
        task_id="dbt_run_etl",
        bash_command=(
            _PREAMBLE
            + _VARS_CMD
            + f'{_DBT_BASE} run --select etl_data --vars "$DBT_VARS"\n'
        ),
        doc_md=(
            "Runs etl_data layer: dwd_user_active_daily, dwd_product_active_daily, "
            "dim_user_product_relationship. All incremental with idempotent pre_hook DELETE."
        ),
    )

    # ── 5. aggregation_data layer (retention, funnels) ──────────────────────
    dbt_run_agg = BashOperator(
        task_id="dbt_run_aggregation",
        bash_command=(
            _PREAMBLE
            + _VARS_CMD
            + f'{_DBT_BASE} run --select aggregation_data --vars "$DBT_VARS"\n'
        ),
        doc_md=(
            "Runs aggregation_data layer: dws_user_daily_retention and funnel models. "
            "Pre_hook DELETE covers a 7-day look-back to allow D7 retention to settle."
        ),
    )

    # ── 6. report_data layer (BI-facing views) ──────────────────────────────
    dbt_run_report = BashOperator(
        task_id="dbt_run_report",
        bash_command=(
            _PREAMBLE
            + _VARS_CMD
            + f'{_DBT_BASE} run --select report_data --vars "$DBT_VARS"\n'
        ),
        doc_md=(
            "Refreshes report_data views (vw_app_daily_active_users, etc.). "
            "Views are recreated on every run; no incremental logic needed."
        ),
    )

    # ── 7. dbt test (data quality gate) ─────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            _PREAMBLE
            + _VARS_CMD
            + f'{_DBT_BASE} test --vars "$DBT_VARS"\n'
        ),
        doc_md=(
            "Runs all dbt schema tests (not_null, unique, accepted_values, "
            "relationships) across all layers. Pipeline is considered failed "
            "if any test fails."
        ),
    )

    # ── Dependency chain ─────────────────────────────────────────────────────
    # seed → meta → source → etl → aggregation → report → test
    (
        dbt_seed
        >> dbt_run_meta
        >> dbt_run_source
        >> dbt_run_etl
        >> dbt_run_agg
        >> dbt_run_report
        >> dbt_test
    )
