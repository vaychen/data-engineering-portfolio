"""
dags/pipeline_main_dag.py

Entry-point DAG for the analytics pipeline.

Responsibilities
----------------
1. Warm up the Redshift Serverless endpoint (first query after idle can be slow).
2. Refresh all dimension tables in parallel (dim_product, dim_date, dim_device_model).
3. Trigger pipeline_source_dag to begin the ODS load stage.

Schedule: daily at 01:00 UTC.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.common.config import GLOBAL_ARGS, REDSHIFT_CONN_ID, get_backfill_scan_date
from plugins.operators.redshift_sql_operator import RedshiftSQLOperator

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DAG_ID = "pipeline_main_dag"
BACKFILL_PARAMS = {"backfill_scan_date": 1}  # overridden at runtime via Variable

# ---------------------------------------------------------------------------
# Branch logic
# ---------------------------------------------------------------------------


def _should_warmup(**context) -> str:
    """
    Skip the warm-up task during manual backfill runs to save time.

    A run is treated as a backfill when the Airflow Variable
    `backfill_scan_date` is greater than 1.
    """
    scan_days = get_backfill_scan_date()
    if scan_days > 1:
        return "skip_warmup"
    return "warmup_redshift"


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id=DAG_ID,
    default_args=GLOBAL_ARGS,
    description="Main entry point: DIM refresh → trigger source load pipeline",
    schedule_interval="0 1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["analytics", "main", "daily"],
    params=BACKFILL_PARAMS,
) as dag:

    # ------------------------------------------------------------------ #
    # Branch: decide whether to warm up Redshift                          #
    # ------------------------------------------------------------------ #

    branch = BranchPythonOperator(
        task_id="branch_warmup_check",
        python_callable=_should_warmup,
    )

    warmup = PostgresOperator(
        task_id="warmup_redshift",
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql="SELECT 1;",
    )

    skip_warmup = PythonOperator(
        task_id="skip_warmup",
        python_callable=lambda: None,
    )

    # ------------------------------------------------------------------ #
    # DIM refresh task group (all tables refresh in parallel)             #
    # ------------------------------------------------------------------ #

    with TaskGroup(group_id="refresh_dimensions") as refresh_dims:

        refresh_dim_product = RedshiftSQLOperator(
            task_id="refresh_dim_product",
            sql_files=["dml/dim_product_refresh.sql"],
            params={"backfill_scan_date": "{{ var.value.backfill_scan_date | default(1) }}"},
            redshift_conn_id=REDSHIFT_CONN_ID,
        )

        refresh_dim_date = RedshiftSQLOperator(
            task_id="refresh_dim_date",
            sql_files=["dml/dim_date_refresh.sql"],
            params={"backfill_scan_date": "{{ var.value.backfill_scan_date | default(1) }}"},
            redshift_conn_id=REDSHIFT_CONN_ID,
        )

        refresh_dim_device_model = RedshiftSQLOperator(
            task_id="refresh_dim_device_model",
            sql_files=["dml/dim_device_model_refresh.sql"],
            params={"backfill_scan_date": "{{ var.value.backfill_scan_date | default(1) }}"},
            redshift_conn_id=REDSHIFT_CONN_ID,
        )

        # All three run in parallel — no intra-group dependencies.

    # ------------------------------------------------------------------ #
    # Trigger next stage                                                   #
    # ------------------------------------------------------------------ #

    trigger_source = TriggerDagRunOperator(
        task_id="trigger_pipeline_source_dag",
        trigger_dag_id="pipeline_source_dag",
        wait_for_completion=False,
        reset_dag_run=True,
        conf={"triggered_by": DAG_ID},
    )

    # ------------------------------------------------------------------ #
    # Task dependencies                                                    #
    # ------------------------------------------------------------------ #

    branch >> [warmup, skip_warmup] >> refresh_dims >> trigger_source
