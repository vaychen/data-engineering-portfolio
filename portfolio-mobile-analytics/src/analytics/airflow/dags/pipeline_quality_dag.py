"""
dags/pipeline_quality_dag.py

Data quality DAG — triggered after pipeline_export_dag completes.

Responsibilities
----------------
1. Source delivery checks: assert that today's nova ODS event count is non-zero
   and that day-over-day volume change is within ±50 % (catches upstream
   feed outages or anomalous spikes before downstream consumers are affected).

2. Report data checks: assert that key DWS / reporting tables have non-null
   row counts for the current business date.

A BranchPythonOperator at the top skips the source checks when the pipeline
is running in report-only mode (e.g. partial backfill where ODS was not
reloaded).

schedule_interval=None — triggered exclusively by pipeline_export_dag.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup

from plugins.common.config import GLOBAL_ARGS, REDSHIFT_CONN_ID
from plugins.function.notification import on_success_callback as on_task_success
from plugins.operators.redshift_sql_operator import RedshiftSQLOperator

DAG_ID = "pipeline_quality_dag"

_BACKFILL_PARAMS = {
    "backfill_scan_date": "{{ var.value.backfill_scan_date | default(1) }}"
}

# ---------------------------------------------------------------------------
# Branch logic
# ---------------------------------------------------------------------------


def _branch_source_checks(**context) -> str:
    """
    Skip source-layer DQ checks when running in report-only mode.

    The Airflow Variable ``dq_skip_source_checks`` can be set to ``"true"``
    by operators who are re-exporting without reloading ODS (e.g. fixing a
    report SQL bug without reprocessing raw events).
    """
    skip = Variable.get("dq_skip_source_checks", default_var="false").lower()
    if skip == "true":
        return "skip_source_checks"
    return "source_delivery_checks.check_nova_delivery"


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id=DAG_ID,
    default_args=GLOBAL_ARGS,
    description="Post-export data quality: nova source delivery + report integrity checks",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["analytics", "data-quality", "dq"],
) as dag:

    # ------------------------------------------------------------------ #
    # Branch                                                              #
    # ------------------------------------------------------------------ #

    branch = BranchPythonOperator(
        task_id="branch_source_check",
        python_callable=_branch_source_checks,
        provide_context=True,
    )

    skip_source_checks = PythonOperator(
        task_id="skip_source_checks",
        python_callable=lambda: None,
    )

    # ------------------------------------------------------------------ #
    # Source delivery checks                                              #
    # ------------------------------------------------------------------ #

    with TaskGroup(group_id="source_delivery_checks") as source_dq_group:

        check_nova_delivery = RedshiftSQLOperator(
            task_id="check_nova_delivery",
            sql_files=["dml/dqs_source_delivery_check.sql"],
            params={**_BACKFILL_PARAMS, "source_table": "ods_nova_app_events"},
            redshift_conn_id=REDSHIFT_CONN_ID,
        )

    # ------------------------------------------------------------------ #
    # Report data integrity checks                                        #
    # ------------------------------------------------------------------ #

    with TaskGroup(group_id="report_data_checks") as report_dq_group:

        check_user_active_report = RedshiftSQLOperator(
            task_id="check_user_active_report",
            sql_files=["dml/dqs_report_row_count_check.sql"],
            params={**_BACKFILL_PARAMS, "report_table": "dws_user_retention"},
            redshift_conn_id=REDSHIFT_CONN_ID,
        )

        check_product_active_report = RedshiftSQLOperator(
            task_id="check_product_active_report",
            sql_files=["dml/dqs_report_row_count_check.sql"],
            params={**_BACKFILL_PARAMS, "report_table": "dwd_product_active_daily"},
            redshift_conn_id=REDSHIFT_CONN_ID,
        )

    # ------------------------------------------------------------------ #
    # Terminal success notification                                        #
    # ------------------------------------------------------------------ #

    pipeline_success = PythonOperator(
        task_id="pipeline_complete",
        python_callable=lambda **ctx: None,
        provide_context=True,
        on_success_callback=on_task_success,
        trigger_rule="none_failed_min_one_success",
    )

    # ------------------------------------------------------------------ #
    # Task dependencies                                                    #
    # ------------------------------------------------------------------ #

    branch >> [skip_source_checks, source_dq_group]
    [skip_source_checks, source_dq_group] >> report_dq_group >> pipeline_success
