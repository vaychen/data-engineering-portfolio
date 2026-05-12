"""
dags/pipeline_etl_dag.py

ETL transformation DAG — triggered by pipeline_source_dag.

Responsibilities
----------------
Transform ODS nova events through the warehouse layers:

Chain A — User activity path:
    dwd_user_active_daily
        → dwd_product_active_daily
        → dim_user_product_relationship
        → dws_user_retention

schedule_interval=None — triggered exclusively by pipeline_source_dag.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from plugins.common.config import GLOBAL_ARGS, REDSHIFT_CONN_ID
from plugins.operators.redshift_sql_operator import RedshiftSQLOperator

DAG_ID = "pipeline_etl_dag"

# Jinja param block shared by all transformation tasks.
_BACKFILL_PARAMS = {
    "backfill_scan_date": "{{ var.value.backfill_scan_date | default(1) }}"
}

with DAG(
    dag_id=DAG_ID,
    default_args=GLOBAL_ARGS,
    description="DWD + DWS transformation: user activity chain",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["analytics", "etl", "dwd", "dws"],
) as dag:

    # ------------------------------------------------------------------ #
    # Chain A — User activity                                             #
    # ------------------------------------------------------------------ #

    with TaskGroup(group_id="user_activity_chain") as user_chain:

        dwd_user_active_daily = RedshiftSQLOperator(
            task_id="dwd_user_active_daily",
            sql_files=["dml/dwd_user_active_daily.sql"],
            params=_BACKFILL_PARAMS,
            redshift_conn_id=REDSHIFT_CONN_ID,
        )

        dwd_product_active_daily = RedshiftSQLOperator(
            task_id="dwd_product_active_daily",
            sql_files=["dml/dwd_product_active_daily.sql"],
            params=_BACKFILL_PARAMS,
            redshift_conn_id=REDSHIFT_CONN_ID,
        )

        dim_user_product_relationship = RedshiftSQLOperator(
            task_id="dim_user_product_relationship",
            sql_files=["dml/dim_user_product_relationship.sql"],
            params=_BACKFILL_PARAMS,
            redshift_conn_id=REDSHIFT_CONN_ID,
        )

        dws_user_retention = RedshiftSQLOperator(
            task_id="dws_user_retention",
            sql_files=["dml/dws_user_retention.sql"],
            params=_BACKFILL_PARAMS,
            redshift_conn_id=REDSHIFT_CONN_ID,
        )

        (
            dwd_user_active_daily
            >> dwd_product_active_daily
            >> dim_user_product_relationship
            >> dws_user_retention
        )

    # ------------------------------------------------------------------ #
    # Trigger export                                                       #
    # ------------------------------------------------------------------ #

    trigger_export = TriggerDagRunOperator(
        task_id="trigger_pipeline_export_dag",
        trigger_dag_id="pipeline_export_dag",
        wait_for_completion=False,
        reset_dag_run=True,
        conf={"triggered_by": DAG_ID},
    )

    user_chain >> trigger_export
