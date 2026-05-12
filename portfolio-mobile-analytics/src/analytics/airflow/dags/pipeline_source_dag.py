"""
dags/pipeline_source_dag.py

ODS data load DAG — triggered by pipeline_main_dag.

Responsibilities
----------------
1. Resolve the backfill window (how many days to reload) from an Airflow Variable.
2. Load nova mobile app events into the ODS layer (staging → load → dedupe).
3. Trigger pipeline_etl_dag on completion.

schedule_interval=None — this DAG is exclusively triggered by pipeline_main_dag.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from plugins.common.config import GLOBAL_ARGS, REDSHIFT_CONN_ID
from plugins.operators.redshift_sql_operator import RedshiftSQLOperator

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DAG_ID = "pipeline_source_dag"


def _get_backfill_date(**context) -> int:
    """
    Read backfill_scan_date from Airflow Variables and push it to XCom.

    Downstream tasks pull this value to parameterise their SQL windows,
    allowing ops to widen the reload window by changing a single Variable
    without touching DAG code.
    """
    scan_date: int = int(Variable.get("backfill_scan_date", default_var=1))
    context["ti"].xcom_push(key="backfill_scan_date", value=scan_date)
    return scan_date


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id=DAG_ID,
    default_args=GLOBAL_ARGS,
    description="ODS source load: nova mobile app events",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["analytics", "source", "ods"],
) as dag:

    # ------------------------------------------------------------------ #
    # Resolve backfill window                                              #
    # ------------------------------------------------------------------ #

    get_backfill_date = PythonOperator(
        task_id="get_backfill_date",
        python_callable=_get_backfill_date,
        provide_context=True,
    )

    # ------------------------------------------------------------------ #
    # Chain A — nova mobile app events                                    #
    # ------------------------------------------------------------------ #

    with TaskGroup(group_id="load_nova_events") as nova_group:

        truncate_nova_staging = RedshiftSQLOperator(
            task_id="truncate_nova_staging",
            sql_files=["dml/ods_nova_events_staging_truncate.sql"],
            params={
                "backfill_scan_date": "{{ ti.xcom_pull(task_ids='get_backfill_date', key='backfill_scan_date') }}"
            },
            redshift_conn_id=REDSHIFT_CONN_ID,
        )

        load_nova_events = RedshiftSQLOperator(
            task_id="load_nova_events",
            sql_files=["dml/ods_nova_events_load.sql"],
            params={
                "backfill_scan_date": "{{ ti.xcom_pull(task_ids='get_backfill_date', key='backfill_scan_date') }}"
            },
            redshift_conn_id=REDSHIFT_CONN_ID,
        )

        dedupe_nova_events = RedshiftSQLOperator(
            task_id="dedupe_nova_events",
            sql_files=["dml/ods_nova_events_dedupe.sql"],
            params={
                "backfill_scan_date": "{{ ti.xcom_pull(task_ids='get_backfill_date', key='backfill_scan_date') }}"
            },
            redshift_conn_id=REDSHIFT_CONN_ID,
        )

        truncate_nova_staging >> load_nova_events >> dedupe_nova_events

    # ------------------------------------------------------------------ #
    # Trigger ETL                                                          #
    # ------------------------------------------------------------------ #

    trigger_etl = TriggerDagRunOperator(
        task_id="trigger_pipeline_etl_dag",
        trigger_dag_id="pipeline_etl_dag",
        wait_for_completion=False,
        reset_dag_run=True,
        conf={"triggered_by": DAG_ID},
    )

    # ------------------------------------------------------------------ #
    # Task dependencies                                                    #
    # ------------------------------------------------------------------ #

    get_backfill_date >> nova_group >> trigger_etl
