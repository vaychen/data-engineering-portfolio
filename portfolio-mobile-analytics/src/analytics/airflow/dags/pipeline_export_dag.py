"""
dags/pipeline_export_dag.py

Export DAG — triggered by pipeline_etl_dag.

Responsibilities
----------------
Stream app reporting tables from Amazon Redshift Serverless to Aurora MySQL
so that downstream BI tools and application APIs can consume pre-aggregated
data without hitting the warehouse directly.

Exported tables (ADS layer)
---------------------------
- ads_user_active_report    ← sourced from dws_user_retention
- ads_product_active_report ← sourced from dwd_product_active_daily

Export pattern
--------------
Each export task uses RedshiftToMySQLOperator, which:
  1. Streams the Redshift result set in configurable chunks (default 20,000 rows)
     using a server-side cursor to avoid memory exhaustion on large result sets.
  2. Deletes existing rows for the target business date before re-inserting
     (idempotent DELETE + bulk INSERT).
  3. Prunes rows older than 2 days from MySQL after each successful insert.

schedule_interval=None — triggered exclusively by pipeline_etl_dag.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from plugins.common.config import GLOBAL_ARGS, REDSHIFT_CONN_ID, MYSQL_CONN_ID
from plugins.operators.redshift_to_mysql_operator import RedshiftToMySQLOperator

DAG_ID = "pipeline_export_dag"

# Business date = yesterday (the partition written by the ETL stage).
_BUSINESS_DATE = "{{ ds }}"

with DAG(
    dag_id=DAG_ID,
    default_args=GLOBAL_ARGS,
    description="Redshift → Aurora MySQL export: app reporting tables",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["analytics", "export", "ads", "mysql"],
) as dag:

    # ------------------------------------------------------------------ #
    # Export: user active report                                          #
    # Sourced from dws_user_retention                                     #
    # Columns: cohort_date, partition_date, days_since_cohort,           #
    #          client_name, product_id, cohort_size, retained_users,     #
    #          retention_rate                                             #
    # ------------------------------------------------------------------ #

    export_user_active_report = RedshiftToMySQLOperator(
        task_id="export_user_active_report",
        redshift_conn_id=REDSHIFT_CONN_ID,
        mysql_conn_id=MYSQL_CONN_ID,
        mysql_table="ads_user_active_report",
        business_date=_BUSINESS_DATE,
        chunk_size=20_000,
        redshift_sql="""
            SELECT
                cohort_date,
                partition_date,
                days_since_cohort,
                client_name,
                product_id,
                cohort_size,
                retained_users,
                retention_rate
            FROM analytics_dw.dws_user_retention
            WHERE partition_date = %(business_date)s
            ORDER BY cohort_date, client_name, product_id
        """,
    )

    # ------------------------------------------------------------------ #
    # Export: product active report                                       #
    # Sourced from dwd_product_active_daily                               #
    # Columns: event_local_date, product_guid, product_id, product_name, #
    #          firmware_version, client_variant, first_seen_ts,          #
    #          last_seen_ts, active_user_count, event_count,             #
    #          partition_date                                             #
    # ------------------------------------------------------------------ #

    export_product_active_report = RedshiftToMySQLOperator(
        task_id="export_product_active_report",
        redshift_conn_id=REDSHIFT_CONN_ID,
        mysql_conn_id=MYSQL_CONN_ID,
        mysql_table="ads_product_active_report",
        business_date=_BUSINESS_DATE,
        chunk_size=20_000,
        redshift_sql="""
            SELECT
                event_local_date,
                product_guid,
                product_id,
                product_name,
                firmware_version,
                client_variant,
                first_seen_ts,
                last_seen_ts,
                active_user_count,
                event_count,
                partition_date
            FROM analytics_dw.dwd_product_active_daily
            WHERE partition_date = %(business_date)s
            ORDER BY product_guid
        """,
    )

    # ------------------------------------------------------------------ #
    # Trigger quality DAG                                                 #
    # ------------------------------------------------------------------ #

    trigger_quality = TriggerDagRunOperator(
        task_id="trigger_pipeline_quality_dag",
        trigger_dag_id="pipeline_quality_dag",
        wait_for_completion=False,
        reset_dag_run=True,
        conf={"triggered_by": DAG_ID},
    )

    # ------------------------------------------------------------------ #
    # Task dependencies                                                    #
    # ------------------------------------------------------------------ #

    [export_user_active_report, export_product_active_report] >> trigger_quality
