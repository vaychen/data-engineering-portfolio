"""
plugins/common/config.py

Central configuration for the analytics pipeline.
All environment-specific values are read from Airflow Variables or OS environment
variables so that no secrets appear in source code.
"""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Any, Dict

from airflow.models import Variable

from plugins.function.notification import on_failure_callback as on_task_failure

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

_DEFAULT_ENV = "local"


def get_environment() -> str:
    """Return the current Airflow deployment environment."""
    return Variable.get("airflow_environment", default_var=_DEFAULT_ENV)


# ---------------------------------------------------------------------------
# Connection IDs
# ---------------------------------------------------------------------------

# These are Airflow connection IDs, not raw credentials.
# The actual host / password is stored in the Airflow Connections UI or
# AWS Secrets Manager (via the SecretsManagerBackend).
REDSHIFT_CONN_ID: str = os.environ.get("REDSHIFT_CONN_ID", "redshift_default")
MYSQL_CONN_ID: str = os.environ.get("MYSQL_CONN_ID", "mysql_reporting_default")

# ---------------------------------------------------------------------------
# IAM / AWS
# ---------------------------------------------------------------------------

REDSHIFT_IAM_ROLE_ARN: str = os.environ.get(
    "REDSHIFT_IAM_ROLE_ARN",
    "arn:aws:iam::123456789012:role/RedshiftSpectrumRole",
)

SNS_ALERT_TOPIC_ARN: str = os.environ.get(
    "SNS_ALERT_TOPIC_ARN",
    "arn:aws:sns:us-east-1:123456789012:analytics-pipeline-alerts",
)

AWS_REGION: str = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# Database name constants
# ---------------------------------------------------------------------------

# Redshift Serverless databases
DB_ANALYTICS_DW = "analytics_dw"          # primary data warehouse (ODS → DWS)
DB_ANALYTICS_METADATA = "analytics_metadata"  # pipeline run metadata & audit logs
DB_ANALYTICS_REPORT = "analytics_report"  # ADS / reporting views exposed to BI

# ---------------------------------------------------------------------------
# Backfill window
# ---------------------------------------------------------------------------

def get_backfill_scan_date() -> int:
    """
    Number of days back to include in the current run's backfill window.

    Reads from the 'backfill_scan_date' Airflow Variable so operators can
    widen the window without a DAG code change.  Default is 1 (yesterday only).
    """
    return int(Variable.get("backfill_scan_date", default_var=1))

# ---------------------------------------------------------------------------
# Default DAG arguments
# ---------------------------------------------------------------------------

GLOBAL_ARGS: Dict[str, Any] = {
    "owner": "analytics-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_task_failure,
    "email_on_failure": False,
    "email_on_retry": False,
}
