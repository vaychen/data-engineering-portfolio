"""
plugins/common/notifications.py

Airflow callback functions that publish pipeline health events to AWS SNS.
Attach `on_task_failure` to `default_args` so every task failure triggers an alert.
Attach `on_task_success` to the final task of a DAG for end-to-end success notification.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

_SNS_TOPIC_ARN: str = os.environ.get(
    "SNS_ALERT_TOPIC_ARN",
    "arn:aws:sns:us-east-1:123456789012:analytics-pipeline-alerts",
)
_AWS_REGION: str = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")


def _publish_to_sns(subject: str, payload: Dict[str, Any]) -> None:
    """Low-level helper — publish a JSON payload to the configured SNS topic."""
    try:
        client = boto3.client("sns", region_name=_AWS_REGION)
        client.publish(
            TopicArn=_SNS_TOPIC_ARN,
            Subject=subject[:100],  # SNS subject max length is 100 chars
            Message=json.dumps(payload, default=str, indent=2),
        )
        log.info("SNS notification sent: %s", subject)
    except ClientError as exc:
        # Never let a notification failure break the pipeline.
        log.error("Failed to publish SNS notification: %s", exc)


def on_task_failure(context: Dict[str, Any]) -> None:
    """
    Airflow on_failure_callback.

    Called automatically by the scheduler when any task enters the FAILED state.
    Publishes a structured alert message so on-call engineers can triage quickly.
    """
    dag_id: str = context["dag"].dag_id
    task_id: str = context["task_instance"].task_id
    execution_date: str = str(context["execution_date"])
    log_url: str = context["task_instance"].log_url
    exception: str = str(context.get("exception", "unknown error"))

    subject = f"[PIPELINE FAILURE] {dag_id} / {task_id}"
    payload = {
        "status": "FAILED",
        "dag_id": dag_id,
        "task_id": task_id,
        "execution_date": execution_date,
        "log_url": log_url,
        "exception": exception,
    }
    _publish_to_sns(subject, payload)


def on_task_success(context: Dict[str, Any]) -> None:
    """
    Airflow on_success_callback.

    Attach to the terminal task of a DAG to signal that the full pipeline
    completed successfully.  Useful for downstream SLA monitoring.
    """
    dag_id: str = context["dag"].dag_id
    task_id: str = context["task_instance"].task_id
    execution_date: str = str(context["execution_date"])
    duration: float = context["task_instance"].duration or 0.0

    subject = f"[PIPELINE SUCCESS] {dag_id}"
    payload = {
        "status": "SUCCESS",
        "dag_id": dag_id,
        "terminal_task_id": task_id,
        "execution_date": execution_date,
        "duration_seconds": round(duration, 2),
    }
    _publish_to_sns(subject, payload)
