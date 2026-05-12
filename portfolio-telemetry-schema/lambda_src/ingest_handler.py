from __future__ import annotations

import json
import os
import uuid
from typing import Any

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.event_handler import APIGatewayRestResolver
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.typing import LambdaContext
from pydantic import BaseModel, ValidationError
from pydantic_settings import BaseSettings

from schemas.envelope.app_record import NovaTelemetryRecord
from schemas.envelope.device_record import SentinelTelemetryRecord

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class IngestSettings(BaseSettings):
    firehose_nova_stream: str
    firehose_sentinel_stream: str
    quarantine_bucket: str
    quarantine_prefix: str = "quarantine/"
    aws_region: str = "us-east-1"

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = IngestSettings()  # type: ignore[call-arg]

# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------

firehose = boto3.client("firehose", region_name=settings.aws_region)
s3 = boto3.client("s3", region_name=settings.aws_region)

# ---------------------------------------------------------------------------
# Lambda Powertools
# ---------------------------------------------------------------------------

logger = Logger()
tracer = Tracer()
metrics = Metrics()
app = APIGatewayRestResolver()

# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------


class RecordFailureDetail(BaseModel):
    index: int
    error_code: str
    error_message: str


class PutRecordsResponse(BaseModel):
    failed_record_count: int
    failures: list[RecordFailureDetail]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _quarantine_record(
    raw: dict[str, Any],
    error: str,
    stream_type: str,
) -> None:
    """Write an invalid record to the S3 quarantine prefix."""
    key = (
        f"{settings.quarantine_prefix}{stream_type}/"
        f"{uuid.uuid4()}.json"
    )
    body = json.dumps({"raw": raw, "validation_error": error}, default=str)
    s3.put_object(
        Bucket=settings.quarantine_bucket,
        Key=key,
        Body=body.encode(),
        ContentType="application/json",
    )
    logger.warning("Quarantined invalid record", extra={"s3_key": key, "stream": stream_type})


def _put_to_firehose(stream_name: str, records: list[dict[str, Any]]) -> int:
    """Send validated records to Kinesis Firehose in a single batch call.

    Returns the number of records that Firehose reported as failed.
    """
    firehose_records = [
        {"Data": (json.dumps(r, default=str) + "\n").encode()} for r in records
    ]
    response = firehose.put_record_batch(
        DeliveryStreamName=stream_name,
        Records=firehose_records,
    )
    return response.get("FailedPutCount", 0)


def _process_batch(
    raw_records: list[Any],
    model_class: type[NovaTelemetryRecord] | type[SentinelTelemetryRecord],
    stream_name: str,
    stream_type: str,
) -> PutRecordsResponse:
    """Validate a batch of raw dicts, forward valid ones to Firehose,
    quarantine invalid ones to S3.
    """
    valid: list[dict[str, Any]] = []
    failures: list[RecordFailureDetail] = []

    for idx, raw in enumerate(raw_records):
        if not isinstance(raw, dict):
            failures.append(
                RecordFailureDetail(
                    index=idx,
                    error_code="INVALID_TYPE",
                    error_message=f"Expected object, got {type(raw).__name__}",
                )
            )
            _quarantine_record({"raw": str(raw)}, "not a JSON object", stream_type)
            continue

        try:
            record = model_class.model_validate(raw)
            valid.append(record.model_dump(mode="json"))
        except ValidationError as exc:
            error_msg = exc.json()
            failures.append(
                RecordFailureDetail(
                    index=idx,
                    error_code="SCHEMA_VALIDATION_ERROR",
                    error_message=error_msg,
                )
            )
            _quarantine_record(raw, error_msg, stream_type)

    metrics.add_metric(name="RecordsReceived", unit=MetricUnit.Count, value=len(raw_records))
    metrics.add_metric(name="RecordsValid", unit=MetricUnit.Count, value=len(valid))
    metrics.add_metric(name="RecordsInvalid", unit=MetricUnit.Count, value=len(failures))

    if valid:
        firehose_failed = _put_to_firehose(stream_name, valid)
        if firehose_failed:
            logger.error(
                "Firehose reported failed records",
                extra={"stream": stream_name, "failed_count": firehose_failed},
            )

    return PutRecordsResponse(
        failed_record_count=len(failures),
        failures=failures,
    )


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------


@app.post("/v1/records/nova")
@tracer.capture_method
def ingest_nova() -> dict[str, Any]:
    """Ingest a batch of nova (mobile app) telemetry records."""
    body = app.current_event.json_body
    if not isinstance(body, list):
        body = [body]

    result = _process_batch(
        raw_records=body,
        model_class=NovaTelemetryRecord,
        stream_name=settings.firehose_nova_stream,
        stream_type="nova",
    )
    status_code = 207 if result.failed_record_count else 200
    return app.current_event.resolved_headers_field, status_code, result.model_dump()


@app.post("/v1/records/sentinel")
@tracer.capture_method
def ingest_sentinel() -> dict[str, Any]:
    """Ingest a batch of sentinel (IoT firmware) telemetry records."""
    body = app.current_event.json_body
    if not isinstance(body, list):
        body = [body]

    result = _process_batch(
        raw_records=body,
        model_class=SentinelTelemetryRecord,
        stream_name=settings.firehose_sentinel_stream,
        stream_type="sentinel",
    )
    status_code = 207 if result.failed_record_count else 200
    return app.current_event.resolved_headers_field, status_code, result.model_dump()


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
def handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    return app.resolve(event, context)
