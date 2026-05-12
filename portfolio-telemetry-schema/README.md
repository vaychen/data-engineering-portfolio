# Telemetry Schema SDK

A production-grade Python SDK defining the telemetry schema layer for a serverless IoT data pipeline on AWS. Covers two distinct telemetry streams — **nova** (mobile app events) and **sentinel** (IoT device firmware events via protobuf) — with a shared Pydantic v2 schema hierarchy, Lambda ingest handlers, and flattening utilities for downstream analytics.

---

## Architecture Overview

```
Mobile App (nova)          IoT Device (sentinel)
      │                           │
      │  HTTPS POST               │  HTTPS POST
      ▼                           ▼
┌─────────────────────────────────────────────┐
│         API Gateway (REST)                  │
│   POST /v1/records/nova                     │
│   POST /v1/records/sentinel                 │
└───────────────────┬─────────────────────────┘
                    │
                    ▼
          ┌─────────────────┐
          │  Lambda Ingest  │  (aws-lambda-powertools)
          │  Handler        │
          │                 │
          │ 1. Parse body   │
          │ 2. Pydantic     │
          │    validate     │
          │ 3. Split        │
          │    valid/invalid│
          └────┬───────┬────┘
               │       │
        valid  │       │ invalid
               ▼       ▼
      Kinesis Firehose  S3 Quarantine Bucket
               │
               ▼
          S3 (Parquet)
               │
      ┌────────┴────────┐
      ▼                 ▼
   Athena           Redshift
  (ad-hoc)        (dashboards)
```

Records are validated against a strict Pydantic v2 schema hierarchy. Valid records are forwarded to Kinesis Firehose in batches; invalid records are written to an S3 quarantine prefix with the raw payload and validation error details for later triage.

---

## Telemetry Streams

### nova — Mobile App Events

Events emitted by the **nova** mobile application (iOS, Android, HarmonyOS). Each event is wrapped in a `NovaTelemetryRecord` envelope containing:

- `ClientContext` — app name/variant/version, session info, foreground/background state
- `NovaMobilePayload` — a **discriminated union** of 3 event subtypes covering:
  - Device pairing (`device_pairing_record`) — successful Bluetooth connection with full product context
  - User authentication (`user_auth_record`) — successful login via a third-party identity provider
  - Device session (`device_session_record`) — session initialisation capturing OS and hardware environment

### sentinel — IoT Device Firmware Events

Events emitted by **sentinel** IoT devices. Firmware telemetry is serialized as protobuf, base64-encoded, and wrapped in a `SentinelTelemetryRecord` envelope containing:

- `ClientContext` — the companion mobile app session that triggered the event
- `SentinelDataContext` — device uptime, boot count (session), firmware platform, signing/encryption flags, a `schema_id` identifying the protobuf schema variant, and the base64-encoded protobuf payload

---

## Key Design Patterns

### 1. Pydantic v2 Discriminated Union (3 event types)

All nova event types are collected into `NovaMobilePayload` — an `Annotated` discriminated union keyed on the `event_name` literal field. Pydantic routes each incoming payload to the correct model at validation time with no `if/elif` branching.

```python
NovaMobilePayload = Annotated[
    Union[DevicePairingPayload, UserAuthPayload, DeviceSessionPayload],
    Field(discriminator="event_name"),
]
```

### 2. ProductContext Hierarchy (base → basic → extended)

Three-level inheritance with progressive field requirements:

| Level | Class | Required fields |
|---|---|---|
| Base | `ProductContext` | none (all Optional) |
| Basic | `BasicProductContext` | name, product_id, variant |
| Extended | `ExtendProductContext` | + guid (UUID), firmware_version |

Higher-fidelity events (e.g. successful device pairing) require `ExtendProductContext`, enforcing richer device context where the firmware version is known.

### 3. Record Flattening for Athena / Redshift

`flatten_nova_record` and `flatten_sentinel_record` recursively unnest nested context dicts into a flat key-value structure suitable for columnar storage. Nested keys are prefixed (`client_`, `nova_`, `sentinel_`) to avoid collisions.

```python
# Nested input
{"client_ctx": {"session_id": 42, "version": "1.2.3"}}

# Flat output
{"client_session_id": 42, "client_version": "1.2.3"}
```

### 4. Lambda Ingest with Lambda Powertools

The ingest Lambda uses [AWS Lambda Powertools](https://docs.powertools.aws.dev/lambda/python/) for structured logging, X-Ray tracing, CloudWatch metrics, and API Gateway routing. Batch validation errors are isolated per-record so one bad record never drops the rest of the batch.

---

## Repository Structure

```
.
├── README.md
├── requirements.txt
├── conftest.py                      # pytest sys.path bootstrap
├── schemas/
│   ├── context/
│   │   ├── client_ctx.py            # ClientContext model
│   │   └── product_ctx.py           # ProductContext hierarchy
│   ├── mobile/
│   │   ├── device_pair.py           # DevicePairingPayload schema
│   │   ├── user_auth.py             # UserAuthPayload schema
│   │   ├── device_session.py        # DeviceSessionPayload schema
│   │   └── __init__.py              # NovaMobilePayload discriminated union
│   ├── envelope/
│   │   ├── app_record.py            # NovaTelemetryRecord envelope
│   │   └── device_record.py         # SentinelTelemetryRecord envelope
│   └── data-domains/
│       ├── device_pairing_record.py # Flat schema: device pairing event
│       ├── user_auth_record.py      # Flat schema: user authentication event
│       └── device_session_record.py # Flat schema: device session event
├── lambda_src/
│   ├── ingest_handler.py            # Lambda handler (API GW → Firehose/S3)
│   └── flatten.py                   # Record flattening utilities
└── tests/
    ├── data/
    │   ├── device-pairing-record.json
    │   ├── user-auth-record.json
    │   └── device-session-record.json
    ├── test_nova_record.py           # NovaTelemetryRecord unit tests
    ├── test_sentinel_record.py       # SentinelTelemetryRecord unit tests
    └── test_flatten_record.py        # flatten_nova_record unit tests
```

---

## Stack

| Component | Technology |
|---|---|
| Language | Python 3.13 |
| Schema validation | Pydantic v2 |
| Lambda runtime | AWS Lambda (arm64) |
| Observability | AWS Lambda Powertools v3 |
| Ingest pipeline | API Gateway → Lambda → Kinesis Firehose → S3 |
| Analytics | AWS Athena, Amazon Redshift |
| Device payload format | Protobuf (base64-encoded) |
| AWS SDK | boto3 |

---

## Running Tests

```bash
pip install -r requirements.txt
pytest tests/ -v
```

---

## Environment Variables (Lambda)

| Variable | Description |
|---|---|
| `FIREHOSE_NOVA_STREAM` | Kinesis Firehose delivery stream name for nova records |
| `FIREHOSE_SENTINEL_STREAM` | Kinesis Firehose delivery stream name for sentinel records |
| `QUARANTINE_BUCKET` | S3 bucket name for invalid record quarantine |
| `QUARANTINE_PREFIX` | S3 key prefix for quarantine objects (default: `quarantine/`) |
| `AWS_REGION` | AWS region |
| `POWERTOOLS_SERVICE_NAME` | Lambda Powertools service name tag |
| `POWERTOOLS_METRICS_NAMESPACE` | CloudWatch metrics namespace |
