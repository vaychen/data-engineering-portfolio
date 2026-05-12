from __future__ import annotations

import base64
import re
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, PositiveInt, field_validator

from schemas.context.client_ctx import ClientContext
from schemas.context.product_ctx import ExtendProductContext

_SCHEMA_ID_RE = re.compile(r"^[a-z_]+-[0-9a-f]{7}$")

# Maximum allowed decoded protobuf payload size (bytes).
_MAX_PAYLOAD_BYTES = 16_384


class SentinelDataContext(BaseModel):
    """Context block carrying the firmware telemetry payload from a sentinel device.

    The `payload` field contains the raw protobuf message, base64-encoded for
    safe transport over JSON. Validation checks that the string is valid base64
    and that the decoded length is within the allowed range.

    `schema_id` identifies both the logical event type and the specific protobuf
    schema version used to encode the payload, following the pattern:
        <event_type>-<7-char git sha>
    e.g. "bt_connectivity-a1b2c3d"
    """

    schema_id: str
    mono_time: int  # device uptime in milliseconds at time of event
    session_id: PositiveInt  # boot count; incremented on each device restart
    firmware_platform_id: str
    is_signed: bool
    is_encrypted: bool
    component_guid: UUID | None = None
    product_ctx: ExtendProductContext
    payload: str  # base64-encoded protobuf message

    @field_validator("schema_id")
    @classmethod
    def validate_schema_id(cls, v: str) -> str:
        if not _SCHEMA_ID_RE.match(v):
            raise ValueError(
                "schema_id must match pattern ^[a-z_]+-[0-9a-f]{7}$, "
                f"e.g. 'bt_connectivity-a1b2c3d', got: {v!r}"
            )
        return v

    @field_validator("payload")
    @classmethod
    def validate_payload(cls, v: str) -> str:
        try:
            decoded = base64.b64decode(v, validate=True)
        except Exception as exc:
            raise ValueError(
                f"payload must be a valid base64-encoded string: {exc}"
            ) from exc

        length = len(decoded)
        if length < 1:
            raise ValueError("payload decoded length must be at least 1 byte")
        if length > _MAX_PAYLOAD_BYTES:
            raise ValueError(
                f"payload decoded length {length} bytes exceeds maximum "
                f"{_MAX_PAYLOAD_BYTES} bytes"
            )
        return v


class SentinelTelemetryRecord(BaseModel):
    """Top-level envelope for a sentinel (IoT firmware) telemetry record.

    Sentinel devices emit protobuf-encoded telemetry payloads. The companion
    nova mobile app acts as a relay, forwarding the raw bytes alongside its own
    session context. The Lambda ingest handler decodes and validates this
    envelope before forwarding to Kinesis Firehose.

    Schema version 2 is the current supported version.
    """

    event_name: Literal["sentinel-telemetry-record"]
    event_schema_version: Literal[2]
    event_guid: UUID
    event_timestamp: AwareDatetime
    client_ctx: ClientContext
    sentinel_ctx: SentinelDataContext
