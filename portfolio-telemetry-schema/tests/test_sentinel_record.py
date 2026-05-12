"""Unit tests for SentinelTelemetryRecord schema validation."""
from __future__ import annotations

import base64
import uuid
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from schemas.envelope.device_record import SentinelTelemetryRecord

# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------

_NOW = datetime.now(tz=timezone.utc).isoformat()

_CLIENT_CTX = {
    "name": "nova-android",
    "variant": "prod",
    "version": "3.12.0",
    "build": "2024.03.15.1001",
    "client_id": "550e8400-e29b-41d4-a716-446655440000",
    "mono_time": 98765,
    "is_background": False,
    "session_id": 2,
    "time_since_last_event": None,
}

_PRODUCT_CTX_EXTEND = {
    "name": "PRODUCT_B",
    "product_id": "0xDEAD",
    "variant": 1,
    "guid": "660e8400-e29b-41d4-a716-446655440002",
    "firmware_version": "2.0.1",
}

# A small valid protobuf-ish payload (≤ 16 384 bytes when decoded).
_VALID_PAYLOAD = base64.b64encode(b"\x08\x01\x12\x03abc").decode()

_VALID_SENTINEL_CTX = {
    "schema_id": "bt_connectivity-a1b2c3d",
    "mono_time": 500_000,
    "session_id": 5,
    "firmware_platform_id": "platform-arm-v1",
    "is_signed": True,
    "is_encrypted": False,
    "component_guid": None,
    "product_ctx": _PRODUCT_CTX_EXTEND,
    "payload": _VALID_PAYLOAD,
}


def _base_envelope(sentinel_ctx: dict | None = None) -> dict:
    return {
        "event_name": "sentinel-telemetry-record",
        "event_schema_version": 2,
        "event_guid": str(uuid.uuid4()),
        "event_timestamp": _NOW,
        "client_ctx": _CLIENT_CTX,
        "sentinel_ctx": sentinel_ctx or _VALID_SENTINEL_CTX,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_valid_record() -> None:
    """A fully populated valid sentinel record should parse without error."""
    record = SentinelTelemetryRecord.model_validate(_base_envelope())

    assert record.event_name == "sentinel-telemetry-record"
    assert record.event_schema_version == 2
    assert record.client_ctx.name == "nova-android"
    assert record.sentinel_ctx.schema_id == "bt_connectivity-a1b2c3d"
    assert record.sentinel_ctx.session_id == 5
    assert record.sentinel_ctx.is_signed is True
    assert record.sentinel_ctx.product_ctx.name == "PRODUCT_B"
    assert record.sentinel_ctx.product_ctx.firmware_version == "2.0.1"
    assert str(record.sentinel_ctx.product_ctx.guid) == "660e8400-e29b-41d4-a716-446655440002"


def test_invalid_schema_id_format() -> None:
    """schema_id not matching the required pattern should raise ValidationError."""
    bad_ctx = {**_VALID_SENTINEL_CTX, "schema_id": "BtConnectivity-ZZZZZZZZ"}

    with pytest.raises(ValidationError) as exc_info:
        SentinelTelemetryRecord.model_validate(_base_envelope(bad_ctx))

    errors = exc_info.value.errors()
    assert any("schema_id" in str(e["loc"]) for e in errors)


def test_payload_too_large() -> None:
    """A base64-encoded payload decoding to > 16 384 bytes should raise ValidationError."""
    oversized_raw = b"X" * 16_385
    oversized_b64 = base64.b64encode(oversized_raw).decode()
    bad_ctx = {**_VALID_SENTINEL_CTX, "payload": oversized_b64}

    with pytest.raises(ValidationError) as exc_info:
        SentinelTelemetryRecord.model_validate(_base_envelope(bad_ctx))

    errors = exc_info.value.errors()
    assert any("payload" in str(e["loc"]) for e in errors)
    assert any("16384" in str(e.get("msg", "")) for e in errors)


def test_payload_not_base64() -> None:
    """A payload string that is not valid base64 should raise ValidationError."""
    bad_ctx = {**_VALID_SENTINEL_CTX, "payload": "this is not base64!!!"}

    with pytest.raises(ValidationError) as exc_info:
        SentinelTelemetryRecord.model_validate(_base_envelope(bad_ctx))

    errors = exc_info.value.errors()
    assert any("payload" in str(e["loc"]) for e in errors)


def test_session_id_zero() -> None:
    """session_id=0 should be rejected because it must be a positive integer."""
    bad_ctx = {**_VALID_SENTINEL_CTX, "session_id": 0}

    with pytest.raises(ValidationError) as exc_info:
        SentinelTelemetryRecord.model_validate(_base_envelope(bad_ctx))

    errors = exc_info.value.errors()
    assert any("session_id" in str(e["loc"]) for e in errors)


def test_missing_product_guid() -> None:
    """ExtendProductContext requires guid; omitting it should raise ValidationError."""
    ctx_no_guid = {
        "name": "PRODUCT_B",
        "product_id": "0xDEAD",
        "variant": 1,
        # guid deliberately omitted
        "firmware_version": "2.0.1",
    }
    bad_ctx = {**_VALID_SENTINEL_CTX, "product_ctx": ctx_no_guid}

    with pytest.raises(ValidationError) as exc_info:
        SentinelTelemetryRecord.model_validate(_base_envelope(bad_ctx))

    errors = exc_info.value.errors()
    assert any("guid" in str(e["loc"]) for e in errors)
