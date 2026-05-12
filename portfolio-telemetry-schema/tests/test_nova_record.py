"""Unit tests for NovaTelemetryRecord schema validation."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from schemas.envelope.app_record import NovaTelemetryRecord
from schemas.mobile.device_pair import DevicePairingPayload
from schemas.mobile.user_auth import UserAuthPayload
from schemas.mobile.device_session import DeviceSessionPayload

# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------

_CLIENT_CTX = {
    "name": "nova-ios",
    "variant": "prod",
    "version": "3.12.0",
    "build": "2024.03.15.1001",
    "client_id": "550e8400-e29b-41d4-a716-446655440000",
    "mono_time": 123456,
    "is_background": False,
    "session_id": 7,
    "time_since_last_event": 500,
}

_PRODUCT_CTX_EXTEND = {
    "name": "PRODUCT_A",
    "product_id": "0x1A2B",
    "variant": 3,
    "guid": "550e8400-e29b-41d4-a716-446655440001",
    "firmware_version": "1.4.2",
}

_NOW = datetime.now(tz=timezone.utc).isoformat()


def _base_envelope(nova_ctx: dict) -> dict:
    return {
        "event_name": "nova-telemetry-record",
        "event_schema_version": 2,
        "event_guid": str(uuid.uuid4()),
        "event_timestamp": _NOW,
        "client_ctx": _CLIENT_CTX,
        "nova_ctx": nova_ctx,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_valid_device_pairing() -> None:
    """A well-formed device pairing record should validate without error."""
    record_dict = _base_envelope(
        {
            "event_name": "device_pairing_record",
            "oob_bt_connect": "connected",
            "product_ctx": _PRODUCT_CTX_EXTEND,
        }
    )
    record = NovaTelemetryRecord.model_validate(record_dict)

    assert record.event_name == "nova-telemetry-record"
    assert record.event_schema_version == 2
    assert record.client_ctx.name == "nova-ios"
    assert record.client_ctx.session_id == 7
    assert isinstance(record.nova_ctx, DevicePairingPayload)
    assert record.nova_ctx.oob_bt_connect == "connected"
    assert record.nova_ctx.product_ctx.firmware_version == "1.4.2"


def test_valid_user_auth() -> None:
    """A well-formed user auth record should validate without error."""
    user_id = "a" * 64  # valid 64-char hex SHA-256
    record_dict = _base_envelope(
        {
            "event_name": "user_auth_record",
            "identity_provider": "apple_id",
            "user_id": user_id,
            "user_metadata": {"plan": "premium"},
        }
    )
    record = NovaTelemetryRecord.model_validate(record_dict)

    assert isinstance(record.nova_ctx, UserAuthPayload)
    assert record.nova_ctx.identity_provider == "apple_id"
    assert record.nova_ctx.user_id == user_id
    assert record.nova_ctx.user_metadata == {"plan": "premium"}


def test_valid_device_session() -> None:
    """A well-formed device session record should validate without error."""
    record_dict = _base_envelope(
        {
            "event_name": "device_session_record",
            "sdk_version": "2.0.0",
            "device_os": "iOS",
            "device_os_version": "17.4.1",
            "device_brand": "Apple",
            "device_model": "iPhone15,2",
        }
    )
    record = NovaTelemetryRecord.model_validate(record_dict)

    assert isinstance(record.nova_ctx, DeviceSessionPayload)
    assert record.nova_ctx.device_os == "iOS"
    assert record.nova_ctx.sdk_version == "2.0.0"


def test_invalid_event_name() -> None:
    """Wrong top-level event_name should raise ValidationError."""
    record_dict = _base_envelope(
        {
            "event_name": "device_pairing_record",
            "oob_bt_connect": "connected",
            "product_ctx": _PRODUCT_CTX_EXTEND,
        }
    )
    record_dict["event_name"] = "wrong-event-name"

    with pytest.raises(ValidationError) as exc_info:
        NovaTelemetryRecord.model_validate(record_dict)

    errors = exc_info.value.errors()
    assert any(e["loc"] == ("event_name",) for e in errors)


def test_invalid_client_id() -> None:
    """Malformed UUID in client_ctx.client_id should raise ValidationError."""
    record_dict = _base_envelope(
        {
            "event_name": "device_pairing_record",
            "oob_bt_connect": "connected",
            "product_ctx": _PRODUCT_CTX_EXTEND,
        }
    )
    record_dict["client_ctx"] = {**_CLIENT_CTX, "client_id": "not-a-uuid"}

    with pytest.raises(ValidationError) as exc_info:
        NovaTelemetryRecord.model_validate(record_dict)

    errors = exc_info.value.errors()
    assert any("client_id" in str(e["loc"]) for e in errors)


def test_invalid_product_guid() -> None:
    """A product GUID that is not a valid UUID should raise ValidationError."""
    bad_ctx = {**_PRODUCT_CTX_EXTEND, "guid": "not-a-valid-guid-at-all"}
    record_dict = _base_envelope(
        {
            "event_name": "device_pairing_record",
            "oob_bt_connect": "connected",
            "product_ctx": bad_ctx,
        }
    )

    with pytest.raises(ValidationError) as exc_info:
        NovaTelemetryRecord.model_validate(record_dict)

    errors = exc_info.value.errors()
    assert any("guid" in str(e["loc"]) for e in errors)


def test_missing_required_field() -> None:
    """A record missing event_guid should raise ValidationError."""
    record_dict = _base_envelope(
        {
            "event_name": "device_pairing_record",
            "oob_bt_connect": "connected",
            "product_ctx": _PRODUCT_CTX_EXTEND,
        }
    )
    del record_dict["event_guid"]

    with pytest.raises(ValidationError) as exc_info:
        NovaTelemetryRecord.model_validate(record_dict)

    errors = exc_info.value.errors()
    assert any("event_guid" in str(e["loc"]) for e in errors)


def test_discriminated_union_resolution() -> None:
    """The discriminated union should resolve to the correct payload subtype."""
    record_pairing = NovaTelemetryRecord.model_validate(
        _base_envelope(
            {
                "event_name": "device_pairing_record",
                "oob_bt_connect": "connected",
                "product_ctx": _PRODUCT_CTX_EXTEND,
            }
        )
    )
    assert isinstance(record_pairing.nova_ctx, DevicePairingPayload)

    record_auth = NovaTelemetryRecord.model_validate(
        _base_envelope(
            {
                "event_name": "user_auth_record",
                "identity_provider": "google",
                "user_id": "b" * 64,
            }
        )
    )
    assert isinstance(record_auth.nova_ctx, UserAuthPayload)

    record_session = NovaTelemetryRecord.model_validate(
        _base_envelope(
            {
                "event_name": "device_session_record",
                "sdk_version": "2.0.0",
                "device_os": "Android",
                "device_os_version": "14",
                "device_brand": "Samsung",
                "device_model": "SM-S928B",
            }
        )
    )
    assert isinstance(record_session.nova_ctx, DeviceSessionPayload)
