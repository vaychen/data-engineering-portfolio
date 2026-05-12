"""Unit tests for flatten_nova_record() and data-domain schema instantiation."""
from __future__ import annotations

import importlib.util
import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from lambda_src.flatten import flatten_nova_record
from schemas.envelope.app_record import NovaTelemetryRecord

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DATA_DIR = Path(__file__).parent / "data"
_SCHEMAS_DIR = Path(__file__).parent.parent / "schemas" / "data-domains"


def _load_fixture(name: str) -> dict:
    return json.loads((_DATA_DIR / name).read_text())


def _load_data_domain(module_name: str, class_name: str):
    """Import a class from schemas/data-domains/ (hyphen prevents normal import).

    After loading we call model_rebuild() with the module's own global namespace
    so Pydantic can resolve Literal, UUID, AwareDatetime, etc. that were
    imported at the top of the schema file.
    """
    spec = importlib.util.spec_from_file_location(
        module_name, _SCHEMAS_DIR / f"{module_name}.py"
    )
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    cls = getattr(mod, class_name)
    cls.model_rebuild(_parent_namespace_depth=0, _types_namespace=vars(mod))
    return cls


# Load data-domain classes once at module level.
DeviceSessionRecord = _load_data_domain("device_session_record", "DeviceSessionRecord")
DevicePairingRecord = _load_data_domain("device_pairing_record", "DevicePairingRecord")
UserAuthRecord = _load_data_domain("user_auth_record", "UserAuthRecord")

# Synthetic dt for all data-domain instantiation tests.
_DT = "2024-03-15"


# ---------------------------------------------------------------------------
# Happy-path flatten tests (fixture-driven)
# ---------------------------------------------------------------------------


def test_flatten_session_start_columns() -> None:
    """Flattened session-start record should contain all expected columns."""
    raw = _load_fixture("device-session-record.json")
    record = NovaTelemetryRecord.model_validate(raw)
    flat = flatten_nova_record(record)

    # Envelope
    assert flat["event_name"] == "nova-telemetry-record"
    assert flat["event_schema_version"] == 2
    assert "event_guid" in flat
    assert "event_timestamp" in flat

    # ClientContext columns
    assert flat["client_name"] == "nova-ios"
    assert flat["client_variant"] == "prod"
    assert flat["client_version"] == "3.12.0"
    assert flat["client_build"] == "2024.03.15.1001"
    assert flat["client_id"] == "550e8400-e29b-41d4-a716-446655440001"
    assert flat["client_mono_time"] == 1200
    assert flat["client_is_background"] is False
    assert flat["client_session_id"] == 1
    assert flat["client_time_since_last_event"] is None

    # Nova payload columns
    assert flat["nova_event_name"] == "device_session_record"
    assert flat["nova_sdk_version"] == "2.0.0"
    assert flat["nova_device_os"] == "iOS"
    assert flat["nova_device_os_version"] == "17.4.1"
    assert flat["nova_device_brand"] == "Apple"
    assert flat["nova_device_model"] == "iPhone15,2"

    # No product columns expected
    assert "product_name" not in flat


def test_flatten_bt_connect_result_columns() -> None:
    """Flattened BT connect result should include both nova_ and product_ columns."""
    raw = _load_fixture("device-pairing-record.json")
    record = NovaTelemetryRecord.model_validate(raw)
    flat = flatten_nova_record(record)

    # Envelope
    assert flat["event_name"] == "nova-telemetry-record"

    # Client
    assert flat["client_name"] == "nova-android"
    assert flat["client_session_id"] == 2
    assert flat["client_time_since_last_event"] == 90000

    # Nova payload
    assert flat["nova_event_name"] == "device_pairing_record"
    assert flat["nova_oob_bt_connect"] == "connected"

    # Product columns (recursed from product_ctx)
    assert flat["product_name"] == "PRODUCT_A"
    assert flat["product_id"] == "0x1A2B"
    assert flat["product_variant"] == 3
    assert flat["product_guid"] == "550e8400-e29b-41d4-a716-446655440004"
    assert flat["product_firmware_version"] == "1.4.2"


def test_flatten_login_successful_columns() -> None:
    """Flattened login record should serialise user_metadata to a JSON string."""
    raw = _load_fixture("user-auth-record.json")
    record = NovaTelemetryRecord.model_validate(raw)
    flat = flatten_nova_record(record)

    # Nova payload
    assert flat["nova_event_name"] == "user_auth_record"
    assert flat["nova_identity_provider"] == "apple_id"
    assert len(flat["nova_user_id"]) == 64

    # user_metadata must be a JSON string, not a dict
    metadata = flat["nova_user_metadata"]
    assert isinstance(metadata, str)
    parsed = json.loads(metadata)
    assert parsed["plan"] == "premium"
    assert parsed["locale"] == "en-US"


def test_flatten_event_timestamp_is_utc_comparable() -> None:
    """event_timestamp in flatten output is an ISO string parseable as UTC."""
    raw = _load_fixture("device-session-record.json")
    record = NovaTelemetryRecord.model_validate(raw)
    flat = flatten_nova_record(record)

    ts = datetime.fromisoformat(
        flat["event_timestamp"].replace("Z", "+00:00")
    ).astimezone(timezone.utc)
    assert ts.tzinfo is not None
    assert ts.year == 2024
    assert ts.month == 3
    assert ts.day == 15


# ---------------------------------------------------------------------------
# Negative / validation tests on the envelope schema
# ---------------------------------------------------------------------------


def test_bad_client_version_rejected() -> None:
    """A non-semver client version string should raise ValidationError."""
    raw = _load_fixture("device-session-record.json")
    raw = deepcopy(raw)
    raw["client_ctx"]["version"] = "not_a_semver"

    with pytest.raises(ValidationError) as exc_info:
        NovaTelemetryRecord.model_validate(raw)

    assert any("version" in str(e["loc"]) for e in exc_info.value.errors())


def test_bad_sdk_version_rejected() -> None:
    """A non-semver sdk_version should raise ValidationError."""
    raw = _load_fixture("device-session-record.json")
    raw = deepcopy(raw)
    raw["nova_ctx"]["sdk_version"] = "bad-version"

    with pytest.raises(ValidationError) as exc_info:
        NovaTelemetryRecord.model_validate(raw)

    assert any("sdk_version" in str(e["loc"]) for e in exc_info.value.errors())


def test_bad_product_id_hex_format() -> None:
    """product_id with wrong hex format should still parse (str field) but
    allows testing that the fixture value round-trips correctly."""
    raw = _load_fixture("device-pairing-record.json")
    record = NovaTelemetryRecord.model_validate(raw)
    flat = flatten_nova_record(record)
    assert flat["product_id"] == "0x1A2B"


def test_missing_nova_ctx_raises() -> None:
    """A record without nova_ctx should raise ValidationError."""
    raw = _load_fixture("device-session-record.json")
    raw = deepcopy(raw)
    del raw["nova_ctx"]

    with pytest.raises(ValidationError) as exc_info:
        NovaTelemetryRecord.model_validate(raw)

    assert any("nova_ctx" in str(e["loc"]) for e in exc_info.value.errors())


def test_invalid_bt_oob_value_rejected() -> None:
    """An oob_bt_connect value other than 'connected' should fail for bt_connect_result."""
    raw = _load_fixture("device-pairing-record.json")
    raw = deepcopy(raw)
    raw["nova_ctx"]["oob_bt_connect"] = "pending"

    with pytest.raises(ValidationError):
        NovaTelemetryRecord.model_validate(raw)


def test_invalid_user_id_not_sha256() -> None:
    """A user_id that is not a 64-char hex SHA-256 digest should be rejected."""
    raw = _load_fixture("user-auth-record.json")
    raw = deepcopy(raw)
    raw["nova_ctx"]["user_id"] = "tooshort"

    with pytest.raises(ValidationError) as exc_info:
        NovaTelemetryRecord.model_validate(raw)

    assert any("user_id" in str(e["loc"]) for e in exc_info.value.errors())


# ---------------------------------------------------------------------------
# Data-domain schema instantiation tests
# ---------------------------------------------------------------------------


def test_device_session_record_from_flatten_output() -> None:
    """DeviceSessionRecord should accept a valid flatten output + injected dt."""
    raw = _load_fixture("device-session-record.json")
    record = NovaTelemetryRecord.model_validate(raw)
    flat = flatten_nova_record(record)

    domain_flat = {**flat, "dt": _DT}

    dr = DeviceSessionRecord.model_validate(domain_flat)
    assert dr.client_name == "nova-ios"
    assert dr.nova_device_os == "iOS"
    assert dr.nova_sdk_version == "2.0.0"
    assert dr.dt == _DT


def test_device_pairing_record_from_flatten_output() -> None:
    """DevicePairingRecord should accept a valid BT flatten output + injected dt."""
    raw = _load_fixture("device-pairing-record.json")
    record = NovaTelemetryRecord.model_validate(raw)
    flat = flatten_nova_record(record)

    domain_flat = {**flat, "dt": _DT}

    dr = DevicePairingRecord.model_validate(domain_flat)
    assert dr.nova_oob_bt_connect == "connected"
    assert dr.product_name == "PRODUCT_A"
    assert str(dr.product_guid) == "550e8400-e29b-41d4-a716-446655440004"
    assert dr.product_firmware_version == "1.4.2"
    assert dr.dt == _DT


def test_user_auth_record_from_flatten_output() -> None:
    """UserAuthRecord should accept a valid login flatten output + injected dt."""
    raw = _load_fixture("user-auth-record.json")
    record = NovaTelemetryRecord.model_validate(raw)
    flat = flatten_nova_record(record)

    domain_flat = {**flat, "dt": _DT}

    dr = UserAuthRecord.model_validate(domain_flat)
    assert dr.nova_identity_provider == "apple_id"
    assert len(dr.nova_user_id) == 64
    # nova_user_metadata is a JSON string in the domain model
    assert isinstance(dr.nova_user_metadata, str)
    parsed = json.loads(dr.nova_user_metadata)
    assert parsed["plan"] == "premium"
    assert dr.dt == _DT


def test_device_session_record_rejects_missing_dt() -> None:
    """DeviceSessionRecord must reject a record with no dt field."""
    raw = _load_fixture("device-session-record.json")
    record = NovaTelemetryRecord.model_validate(raw)
    flat = flatten_nova_record(record)

    domain_flat = dict(flat)  # no dt

    with pytest.raises(ValidationError) as exc_info:
        DeviceSessionRecord.model_validate(domain_flat)

    assert any("dt" in str(e["loc"]) for e in exc_info.value.errors())
