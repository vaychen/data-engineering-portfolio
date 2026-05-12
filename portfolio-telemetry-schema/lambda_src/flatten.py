from __future__ import annotations

import json
from typing import Any

from schemas.envelope.app_record import NovaTelemetryRecord
from schemas.envelope.device_record import SentinelTelemetryRecord


def extract_prefix(key_name: str, sep: str = "_") -> str:
    """Return the first segment of a snake_case key name.

    Used to derive a clean column prefix from a context key so that nested
    fields do not accumulate the full key path as a prefix.

    Examples
    --------
    >>> extract_prefix("client_ctx")
    'client'
    >>> extract_prefix("nova_ctx")
    'nova'
    >>> extract_prefix("product_ctx")
    'product'
    """
    return key_name.split(sep)[0]


def unnest_context(value: dict[str, Any], prefix: str | None = None) -> dict[str, Any]:
    """Recursively flatten a nested dict into a single-level dict.

    For each key ``k`` and value ``v``:
    - If ``v`` is a nested dict, recurse using only the *first segment* of
      ``k`` as the new prefix (via ``extract_prefix``).  This prevents the
      full accumulated key path from doubling up on already-prefixed field
      names (e.g. ``client_ctx.client_id`` → ``client_id``, not
      ``client_client_id``).
    - Scalar values are emitted with the current ``prefix`` applied, unless
      the key already starts with the prefix (again preventing doubling).

    Notes
    -----
    The raw nested-dict value is also stored under its prefixed key alongside
    the recursively expanded fields.  Downstream consumers (SQL columns,
    Pydantic models) simply ignore the extra key.

    Examples
    --------
    >>> unnest_context({"name": "nova-ios", "client_id": "abc"}, prefix="client")
    {"client_name": "nova-ios", "client_id": "abc"}

    >>> unnest_context({"product_ctx": {"guid": "x", "product_id": "0x1A2B"}}, prefix="nova")
    {"nova_product_ctx": {...}, "product_guid": "x", "product_id": "0x1A2B"}
    """
    data: dict[str, Any] = {}

    for k, v in value.items():
        if isinstance(v, dict):
            # Recurse using only the immediate key's first segment as prefix,
            # so nested keys are not double-prefixed.
            data.update(unnest_context(value=v, prefix=extract_prefix(k)))

        if prefix is not None:
            if k.startswith(prefix):
                data[k] = v
            else:
                data[f"{prefix}_{k}"] = v
        else:
            data[k] = v

    return data


def flatten_nova_record(raw_record: NovaTelemetryRecord) -> dict[str, Any]:
    """Flatten a validated NovaTelemetryRecord into a single-level dict.

    Flattening rules
    ----------------
    - Envelope fields (``event_name``, ``event_schema_version``,
      ``event_guid``, ``event_timestamp``) are kept at the top level.
    - ``client_ctx`` fields are prefixed with ``client_``.
      Because ``client_ctx.client_id`` already starts with ``client``, it
      is emitted as ``client_id`` (no double-prefix).
    - ``nova_ctx`` fields are prefixed with ``nova_``.
    - Nested ``product_ctx`` inside ``nova_ctx`` is recursed with prefix
      ``product_``, producing ``product_name``, ``product_id``,
      ``product_guid``, ``product_firmware_version``, etc.
    - ``user_metadata`` (arbitrary dict on ``nova_login_successful``) is
      serialised to a JSON string before flattening to prevent unpredictable
      column expansion.

    The resulting dict is suitable for writing as a newline-delimited JSON
    record consumed by Firehose → S3 → Glue → Redshift Spectrum.

    Column name examples
    --------------------
    ClientContext  →  client_name, client_variant, client_version,
                      client_build, client_id, client_mono_time,
                      client_is_background, client_session_id,
                      client_time_since_last_event
    NovaSessionStart → nova_event_name, nova_sdk_version,
                       nova_device_os, nova_device_os_version,
                       nova_device_brand, nova_device_model
    NovaBtConnectResult → nova_event_name, nova_oob_bt_connect,
                          product_name, product_id, product_variant,
                          product_guid, product_firmware_version
    NovaLoginSuccessful → nova_event_name, nova_identity_provider,
                          nova_user_id, nova_user_metadata (JSON string)
    """
    dumped = raw_record.model_dump(mode="json")

    # Serialise user_metadata to JSON before flattening so the arbitrary
    # key-value pairs inside it do not expand into top-level columns.
    nova_ctx = dumped.get("nova_ctx", {})
    if "user_metadata" in nova_ctx and isinstance(nova_ctx["user_metadata"], dict):
        nova_ctx = {**nova_ctx, "user_metadata": json.dumps(nova_ctx["user_metadata"], default=str)}
        dumped = {**dumped, "nova_ctx": nova_ctx}

    return unnest_context(dumped)


def flatten_sentinel_record(
    raw_record: SentinelTelemetryRecord,
    payload_json: dict[str, Any],
) -> dict[str, Any]:
    """Flatten a validated SentinelTelemetryRecord into a single-level dict.

    In addition to the envelope and context flattening performed for nova
    records, this function:

    - Extracts ``schema_name`` from the ``schema_id`` field (the part before
      the ``-<sha>`` suffix), making it easy to partition or filter by event
      type in Athena/Redshift.
    - Stores ``raw_payload`` as the original base64 string.
    - Stores ``payload`` as a JSON-serialised string of the decoded protobuf
      data (supplied by the caller as ``payload_json`` after proto decoding).

    Parameters
    ----------
    raw_record:
        The validated SentinelTelemetryRecord envelope.
    payload_json:
        The protobuf payload decoded into a plain Python dict by the caller.
    """
    dumped = raw_record.model_dump(mode="json")

    sentinel_ctx = dumped.get("sentinel_ctx", {}).copy()
    raw_payload_b64: str = sentinel_ctx.pop("payload", "")
    schema_id: str = sentinel_ctx.get("schema_id", "")

    # Derive the human-readable schema name by stripping the commit-sha suffix.
    # e.g. "bt_connectivity-a1b2c3d" → "bt_connectivity"
    schema_name = schema_id.rsplit("-", 1)[0] if "-" in schema_id else schema_id

    flat = unnest_context({**dumped, "sentinel_ctx": sentinel_ctx})

    flat["schema_name"] = schema_name
    flat["raw_payload"] = raw_payload_b64
    flat["payload"] = json.dumps(payload_json, default=str)

    return flat
