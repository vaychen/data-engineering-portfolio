from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, PositiveInt


class DeviceSessionRecord(BaseModel):
    """Flattened schema for a mobile client session initialisation record.

    Emitted once per app session when the client starts up. Captures the
    device environment so that all subsequent events in the session can be
    attributed to a specific OS and hardware configuration.

    Column derivation
    -----------------
    Envelope fields  →  top-level, no prefix change.
    ClientContext    →  ``client_ctx`` key stripped; prefix ``client_`` applied
                        (``client_id`` already starts with ``client`` so no
                        double-prefix).
    Session payload  →  ``nova_ctx`` key stripped; prefix ``nova_`` applied.
    Partition        →  ``dt`` added by the Firehose S3 prefix.

    No ProductContext on this record type.
    """

    # ------------------------------------------------------------------
    # Envelope
    # ------------------------------------------------------------------
    event_name: Literal["nova-telemetry-record"]
    event_schema_version: Literal[2]
    event_guid: UUID
    event_timestamp: AwareDatetime

    # ------------------------------------------------------------------
    # ClientContext  (prefix: client_)
    # ------------------------------------------------------------------
    client_name: Literal["nova-ios", "nova-android", "nova-ohos"]
    client_variant: Literal["dev", "staging", "prod"]
    client_version: str
    client_build: str
    client_id: UUID
    client_mono_time: int          # ms since app start
    client_is_background: bool
    client_session_id: PositiveInt
    client_time_since_last_event: int | None = None

    # ------------------------------------------------------------------
    # Session payload  (prefix: nova_)
    # ------------------------------------------------------------------
    nova_event_name: Literal["device_session_record"]
    nova_sdk_version: str
    nova_device_os: Literal["Android", "iOS", "HarmonyOS"]
    nova_device_os_version: str
    nova_device_brand: str
    nova_device_model: str

    # ------------------------------------------------------------------
    # Partition
    # ------------------------------------------------------------------
    dt: str  # YYYY-MM-DD, injected by Firehose S3 prefix
