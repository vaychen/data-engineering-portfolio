from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, Field, PositiveInt


class DevicePairingRecord(BaseModel):
    """Flattened schema for a successful mobile-to-device pairing record.

    Emitted when the mobile client completes a Bluetooth connection handshake
    with a paired device. At this point the firmware version and device GUID
    are known, so the full product context is available.

    Column derivation
    -----------------
    Envelope fields  →  top-level, no prefix change.
    ClientContext    →  prefix ``client_``.
    Pairing payload  →  ``nova_ctx`` key stripped; prefix ``nova_``.
    ProductContext   →  ``product_ctx`` inside ``nova_ctx`` recursed with
                        prefix ``product_``. All product fields required.
    Partition        →  ``dt``.
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
    client_mono_time: int
    client_is_background: bool
    client_session_id: PositiveInt
    client_time_since_last_event: int | None = None

    # ------------------------------------------------------------------
    # Pairing payload  (prefix: nova_)
    # ------------------------------------------------------------------
    nova_event_name: Literal["device_pairing_record"]
    nova_oob_bt_connect: Literal["connected"]

    # ------------------------------------------------------------------
    # ProductContext  (prefix: product_) — all fields required
    # ------------------------------------------------------------------
    product_name: str
    product_id: str
    product_variant: Annotated[int, Field(ge=0, le=19)]
    product_guid: UUID
    product_firmware_version: str

    # ------------------------------------------------------------------
    # Partition
    # ------------------------------------------------------------------
    dt: str
