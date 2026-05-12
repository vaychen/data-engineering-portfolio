from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from schemas.context.product_ctx import ExtendProductContext


class DevicePairingPayload(BaseModel):
    """Bluetooth connection completed successfully.

    Requires ExtendProductContext because the firmware version and device
    GUID are available after a successful handshake.
    """

    event_name: Literal["device_pairing_record"]
    oob_bt_connect: Literal["connected"]
    product_ctx: ExtendProductContext
