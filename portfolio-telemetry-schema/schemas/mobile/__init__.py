from __future__ import annotations

from typing import Annotated, Union

from pydantic import Field

from schemas.mobile.device_pair import DevicePairingPayload
from schemas.mobile.user_auth import UserAuthPayload
from schemas.mobile.device_session import DeviceSessionPayload

# Discriminated union of all nova mobile event payload types.
# Pydantic uses the `event_name` literal field to route each incoming payload
# to the correct model at validation time — no explicit if/elif dispatch needed.
NovaMobilePayload = Annotated[
    Union[
        DevicePairingPayload,
        UserAuthPayload,
        DeviceSessionPayload,
    ],
    Field(discriminator="event_name"),
]

__all__ = [
    "NovaMobilePayload",
    "DevicePairingPayload",
    "UserAuthPayload",
    "DeviceSessionPayload",
]
