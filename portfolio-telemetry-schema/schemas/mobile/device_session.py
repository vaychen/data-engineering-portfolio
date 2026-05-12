from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, field_validator

_NUMERIC_VERSION_RE = re.compile(r"^\d+(\.\d+)*$")
_SEMVER_RE = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
)


class DeviceSessionPayload(BaseModel):
    """Emitted once per app session when the nova client initialises.

    Captures the device environment so that all subsequent events in the
    session can be attributed to a specific OS/hardware configuration.
    """

    event_name: Literal["device_session_record"]
    sdk_version: str
    device_os: Literal["Android", "iOS", "HarmonyOS"]
    device_os_version: str  # e.g. "17.4.1" or "14"
    device_brand: str
    device_model: str

    @field_validator("sdk_version")
    @classmethod
    def validate_sdk_semver(cls, v: str) -> str:
        if not _SEMVER_RE.match(v):
            raise ValueError(f"sdk_version must be a valid semver string, got: {v!r}")
        return v

    @field_validator("device_os_version")
    @classmethod
    def validate_os_version(cls, v: str) -> str:
        if not _NUMERIC_VERSION_RE.match(v):
            raise ValueError(
                f"device_os_version must be a numeric version string (e.g. '17.4.1'), got: {v!r}"
            )
        return v
