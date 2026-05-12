from __future__ import annotations

import re
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, field_validator, PositiveInt

_SEMVER_RE = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
)


class ClientContext(BaseModel):
    """Context describing the nova mobile client that produced the event."""

    name: Literal["nova-ios", "nova-android", "nova-ohos"]
    variant: Literal["dev", "staging", "prod"]
    version: str
    build: str  # App build identifier, e.g. "2024.01.15.1234"
    client_id: UUID
    mono_time: int  # milliseconds since app start
    is_background: bool
    session_id: PositiveInt
    time_since_last_event: int | None = None

    @field_validator("version")
    @classmethod
    def validate_semver(cls, v: str) -> str:
        if not _SEMVER_RE.match(v):
            raise ValueError(f"version must be a valid semver string, got: {v!r}")
        return v
