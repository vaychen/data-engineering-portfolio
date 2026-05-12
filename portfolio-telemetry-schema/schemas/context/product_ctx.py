from __future__ import annotations

import re
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

_SEMVER_RE = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
)

_PRODUCT_ID_RE = re.compile(r"^0x[0-9A-Fa-f]{4}$")


class ProductContext(BaseModel):
    """Base product context — all fields optional.

    Used for early-funnel events where full device context may not yet be
    available (e.g. onboarding screens before a device is paired).
    """

    name: str | None = None
    product_id: str | None = None  # hex string, e.g. "0x1A2B"
    variant: Annotated[int, Field(ge=0, le=19)] | None = None
    guid: UUID | None = None
    firmware_version: str | None = None

    @field_validator("product_id")
    @classmethod
    def validate_product_id(cls, v: str | None) -> str | None:
        if v is not None and not _PRODUCT_ID_RE.match(v):
            raise ValueError(
                f"product_id must match pattern ^0x[0-9A-Fa-f]{{4}}$, got: {v!r}"
            )
        return v

    @field_validator("firmware_version")
    @classmethod
    def validate_firmware_semver(cls, v: str | None) -> str | None:
        if v is not None and not _SEMVER_RE.match(v):
            raise ValueError(
                f"firmware_version must be a valid semver string, got: {v!r}"
            )
        return v


class BasicProductContext(ProductContext):
    """Product context with mandatory identifying fields.

    Required for events where the device has been identified but full
    firmware metadata may not be available.
    """

    name: str
    product_id: str
    variant: Annotated[int, Field(ge=0, le=19)]


class ExtendProductContext(BasicProductContext):
    """Fully-qualified product context including firmware identity.

    Required for events emitted after a successful BT connection, where
    the firmware version and device GUID are known.
    """

    guid: UUID
    firmware_version: str

    @field_validator("firmware_version")
    @classmethod
    def validate_firmware_semver(cls, v: str) -> str:  # type: ignore[override]
        if not _SEMVER_RE.match(v):
            raise ValueError(
                f"firmware_version must be a valid semver string, got: {v!r}"
            )
        return v
