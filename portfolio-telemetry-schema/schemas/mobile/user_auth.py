from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, field_validator

_SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")


class UserAuthPayload(BaseModel):
    """User successfully authenticated."""

    event_name: Literal["user_auth_record"]
    identity_provider: Literal["apple_id", "anonymous", "wechat", "google"]
    user_id: str  # SHA-256 hex digest of the canonical user identifier
    user_metadata: dict | None = None

    @field_validator("user_id")
    @classmethod
    def validate_user_id(cls, v: str) -> str:
        if not _SHA256_HEX_RE.match(v):
            raise ValueError(
                "user_id must be a 64-character lowercase hex SHA-256 digest"
            )
        return v
