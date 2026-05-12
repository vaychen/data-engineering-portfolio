from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, PositiveInt


class UserAuthRecord(BaseModel):
    """Flattened schema for a successful user authentication record.

    Emitted when the mobile client completes an authentication flow via a
    third-party identity provider. The user identity is represented as a
    SHA-256 digest so no raw PII is stored in the pipeline.

    Column derivation
    -----------------
    Envelope fields  →  top-level, no prefix change.
    ClientContext    →  prefix ``client_``.
    Auth payload     →  ``nova_ctx`` key stripped; prefix ``nova_``.
    No ProductContext on this record type.
    Partition        →  ``dt``.

    Notes
    -----
    ``nova_user_metadata`` is an arbitrary key-value bag on the source schema.
    ``flatten_nova_record`` serialises it to a JSON string before flattening
    so it lands as a single TEXT column rather than expanding unpredictably.

    ``nova_user_id`` is a 64-character lowercase hex SHA-256 digest of the
    canonical user identifier.
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
    # Auth payload  (prefix: nova_)
    # ------------------------------------------------------------------
    nova_event_name: Literal["user_auth_record"]
    nova_identity_provider: Literal["apple_id", "anonymous", "wechat", "google"]
    nova_user_id: str                    # SHA-256 hex digest, 64 chars
    nova_user_metadata: str | None = None  # JSON-serialised dict

    # ------------------------------------------------------------------
    # Partition
    # ------------------------------------------------------------------
    dt: str
