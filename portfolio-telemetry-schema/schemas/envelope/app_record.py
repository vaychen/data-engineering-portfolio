from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel

from schemas.context.client_ctx import ClientContext
from schemas.mobile import NovaMobilePayload


class NovaTelemetryRecord(BaseModel):
    """Top-level envelope for a nova (mobile app) telemetry record.

    This is the canonical schema validated at the Lambda ingest boundary.
    Each record carries exactly one mobile event payload inside `nova_ctx`,
    routed by the discriminated union on `event_name`.

    Schema version 2 is the current supported version. Records with an
    unsupported version are rejected at validation time and quarantined.
    """

    event_name: Literal["nova-telemetry-record"]
    event_schema_version: Literal[2]
    event_guid: UUID
    event_timestamp: AwareDatetime
    client_ctx: ClientContext
    nova_ctx: NovaMobilePayload
