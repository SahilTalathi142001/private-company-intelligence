from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
import json
import uuid


EVENT_TYPES = {
    "COMPANY_ONBOARDED",
    "SOURCE_ENABLED",
    "SOURCE_DISABLED",
    "NEW_SEC_FILING",
    "SOURCE_DATA_AVAILABLE",
    "DOCUMENT_RECEIVED",
    "PIPELINE_REQUESTED",
    "PIPELINE_COMPLETED",
    "PIPELINE_FAILED",
    "DQ_FAILED",
    "DQ_PROMOTED",
}

EVENT_STATUSES = {
    "PENDING",
    "PROCESSING",
    "COMPLETED",
    "FAILED",
    "QUARANTINED",
}


@dataclass
class PlatformEvent:
    event_type: str
    company_id: str
    source_id: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)

    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_version: str = "1.0"
    occurred_at_utc: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    correlation_id: str = field(
        default_factory=lambda: str(uuid.uuid4())
    )
    causation_id: str | None = None
    status: str = "PENDING"
    retry_count: int = 0

    def validate(self) -> None:
        if self.event_type not in EVENT_TYPES:
            raise ValueError(
                f"Invalid event_type: {self.event_type}"
            )

        if not self.company_id:
            raise ValueError("company_id is required")

        if self.status not in EVENT_STATUSES:
            raise ValueError(
                f"Invalid event status: {self.status}"
            )

        if not self.event_id:
            raise ValueError("event_id is required")

        if not self.correlation_id:
            raise ValueError("correlation_id is required")

    def to_dict(self) -> dict[str, Any]:
        self.validate()

        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "event_version": self.event_version,
            "occurred_at_utc": self.occurred_at_utc,
            "source_id": self.source_id,
            "company_id": self.company_id,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "status": self.status,
            "payload": self.payload,
            "retry_count": self.retry_count,
        }

    def to_record(self) -> dict[str, Any]:
        record = self.to_dict()
        record["payload"] = json.dumps(
            record["payload"],
            sort_keys=True,
        )
        return record