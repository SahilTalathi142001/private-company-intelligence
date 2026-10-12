from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Optional
import uuid

RUN_STATUSES = {"PENDING", "RUNNING", "SUCCESS", "FAILED", "QUARANTINED", "CANCELLED","PARTIAL"}
TASK_STATUSES = {"PENDING", "RUNNING", "SUCCESS", "FAILED", "QUARANTINED", "SKIPPED", "BLOCKED", "CAPABILITY_BLOCKED"}
TRIGGER_TYPES = {"EVENT", "MANUAL", "SCHEDULED", "RETRY"}

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def new_id():
    return str(uuid.uuid4())

@dataclass
class PipelineRun:
    run_id: str
    company_id: str
    trigger_type: str
    trigger_event_id: Optional[str]
    correlation_id: str
    pipeline_version: str
    status: str
    started_at_utc: Optional[str]
    completed_at_utc: Optional[str]
    error_message: Optional[str]
    source_id: Optional[str] = None
    metadata: dict = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        company_id,
        correlation_id,
        pipeline_version="1.0.0",
        trigger_type="MANUAL",
        trigger_event_id=None,
        source_id=None,
        metadata=None,
    ):
        if trigger_type not in TRIGGER_TYPES:
            raise ValueError(f"Invalid trigger_type: {trigger_type}")

        return cls(
            new_id(),
            company_id,
            trigger_type,
            trigger_event_id,
            correlation_id,
            pipeline_version,
            "PENDING",
            None,
            None,
            None,
            source_id,
            {} if metadata is None else metadata,
        )

    def validate(self):
        if not self.run_id or not self.company_id or not self.correlation_id:
            raise ValueError("run_id, company_id and correlation_id are required")
        if self.trigger_type not in TRIGGER_TYPES:
            raise ValueError(f"Invalid trigger_type: {self.trigger_type}")
        if self.status not in RUN_STATUSES:
            raise ValueError(f"Invalid run status: {self.status}")

    def to_dict(self):
        self.validate()
        return asdict(self)

    def __contains__(self, key):
        return key in self.__dataclass_fields__

    def __getitem__(self, key):
        if key not in self.__dataclass_fields__:
            raise KeyError(key)
        return getattr(self, key)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default


@dataclass
class TaskRun:
    task_id: str
    run_id: str
    company_id: str
    component_id: str
    task_sequence: int
    status: str
    attempt_number: int
    max_attempts: int
    started_at_utc: Optional[str]
    completed_at_utc: Optional[str]
    error_message: Optional[str]
    dq_status: Optional[str]
    blocked_by: Optional[str]

    @classmethod
    def create(cls, run_id, company_id, component_id, task_sequence, max_attempts=3):
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        return cls(new_id(), run_id, company_id, component_id, task_sequence, "PENDING", 0,
                   max_attempts, None, None, None, None, None)

    def validate(self):
        if not self.task_id or not self.run_id or not self.component_id:
            raise ValueError("task_id, run_id and component_id are required")
        if self.status not in TASK_STATUSES:
            raise ValueError(f"Invalid task status: {self.status}")
        if self.attempt_number < 0 or self.attempt_number > self.max_attempts:
            raise ValueError("attempt_number must be between 0 and max_attempts")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

    def to_dict(self):
        self.validate()
        return asdict(self)
