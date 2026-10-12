from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from typing import Any
import uuid


@dataclass(frozen=True)
class ExecutionContext:
    run_id: str
    task_id: str
    company_id: str
    cik: str
    correlation_id: str
    trigger_event_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    

    
    @classmethod
    def from_task(cls, run: dict, task: dict) -> "ExecutionContext":
        if not isinstance(run, dict) and is_dataclass(run) and not isinstance(run, type):
            run = asdict(run)
        cik = str(run["company_id"]).replace("SEC_", "").zfill(10)

        source_id = run.get("source_id")
        if source_id is not None:
            source_id = str(source_id).strip().upper()

        metadata = run.get("metadata") or {}
        if not isinstance(metadata, dict):
            metadata = {}

        return cls(
        run_id=str(run["run_id"]),
        task_id=str(task["task_id"]),
        company_id=str(run["company_id"]),
        cik=cik,
        correlation_id=str(run["correlation_id"]),
        trigger_event_id=run.get("trigger_event_id"),
        metadata={
            "source_id": source_id,
            "payload": metadata.get("payload") or {},
        },
    )


    
    @staticmethod
    def new(company_id: str, trigger_event_id: str | None = None) -> "ExecutionContext":
        cik = str(company_id).replace("SEC_", "").zfill(10)
        run_id = str(uuid.uuid4())
        return ExecutionContext(
            run_id,
            "",
            f"SEC_{cik}",
            cik,
            str(uuid.uuid4()),
            trigger_event_id,
        )

