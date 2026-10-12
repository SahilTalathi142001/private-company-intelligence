from __future__ import annotations

from pathlib import Path
from typing import Optional
import json

from company_platform.events.event_store import EventStore
from company_platform.events.models import PlatformEvent
from company_platform.incremental.planner import IncrementalPlanner
from company_platform.pipeline_model.pipeline_registry import PipelineRegistry
from company_platform.run_control.run_manager import RunManager
from company_platform.run_control.sqlite_run_store import SQLiteRunStore
from company_platform.dq_gates.gate_engine import DQGateEngine
from company_platform.orchestrator.artifact_validator import validate_artifact
from company_platform.orchestrator.engine import Orchestrator
from company_platform.orchestrator.event_router import EventRouter
from company_platform.integration_v2.components import HANDLERS
from company_platform.registry.company_registry import (
    mark_onboarding_running, mark_onboarding_active, mark_onboarding_partial,
    mark_onboarding_failed,
)

CONTROL_PLANE_PRECOMPLETED = {"COMPANY_ONBOARDED": {"company_registry", "source_registry"}}

class EventConsumer:
    """A3 -> A6 -> A8 bridge with duplicate-delivery guards."""

    def __init__(self, event_store=None, pipeline_registry=None, router=None, orchestrator=None):
        self.event_store = event_store or EventStore()
        self.pipeline_registry = pipeline_registry or PipelineRegistry()
        self.router = router or EventRouter()
        self.orchestrator = orchestrator or self._build_orchestrator()

    def _build_orchestrator(self):
        planner = IncrementalPlanner(self.pipeline_registry)
        return Orchestrator(self.pipeline_registry, RunManager(store=SQLiteRunStore()), planner,
                            DQGateEngine(), component_handlers=HANDLERS,
                            dependency_artifact_validator=validate_artifact)

    def consume_event(self, event_id: str):
        existing = self.event_store.get(event_id)
        if existing is None:
            raise KeyError(f"Event not found: {event_id}")
        status = str(existing.get("status"))
        if status == "COMPLETED":
            return {"event_id": event_id, "event_status": status,
                    "duplicate": True, "action": "NO_OP_ALREADY_COMPLETED"}
        if status == "QUARANTINED":
            return {"event_id": event_id, "event_status": status,
                    "duplicate": True, "action": "NO_OP_QUARANTINED"}
        if status == "FAILED":
            return {"event_id": event_id, "event_status": status,
                    "duplicate": True, "action": "EXPLICIT_RETRY_REQUIRED"}
        try:
            row = self.event_store.claim(event_id)
        except ValueError:
            return {"event_id": event_id, "event_status": "PROCESSING",
                    "duplicate": True, "action": "NO_OP_ALREADY_CLAIMED"}
        return self._process(self._row_to_event(row), row.get("_claim_token"))

    def consume_next(self):
        row = self.event_store.claim_next()
        if row is None:
            return None
        return self._process(self._row_to_event(row), row.get("_claim_token"))

    def _process(self, event: PlatformEvent, claim_token=None):
        # Keep routing and validation inside the guarded section: failures here
        # must not strand an event until its lease expires.
        company_id = None
        try:
            route = self.router.route(event)
            if route is None:
                self.event_store.complete(event.event_id, claim_token=claim_token)
                return {"event_id": event.event_id, "status": "COMPLETED",
                        "reason": "EVENT_NOT_ORCHESTRATED", "duplicate": False}

            company_id = route.get("company_id")
            if not company_id:
                raise ValueError(f"Event {event.event_id} has no company_id")

            if event.event_type == "COMPANY_ONBOARDED":
                mark_onboarding_running(company_id)
            run, decision = self.orchestrator.create_run(
                company_id=company_id, correlation_id=event.correlation_id,
                change_type=route["change_type"], trigger_type="EVENT",
                trigger_event_id=event.event_id,source_id=event.source_id or route.get("source_id"),
                 metadata={
        "payload": event.payload or {},
    },
            )
            if (
                getattr(run, "status", None) == "FAILED"
                and hasattr(self.orchestrator, "retry_run")
            ):
                self.orchestrator.retry_run(run.run_id)
            self._precomplete_control_plane_tasks(run.run_id, event.event_type)
            run_status = self.orchestrator.execute_run(run.run_id)
            if event.event_type == "COMPANY_ONBOARDED":
                if run_status == "SUCCESS": mark_onboarding_active(company_id)
                elif run_status == "PARTIAL": mark_onboarding_partial(company_id)
                else: mark_onboarding_failed(company_id, f"Pipeline run status={run_status}")
            if run_status in {"SUCCESS", "PARTIAL"}:
                self.event_store.complete(event.event_id, claim_token=claim_token)
            elif run_status == "QUARANTINED":
                self.event_store.quarantine(event.event_id, claim_token=claim_token)
            else:
                self.event_store.fail(event.event_id, claim_token=claim_token)
            return {"event_id": event.event_id, "run_id": run.run_id,
                    "run_status": run_status, "decision": decision,
                    "event_status": "COMPLETED" if run_status in {"SUCCESS", "PARTIAL"} else run_status,
                    "duplicate": False}
        except Exception as exc:
            # Token fencing ensures this worker cannot fail an event reclaimed
            # by another worker. Preserve the original exception if ownership
            # was already lost while reporting the failure.
            try:
                self.event_store.fail(event.event_id, claim_token=claim_token)
            except RuntimeError:
                pass
            if event.event_type == "COMPANY_ONBOARDED" and company_id:
                mark_onboarding_failed(company_id, str(exc))
            raise

    def _precomplete_control_plane_tasks(self, run_id, event_type):
        components = CONTROL_PLANE_PRECOMPLETED.get(event_type, set())
        if not components: return
        tasks = self.orchestrator.run_manager.store.get_tasks_for_run(run_id)
        for component_id in components:
            rows = tasks[tasks["component_id"].astype(str) == component_id]
            if not rows.empty:
                self.orchestrator.run_manager.succeed_task(
                    rows.iloc[0]["task_id"], dq_status="CONTROL_PLANE_COMPLETED")

    @staticmethod
    def _row_to_event(row: dict) -> PlatformEvent:
        payload = row.get("payload")
        if isinstance(payload, str):
            try: payload = json.loads(payload.replace("'", '"'))
            except Exception: payload = {"raw_payload": payload}
        return PlatformEvent(
            event_id=str(row["event_id"]), event_type=str(row["event_type"]),
            event_version=str(row["event_version"]), occurred_at_utc=str(row["occurred_at_utc"]),
            source_id=None if row.get("source_id") in (None, "nan") else str(row.get("source_id")),
            company_id=None if row.get("company_id") in (None, "nan") else str(row.get("company_id")),
            correlation_id=str(row["correlation_id"]),
            causation_id=None if row.get("causation_id") in (None, "nan") else str(row.get("causation_id")),
            status=str(row["status"]), payload=payload if isinstance(payload, dict) else {"raw_payload": payload},
            retry_count=int(row.get("retry_count") or 0),
        )

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Process platform events")
    parser.add_argument("--event-id")
    parser.add_argument("--retry-event-id")
    parser.add_argument("--max-retries", type=int, default=3)
    args = parser.parse_args()
    consumer = EventConsumer()
    if args.retry_event_id:
        print(consumer.event_store.retry(args.retry_event_id, args.max_retries))
        return
    result = consumer.consume_event(args.event_id) if args.event_id else consumer.consume_next()
    if result is None:
        print("NO_PENDING_EVENTS"); return
    print("EVENT CONSUMER RESULT")
    print("=" * 70)
    for key, value in result.items(): print(f"{key}: {value}")

if __name__ == "__main__":
    main()
