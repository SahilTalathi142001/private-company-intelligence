from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import uuid

from company_platform.events.event_store import EventStore
from company_platform.events.models import PlatformEvent
from company_platform.incremental.planner import IncrementalPlanner
from company_platform.pipeline_model.pipeline_registry import PipelineRegistry
from company_platform.orchestrator.event_consumer import EventConsumer
from company_platform.orchestrator.engine import Orchestrator
from company_platform.run_control.run_manager import RunManager
from company_platform.run_control.sqlite_run_store import SQLiteRunStore


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEST_DB_DIR = PROJECT_ROOT / "data" / "test" / "integration"
TEST_DB_DIR.mkdir(parents=True, exist_ok=True)


APPLE_CIK = "0000320193"
MICROSOFT_CIK = "0000789019"
APPLE = f"SEC_{APPLE_CIK}"
MICROSOFT = f"SEC_{MICROSOFT_CIK}"


def _db(name: str) -> Path:
    return TEST_DB_DIR / f"step22_{name}_{uuid.uuid4().hex}.db"


def _event(
    company_id: str,
    cik: str,
    *,
    event_type: str = "NEW_SEC_FILING",
    source_id: str = "SEC",
) -> PlatformEvent:
    return PlatformEvent(
        event_type=event_type,
        company_id=company_id,
        source_id=source_id,
        payload={
            "cik": cik,
            "accession_number": f"STEP22-{uuid.uuid4().hex}",
            "form": "4",
            "primary_document": "step22-test.xml",
        },
    )


class _FakeOrchestrator:
    """
    Minimal A8 contract used to test A3/A5 lifecycle behavior without
    executing the real business handlers.

    Step 22 separates orchestration guarantees from business-component tests.
    """

    def __init__(self, run_status: str = "SUCCESS"):
        self.run_status = run_status
        self.calls = []
        self._run_id = f"step22-run-{uuid.uuid4()}"

    def create_run(
        self,
        *,
        company_id,
        correlation_id,
        change_type,
        trigger_type,
        trigger_event_id,
        source_id,
        metadata=None,
    ):
        self.calls.append(
            {
                "operation": "create_run",
                "company_id": company_id,
                "correlation_id": correlation_id,
                "change_type": change_type,
                "trigger_type": trigger_type,
                "trigger_event_id": trigger_event_id,
                "source_id": source_id,
                "metadata": metadata,
            }
        )

        run = SimpleNamespace(run_id=self._run_id)
        decision = {
            "processing_mode": "INCREMENTAL",
            "root_component_id": "sec_ingestion",
            "affected_components": [
                "sec_ingestion",
                "document_ingestion",
                "document_extraction",
                "document_classification",
                "document_understanding",
                "structured_event_persistence",
                "company_state_projection",
                "company_state_investment_intelligence",
            ],
            "reason_code": "SEC_FILING_AFFECTS_FINANCIAL_AND_DOWNSTREAM_ARTIFACTS",
        }
        return run, decision

    def execute_run(self, run_id):
        self.calls.append(
            {
                "operation": "execute_run",
                "run_id": run_id,
            }
        )
        return self.run_status


class _RetryingFakeOrchestrator:
    def __init__(self):
        self.run_id = f"step22-retry-run-{uuid.uuid4()}"
        self.status = "PENDING"
        self.retry_calls = 0
        self.execute_calls = 0

    def create_run(self, **kwargs):
        return SimpleNamespace(run_id=self.run_id, status=self.status), {}

    def retry_run(self, run_id):
        assert run_id == self.run_id
        self.retry_calls += 1
        self.status = "PENDING"

    def execute_run(self, run_id):
        assert run_id == self.run_id
        self.execute_calls += 1
        self.status = "FAILED" if self.execute_calls == 1 else "SUCCESS"
        return self.status


def test_step22_sec_event_plan_reaches_full_event_driven_chain():
    """
    The production dependency graph must connect the SEC event root through
    the complete event-driven intelligence branch.
    """
    registry = PipelineRegistry()
    planner = IncrementalPlanner(registry)

    decision = planner.plan_for_change(
        change_type="NEW_SEC_FILING",
        source_id="SEC",
    )

    affected = decision["affected_components"]

    assert decision["processing_mode"] == "INCREMENTAL"
    assert decision["root_component_id"] == "sec_ingestion"

    expected = [
        "sec_ingestion",
        "document_ingestion",
        "document_extraction",
        "document_classification",
        "document_understanding",
        "structured_event_persistence",
        "company_state_projection",
        "company_state_investment_intelligence",
    ]

    for component_id in expected:
        assert component_id in affected, (
            f"Step 22 chain missing component: {component_id}; "
            f"plan={affected}"
        )


def test_step22_event_success_persists_run_lineage_and_completes_event():
    """
    EventStore -> EventConsumer -> Orchestrator contract.

    Verifies that the same event identity reaches the orchestration boundary
    with company/source/correlation lineage intact and the event becomes
    COMPLETED only after the run succeeds.
    """
    event_store = EventStore(path=str(_db("success")))
    event = _event(APPLE, APPLE_CIK)
    event_store.append(event)

    orchestrator = _FakeOrchestrator(run_status="SUCCESS")
    consumer = EventConsumer(
        event_store=event_store,
        orchestrator=orchestrator,
    )

    result = consumer.consume_event(event.event_id)

    assert result["event_id"] == event.event_id
    assert result["run_status"] == "SUCCESS"
    assert result["event_status"] == "COMPLETED"
    assert result["duplicate"] is False

    create_call = next(
        call for call in orchestrator.calls
        if call["operation"] == "create_run"
    )

    assert create_call["company_id"] == APPLE
    assert create_call["source_id"] == "SEC"
    assert create_call["trigger_event_id"] == event.event_id
    assert create_call["correlation_id"] == event.correlation_id
    assert create_call["trigger_type"] == "EVENT"
    assert create_call["metadata"]["payload"] == event.payload

    stored = event_store.get(event.event_id)
    assert stored is not None
    assert stored["status"] == "COMPLETED"


def test_step22_duplicate_event_is_noop_and_does_not_create_second_run():
    """
    Re-delivery of a completed event must not execute the orchestration again.
    """
    event_store = EventStore(path=str(_db("duplicate")))
    event = _event(APPLE, APPLE_CIK)
    event_store.append(event)

    orchestrator = _FakeOrchestrator(run_status="SUCCESS")
    consumer = EventConsumer(
        event_store=event_store,
        orchestrator=orchestrator,
    )

    first = consumer.consume_event(event.event_id)
    second = consumer.consume_event(event.event_id)

    assert first["duplicate"] is False
    assert first["event_status"] == "COMPLETED"

    assert second["duplicate"] is True
    assert second["action"] == "NO_OP_ALREADY_COMPLETED"
    assert second["event_status"] == "COMPLETED"

    create_calls = [
        call for call in orchestrator.calls
        if call["operation"] == "create_run"
    ]
    execute_calls = [
        call for call in orchestrator.calls
        if call["operation"] == "execute_run"
    ]

    assert len(create_calls) == 1
    assert len(execute_calls) == 1


def test_step22_failed_run_requires_explicit_retry():
    """
    A failed orchestration must become FAILED at the event layer and must not
    silently retry on the next delivery.
    """
    event_store = EventStore(path=str(_db("failure")))
    event = _event(APPLE, APPLE_CIK)
    event_store.append(event)

    orchestrator = _FakeOrchestrator(run_status="FAILED")
    consumer = EventConsumer(
        event_store=event_store,
        orchestrator=orchestrator,
    )

    result = consumer.consume_event(event.event_id)

    assert result["run_status"] == "FAILED"
    assert result["event_status"] == "FAILED"
    assert result["duplicate"] is False

    stored = event_store.get(event.event_id)
    assert stored is not None
    assert stored["status"] == "FAILED"

    replay = consumer.consume_event(event.event_id)

    assert replay["duplicate"] is True
    assert replay["action"] == "EXPLICIT_RETRY_REQUIRED"
    assert replay["event_status"] == "FAILED"

    execute_calls = [
        call for call in orchestrator.calls
        if call["operation"] == "execute_run"
    ]
    assert len(execute_calls) == 1


def test_step22_explicit_event_retry_resumes_existing_failed_run():
    event_store = EventStore(path=str(_db("explicit_retry")))
    event = _event(APPLE, APPLE_CIK)
    event_store.append(event)
    orchestrator = _RetryingFakeOrchestrator()
    consumer = EventConsumer(
        event_store=event_store,
        orchestrator=orchestrator,
    )

    first = consumer.consume_event(event.event_id)
    assert first["run_status"] == "FAILED"
    assert event_store.retry(event.event_id, max_retries=3)["status"] == "PENDING"

    second = consumer.consume_event(event.event_id)

    assert second["run_id"] == first["run_id"]
    assert second["run_status"] == "SUCCESS"
    assert second["event_status"] == "COMPLETED"
    assert orchestrator.retry_calls == 1
    assert orchestrator.execute_calls == 2


def test_step22_run_manager_deduplicates_trigger_event_id():
    """
    A5 must not create two pipeline runs for the same triggering event.
    """
    store = SQLiteRunStore(db_path=str(_db("run_manager")))
    manager = RunManager(store=store)

    event_id = f"STEP22-EVENT-{uuid.uuid4()}"

    run1 = manager.create_run_from_plan(
        company_id=APPLE,
        correlation_id=str(uuid.uuid4()),
        plan=["sec_ingestion"],
        trigger_type="EVENT",
        trigger_event_id=event_id,
    )

    run2 = manager.create_run_from_plan(
        company_id=APPLE,
        correlation_id=str(uuid.uuid4()),
        plan=["sec_ingestion"],
        trigger_type="EVENT",
        trigger_event_id=event_id,
    )

    assert run1.run_id == run2.run_id

    runs = store.runs()
    matching = runs[runs["trigger_event_id"].astype(str) == event_id]

    assert len(matching) == 1


def test_step22_retry_resumes_blocked_tasks_after_external_dependency_recovers():
    store = SQLiteRunStore(db_path=str(_db("retry_resume")))
    manager = RunManager(store=store)
    dependency_available = False
    handler_calls = []

    class Registry:
        components = {
            "sec_ingestion": SimpleNamespace(
                depends_on=[],
                downstream=[],
            ),
            "investment_intelligence": SimpleNamespace(
                depends_on=["valuation"],
                downstream=["company_discovery"],
            ),
            "company_discovery": SimpleNamespace(
                depends_on=["investment_intelligence"],
                downstream=[],
            ),
        }

        def get(self, component_id):
            return self.components[component_id]

        def affected_downstream(self, component_id):
            return self.components[component_id].downstream

    orchestrator = Orchestrator(
        pipeline_registry=Registry(),
        run_manager=manager,
        incremental_planner=None,
        dq_gate_engine=None,
        component_handlers={
            component_id: lambda component_id=component_id, **kwargs: handler_calls.append(component_id)
            for component_id in (
                "sec_ingestion",
                "investment_intelligence",
                "company_discovery",
            )
        },
        dependency_artifact_validator=lambda *args: dependency_available,
    )
    run = manager.create_run_from_plan(
        company_id=APPLE,
        correlation_id=str(uuid.uuid4()),
        plan=[
            "sec_ingestion",
            "investment_intelligence",
            "company_discovery",
        ],
    )

    assert orchestrator.execute_run(run.run_id) == "FAILED"
    first_attempt = store.get_tasks_for_run(run.run_id).set_index("component_id")
    assert first_attempt.loc["sec_ingestion", "status"] == "SUCCESS"
    assert first_attempt.loc["investment_intelligence", "status"] == "BLOCKED"
    assert first_attempt.loc["company_discovery", "status"] == "BLOCKED"

    dependency_available = True
    assert orchestrator.retry_run(run.run_id) == 2
    assert orchestrator.execute_run(run.run_id) == "SUCCESS"
    assert handler_calls.count("sec_ingestion") == 1
    assert handler_calls.count("investment_intelligence") == 1
    assert handler_calls.count("company_discovery") == 1


def test_step22_company_isolation_is_preserved_at_event_boundary():
    """
    Events for different companies must retain their own company identity.
    """
    event_store = EventStore(path=str(_db("isolation")))

    apple_event = _event(APPLE, APPLE_CIK)
    microsoft_event = _event(MICROSOFT, MICROSOFT_CIK)

    event_store.append(apple_event)
    event_store.append(microsoft_event)

    apple = event_store.get(apple_event.event_id)
    microsoft = event_store.get(microsoft_event.event_id)

    assert apple["company_id"] == APPLE
    assert microsoft["company_id"] == MICROSOFT
    assert apple["company_id"] != microsoft["company_id"]

    consumer = EventConsumer(
        event_store=event_store,
        orchestrator=_FakeOrchestrator(),
    )

    apple_platform_event = consumer._row_to_event(apple)
    microsoft_platform_event = consumer._row_to_event(microsoft)

    apple_route = consumer.router.route(apple_platform_event)
    microsoft_route = consumer.router.route(microsoft_platform_event)

    assert apple_route["company_id"] == APPLE
    assert microsoft_route["company_id"] == MICROSOFT
    assert apple_route["company_id"] != microsoft_route["company_id"]
