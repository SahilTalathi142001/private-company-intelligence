from pathlib import Path
import uuid
import json
import pandas as pd

from company_platform.events.event_store import EventStore
from company_platform.events.models import PlatformEvent
from company_platform.orchestrator.event_consumer import EventConsumer
from company_platform.run_control.sqlite_run_store import SQLiteRunStore
from company_platform.events.structured_event_store import (
    StructuredEventStore,
)
from sec_ingestion import facts
from company_platform.company_intelligence.company_state_store import (
    CompanyStateStore,
)

CIK_APPLE = "0000320193"
APPLE = f"SEC_{CIK_APPLE}"

PROJECT_ROOT = Path(__file__).resolve().parents[2]

TEST_DB_DIR = (
    PROJECT_ROOT
    / "data"
    / "test"
    / "integration"
)

TEST_DB_DIR.mkdir(
    parents=True,
    exist_ok=True,
)


def _event_db(name: str) -> Path:
    return (
        TEST_DB_DIR
        / f"{name}_{uuid.uuid4().hex}.db"
    )


def test_document_received_executes_full_document_pipeline():
    """
    DOCUMENT_RECEIVED
        -> EventConsumer
        -> Orchestrator
        -> document_ingestion
        -> document_extraction
        -> document_classification
        -> document_understanding
        -> SUCCESS
    """

    # --------------------------------------------------------------
    # 1. Create a real staged document
    # --------------------------------------------------------------

    document_id = (
        f"TEST-E2E-DOCUMENT-UNDERSTANDING-{uuid.uuid4().hex}"
    )

    staged = (
        TEST_DB_DIR
        / "e2e-document-understanding.txt"
    )

    staged.write_text(
        """
        Form D Filing

        Issuer Name: Example Technologies Inc.
        Total Offering Amount: $10,000,000
        Total Amount Sold: $4,000,000
        Filing Date: 2026-10-05
        """,
        encoding="utf-8",
    )

    # --------------------------------------------------------------
    # 2. Create a real DOCUMENT_RECEIVED event
    # --------------------------------------------------------------

    event_store = EventStore(
        path=str(
            _event_db(
                "document_received_e2e"
            )
        )
    )

    event = PlatformEvent(
        event_type="DOCUMENT_RECEIVED",
        company_id=APPLE,
        source_id="DOCUMENT",
        payload={
            "document_id": document_id,
            "filename": (
                "form-d-e2e-document.txt"
            ),
            "staged_path": str(staged),
        },
    )

    event_store.append(event)

    # --------------------------------------------------------------
    # 3. Run through the REAL EventConsumer
    # --------------------------------------------------------------

    consumer = EventConsumer(
        event_store=event_store
    )

    result = consumer.consume_event(
        event.event_id
    )

    # --------------------------------------------------------------
    # 4. Verify EventConsumer result
    # --------------------------------------------------------------

    assert result["event_id"] == event.event_id

    assert result["duplicate"] is False

    assert result["run_status"] == "SUCCESS"

    assert result["event_status"] == "COMPLETED"

    assert result["run_id"]

    run_id = result["run_id"]

    # --------------------------------------------------------------
    # 5. Verify event store state
    # --------------------------------------------------------------

    stored_event = event_store.get(
        event.event_id
    )

    assert stored_event is not None

    assert stored_event["status"] == "COMPLETED"

    # --------------------------------------------------------------
    # 6. Read the SAME SQLite run store used by EventConsumer
    # --------------------------------------------------------------

    run_store = SQLiteRunStore()

    tasks = (
        run_store
        .get_tasks_for_run(run_id)
    )

    assert not tasks.empty

    # --------------------------------------------------------------
    # 7. Verify every document pipeline task actually ran
    # --------------------------------------------------------------

    expected_components = [
        "document_ingestion",
        "document_extraction",
        "document_classification",
        "document_understanding",
        "structured_event_persistence",
        "company_state_projection",
    ]

    actual_components = (
        tasks["component_id"]
        .astype(str)
        .tolist()
    )

    for component_id in expected_components:
        assert component_id in actual_components

    # --------------------------------------------------------------
    # 8. Verify every task is SUCCESS
    # --------------------------------------------------------------

    document_tasks = tasks[
        tasks["component_id"].isin(
            expected_components
        )
    ]

    assert set(
        document_tasks["status"].astype(str)
    ) == {"SUCCESS"}

    # --------------------------------------------------------------
    # 9. Verify document_understanding task specifically
    # --------------------------------------------------------------

    understanding_tasks = tasks[
        tasks["component_id"]
        == "document_understanding"
    ]

    assert len(
        understanding_tasks
    ) == 1

    understanding_task = (
        understanding_tasks.iloc[0]
    )

    assert (
        understanding_task["status"]
        == "SUCCESS"
    )

    # --------------------------------------------------------------
    # 10. Verify the actual Silver understanding artifact
    # --------------------------------------------------------------

    artifact = (
        PROJECT_ROOT
        / "data"
        / "silver"
        / "documents"
        / f"cik={CIK_APPLE}"
        / document_id
        / "document_understanding.parquet"
    )

    assert artifact.exists()

    frame = pd.read_parquet(
        artifact
    )

    assert not frame.empty

    assert (
        frame["document_id"].astype(str).eq(
            document_id
        ).all()
    )

    assert (
        frame["cik"].astype(str)
        .str.replace(
            r"\.0$",
            "",
            regex=True,
        )
        .str.zfill(10)
        .eq(CIK_APPLE)
        .all()
    )

    assert (
        frame["understanding_status"]
        .astype(str)
        .eq("SUCCESS")
        .all()
    )

    row = frame.iloc[0]

    assert row["document_class"] == "SEC_FORM_D"
    assert row["event_type"] == "SEC_FORM_D_FILING"
    assert row["event_status"] == "CONFIRMED"
    assert float(row["event_confidence"]) == 1.0
    assert row["understanding_status"] == "SUCCESS"

    facts = json.loads(row["facts_json"])

    assert facts["issuer_name"] == "Example Technologies Inc."
    assert facts["total_offering_amount"] == "10,000,000.0"
    assert facts["total_amount_sold"] == "4,000,000.0"
    assert facts["filing_date"] == "2026-10-05"


    structured_store = StructuredEventStore()

    events = structured_store.list_events(
        company_id="SEC_0000320193"
    )

    event_rows = events[
    events["document_id"].astype(str)
    == document_id
    ]

    assert len(event_rows) == 1

    structured_event = event_rows.iloc[0]

    assert structured_event["event_type"] == "SEC_FORM_D_FILING"
    assert structured_event["status"] == "PERSISTED"
    assert structured_event["company_id"] == "SEC_0000320193"
    assert structured_event["cik"] == "0000320193"
    assert structured_event["document_id"] == document_id
    assert structured_event["source_environment"] == "TEST"

    assert structured_event["source_event_id"] == event.event_id
    assert structured_event["source_run_id"] == run_id

    assert structured_event["source_document_sha256"]

    facts = json.loads(
    structured_event["facts_json"]
)

    assert facts["issuer_name"] == "Example Technologies Inc."
    assert facts["total_offering_amount"] == "10,000,000.0"
    assert facts["total_amount_sold"] == "4,000,000.0"
    assert facts["filing_date"] == "2026-10-05"

    state_store = CompanyStateStore()

    state = state_store.get(
    "SEC_0000320193"
    )

    assert state is not None
    assert state["status"] == "NO_DATA"
    assert state["funding_event_count"] == 0
    assert state["latest_funding_event_id"] is None
    assert state["last_source_document_id"] is None
    assert not events[
        events["document_id"].astype(str).eq(document_id)
    ]["source_environment"].ne("TEST").any()