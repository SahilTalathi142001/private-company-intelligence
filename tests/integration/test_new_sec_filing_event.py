from pathlib import Path
import pandas as pd
import uuid

from company_platform.events.event_store import EventStore
from company_platform.events.models import PlatformEvent
from company_platform.orchestrator.event_consumer import EventConsumer
from company_platform.incremental.planner import IncrementalPlanner
from company_platform.pipeline_model.pipeline_registry import PipelineRegistry


CIK_APPLE = "0000320193"
CIK_MICROSOFT = "0000789019"

APPLE = f"SEC_{CIK_APPLE}"
MICROSOFT = f"SEC_{CIK_MICROSOFT}"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEST_DB_DIR = PROJECT_ROOT / "data" / "test" / "integration"
TEST_DB_DIR.mkdir(parents=True, exist_ok=True)


def _pdf_with_text(text: str) -> bytes:
    content = f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET".encode("ascii")
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        (
            f"<< /Length {len(content)} >>\nstream\n".encode("ascii")
            + content
            + b"\nendstream"
        ),
    ]

    pdf = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for index, obj in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{index} 0 obj\n".encode("ascii"))
        pdf.extend(obj)
        pdf.extend(b"\nendobj\n")

    xref_offset = len(pdf)
    pdf.extend(f"xref\n0 {len(offsets)}\n".encode("ascii"))
    pdf.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.extend(f"{offset:010} 00000 n \n".encode("ascii"))
    pdf.extend(
        f"trailer\n<< /Size {len(offsets)} /Root 1 0 R >>\n"
        f"startxref\n{xref_offset}\n%%EOF\n".encode("ascii")
    )
    return bytes(pdf)


def _event_db(name: str) -> Path:
    return TEST_DB_DIR / f"{name}_{uuid.uuid4().hex}.db"


def _sec_event(
    company_id: str,
    cik: str,
    *,
    accession: str,
    event_type: str = "NEW_SEC_FILING",
    source_id: str = "SEC",
) -> PlatformEvent:
    return PlatformEvent(
        event_type=event_type,
        company_id=company_id,
        source_id=source_id,
        payload={
            "cik": cik,
            "accession_number": accession,
            "form": "4",
            "primary_document": "test-document.xml",
            "primary_doc_description": "Integration test SEC filing",
        },
    )


def test_new_sec_filing_runs_sec_to_document_ingestion():
    """
    NEW_SEC_FILING
        -> EventRouter
        -> IncrementalPlanner
        -> sec_ingestion
        -> document_ingestion
        -> downstream pipeline
        -> COMPLETED

    Also verifies duplicate delivery is idempotent.
    """
    event_store = EventStore(
        path=str(_event_db("new_sec_filing"))
    )

    event = _sec_event(
        APPLE,
        CIK_APPLE,
        accession="TEST-INTEGRATION-SEC-001",
    )

    event_store.append(event)

    consumer = EventConsumer(event_store=event_store)

    result = consumer.consume_event(event.event_id)

    assert result["event_id"] == event.event_id
    assert result["run_status"] == "SUCCESS"
    assert result["event_status"] == "COMPLETED"
    assert result["duplicate"] is False

    decision = result["decision"]

    assert decision["processing_mode"] == "INCREMENTAL"
    assert decision["root_component_id"] == "sec_ingestion"
    assert (
        decision["reason_code"]
        == "SEC_FILING_AFFECTS_FINANCIAL_AND_DOWNSTREAM_ARTIFACTS"
    )

    affected = set(decision["affected_components"])

    assert "sec_ingestion" in affected
    assert "document_ingestion" in affected

    stored_event = event_store.get(event.event_id)

    assert stored_event is not None
    assert stored_event["status"] == "COMPLETED"

    # Duplicate delivery must not execute the pipeline again.
    duplicate_result = consumer.consume_event(event.event_id)

    assert duplicate_result["event_id"] == event.event_id
    assert duplicate_result["duplicate"] is True
    assert duplicate_result["action"] == "NO_OP_ALREADY_COMPLETED"
    assert duplicate_result["event_status"] == "COMPLETED"


def test_document_received_routes_to_document_ingestion():
    """
    Contract test for DOCUMENT_RECEIVED.

    Uploaded-document execution is intentionally not tested here because
    document_ingestion currently rejects DOCUMENT without a staged_path
    implementation. This verifies the routing/planning contract only.
    """
    planner = IncrementalPlanner(PipelineRegistry())

    decision = planner.plan_for_change(
        change_type="DOCUMENT_RECEIVED",
        source_id="DOCUMENT",
    )

    assert decision["processing_mode"] == "INCREMENTAL"
    assert decision["root_component_id"] == "document_ingestion"
    assert decision["reason_code"] == "NEW_DOCUMENT"

    assert decision["affected_components"][0] == "document_ingestion"


def test_failed_event_requires_explicit_retry():
    """
    FAILED events must not automatically execute again.

    FAILED
        -> consume_event()
        -> EXPLICIT_RETRY_REQUIRED

    Then verify EventStore.retry():

    FAILED
        -> retry()
        -> PENDING
        -> retry_count + 1
    """
    event_store = EventStore(
        path=str(_event_db("failed_event"))
    )

    event = _sec_event(
        APPLE,
        CIK_APPLE,
        accession="TEST-INTEGRATION-FAILED-001",
    )

    event_store.append(event)

    claimed = event_store.claim(event.event_id)

    assert claimed is not None

    claim_token = claimed.get("_claim_token")

    event_store.fail(
        event.event_id,
        claim_token=claim_token,
    )

    failed_event = event_store.get(event.event_id)

    assert failed_event is not None
    assert failed_event["status"] == "FAILED"
    assert failed_event["retry_count"] == 0

    consumer = EventConsumer(event_store=event_store)

    # Consumer must not silently retry a failed event.
    result = consumer.consume_event(event.event_id)

    assert result["event_id"] == event.event_id
    assert result["duplicate"] is True
    assert result["action"] == "EXPLICIT_RETRY_REQUIRED"
    assert result["event_status"] == "FAILED"

    # Explicit retry moves the event back to PENDING.
    retried_event = event_store.retry(
        event.event_id,
        max_retries=3,
    )

    assert retried_event is not None
    assert retried_event["status"] == "PENDING"
    assert retried_event["retry_count"] == 1


def test_cross_company_event_isolation():
    """
    Verify that company identity is preserved through:

        EventStore
            -> EventConsumer
            -> EventRouter
            -> IncrementalPlanner

    A Microsoft event must never be routed as Apple's event.
    """
    event_store = EventStore(
        path=str(_event_db("company_isolation"))
    )

    event = _sec_event(
        MICROSOFT,
        CIK_MICROSOFT,
        accession="TEST-INTEGRATION-ISOLATION-001",
    )

    event_store.append(event)

    stored_event = event_store.get(event.event_id)

    assert stored_event is not None
    assert stored_event["company_id"] == MICROSOFT
    assert stored_event["company_id"] != APPLE
    assert stored_event["source_id"] == "SEC"

    consumer = EventConsumer(event_store=event_store)

    platform_event = consumer._row_to_event(stored_event)

    route = consumer.router.route(platform_event)

    assert route["change_type"] == "NEW_SEC_FILING"
    assert route["company_id"] == MICROSOFT
    assert route["company_id"] != APPLE
    assert route["source_id"] == "SEC"

    decision = consumer.orchestrator.build_plan(
        change_type=route["change_type"],
        company_id=route["company_id"],
        source_id=route["source_id"],
    )

    assert decision["processing_mode"] == "INCREMENTAL"
    assert decision["root_component_id"] == "sec_ingestion"
def test_document_received_is_idempotent():
    staged = (
        PROJECT_ROOT
        / "data"
        / "test"
        / "integration"
        / "idempotent-document.txt"
    )

    staged.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    staged.write_text(
        "idempotent integration test",
        encoding="utf-8",
    )

    event_store = EventStore(
        path=str(_event_db("document_idempotency"))
    )

    event = PlatformEvent(
        event_type="DOCUMENT_RECEIVED",
        company_id=APPLE,
        source_id="DOCUMENT",
        payload={
            "document_id": f"TEST-DOCUMENT-IDEMPOTENT-{uuid.uuid4().hex}",
            "filename": "idempotent-document.txt",
            "staged_path": str(staged),
        },
    )

    event_store.append(event)

    consumer = EventConsumer(
        event_store=event_store
    )

    first = consumer.consume_event(
        event.event_id
    )

    assert first["run_status"] == "SUCCESS"
    assert first["event_status"] == "COMPLETED"

    second = consumer.consume_event(
        event.event_id
    )

    assert second["duplicate"] is True
    assert second["action"] == "NO_OP_ALREADY_COMPLETED"
    assert second["event_status"] == "COMPLETED"
def test_document_received_ingests_uploaded_document(tmp_path):
    """
    DOCUMENT_RECEIVED
        -> EventRouter
        -> IncrementalPlanner
        -> document_ingestion
        -> company-scoped bronze artifact
        -> COMPLETED
    """

    staged = tmp_path / "integration-document.pdf"
    staged.write_bytes(_pdf_with_text("integration-test-document"))
    document_id = f"TEST-DOCUMENT-{uuid.uuid4().hex}"

    event_store = EventStore(
        path=str(_event_db("document_received"))
    )

    event = PlatformEvent(
        event_type="DOCUMENT_RECEIVED",
        company_id=APPLE,
        source_id="DOCUMENT",
        payload={
            "document_id": document_id,
            "filename": "integration-document.pdf",
            "staged_path": str(staged),
            "document_type": "PDF",
        },
    )

    event_store.append(event)

    consumer = EventConsumer(
        event_store=event_store
    )

    result = consumer.consume_event(
        event.event_id
    )

    assert result["run_status"] == "SUCCESS"
    assert result["event_status"] == "COMPLETED"
    assert result["duplicate"] is False

    decision = result["decision"]

    assert decision["processing_mode"] == "INCREMENTAL"
    assert decision["root_component_id"] == "document_ingestion"
    assert decision["reason_code"] == "NEW_DOCUMENT"

    affected = set(
        decision["affected_components"]
    )

    assert "document_ingestion" in affected

    document_root = (
        PROJECT_ROOT
        / "data"
        / "bronze"
        / "documents"
        / f"cik={CIK_APPLE}"
        / document_id
    )

    stored_document = (
        document_root
        / "integration-document.pdf"
    )

    manifest = (
        document_root
        / "document_manifest.parquet"
    )

    assert stored_document.is_file()
    assert manifest.is_file()

    manifest_df = pd.read_parquet(manifest)

    assert len(manifest_df) == 1
    assert manifest_df.iloc[0]["document_id"] == document_id
    assert manifest_df.iloc[0]["cik"] == CIK_APPLE
    assert manifest_df.iloc[0]["company_id"] == APPLE