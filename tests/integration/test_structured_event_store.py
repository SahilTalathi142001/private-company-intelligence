import json

import pytest

from company_platform.events.structured_event_store import (
    StructuredEventStore,
)


def test_structured_event_persistence_is_idempotent(tmp_path):
    store = StructuredEventStore(
        tmp_path / "structured_events.db"
    )

    kwargs = {
        "event_type": "SEC_FORM_D_FILING",
        "company_id": "SEC_0000320193",
        "cik": "0000320193",
        "document_id": "DOC-001",
        "document_class": "SEC_FORM_D",
        "source_document_path": "/bronze/DOC-001/form-d.txt",
        "source_document_sha256": "abc123",
        "source_event_id": "source-event-001",
        "source_run_id": "run-001",
        "event_confidence": 1.0,
        "facts": {
            "issuer_name": "Example Technologies Inc.",
            "filing_date": "2026-10-05",
            "total_amount_sold": "4000000",
        },
    }

    first = store.persist(**kwargs)
    second = store.persist(**kwargs)

    assert first["duplicate"] is False
    assert second["duplicate"] is True

    assert first["event_id"] == second["event_id"]

    events = store.list_events(
        company_id="SEC_0000320193"
    )

    assert len(events) == 1


def test_structured_event_id_is_deterministic(tmp_path):
    store = StructuredEventStore(
        tmp_path / "structured_events.db"
    )

    first = store.build_event_id(
        company_id="SEC_0000320193",
        document_id="DOC-001",
        event_type="SEC_FORM_D_FILING",
        source_document_sha256="abc123",
    )

    second = store.build_event_id(
        company_id="SEC_0000320193",
        document_id="DOC-001",
        event_type="SEC_FORM_D_FILING",
        source_document_sha256="abc123",
    )

    assert first == second
    assert len(first) == 64


def test_structured_event_preserves_provenance(tmp_path):
    store = StructuredEventStore(
        tmp_path / "structured_events.db"
    )

    result = store.persist(
        event_type="SEC_FORM_D_FILING",
        company_id="SEC_0000320193",
        cik="0000320193",
        document_id="DOC-001",
        document_class="SEC_FORM_D",
        source_document_path="/bronze/DOC-001/form-d.txt",
        source_document_sha256="abc123",
        source_event_id="DOCUMENT-EVENT-001",
        source_run_id="RUN-001",
        event_confidence=1.0,
        facts={
            "issuer_name": "Example Technologies Inc."
        },
    )

    stored = store.get(result["event_id"])

    assert stored["source_event_id"] == "DOCUMENT-EVENT-001"
    assert stored["source_run_id"] == "RUN-001"
    assert stored["source_document_sha256"] == "abc123"
    assert stored["status"] == "PERSISTED"

    facts = json.loads(stored["facts_json"])

    assert facts["issuer_name"] == "Example Technologies Inc."


def test_structured_event_status_lifecycle(tmp_path):
    store = StructuredEventStore(
        tmp_path / "structured_events.db"
    )

    result = store.persist(
        event_type="SEC_FORM_D_FILING",
        company_id="SEC_0000320193",
        cik="0000320193",
        document_id="DOC-001",
        document_class="SEC_FORM_D",
        source_document_path="/bronze/DOC-001/form-d.txt",
        source_document_sha256="abc123",
        source_event_id="EVENT-001",
        source_run_id="RUN-001",
        event_confidence=1.0,
        facts={},
    )

    assert result["status"] == "PERSISTED"

    updated = store.update_status(
        result["event_id"],
        "PROCESSED",
    )

    assert updated["status"] == "PROCESSED"