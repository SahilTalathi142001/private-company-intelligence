from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from company_platform.events.event_store import EventStore
from company_platform.recovery.recovery import (
    RECOVERY_VERSION,
    inspect_recovery,
    recover_failed_event,
    write_recovery_artifacts,
)


def make_event_db(path: Path, event):
    store = EventStore(path=str(path))
    from company_platform.events.models import PlatformEvent
    e = PlatformEvent(
        event_id=event["event_id"],
        event_type=event.get("event_type", "NEW_SEC_FILING"),
        event_version="1.0",
        occurred_at_utc="2026-10-06T00:00:00+00:00",
        source_id="SEC",
        company_id=event["company_id"],
        correlation_id="corr-1",
        payload={},
    )
    store.append(e)
    claimed = store.claim(e.event_id)
    store.fail(e.event_id, claim_token=claimed["_claim_token"])
    return store


def test_retryable_event_is_recovered(tmp_path):
    db = tmp_path / "control.db"
    make_event_db(db, {"event_id": "E1", "company_id": "SEC_1"})
    result = recover_failed_event("E1", control_db=db)
    assert result["status"] == "APPLIED"
    assert result["resulting_status"] == "PENDING"
    assert result["retry_count"] == 1

    store = EventStore(path=str(db))
    assert store.get("E1")["status"] == "PENDING"


def test_retry_limit_is_enforced(tmp_path):
    db = tmp_path / "control.db"
    store = make_event_db(db, {"event_id": "E1", "company_id": "SEC_1"})
    store.retry("E1", max_retries=3)
    claimed = store.claim("E1")
    store.fail("E1", claim_token=claimed["_claim_token"])
    store.retry("E1", max_retries=3)
    claimed = store.claim("E1")
    store.fail("E1", claim_token=claimed["_claim_token"])
    store.retry("E1", max_retries=3)
    claimed = store.claim("E1")
    store.fail("E1", claim_token=claimed["_claim_token"])
    with pytest.raises(ValueError, match="Retry limit reached"):
        recover_failed_event("E1", control_db=db, max_retries=3)


def test_non_failed_event_is_not_mutated(tmp_path):
    db = tmp_path / "control.db"
    store = EventStore(path=str(db))
    from company_platform.events.models import PlatformEvent
    e = PlatformEvent(
        event_id="E1", event_type="NEW_SEC_FILING", event_version="1.0",
        occurred_at_utc="2026-10-06T00:00:00+00:00", source_id="SEC",
        company_id="SEC_1", correlation_id="corr-1", payload={}
    )
    store.append(e)
    with pytest.raises(ValueError, match="Only FAILED"):
        recover_failed_event("E1", control_db=db)
    assert store.get("E1")["status"] == "PENDING"


def test_idempotent_recovery(tmp_path):
    db = tmp_path / "control.db"
    make_event_db(db, {"event_id": "E1", "company_id": "SEC_1"})
    key = "operator:E1:retry"
    first = recover_failed_event("E1", control_db=db, idempotency_key=key)
    second = recover_failed_event("E1", control_db=db, idempotency_key=key)
    assert first["status"] == "APPLIED"
    assert second["status"] == "ALREADY_APPLIED"
    assert second["idempotent"] is True
    assert second["retry_count"] == 1


def test_company_identity_is_preserved(tmp_path):
    db = tmp_path / "control.db"
    make_event_db(db, {"event_id": "E1", "company_id": "SEC_0000320193"})
    result = recover_failed_event("E1", control_db=db)
    assert result["company_id"] == "SEC_0000320193"


def test_audit_is_written(tmp_path):
    db = tmp_path / "control.db"
    make_event_db(db, {"event_id": "E1", "company_id": "SEC_1"})
    recover_failed_event("E1", control_db=db, actor="operator", reason="verified source")
    artifacts = inspect_recovery(db)
    assert len(artifacts["recovery_actions"]) == 1
    row = artifacts["recovery_actions"].iloc[0]
    assert row["action_type"] == "EVENT_RETRY"
    assert row["actor"] == "operator"
    assert row["reason"] == "verified source"


def test_inspection_does_not_mutate_event_state(tmp_path):
    db = tmp_path / "control.db"
    store = make_event_db(db, {"event_id": "E1", "company_id": "SEC_1"})
    before = store.get("E1")
    inspect_recovery(db)
    after = store.get("E1")
    assert before["status"] == after["status"] == "FAILED"
    assert before["retry_count"] == after["retry_count"]


def test_artifacts_are_written(tmp_path):
    db = tmp_path / "control.db"
    make_event_db(db, {"event_id": "E1", "company_id": "SEC_1"})
    artifacts = inspect_recovery(db)
    output = tmp_path / "gold"
    write_recovery_artifacts(artifacts, output)
    assert (output / "summary.parquet").exists()
    assert (output / "recovery_inventory.parquet").exists()
    assert (output / "recovery_actions.parquet").exists()
    assert artifacts["summary"].iloc[0]["recovery_version"] == RECOVERY_VERSION
