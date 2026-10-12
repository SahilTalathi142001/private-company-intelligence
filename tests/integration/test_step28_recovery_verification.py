from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from company_platform.events.event_store import EventStore
from company_platform.events.models import PlatformEvent
from company_platform.recovery.recovery import recover_failed_event
from company_platform.recovery_verification.verification import (
    VERIFICATION_VERSION,
    verify_event_recovery,
    verify_recovery_history,
    write_artifacts,
)


def _failed_event(db: Path, event_id="E1", company_id="SEC_0000320193"):
    store = EventStore(path=str(db))
    event = PlatformEvent(
        event_id=event_id,
        event_type="NEW_SEC_FILING",
        event_version="1.0",
        occurred_at_utc="2026-10-06T00:00:00+00:00",
        source_id="SEC",
        company_id=company_id,
        correlation_id=f"corr-{event_id}",
        payload={},
    )
    store.append(event)
    claimed = store.claim(event_id)
    store.fail(event_id, claim_token=claimed["_claim_token"])
    return store


def test_failed_event_is_not_declared_recovered(tmp_path):
    db = tmp_path / "control.db"
    _failed_event(db)
    result = verify_event_recovery("E1", control_db=db)
    assert result["verification_status"] == "RECOVERY_FAILED"
    assert result["event_status"] == "FAILED"


def test_pending_event_is_incomplete(tmp_path):
    db = tmp_path / "control.db"
    store = _failed_event(db)
    store.retry("E1", max_retries=3)
    result = verify_event_recovery("E1", control_db=db)
    assert result["verification_status"] == "RECOVERY_INCOMPLETE"
    assert result["event_status"] == "PENDING"


def test_missing_event_raises(tmp_path):
    with pytest.raises(KeyError):
        verify_event_recovery("MISSING", control_db=tmp_path / "control.db")


def test_recovery_history_has_no_actions_before_step27(tmp_path):
    db = tmp_path / "control.db"
    _failed_event(db)
    artifacts = verify_recovery_history(db)
    assert artifacts["summary"].iloc[0]["recovery_actions_verified"] == 0
    assert artifacts["summary"].iloc[0]["verification_readiness"] == "READY"
    with sqlite3.connect(db) as conn:
        table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='recovery_actions'"
        ).fetchone()
    assert table is None


def test_recovered_event_without_run_is_partial_or_failed(tmp_path):
    db = tmp_path / "control.db"
    _failed_event(db)
    recover_failed_event("E1", control_db=db, reason="verified")
    result = verify_event_recovery("E1", control_db=db)
    assert result["verification_status"] in {"RECOVERY_INCOMPLETE", "RECOVERY_PARTIAL"}
    assert result["run_count"] == 0


def test_company_isolation_is_preserved(tmp_path):
    db = tmp_path / "control.db"
    _failed_event(db, "E1", "SEC_0000789019")
    recover_failed_event("E1", control_db=db, reason="verified")
    result = verify_event_recovery("E1", control_db=db)
    assert result["company_id"] == "SEC_0000789019"
    assert result["cik"] == "0000789019"


def test_artifacts_can_be_written(tmp_path):
    db = tmp_path / "control.db"
    _failed_event(db)
    artifacts = verify_recovery_history(db)
    output = tmp_path / "gold"
    write_artifacts(artifacts, output)
    assert (output / "summary.parquet").exists()
    assert (output / "recovery_verification.parquet").exists()
    assert (output / "verification_checks.parquet").exists()
    assert artifacts["summary"].iloc[0]["verification_version"] == VERIFICATION_VERSION


def test_recovery_verification_is_read_only(tmp_path):
    db = tmp_path / "control.db"
    store = _failed_event(db)
    before = store.get("E1")
    verify_event_recovery("E1", control_db=db)
    after = store.get("E1")
    assert before["status"] == after["status"] == "FAILED"
    assert before["retry_count"] == after["retry_count"]


def test_recovery_history_only_follows_applied_actions(tmp_path):
    db = tmp_path / "control.db"
    _failed_event(db, "E1")
    _failed_event(db, "E2", "SEC_0000789019")
    recover_failed_event("E1", control_db=db, reason="verified")
    artifacts = verify_recovery_history(db)
    assert artifacts["summary"].iloc[0]["recovery_actions_verified"] == 1
    assert artifacts["recovery_verification"].iloc[0]["event_id"] == "E1"
