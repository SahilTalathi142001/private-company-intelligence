from pathlib import Path
import sqlite3
import json

import pandas as pd
import pytest

from company_platform.reconciliation.reconciliation import (
    RECONCILIATION_VERSION,
    reconcile_all,
    reconcile_company,
    write_artifacts,
)


def _control_db(tmp_path):
    db = tmp_path / "control.db"
    c = sqlite3.connect(db)
    c.execute("CREATE TABLE events (event_id TEXT PRIMARY KEY, data TEXT)")
    c.execute("CREATE TABLE pipeline_runs (run_id TEXT PRIMARY KEY, trigger_event_id TEXT, data TEXT)")
    c.execute("CREATE TABLE task_runs (task_id TEXT PRIMARY KEY, run_id TEXT, component_id TEXT, data TEXT)")
    event = {
        "event_id":"E1","company_id":"SEC_0000320193","status":"COMPLETED",
        "event_type":"NEW_SEC_FILING"
    }
    run = {
        "run_id":"R1","company_id":"SEC_0000320193",
        "trigger_event_id":"E1","status":"SUCCESS"
    }
    task = {
        "task_id":"T1","run_id":"R1","company_id":"SEC_0000320193",
        "component_id":"sec_ingestion","status":"SUCCESS"
    }
    c.execute("INSERT INTO events VALUES (?,?)", ("E1", json.dumps(event)))
    c.execute("INSERT INTO pipeline_runs VALUES (?,?,?)", ("R1","E1",json.dumps(run)))
    c.execute("INSERT INTO task_runs VALUES (?,?,?,?)", ("T1","R1","sec_ingestion",json.dumps(task)))
    c.commit(); c.close()
    return db


def test_version():
    assert RECONCILIATION_VERSION == "29.0.0"


def test_complete_event_run_task_lineage_is_reconciled(tmp_path, monkeypatch):
    db = _control_db(tmp_path)
    import company_platform.reconciliation.reconciliation as m
    monkeypatch.setattr(m, "CONTROL_DB", db)
    checks = m.reconcile_company("SEC_0000320193")
    assert checks
    assert all(x["passed"] for x in checks if x["check"] in {
        "EVENT_RUN_LINEAGE","RUN_TASK_COMPLETENESS","COMPANY_IDENTITY"
    })


def test_structured_event_schema_is_supported(tmp_path, monkeypatch):
    db = tmp_path / "structured_control.db"
    c = sqlite3.connect(db)
    c.executescript("""
    CREATE TABLE events (
        event_id TEXT PRIMARY KEY, event_type TEXT, event_version TEXT,
        occurred_at_utc TEXT, source_id TEXT, company_id TEXT,
        correlation_id TEXT, causation_id TEXT, status TEXT NOT NULL,
        payload TEXT, retry_count INTEGER NOT NULL DEFAULT 0,
        worker_id TEXT, claim_token TEXT, lease_until REAL, updated_at REAL NOT NULL
    );
    CREATE TABLE pipeline_runs (
        run_id TEXT PRIMARY KEY, trigger_event_id TEXT, data TEXT
    );
    CREATE TABLE task_runs (
        task_id TEXT PRIMARY KEY, run_id TEXT, component_id TEXT, data TEXT
    );
    """)
    company_id = "SEC_0000320193"
    event = (
        "E1", "NEW_SEC_FILING", "1.0", "2026-10-06T00:00:00+00:00",
        "SEC", company_id, "C1", None, "COMPLETED", "{}", 0, None, None,
        None, 0,
    )
    run = {
        "run_id": "R1", "company_id": company_id,
        "trigger_event_id": "E1", "status": "SUCCESS",
    }
    task = {
        "task_id": "T1", "run_id": "R1", "company_id": company_id,
        "component_id": "sec_ingestion", "status": "SUCCESS",
    }
    c.execute(
        "INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        event,
    )
    c.execute(
        "INSERT INTO pipeline_runs VALUES (?,?,?)",
        ("R1", "E1", json.dumps(run)),
    )
    c.execute(
        "INSERT INTO task_runs VALUES (?,?,?,?)",
        ("T1", "R1", "sec_ingestion", json.dumps(task)),
    )
    c.commit()
    c.close()

    import company_platform.reconciliation.reconciliation as m
    monkeypatch.setattr(m, "CONTROL_DB", db)
    checks = m.reconcile_company(company_id)
    assert all(
        check["passed"]
        for check in checks
        if check["check"] in {
            "EVENT_RUN_LINEAGE",
            "RUN_TASK_COMPLETENESS",
            "COMPANY_IDENTITY",
        }
    )


def test_missing_run_is_detected(tmp_path, monkeypatch):
    db = _control_db(tmp_path)
    c = sqlite3.connect(db)
    c.execute("UPDATE events SET data=? WHERE event_id='E1'",
              (json.dumps({"event_id":"E1","company_id":"SEC_0000320193","status":"COMPLETED"}),))
    c.execute("DELETE FROM pipeline_runs")
    c.execute("DELETE FROM task_runs")
    c.commit(); c.close()
    import company_platform.reconciliation.reconciliation as m
    monkeypatch.setattr(m, "CONTROL_DB", db)
    checks = m.reconcile_company("SEC_0000320193")
    row = next(x for x in checks if x["check"] == "EVENT_RUN_LINEAGE")
    assert row["passed"] is False


def test_company_mismatch_is_detected(tmp_path, monkeypatch):
    db = _control_db(tmp_path)
    c = sqlite3.connect(db)
    c.execute("UPDATE pipeline_runs SET data=? WHERE run_id='R1'",
              (json.dumps({"run_id":"R1","company_id":"SEC_0000789019",
                           "trigger_event_id":"E1","status":"SUCCESS"}),))
    c.commit(); c.close()
    import company_platform.reconciliation.reconciliation as m
    monkeypatch.setattr(m, "CONTROL_DB", db)
    checks = m.reconcile_company("SEC_0000320193")
    row = next(x for x in checks if x["check"] == "COMPANY_IDENTITY")
    assert row["passed"] is False


def test_structured_lineage_uses_retained_run_when_event_row_is_pruned(
    tmp_path, monkeypatch
):
    db = _control_db(tmp_path)
    c = sqlite3.connect(db)
    c.execute("DELETE FROM events WHERE event_id='E1'")
    c.commit()
    c.close()

    structured_db = tmp_path / "structured_events.db"
    c = sqlite3.connect(structured_db)
    c.execute("""
        CREATE TABLE structured_events (
            event_id TEXT, company_id TEXT, source_event_id TEXT,
            source_run_id TEXT, document_id TEXT,
            source_document_sha256 TEXT, source_document_path TEXT
        )
    """)
    c.execute(
        "INSERT INTO structured_events VALUES (?,?,?,?,?,?,?)",
        ("SE1", "SEC_0000320193", "E1", "R1", "DOC1", "sha256", "doc.pdf"),
    )
    c.commit()
    c.close()

    import company_platform.reconciliation.reconciliation as m
    monkeypatch.setattr(m, "CONTROL_DB", db)
    monkeypatch.setattr(m, "STRUCTURED_DB", structured_db)
    checks = m.reconcile_company("SEC_0000320193")
    row = next(
        check for check in checks
        if check["check"] == "EVENT_STRUCTURED_EVENT_LINEAGE"
    )
    assert row["passed"] is True


def test_structured_lineage_rejects_run_for_different_source_event(
    tmp_path, monkeypatch
):
    db = _control_db(tmp_path)
    structured_db = tmp_path / "structured_events.db"
    c = sqlite3.connect(structured_db)
    c.execute("""
        CREATE TABLE structured_events (
            event_id TEXT, company_id TEXT, source_event_id TEXT,
            source_run_id TEXT, document_id TEXT,
            source_document_sha256 TEXT, source_document_path TEXT
        )
    """)
    c.execute(
        "INSERT INTO structured_events VALUES (?,?,?,?,?,?,?)",
        ("SE1", "SEC_0000320193", "E2", "R1", "DOC1", "sha256", "doc.pdf"),
    )
    c.commit()
    c.close()

    import company_platform.reconciliation.reconciliation as m
    monkeypatch.setattr(m, "CONTROL_DB", db)
    monkeypatch.setattr(m, "STRUCTURED_DB", structured_db)
    checks = m.reconcile_company("SEC_0000320193")
    row = next(
        check for check in checks
        if check["check"] == "EVENT_STRUCTURED_EVENT_LINEAGE"
    )
    assert row["passed"] is False


def test_empty_control_plane_is_ready(tmp_path, monkeypatch):
    db = tmp_path / "empty.db"
    c = sqlite3.connect(db)
    c.execute("CREATE TABLE events (event_id TEXT PRIMARY KEY, data TEXT)")
    c.execute("CREATE TABLE pipeline_runs (run_id TEXT PRIMARY KEY, trigger_event_id TEXT, data TEXT)")
    c.execute("CREATE TABLE task_runs (task_id TEXT PRIMARY KEY, run_id TEXT, component_id TEXT, data TEXT)")
    c.commit(); c.close()
    import company_platform.reconciliation.reconciliation as m
    monkeypatch.setattr(m, "CONTROL_DB", db)
    artifacts = m.reconcile_all(db)
    assert artifacts["summary"].iloc[0]["reconciliation_status"] == "READY"


def test_artifacts_write(tmp_path, monkeypatch):
    db = _control_db(tmp_path)
    import company_platform.reconciliation.reconciliation as m
    monkeypatch.setattr(m, "CONTROL_DB", db)
    artifacts = m.reconcile_all(db)
    out = tmp_path / "gold"
    write_artifacts(artifacts, out)
    assert (out / "summary.parquet").exists()
    assert (out / "reconciliation_checks.parquet").exists()
    assert (out / "company_reconciliation.parquet").exists()


def test_no_mutation(tmp_path, monkeypatch):
    db = _control_db(tmp_path)
    before = Path(db).read_bytes()
    import company_platform.reconciliation.reconciliation as m
    monkeypatch.setattr(m, "CONTROL_DB", db)
    m.reconcile_all(db)
    after = Path(db).read_bytes()
    assert before == after


def test_company_identity_filtering(tmp_path, monkeypatch):
    db = _control_db(tmp_path)
    c = sqlite3.connect(db)
    event = {"event_id":"E2","company_id":"SEC_0000789019","status":"FAILED"}
    c.execute("INSERT INTO events VALUES (?,?)", ("E2", json.dumps(event)))
    c.commit(); c.close()
    import company_platform.reconciliation.reconciliation as m
    monkeypatch.setattr(m, "CONTROL_DB", db)
    checks = m.reconcile_company("SEC_0000320193")
    assert all(x["company_id"] == "SEC_0000320193" for x in checks)


def test_summary_counts(tmp_path, monkeypatch):
    db = _control_db(tmp_path)
    import company_platform.reconciliation.reconciliation as m
    monkeypatch.setattr(m, "CONTROL_DB", db)
    artifacts = m.reconcile_all(db)
    s = artifacts["summary"].iloc[0]
    assert s["companies_checked"] == 1
    assert s["checks"] >= 3
