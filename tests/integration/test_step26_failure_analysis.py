from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd

from company_platform.failure_analysis.failure_analysis import (
    ANALYSIS_VERSION,
    analyze_failures,
    write_analysis,
)


def make_db(path: Path, events, runs, tasks, audits=()):
    con = sqlite3.connect(path)
    con.executescript("""
    CREATE TABLE events (
        event_id TEXT PRIMARY KEY, data TEXT NOT NULL
    );
    CREATE TABLE pipeline_runs (
        run_id TEXT PRIMARY KEY, trigger_event_id TEXT UNIQUE, data TEXT NOT NULL
    );
    CREATE TABLE task_runs (
        task_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, component_id TEXT NOT NULL, data TEXT NOT NULL
    );
    CREATE TABLE task_recovery_audit (
        audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id TEXT NOT NULL, from_status TEXT NOT NULL, to_status TEXT NOT NULL,
        reason TEXT NOT NULL, created_at_utc TEXT NOT NULL
    );
    """)
    for e in events:
        con.execute("INSERT INTO events VALUES(?,?)", (e["event_id"], json.dumps(e)))
    for r in runs:
        con.execute("INSERT INTO pipeline_runs VALUES(?,?,?)",
                    (r["run_id"], r.get("trigger_event_id"), json.dumps(r)))
    for t in tasks:
        con.execute("INSERT INTO task_runs VALUES(?,?,?,?)",
                    (t["task_id"], t["run_id"], t["component_id"], json.dumps(t)))
    for a in audits:
        con.execute(
            "INSERT INTO task_recovery_audit(task_id,from_status,to_status,reason,created_at_utc) VALUES(?,?,?,?,?)",
            (a["task_id"], a["from_status"], a["to_status"], a["reason"], a["created_at_utc"]),
        )
    con.commit()
    con.close()


def test_retryable_failed_event(tmp_path):
    db = tmp_path / "control.db"
    make_db(db, [{
        "event_id": "E1", "status": "FAILED", "retry_count": 1,
        "company_id": "SEC_0000000001", "event_type": "NEW_SEC_FILING"
    }], [], [])
    out = analyze_failures(db)
    row = out["failure_inventory"].query("failure_domain == 'EVENT'").iloc[0]
    assert row["classification"] == "RETRYABLE"
    assert bool(row["retry_eligible"]) is True
    assert row["recovery_action"] == "RETRY_EVENT"


def test_structured_event_store_schema_is_supported(tmp_path):
    db = tmp_path / "control.db"
    con = sqlite3.connect(db)
    con.executescript("""
    CREATE TABLE events (
        event_id TEXT PRIMARY KEY, event_type TEXT, event_version TEXT,
        occurred_at_utc TEXT, source_id TEXT, company_id TEXT,
        correlation_id TEXT, causation_id TEXT, status TEXT NOT NULL,
        payload TEXT, retry_count INTEGER NOT NULL DEFAULT 0,
        worker_id TEXT, claim_token TEXT, lease_until REAL, updated_at REAL NOT NULL
    );
    CREATE TABLE pipeline_runs (
        run_id TEXT PRIMARY KEY, trigger_event_id TEXT UNIQUE, data TEXT NOT NULL
    );
    CREATE TABLE task_runs (
        task_id TEXT PRIMARY KEY, run_id TEXT NOT NULL,
        component_id TEXT NOT NULL, data TEXT NOT NULL
    );
    CREATE TABLE task_recovery_audit (
        audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id TEXT NOT NULL, from_status TEXT NOT NULL, to_status TEXT NOT NULL,
        reason TEXT NOT NULL, created_at_utc TEXT NOT NULL
    );
    """)
    con.execute(
        "INSERT INTO events(event_id,event_type,company_id,status,payload,retry_count,updated_at) "
        "VALUES(?,?,?,?,?,?,?)",
        ("E1", "NEW_SEC_FILING", "SEC_0000000001", "FAILED", "{}", 1, 0),
    )
    con.commit()
    con.close()

    out = analyze_failures(db)
    row = out["failure_inventory"].query("failure_domain == 'EVENT'").iloc[0]
    assert row["event_id"] == "E1"
    assert row["classification"] == "RETRYABLE"


def test_retry_exhausted_event(tmp_path):
    db = tmp_path / "control.db"
    make_db(db, [{
        "event_id": "E1", "status": "FAILED", "retry_count": 3,
        "company_id": "SEC_0000000001", "event_type": "NEW_SEC_FILING"
    }], [], [])
    out = analyze_failures(db)
    row = out["failure_inventory"].query("failure_domain == 'EVENT'").iloc[0]
    assert row["classification"] == "RETRY_EXHAUSTED"
    assert bool(row["retry_eligible"]) is False


def test_failed_run_identifies_root_and_blocked(tmp_path):
    db = tmp_path / "control.db"
    make_db(db, [], [{
        "run_id": "R1", "trigger_event_id": "E1",
        "company_id": "SEC_0000000001", "status": "FAILED"
    }], [
        {"task_id": "T1", "run_id": "R1", "component_id": "document_extraction",
         "status": "FAILED", "error_message": "parser failed"},
        {"task_id": "T2", "run_id": "R1", "component_id": "document_classification",
         "status": "BLOCKED", "blocked_by": "document_extraction"},
    ])
    out = analyze_failures(db)
    run_row = out["failure_inventory"].query("failure_domain == 'RUN'").iloc[0]
    assert run_row["root_component_id"] == "document_extraction"
    assert run_row["blocked_task_count"] == 1
    assert run_row["recovery_action"] == "RETRY_TRIGGER_EVENT"


def test_task_lineage_is_preserved(tmp_path):
    db = tmp_path / "control.db"
    make_db(db, [], [{
        "run_id": "R1", "trigger_event_id": "E1",
        "company_id": "SEC_0000000001", "status": "FAILED"
    }], [{
        "task_id": "T1", "run_id": "R1", "component_id": "sec_ingestion",
        "status": "FAILED", "attempt_number": 3, "max_attempts": 3,
        "error_message": "ingestion failed"
    }])
    out = analyze_failures(db)
    task_row = out["failure_inventory"].query("failure_domain == 'TASK'").iloc[0]
    assert task_row["root_task_id"] == "T1"
    assert task_row["root_component_id"] == "sec_ingestion"
    assert task_row["run_id"] == "R1"


def test_company_isolation(tmp_path):
    db = tmp_path / "control.db"
    make_db(db, [
        {"event_id": "E1", "status": "FAILED", "retry_count": 0,
         "company_id": "SEC_0000000001", "event_type": "NEW_SEC_FILING"},
        {"event_id": "E2", "status": "FAILED", "retry_count": 3,
         "company_id": "SEC_0000000002", "event_type": "NEW_SEC_FILING"},
    ], [], [])
    out = analyze_failures(db)
    company = out["company_recovery"].set_index("company_id")
    assert int(company.loc["SEC_0000000001", "retryable_rows"]) == 1
    assert int(company.loc["SEC_0000000002", "retryable_rows"]) == 0


def test_recovery_audit_is_read_only_input(tmp_path):
    db = tmp_path / "control.db"
    make_db(db, [], [], [], [{
        "task_id": "T1", "from_status": "RUNNING", "to_status": "PENDING",
        "reason": "operator reconciliation", "created_at_utc": "2026-10-06T00:00:00+00:00"
    }])
    out = analyze_failures(db)
    assert len(out["task_recovery_audit"]) == 1
    assert out["task_recovery_audit"].iloc[0]["to_status"] == "PENDING"


def test_outputs_and_readiness(tmp_path):
    db = tmp_path / "control.db"
    output = tmp_path / "out"
    make_db(db, [{
        "event_id": "E1", "status": "FAILED", "retry_count": 0,
        "company_id": "SEC_0000000001", "event_type": "NEW_SEC_FILING"
    }], [], [])
    artifacts = analyze_failures(db)
    write_analysis(artifacts, output)
    assert artifacts["summary"].iloc[0]["readiness_status"] == "NOT_READY"
    assert artifacts["summary"].iloc[0]["analysis_version"] == ANALYSIS_VERSION
    assert (output / "failure_inventory.parquet").exists()
    assert (output / "summary.parquet").exists()
