from __future__ import annotations
import sqlite3
from pathlib import Path
import pandas as pd
from company_platform.operational_health.operational_health import (
    HEALTH_VERSION, build_operational_health, evaluate_health, write_operational_health
)

def _summary(**overrides):
    base = dict(event_pending_count=0, event_stale_processing_count=0,
                event_failed_count=0, run_failed_count=0)
    base.update(overrides)
    return pd.Series(base)

def _schema(db):
    with sqlite3.connect(db) as c:
        c.executescript("""
        CREATE TABLE pipeline_runs(run_id TEXT PRIMARY KEY,trigger_event_id TEXT UNIQUE,data TEXT NOT NULL);
        CREATE TABLE task_runs(task_id TEXT PRIMARY KEY,run_id TEXT NOT NULL,component_id TEXT NOT NULL,data TEXT NOT NULL);
        CREATE TABLE task_recovery_audit(audit_id INTEGER PRIMARY KEY AUTOINCREMENT,task_id TEXT NOT NULL,from_status TEXT NOT NULL,to_status TEXT NOT NULL,reason TEXT NOT NULL,created_at_utc TEXT NOT NULL);
        CREATE TABLE events(event_id TEXT PRIMARY KEY,event_type TEXT,event_version TEXT,occurred_at_utc TEXT,source_id TEXT,company_id TEXT,correlation_id TEXT,causation_id TEXT,status TEXT NOT NULL,payload TEXT,retry_count INTEGER NOT NULL DEFAULT 0,worker_id TEXT,claim_token TEXT,lease_until REAL,updated_at REAL NOT NULL);
        """)

def test_step25_healthy_policy():
    r = evaluate_health(_summary())
    assert r["health_status"] == "HEALTHY" and r["readiness_status"] == "READY"

def test_step25_pending_blocks():
    r = evaluate_health(_summary(event_pending_count=1))
    assert r["health_status"] == "CRITICAL" and r["readiness_status"] == "NOT_READY"

def test_step25_stale_blocks():
    r = evaluate_health(_summary(event_stale_processing_count=1))
    assert r["health_status"] == "CRITICAL"

def test_step25_thresholds_are_configurable():
    r = evaluate_health(_summary(event_failed_count=2, run_failed_count=3),
                        max_active_failed_events=2, max_active_failed_runs=3)
    assert r["health_status"] == "HEALTHY"

def test_step25_company_health(tmp_path):
    db = tmp_path / "control.db"; _schema(db)
    import json
    with sqlite3.connect(db) as c:
        for company, eid, status in [("SEC_1","e1","COMPLETED"),("SEC_2","e2","FAILED")]:
            c.execute("""INSERT INTO events(event_id,event_type,event_version,occurred_at_utc,source_id,company_id,correlation_id,causation_id,status,payload,retry_count,worker_id,claim_token,lease_until,updated_at)
                         VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                      (eid,"NEW_SEC_FILING","1.0.0","2026-10-06T00:00:00Z","SEC",company,"c",None,status,json.dumps({}),0,None,None,None,1.0))
    company = build_operational_health(db_path=db)["company_health"].set_index("company_id")
    assert company.loc["SEC_1","company_health_status"] == "HEALTHY"
    assert company.loc["SEC_2","company_health_status"] == "CRITICAL"

def test_step25_outputs(tmp_path):
    db = tmp_path / "control.db"; _schema(db); out = tmp_path / "health"
    paths = write_operational_health(db_path=db, output=out)
    assert set(paths) == {"summary","checks","company_health","event_status","run_status","task_status","recovery_audit"}
    assert pd.read_parquet(paths["summary"]).iloc[0]["health_version"] == HEALTH_VERSION

def test_step25_read_only(tmp_path):
    db = tmp_path / "control.db"; _schema(db)
    with sqlite3.connect(db) as c:
        before = c.execute("SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    build_operational_health(db_path=db)
    with sqlite3.connect(db) as c:
        after = c.execute("SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    assert before == after
