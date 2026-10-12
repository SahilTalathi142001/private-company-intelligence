from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from company_platform.observability.operational_observability import (
    OBSERVABILITY_VERSION,
    build_operational_snapshot,
    write_operational_snapshot,
)


def _db(tmp_path: Path) -> Path:
    return tmp_path / "control.db"


def _create_schema(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE pipeline_runs(
                run_id TEXT PRIMARY KEY,
                trigger_event_id TEXT UNIQUE,
                data TEXT NOT NULL
            );

            CREATE TABLE task_runs(
                task_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                component_id TEXT NOT NULL,
                data TEXT NOT NULL
            );

            CREATE TABLE task_recovery_audit(
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                from_status TEXT NOT NULL,
                to_status TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_at_utc TEXT NOT NULL
            );

            CREATE TABLE events(
                event_id TEXT PRIMARY KEY,
                event_type TEXT,
                event_version TEXT,
                occurred_at_utc TEXT,
                source_id TEXT,
                company_id TEXT,
                correlation_id TEXT,
                causation_id TEXT,
                status TEXT NOT NULL,
                payload TEXT,
                retry_count INTEGER NOT NULL DEFAULT 0,
                worker_id TEXT,
                claim_token TEXT,
                lease_until REAL,
                updated_at REAL NOT NULL
            );
            """
        )


def test_step24_empty_control_plane(tmp_path: Path):
    db = _db(tmp_path)
    _create_schema(db)

    result = build_operational_snapshot(db_path=db)

    assert result["summary"].iloc[0]["event_count"] == 0
    assert result["summary"].iloc[0]["run_count"] == 0
    assert result["summary"].iloc[0]["task_count"] == 0
    assert result["summary"].iloc[0]["observability_version"] == OBSERVABILITY_VERSION


def test_step24_event_metrics(tmp_path: Path):
    db = _db(tmp_path)
    _create_schema(db)

    with sqlite3.connect(db) as conn:
        rows = [
            (
                "e1", "NEW_SEC_FILING", "SEC", "SEC_1",
                "COMPLETED", 0, None, 1.0
            ),
            (
                "e2", "NEW_SEC_FILING", "SEC", "SEC_1",
                "FAILED", 2, None, 2.0
            ),
            (
                "e3", "DOCUMENT_RECEIVED", "DOCUMENT", "SEC_2",
                "PENDING", 0, None, 3.0
            ),
            (
                "e4", "DOCUMENT_RECEIVED", "DOCUMENT", "SEC_2",
                "PROCESSING", 0, 1.0, 4.0
            ),
        ]

        conn.executemany(
            """
            INSERT INTO events(
                event_id,event_type,event_version,occurred_at_utc,
                source_id,company_id,correlation_id,causation_id,
                status,payload,retry_count,worker_id,claim_token,
                lease_until,updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                (
                    e[0], e[1], "1.0.0", "2026-10-06T00:00:00Z",
                    e[2], e[3], "corr", None, e[4], "{}",
                    e[5], None, None, e[6], e[7]
                )
                for e in rows
            ],
        )

    summary = build_operational_snapshot(db_path=db)["summary"].iloc[0]

    assert summary["event_count"] == 4
    assert summary["event_completed_count"] == 1
    assert summary["event_failed_count"] == 1
    assert summary["event_pending_count"] == 1
    assert summary["event_processing_count"] == 1
    assert summary["event_retry_count"] == 2
    assert summary["event_stale_processing_count"] == 1


def test_step24_run_and_task_metrics(tmp_path: Path):
    db = _db(tmp_path)
    _create_schema(db)

    import json

    run = {
        "run_id": "r1",
        "company_id": "SEC_1",
        "trigger_type": "EVENT",
        "trigger_event_id": "e1",
        "correlation_id": "c1",
        "pipeline_version": "2.6.0",
        "status": "SUCCESS",
        "started_at_utc": "2026-10-06T00:00:00+00:00",
        "completed_at_utc": "2026-10-06T00:02:00+00:00",
        "error_message": None,
    }

    tasks = [
        {
            "task_id": "t1",
            "run_id": "r1",
            "company_id": "SEC_1",
            "component_id": "sec_ingestion",
            "task_sequence": 1,
            "status": "SUCCESS",
            "attempt_number": 1,
            "max_attempts": 3,
            "started_at_utc": "2026-10-06T00:00:00+00:00",
            "completed_at_utc": "2026-10-06T00:00:30+00:00",
            "error_message": None,
            "dq_status": "PASS",
            "blocked_by": None,
        },
        {
            "task_id": "t2",
            "run_id": "r1",
            "company_id": "SEC_1",
            "component_id": "document_extraction",
            "task_sequence": 2,
            "status": "FAILED",
            "attempt_number": 1,
            "max_attempts": 3,
            "started_at_utc": "2026-10-06T00:00:30+00:00",
            "completed_at_utc": "2026-10-06T00:01:00+00:00",
            "error_message": "test",
            "dq_status": "FAIL",
            "blocked_by": None,
        },
    ]

    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO pipeline_runs VALUES(?,?,?)",
            (run["run_id"], run["trigger_event_id"], json.dumps(run)),
        )
        for task in tasks:
            conn.execute(
                "INSERT INTO task_runs VALUES(?,?,?,?)",
                (
                    task["task_id"],
                    task["run_id"],
                    task["component_id"],
                    json.dumps(task),
                ),
            )

    result = build_operational_snapshot(db_path=db)
    summary = result["summary"].iloc[0]

    assert summary["run_count"] == 1
    assert summary["run_success_count"] == 1
    assert summary["task_count"] == 2
    assert summary["task_failed_count"] == 1
    assert summary["run_duration_avg_seconds"] == 120.0


def test_step24_company_isolation(tmp_path: Path):
    db = _db(tmp_path)
    _create_schema(db)

    import json

    with sqlite3.connect(db) as conn:
        for company_id, run_id in [
            ("SEC_1", "r1"),
            ("SEC_2", "r2"),
        ]:
            run = {
                "run_id": run_id,
                "company_id": company_id,
                "trigger_type": "EVENT",
                "trigger_event_id": None,
                "correlation_id": run_id,
                "pipeline_version": "2.6.0",
                "status": "SUCCESS",
                "started_at_utc": None,
                "completed_at_utc": None,
                "error_message": None,
            }
            conn.execute(
                "INSERT INTO pipeline_runs VALUES(?,?,?)",
                (run_id, None, json.dumps(run)),
            )

    company = build_operational_snapshot(db_path=db)["company_metrics"]

    assert set(company["company_id"]) == {"SEC_1", "SEC_2"}
    assert company.set_index("company_id").loc["SEC_1", "run_count"] == 1
    assert company.set_index("company_id").loc["SEC_2", "run_count"] == 1


def test_step24_recovery_audit(tmp_path: Path):
    db = _db(tmp_path)
    _create_schema(db)

    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            INSERT INTO task_recovery_audit(
                task_id,from_status,to_status,reason,created_at_utc
            )
            VALUES(?,?,?,?,?)
            """,
            (
                "t1",
                "RUNNING",
                "PENDING",
                "operator reconciliation",
                "2026-10-06T00:00:00+00:00",
            ),
        )

    recovery = build_operational_snapshot(db_path=db)["recovery_audit"]

    assert len(recovery) == 1
    assert recovery.iloc[0]["to_status"] == "PENDING"


def test_step24_writes_outputs(tmp_path: Path):
    db = _db(tmp_path)
    _create_schema(db)
    output = tmp_path / "observability"

    paths = write_operational_snapshot(
        db_path=db,
        output=output,
    )

    expected = {
        "summary",
        "company_metrics",
        "event_status",
        "run_status",
        "task_status",
        "recovery_audit",
    }

    assert set(paths) == expected

    for path in paths.values():
        assert path.exists()
        assert pd.read_parquet(path) is not None


def test_step24_read_only(tmp_path: Path):
    db = _db(tmp_path)
    _create_schema(db)

    with sqlite3.connect(db) as conn:
        before = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT name, sql FROM sqlite_master "
                "WHERE type='table' ORDER BY name"
            )
        }

    build_operational_snapshot(db_path=db)

    with sqlite3.connect(db) as conn:
        after = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT name, sql FROM sqlite_master "
                "WHERE type='table' ORDER BY name"
            )
        }

    assert before == after
