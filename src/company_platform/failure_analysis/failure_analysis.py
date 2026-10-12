from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ANALYSIS_VERSION = "26.0.0"
ROOT = Path(__file__).resolve().parents[3]
CONTROL_DB = ROOT / "data" / "silver" / "platform" / "control.db"
OUTPUT = ROOT / "data" / "gold" / "platform_failure_analysis"

DEFAULT_MAX_EVENT_RETRIES = 3


def _db(path: Path = CONTROL_DB):
    conn = sqlite3.connect(str(path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _load_json_rows(conn, table: str, data_column: str = "data") -> list[dict]:
    rows = conn.execute(f"SELECT * FROM {table}").fetchall()
    result = []
    for row in rows:
        values = dict(row)
        json_column = next(
            (
                column
                for column in (data_column, "event_json", "run_json", "task_json")
                if column in values
            ),
            None,
        )

        if json_column is None:
            if isinstance(values.get("payload"), str):
                values["payload"] = json.loads(values["payload"])
            result.append(values)
            continue

        record = json.loads(values.pop(json_column))
        if not isinstance(record, dict):
            raise ValueError(
                f"Expected JSON object in {table}.{json_column}."
            )
        record.update(values)
        result.append(record)
    return result


def _load_recovery_audit_rows(conn) -> list[dict]:
    rows = conn.execute(
        "SELECT audit_id, task_id, from_status, to_status, reason, "
        "created_at_utc FROM task_recovery_audit ORDER BY audit_id"
    ).fetchall()
    return [dict(row) for row in rows]


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _classify_event(row: dict, run_by_trigger: dict[str, dict], max_retries: int) -> dict:
    status = str(row.get("status") or "")
    retry_count = int(row.get("retry_count") or 0)
    run = run_by_trigger.get(str(row.get("event_id")))
    run_status = str(run.get("status") or "") if run else ""

    if status == "FAILED":
        if retry_count < max_retries:
            classification = "RETRYABLE"
            action = "RETRY_EVENT"
        else:
            classification = "RETRY_EXHAUSTED"
            action = "MANUAL_REVIEW"
    elif status == "QUARANTINED":
        classification = "QUARANTINED"
        action = "MANUAL_REVIEW"
    elif status == "PROCESSING":
        classification = "ACTIVE"
        action = "WAIT_OR_RECONCILE"
    elif status == "PENDING":
        classification = "ACTIVE"
        action = "PROCESS_EVENT"
    else:
        classification = "NON_FAILURE"
        action = "NO_ACTION"

    return {
        "analysis_version": ANALYSIS_VERSION,
        "failure_domain": "EVENT",
        "event_id": row.get("event_id"),
        "run_id": run.get("run_id") if run else None,
        "company_id": row.get("company_id"),
        "event_type": row.get("event_type"),
        "event_status": status,
        "run_status": run_status or None,
        "retry_count": retry_count,
        "max_retries": max_retries,
        "classification": classification,
        "retry_eligible": classification == "RETRYABLE",
        "root_component_id": None,
        "root_task_id": None,
        "blocked_task_count": 0,
        "recovery_action": action,
        "reason": (
            f"Event is FAILED with retry_count={retry_count} below max_retries={max_retries}."
            if classification == "RETRYABLE"
            else f"Event status={status}; run_status={run_status or 'NOT_FOUND'}."
        ),
    }


def _run_failure_row(run: dict, tasks: list[dict], max_retries: int) -> dict:
    run_id = str(run.get("run_id"))
    run_tasks = [t for t in tasks if str(t.get("run_id")) == run_id]
    failed = [t for t in run_tasks if str(t.get("status") or "") == "FAILED"]
    blocked = [t for t in run_tasks if str(t.get("status") or "") == "BLOCKED"]

    root = failed[0] if failed else None
    trigger_event_id = run.get("trigger_event_id")
    classification = "HISTORICAL"
    action = "MANUAL_REPROCESS"

    if str(run.get("status") or "") == "RUNNING":
        classification = "ACTIVE"
        action = "WAIT_OR_RECONCILE"
    elif root is not None:
        classification = "FAILED_ROOT_TASK"
        action = "RETRY_TRIGGER_EVENT" if trigger_event_id else "MANUAL_REPROCESS"
    elif blocked:
        classification = "BLOCKED_DOWNSTREAM"
        action = "RESOLVE_UPSTREAM_FAILURE"

    return {
        "analysis_version": ANALYSIS_VERSION,
        "failure_domain": "RUN",
        "event_id": trigger_event_id,
        "run_id": run_id,
        "company_id": run.get("company_id"),
        "event_type": None,
        "event_status": None,
        "run_status": run.get("status"),
        "retry_count": None,
        "max_retries": max_retries,
        "classification": classification,
        "retry_eligible": bool(trigger_event_id and root is not None),
        "root_component_id": root.get("component_id") if root else None,
        "root_task_id": root.get("task_id") if root else None,
        "blocked_task_count": len(blocked),
        "recovery_action": action,
        "reason": (
            f"Root failed task={root.get('component_id')}."
            if root else
            f"No failed root task identified; blocked_tasks={len(blocked)}."
        ),
    }


def _task_failure_rows(tasks: list[dict], run_by_id: dict[str, dict]) -> list[dict]:
    rows = []
    for task in tasks:
        status = str(task.get("status") or "")
        if status not in {"FAILED", "BLOCKED", "RUNNING"}:
            continue
        run = run_by_id.get(str(task.get("run_id")), {})
        if status == "FAILED":
            classification = "ROOT_TASK_FAILURE"
            action = "RETRY_TRIGGER_EVENT" if run.get("trigger_event_id") else "MANUAL_REPROCESS"
        elif status == "BLOCKED":
            classification = "DOWNSTREAM_BLOCKED"
            action = "RESOLVE_UPSTREAM_FAILURE"
        else:
            classification = "ACTIVE"
            action = "WAIT_OR_RECONCILE"
        rows.append({
            "analysis_version": ANALYSIS_VERSION,
            "failure_domain": "TASK",
            "event_id": run.get("trigger_event_id"),
            "run_id": task.get("run_id"),
            "company_id": task.get("company_id") or run.get("company_id"),
            "event_type": None,
            "event_status": None,
            "run_status": run.get("status"),
            "retry_count": task.get("attempt_number"),
            "max_retries": task.get("max_attempts"),
            "classification": classification,
            "retry_eligible": bool(status == "FAILED" and run.get("trigger_event_id")),
            "root_component_id": task.get("component_id"),
            "root_task_id": task.get("task_id"),
            "blocked_task_count": 0,
            "recovery_action": action,
            "reason": task.get("error_message") or task.get("blocked_by") or f"Task status={status}.",
        })
    return rows


def analyze_failures(control_db: Path = CONTROL_DB, max_event_retries: int = DEFAULT_MAX_EVENT_RETRIES) -> dict[str, pd.DataFrame]:
    with _db(control_db) as conn:
        events = _load_json_rows(conn, "events")
        runs = _load_json_rows(conn, "pipeline_runs")
        tasks = _load_json_rows(conn, "task_runs")
        recovery = _load_recovery_audit_rows(conn)

    run_by_id = {str(r.get("run_id")): r for r in runs if r.get("run_id") is not None}
    run_by_trigger = {
        str(r.get("trigger_event_id")): r
        for r in runs
        if r.get("trigger_event_id")
    }

    event_rows = [
        _classify_event(e, run_by_trigger, max_event_retries)
        for e in events
        if str(e.get("status") or "") in {"FAILED", "QUARANTINED", "PROCESSING", "PENDING"}
    ]

    failed_run_rows = [
        _run_failure_row(r, tasks, max_event_retries)
        for r in runs
        if str(r.get("status") or "") in {"FAILED", "QUARANTINED", "RUNNING"}
    ]

    task_rows = _task_failure_rows(tasks, run_by_id)

    inventory = pd.DataFrame(event_rows + failed_run_rows + task_rows)
    if inventory.empty:
        inventory = pd.DataFrame(columns=[
            "analysis_version", "failure_domain", "event_id", "run_id", "company_id",
            "event_type", "event_status", "run_status", "retry_count", "max_retries",
            "classification", "retry_eligible", "root_component_id", "root_task_id",
            "blocked_task_count", "recovery_action", "reason"
        ])

    retryable = int(inventory["retry_eligible"].fillna(False).sum()) if not inventory.empty else 0
    active = int(inventory["classification"].isin(["ACTIVE"]).sum()) if not inventory.empty else 0
    unresolved = int(inventory["classification"].isin([
        "RETRYABLE", "RETRY_EXHAUSTED", "QUARANTINED",
        "FAILED_ROOT_TASK", "BLOCKED_DOWNSTREAM", "ROOT_TASK_FAILURE",
        "DOWNSTREAM_BLOCKED",
    ]).sum()) if not inventory.empty else 0

    summary = pd.DataFrame([{
        "analysis_version": ANALYSIS_VERSION,
        "generated_at_utc": _iso_now(),
        "failure_rows": int(len(inventory)),
        "retryable_rows": retryable,
        "active_rows": active,
        "unresolved_rows": unresolved,
        "recovery_ready": bool(active == 0 and unresolved == 0),
        "readiness_status": "READY" if active == 0 and unresolved == 0 else "NOT_READY",
        "reason": (
            "No active or unresolved failures."
            if active == 0 and unresolved == 0
            else f"active={active}; unresolved={unresolved}; retryable={retryable}"
        ),
    }])

    company = inventory.groupby("company_id", dropna=False).agg(
        failure_rows=("failure_domain", "size"),
        retryable_rows=("retry_eligible", lambda s: int(s.fillna(False).sum())),
        active_rows=("classification", lambda s: int((s == "ACTIVE").sum())),
        unresolved_rows=("classification", lambda s: int((~s.isin(["ACTIVE", "NON_FAILURE"])).sum())),
    ).reset_index() if not inventory.empty else pd.DataFrame(
        columns=["company_id", "failure_rows", "retryable_rows", "active_rows", "unresolved_rows"]
    )
    if not company.empty:
        company["readiness_status"] = company.apply(
            lambda r: "READY" if r["active_rows"] == 0 and r["unresolved_rows"] == 0 else "NOT_READY",
            axis=1,
        )

    recovery_df = pd.DataFrame(recovery)
    if recovery_df.empty:
        recovery_df = pd.DataFrame(columns=[
            "audit_id", "task_id", "from_status", "to_status", "reason", "created_at_utc"
        ])

    return {
        "summary": summary,
        "failure_inventory": inventory,
        "company_recovery": company,
        "task_recovery_audit": recovery_df,
    }


def write_analysis(artifacts: dict[str, pd.DataFrame], output: Path = OUTPUT) -> None:
    output.mkdir(parents=True, exist_ok=True)
    for name, df in artifacts.items():
        df.to_parquet(output / f"{name}.parquet", index=False)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Step 26 failure analysis and recovery readiness")
    parser.add_argument("--control-db", type=Path, default=CONTROL_DB)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--max-event-retries", type=int, default=DEFAULT_MAX_EVENT_RETRIES)
    args = parser.parse_args(argv)

    artifacts = analyze_failures(args.control_db, args.max_event_retries)
    write_analysis(artifacts, args.output)
    s = artifacts["summary"].iloc[0]
    print("=" * 80)
    print("STEP 26 - FAILURE ANALYSIS & RECOVERY READINESS")
    print(f"Analysis version     : {ANALYSIS_VERSION}")
    print(f"Failure rows         : {int(s['failure_rows'])}")
    print(f"Retryable rows       : {int(s['retryable_rows'])}")
    print(f"Active rows          : {int(s['active_rows'])}")
    print(f"Unresolved rows      : {int(s['unresolved_rows'])}")
    print(f"Recovery readiness   : {s['readiness_status']}")
    print(f"Output               : {args.output}")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
