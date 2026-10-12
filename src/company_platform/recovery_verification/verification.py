from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from company_platform.events.event_store import EventStore
from company_platform.run_control.sqlite_run_store import SQLiteRunStore

VERIFICATION_VERSION = "28.0.0"
ROOT = Path(__file__).resolve().parents[3]
CONTROL_DB = ROOT / "data" / "silver" / "platform" / "control.db"
OUTPUT = ROOT / "data" / "gold" / "platform_recovery_verification"

EXPECTED_TERMINAL_RUN_STATUSES = {"SUCCESS", "PARTIAL", "FAILED", "QUARANTINED"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _db(path: Path = CONTROL_DB):
    c = sqlite3.connect(str(path), timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout=30000")
    return c


def _table_exists(c: sqlite3.Connection, table: str) -> bool:
    return c.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone() is not None


def _event_from_row(row: sqlite3.Row) -> dict:
    values = dict(row)
    serialized = values.pop("data", None)
    if serialized is not None:
        event = json.loads(serialized)
        if not isinstance(event, dict):
            raise ValueError("Expected event data to be a JSON object.")
        event.update(values)
        values = event

    payload = values.get("payload")
    if isinstance(payload, str):
        values["payload"] = json.loads(payload) if payload else {}
    return values


def _event_row(event_id: str, control_db: Path) -> Optional[dict]:
    with _db(control_db) as c:
        if not _table_exists(c, "events"):
            return None
        row = c.execute(
            "SELECT * FROM events WHERE event_id=?", (str(event_id),)
        ).fetchone()
    if not row:
        return None
    return _event_from_row(row)


def _runs_for_event(event_id: str, control_db: Path) -> list[dict]:
    with _db(control_db) as c:
        if not _table_exists(c, "pipeline_runs"):
            return []
        rows = c.execute(
            "SELECT * FROM pipeline_runs WHERE trigger_event_id=?",
            (str(event_id),),
        ).fetchall()
    records = []
    for row in rows:
        values = dict(row)
        serialized = next(
            (column for column in ("data", "run_json") if column in values),
            None,
        )
        if serialized is None:
            records.append(values)
            continue
        run = json.loads(values.pop(serialized))
        if not isinstance(run, dict):
            raise ValueError(
                f"Expected run data to be a JSON object in pipeline_runs.{serialized}."
            )
        run.update(values)
        records.append(run)
    return records


def _artifact_checks(run: dict, root: Path = ROOT) -> tuple[list[dict], str]:
    """Validate that successful terminal runs have the artifacts they claim to produce.

    This is intentionally conservative: it checks the expected artifact locations
    produced by the event-driven document/intelligence chain rather than inventing
    a new artifact validator contract.
    """
    company_id = str(run.get("company_id") or "")
    cik = company_id.replace("SEC_", "").zfill(10)
    checks = []

    def check(name: str, candidates: list[Path], required: bool = True):
        existing = [p for p in candidates if p.exists() and p.stat().st_size > 0]
        passed = bool(existing) if required else True
        checks.append({
            "check": name,
            "passed": passed,
            "path": str(existing[0]) if existing else str(candidates[0]),
            "message": "artifact present" if passed else "expected artifact not found",
        })
        return passed

    # For event-driven SEC/document processing, the document artifacts are the
    # strongest deterministic post-recovery evidence available.
    document_root = root / "data" / "silver" / "documents" / f"cik={cik}"
    metadata_candidates = list(document_root.glob("*/document_metadata.parquet"))
    text_candidates = list(document_root.glob("*/document_text.parquet"))

    # A successful/partial run may legitimately have no document artifact when
    # the root change does not enter document processing. Do not fabricate a
    # failure in that case; record the check as informational.
    if metadata_candidates or text_candidates:
        check("DOCUMENT_METADATA", metadata_candidates)
        check("DOCUMENT_TEXT", text_candidates)
    else:
        checks.append({
            "check": "DOCUMENT_ARTIFACTS",
            "passed": True,
            "path": str(document_root),
            "message": "no document artifacts discovered; not treated as failure",
        })

    return checks, cik


def verify_event_recovery(
    event_id: str,
    *,
    control_db: Path = CONTROL_DB,
    require_run: bool = True,
) -> dict:
    """Verify one explicitly recovered event without mutating platform state."""
    event = _event_row(event_id, control_db)
    if event is None:
        raise KeyError(f"Event not found: {event_id}")

    runs = _runs_for_event(event_id, control_db)
    run_count = len(runs)
    latest_run = runs[-1] if runs else None

    checks = []

    event_status = str(event.get("status") or "")
    checks.append({
        "check": "EVENT_COMPLETED",
        "passed": event_status == "COMPLETED",
        "actual": event_status,
        "message": "event reached COMPLETED" if event_status == "COMPLETED"
                   else f"event status is {event_status}",
    })

    checks.append({
        "check": "RUN_PRESENT",
        "passed": (run_count > 0) if require_run else True,
        "actual": run_count,
        "message": "run exists for trigger event" if run_count else "no run found",
    })

    if latest_run:
        run_status = str(latest_run.get("status") or "")
        checks.append({
            "check": "RUN_TERMINAL",
            "passed": run_status in EXPECTED_TERMINAL_RUN_STATUSES,
            "actual": run_status,
            "message": "run is terminal" if run_status in EXPECTED_TERMINAL_RUN_STATUSES
                       else "run is still active/incomplete",
        })
        checks.append({
            "check": "RUN_SUCCESS_OR_PARTIAL",
            "passed": run_status in {"SUCCESS", "PARTIAL"},
            "actual": run_status,
            "message": "run completed without hard failure"
                     if run_status in {"SUCCESS", "PARTIAL"}
                     else "run did not recover successfully",
        })

        store = SQLiteRunStore(db_path=str(control_db))
        tasks = store.get_tasks_for_run(str(latest_run["run_id"]))
        if tasks.empty:
            checks.append({
                "check": "TASKS_PRESENT",
                "passed": False,
                "actual": 0,
                "message": "no tasks found for recovered run",
            })
        else:
            statuses = set(tasks["status"].astype(str))
            checks.append({
                "check": "NO_FAILED_TASKS",
                "passed": "FAILED" not in statuses,
                "actual": sorted(statuses),
                "message": "no FAILED tasks" if "FAILED" not in statuses
                           else "one or more tasks remain FAILED",
            })
            checks.append({
                "check": "NO_BLOCKED_TASKS",
                "passed": "BLOCKED" not in statuses,
                "actual": sorted(statuses),
                "message": "no generic BLOCKED tasks" if "BLOCKED" not in statuses
                           else "one or more tasks remain BLOCKED",
            })

        artifact_checks, cik = _artifact_checks(latest_run)
        checks.extend(artifact_checks)
    else:
        cik = str(event.get("company_id") or "").replace("SEC_", "").zfill(10)

    failed = [c for c in checks if not c["passed"]]
    if event_status == "COMPLETED" and not failed:
        verification_status = "RECOVERY_SUCCESS"
    elif event_status in {"FAILED", "QUARANTINED"}:
        verification_status = "RECOVERY_FAILED"
    elif event_status in {"PENDING", "PROCESSING"}:
        verification_status = "RECOVERY_INCOMPLETE"
    elif failed:
        verification_status = "RECOVERY_PARTIAL"
    else:
        verification_status = "RECOVERY_INCOMPLETE"

    return {
        "verification_version": VERIFICATION_VERSION,
        "event_id": event_id,
        "company_id": event.get("company_id"),
        "cik": cik,
        "event_status": event_status,
        "run_id": latest_run.get("run_id") if latest_run else None,
        "run_status": latest_run.get("status") if latest_run else None,
        "run_count": run_count,
        "verification_status": verification_status,
        "checks": checks,
        "verified_at_utc": _now(),
    }


def verify_recovery_history(
    control_db: Path = CONTROL_DB,
    *,
    include_non_completed: bool = True,
) -> dict[str, pd.DataFrame]:
    """Verify every event with an applied Step 27 recovery audit record."""
    with _db(control_db) as c:
        if _table_exists(c, "recovery_actions"):
            rows = c.execute(
                "SELECT target_id, company_id, created_at_utc "
                "FROM recovery_actions WHERE action_type='EVENT_RETRY' "
                "ORDER BY created_at_utc"
            ).fetchall()
        else:
            rows = []

    results = []
    check_rows = []
    for row in rows:
        result = verify_event_recovery(row["target_id"], control_db=control_db)
        if include_non_completed or result["verification_status"] == "RECOVERY_SUCCESS":
            results.append({
                k: result[k] for k in (
                    "verification_version", "event_id", "company_id", "cik",
                    "event_status", "run_id", "run_status", "run_count",
                    "verification_status", "verified_at_utc"
                )
            })
        for check in result["checks"]:
            check_rows.append({
                "verification_version": VERIFICATION_VERSION,
                "event_id": result["event_id"],
                "company_id": result["company_id"],
                "run_id": result["run_id"],
                "verification_status": result["verification_status"],
                **check,
            })

    verification_df = pd.DataFrame(results, columns=[
        "verification_version","event_id","company_id","cik","event_status",
        "run_id","run_status","run_count","verification_status","verified_at_utc"
    ])
    checks_df = pd.DataFrame(check_rows, columns=[
        "verification_version","event_id","company_id","run_id",
        "verification_status","check","passed","actual","path","message"
    ])

    if verification_df.empty:
        counts = {"RECOVERY_SUCCESS": 0, "RECOVERY_FAILED": 0,
                  "RECOVERY_PARTIAL": 0, "RECOVERY_INCOMPLETE": 0}
    else:
        counts = verification_df["verification_status"].value_counts().to_dict()

    summary = pd.DataFrame([{
        "verification_version": VERIFICATION_VERSION,
        "generated_at_utc": _now(),
        "recovery_actions_verified": len(verification_df),
        "recovery_success": int(counts.get("RECOVERY_SUCCESS", 0)),
        "recovery_failed": int(counts.get("RECOVERY_FAILED", 0)),
        "recovery_partial": int(counts.get("RECOVERY_PARTIAL", 0)),
        "recovery_incomplete": int(counts.get("RECOVERY_INCOMPLETE", 0)),
        "verification_readiness":
            "READY" if len(verification_df) == 0 or
            (counts.get("RECOVERY_FAILED", 0) == 0 and
             counts.get("RECOVERY_PARTIAL", 0) == 0 and
             counts.get("RECOVERY_INCOMPLETE", 0) == 0)
            else "NOT_READY",
    }])

    return {
        "summary": summary,
        "recovery_verification": verification_df,
        "verification_checks": checks_df,
    }


def write_artifacts(artifacts: dict[str, pd.DataFrame], output: Path = OUTPUT):
    output.mkdir(parents=True, exist_ok=True)
    for name, df in artifacts.items():
        df.to_parquet(output / f"{name}.parquet", index=False)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Step 28 recovery verification")
    parser.add_argument("--event-id")
    parser.add_argument("--control-db", type=Path, default=CONTROL_DB)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args(argv)

    if args.event_id:
        result = verify_event_recovery(args.event_id, control_db=args.control_db)
        print(json.dumps(result, indent=2, default=str))
        return 0 if result["verification_status"] == "RECOVERY_SUCCESS" else 1

    artifacts = verify_recovery_history(args.control_db)
    write_artifacts(artifacts, args.output)
    s = artifacts["summary"].iloc[0]
    print("=" * 80)
    print("STEP 28 - RECOVERY VERIFICATION & POST-RECOVERY VALIDATION")
    print(f"Verification version : {VERIFICATION_VERSION}")
    print(f"Actions verified     : {int(s['recovery_actions_verified'])}")
    print(f"Recovery success     : {int(s['recovery_success'])}")
    print(f"Recovery failed      : {int(s['recovery_failed'])}")
    print(f"Recovery partial     : {int(s['recovery_partial'])}")
    print(f"Recovery incomplete  : {int(s['recovery_incomplete'])}")
    print(f"Readiness            : {s['verification_readiness']}")
    print(f"Output               : {args.output}")
    print("=" * 80)
    return 0 if s["verification_readiness"] == "READY" else 1


if __name__ == "__main__":
    raise SystemExit(main())
