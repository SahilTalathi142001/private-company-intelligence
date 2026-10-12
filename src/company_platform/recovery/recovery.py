from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from company_platform.events.event_store import EventStore

RECOVERY_VERSION = "27.0.0"
ROOT = Path(__file__).resolve().parents[3]
CONTROL_DB = ROOT / "data" / "silver" / "platform" / "control.db"
OUTPUT = ROOT / "data" / "gold" / "platform_recovery"

DEFAULT_MAX_EVENT_RETRIES = 3


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _db(path: Path = CONTROL_DB):
    c = sqlite3.connect(str(path), timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout=30000")
    return c


def _ensure_audit_table(c):
    c.execute("""
        CREATE TABLE IF NOT EXISTS recovery_actions (
            action_id TEXT PRIMARY KEY,
            action_type TEXT NOT NULL,
            target_id TEXT NOT NULL,
            company_id TEXT,
            previous_status TEXT,
            resulting_status TEXT,
            previous_retry_count INTEGER,
            resulting_retry_count INTEGER,
            actor TEXT NOT NULL,
            reason TEXT NOT NULL,
            idempotency_key TEXT NOT NULL UNIQUE,
            created_at_utc TEXT NOT NULL
        )
    """)


def _audit_lookup(c, key: str):
    return c.execute(
        "SELECT * FROM recovery_actions WHERE idempotency_key=?",
        (key,),
    ).fetchone()


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


def _record_audit(c, *, action_id, action_type, target_id, company_id,
                  previous_status, resulting_status, previous_retry_count,
                  resulting_retry_count, actor, reason, idempotency_key):
    c.execute("""
        INSERT INTO recovery_actions
        (action_id,action_type,target_id,company_id,previous_status,resulting_status,
         previous_retry_count,resulting_retry_count,actor,reason,idempotency_key,created_at_utc)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        action_id, action_type, target_id, company_id, previous_status,
        resulting_status, previous_retry_count, resulting_retry_count,
        actor, reason, idempotency_key, _now(),
    ))


def recover_failed_event(
    event_id: str,
    *,
    control_db: Path = CONTROL_DB,
    max_retries: int = DEFAULT_MAX_EVENT_RETRIES,
    actor: str = "step27",
    reason: str = "Controlled recovery of retryable failed event",
    idempotency_key: Optional[str] = None,
) -> dict:
    """Explicitly move one FAILED event to PENDING through EventStore.retry()."""
    if not event_id:
        raise ValueError("event_id is required")
    if max_retries < 0:
        raise ValueError("max_retries must be >= 0")
    if not reason.strip():
        raise ValueError("reason is required")

    key = idempotency_key or f"EVENT_RETRY:{event_id}:{max_retries}"
    c = _db(control_db)
    try:
        _ensure_audit_table(c)
        existing = _audit_lookup(c, key)
        if existing:
            c.commit()
            return {
                "action_id": existing["action_id"],
                "action_type": existing["action_type"],
                "target_id": existing["target_id"],
                "status": "ALREADY_APPLIED",
                "resulting_status": existing["resulting_status"],
                "retry_count": existing["resulting_retry_count"],
                "idempotent": True,
            }

        row = c.execute(
            "SELECT * FROM events WHERE event_id=?", (str(event_id),)
        ).fetchone()
        if not row:
            raise KeyError(f"Event not found: {event_id}")
        event = _event_from_row(row)
        previous_status = str(event.get("status") or "")
        previous_retry = int(event.get("retry_count") or 0)
        company_id = event.get("company_id")

        if previous_status != "FAILED":
            raise ValueError(
                f"Only FAILED events are recoverable; current status={previous_status}"
            )
        if previous_retry >= max_retries:
            raise ValueError(
                f"Retry limit reached: retry_count={previous_retry}, max_retries={max_retries}"
            )

        # Use the production EventStore transition, not a direct SQL status update.
        store = EventStore(path=str(control_db))
        updated = store.retry(event_id, max_retries=max_retries)
        resulting_status = str(updated["status"])
        resulting_retry = int(updated["retry_count"])

        action_id = f"RECOVERY-{event_id}-{resulting_retry}"
        _record_audit(
            c, action_id=action_id, action_type="EVENT_RETRY",
            target_id=event_id, company_id=company_id,
            previous_status=previous_status, resulting_status=resulting_status,
            previous_retry_count=previous_retry, resulting_retry_count=resulting_retry,
            actor=actor, reason=reason.strip(), idempotency_key=key,
        )
        c.commit()
        return {
            "action_id": action_id,
            "action_type": "EVENT_RETRY",
            "target_id": event_id,
            "status": "APPLIED",
            "resulting_status": resulting_status,
            "retry_count": resulting_retry,
            "company_id": company_id,
            "idempotent": False,
        }
    finally:
        c.close()


def inspect_recovery(
    control_db: Path = CONTROL_DB,
    max_retries: int = DEFAULT_MAX_EVENT_RETRIES,
) -> dict[str, pd.DataFrame]:
    """Read-only recovery inventory plus applied recovery history."""
    with _db(control_db) as c:
        _ensure_audit_table(c)
        events = c.execute(
            "SELECT * FROM events WHERE status IN ('FAILED','QUARANTINED')"
        ).fetchall()
        audit = c.execute(
            "SELECT * FROM recovery_actions ORDER BY created_at_utc"
        ).fetchall()
        c.commit()

    rows = []
    for raw in events:
        e = _event_from_row(raw)
        retry_count = int(e.get("retry_count") or 0)
        eligible = str(e.get("status")) == "FAILED" and retry_count < max_retries
        rows.append({
            "recovery_version": RECOVERY_VERSION,
            "event_id": e.get("event_id"),
            "company_id": e.get("company_id"),
            "event_type": e.get("event_type"),
            "status": e.get("status"),
            "retry_count": retry_count,
            "max_retries": max_retries,
            "retry_eligible": eligible,
            "recommended_action": "EVENT_RETRY" if eligible else "MANUAL_REVIEW",
        })

    inventory = pd.DataFrame(rows, columns=[
        "recovery_version","event_id","company_id","event_type","status",
        "retry_count","max_retries","retry_eligible","recommended_action"
    ])
    audit_df = pd.DataFrame([dict(r) for r in audit])
    if audit_df.empty:
        audit_df = pd.DataFrame(columns=[
            "action_id","action_type","target_id","company_id","previous_status",
            "resulting_status","previous_retry_count","resulting_retry_count",
            "actor","reason","idempotency_key","created_at_utc"
        ])

    summary = pd.DataFrame([{
        "recovery_version": RECOVERY_VERSION,
        "generated_at_utc": _now(),
        "failed_events": int((inventory["status"] == "FAILED").sum()) if not inventory.empty else 0,
        "retryable_events": int(inventory["retry_eligible"].sum()) if not inventory.empty else 0,
        "quarantined_events": int((inventory["status"] == "QUARANTINED").sum()) if not inventory.empty else 0,
        "recovery_actions_applied": len(audit_df),
    }])

    return {
        "summary": summary,
        "recovery_inventory": inventory,
        "recovery_actions": audit_df,
    }


def write_recovery_artifacts(artifacts: dict[str, pd.DataFrame], output: Path = OUTPUT):
    output.mkdir(parents=True, exist_ok=True)
    for name, df in artifacts.items():
        df.to_parquet(output / f"{name}.parquet", index=False)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Step 27 controlled failure recovery")
    parser.add_argument("--inspect", action="store_true")
    parser.add_argument("--retry-event-id")
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_EVENT_RETRIES)
    parser.add_argument("--actor", default="step27")
    parser.add_argument("--reason", default="Controlled recovery of retryable failed event")
    parser.add_argument("--idempotency-key")
    parser.add_argument("--control-db", type=Path, default=CONTROL_DB)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args(argv)

    if args.retry_event_id:
        result = recover_failed_event(
            args.retry_event_id,
            control_db=args.control_db,
            max_retries=args.max_retries,
            actor=args.actor,
            reason=args.reason,
            idempotency_key=args.idempotency_key,
        )
        print(json.dumps(result, indent=2, default=str))
        return 0

    artifacts = inspect_recovery(args.control_db, args.max_retries)
    write_recovery_artifacts(artifacts, args.output)
    s = artifacts["summary"].iloc[0]
    print("=" * 80)
    print("STEP 27 - CONTROLLED FAILURE RECOVERY & REMEDIATION")
    print(f"Recovery version     : {RECOVERY_VERSION}")
    print(f"Failed events        : {int(s['failed_events'])}")
    print(f"Retryable events     : {int(s['retryable_events'])}")
    print(f"Quarantined events   : {int(s['quarantined_events'])}")
    print(f"Actions applied      : {int(s['recovery_actions_applied'])}")
    print(f"Output               : {args.output}")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
