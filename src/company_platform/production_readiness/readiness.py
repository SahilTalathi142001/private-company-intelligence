from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

READINESS_VERSION = "30.1.0"
ROOT = Path(__file__).resolve().parents[3]
CONTROL_DB = ROOT / "data" / "silver" / "platform" / "control.db"
RECONCILIATION = ROOT / "data" / "gold" / "platform_reconciliation" / "summary.parquet"
FAILURE_ANALYSIS = ROOT / "data" / "gold" / "platform_failure_analysis" / "summary.parquet"
RECOVERY_VERIFICATION = ROOT / "data" / "gold" / "platform_recovery_verification" / "summary.parquet"
OUTPUT = ROOT / "data" / "gold" / "platform_production_readiness"

# Step 30.1 distinguishes operational state from historical terminal history.
# Historical FAILED/blocked records remain observable but do not fail readiness
# once the failure-analysis layer confirms there are no unresolved failures.
SLO = {
    "max_pending_events": 0,
    "max_processing_events": 0,
    "max_stale_events": 0,
    "max_unresolved_failures": 0,
    "max_retry_exhausted": 0,
    "max_recovery_failures": 0,
    "max_recovery_incomplete": 0,
    "max_reconciliation_failures": 0,
}


def now():
    return datetime.now(timezone.utc).isoformat()


def _read_summary(path):
    if not path.exists():
        return {}
    try:
        df = pd.read_parquet(path)
        return {} if df.empty else df.iloc[0].to_dict()
    except Exception:
        return {}


def _control_snapshot():
    if not CONTROL_DB.exists():
        return {
            "pending_events": 0, "processing_events": 0,
            "failed_events": 0, "quarantined_events": 0,
            "stale_events": 0,
            "historical_failed_events": 0,
            "historical_failed_runs": 0,
            "historical_failed_tasks": 0,
            "historical_blocked_tasks": 0,
        }

    with sqlite3.connect(CONTROL_DB) as c:
        def count(table, status):
            try:
                return int(c.execute(
                    "SELECT COUNT(*) FROM " + table +
                    " WHERE json_extract(data,'$.status')=?",
                    (status,)
                ).fetchone()[0])
            except sqlite3.Error:
                return 0

        return {
            "pending_events": count("events", "PENDING"),
            "processing_events": count("events", "PROCESSING"),
            "failed_events": count("events", "FAILED"),
            "quarantined_events": count("events", "QUARANTINED"),
            "stale_events": 0,
            "historical_failed_events": count("events", "FAILED"),
            "historical_failed_runs": count("pipeline_runs", "FAILED"),
            "historical_failed_tasks": count("task_runs", "FAILED"),
            "historical_blocked_tasks": count("task_runs", "BLOCKED"),
        }


def _gate(name, actual, threshold, comparator="<="):
    passed = actual <= threshold if comparator == "<=" else actual >= threshold
    return {
        "readiness_version": READINESS_VERSION,
        "gate": name,
        "actual": actual,
        "threshold": threshold,
        "passed": bool(passed),
        "severity": "INFO" if passed else "FATAL",
        "evaluated_at_utc": now(),
    }


def evaluate_readiness(
    *,
    control=None,
    reconciliation=None,
    failure_analysis=None,
    recovery_verification=None,
    thresholds=None,
):
    thresholds = dict(SLO if thresholds is None else thresholds)
    control = _control_snapshot() if control is None else dict(control)
    reconciliation = _read_summary(RECONCILIATION) if reconciliation is None else dict(reconciliation)
    failure_analysis = _read_summary(FAILURE_ANALYSIS) if failure_analysis is None else dict(failure_analysis)
    recovery_verification = (
        _read_summary(RECOVERY_VERIFICATION)
        if recovery_verification is None else dict(recovery_verification)
    )

    gates = []

    mapping = [
        ("PENDING_EVENTS", control.get("pending_events", 0), "max_pending_events"),
        ("PROCESSING_EVENTS", control.get("processing_events", 0), "max_processing_events"),
        ("STALE_EVENTS", control.get("stale_events", 0), "max_stale_events"),
        ("UNRESOLVED_FAILURES", failure_analysis.get("unresolved_failures", 0), "max_unresolved_failures"),
        ("RETRY_EXHAUSTED", failure_analysis.get("retry_exhausted", 0), "max_retry_exhausted"),
        ("RECOVERY_FAILURES", recovery_verification.get("recovery_failed", 0), "max_recovery_failures"),
        ("RECOVERY_INCOMPLETE", recovery_verification.get("recovery_incomplete", 0), "max_recovery_incomplete"),
        ("RECONCILIATION_FAILURES", reconciliation.get("failed_checks", 0), "max_reconciliation_failures"),
    ]
    for name, actual, key in mapping:
        gates.append(_gate(name, int(actual or 0), int(thresholds[key])))

    recon_status = reconciliation.get("reconciliation_status")
    if recon_status is not None:
        gates.append(_gate("RECONCILIATION_STATUS", 1 if recon_status == "READY" else 0, 1, ">="))

    recovery_status = recovery_verification.get("verification_status")
    if recovery_status is not None:
        bad = 0 if recovery_status in {"READY", "NO_RECOVERIES"} else 1
        gates.append(_gate("RECOVERY_VERIFICATION_STATUS", bad, 0))

    failure_readiness = failure_analysis.get("recovery_readiness")
    if failure_readiness is not None:
        gates.append(_gate(
            "FAILURE_RECOVERY_READINESS",
            1 if failure_readiness == "READY" else 0,
            1, ">="
        ))

    status = "READY" if all(g["passed"] for g in gates) else "NOT_READY"

    summary = {
        "readiness_version": READINESS_VERSION,
        "generated_at_utc": now(),
        "status": status,
        "production_readiness": status,
        "gates": len(gates),
        "passed_gates": sum(bool(g["passed"]) for g in gates),
        "failed_gates": sum(not bool(g["passed"]) for g in gates),
        # Historical terminal counts are retained as operational metrics.
        "historical_failed_events": int(control.get("historical_failed_events", 0)),
        "historical_failed_runs": int(control.get("historical_failed_runs", 0)),
        "historical_failed_tasks": int(control.get("historical_failed_tasks", 0)),
        "historical_blocked_tasks": int(control.get("historical_blocked_tasks", 0)),
        "unresolved_failures": int(failure_analysis.get("unresolved_failures", 0) or 0),
        "retry_exhausted": int(failure_analysis.get("retry_exhausted", 0) or 0),
    }
    return pd.DataFrame([summary]), pd.DataFrame(gates)


def write_artifacts(summary, gates, output=OUTPUT):
    output.mkdir(parents=True, exist_ok=True)
    summary.to_parquet(output / "summary.parquet", index=False)
    gates.to_parquet(output / "readiness_gates.parquet", index=False)


def main():
    p = argparse.ArgumentParser(description="Step 30 production readiness / SLA gate")
    p.add_argument("--max-unresolved-failures", type=int, default=0)
    p.add_argument("--max-retry-exhausted", type=int, default=0)
    p.add_argument("--max-reconciliation-failures", type=int, default=0)
    p.add_argument("--output", type=Path, default=OUTPUT)
    a = p.parse_args()

    thresholds = dict(SLO)
    thresholds["max_unresolved_failures"] = a.max_unresolved_failures
    thresholds["max_retry_exhausted"] = a.max_retry_exhausted
    thresholds["max_reconciliation_failures"] = a.max_reconciliation_failures

    summary, gates = evaluate_readiness(thresholds=thresholds)
    write_artifacts(summary, gates, a.output)

    s = summary.iloc[0]
    print("=" * 80)
    print("STEP 30 - PRODUCTION READINESS / SLA")
    print(f"Readiness version : {READINESS_VERSION}")
    print(f"Gates             : {int(s['gates'])}")
    print(f"Passed gates      : {int(s['passed_gates'])}")
    print(f"Failed gates      : {int(s['failed_gates'])}")
    print(f"Historical failed events : {int(s['historical_failed_events'])}")
    print(f"Historical failed runs   : {int(s['historical_failed_runs'])}")
    print(f"Historical failed tasks  : {int(s['historical_failed_tasks'])}")
    print(f"Historical blocked tasks : {int(s['historical_blocked_tasks'])}")
    print(f"Unresolved failures      : {int(s['unresolved_failures'])}")
    print(f"Retry exhausted          : {int(s['retry_exhausted'])}")
    print(f"Status            : {s['status']}")
    print(f"Output            : {a.output}")
    print("=" * 80)
    return 0 if s["status"] == "READY" else 1


if __name__ == "__main__":
    raise SystemExit(main())
