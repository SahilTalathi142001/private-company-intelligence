from __future__ import annotations
import logging
from pathlib import Path
from typing import Any
import pandas as pd
from company_platform.observability.operational_observability import build_operational_snapshot

HEALTH_VERSION = "25.0.0"
ROOT = Path(__file__).resolve().parents[3]
CONTROL_DB = ROOT / "data" / "silver" / "platform" / "control.db"
OUTPUT = ROOT / "data" / "gold" / "platform_operational_health"

logger = logging.getLogger("step25_operational_health")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

def _as_int(value: Any) -> int:
    if value is None or pd.isna(value):
        return 0
    return int(value)

def evaluate_health(
    summary: pd.Series,
    *,
    max_pending_events: int = 0,
    max_stale_processing: int = 0,
    max_active_failed_events: int = 0,
    max_active_failed_runs: int = 0,
) -> dict[str, Any]:
    pending = _as_int(summary.get("event_pending_count"))
    stale = _as_int(summary.get("event_stale_processing_count"))
    failed_events = _as_int(summary.get("event_failed_count"))
    failed_runs = _as_int(summary.get("run_failed_count"))

    reasons = []
    if stale > max_stale_processing:
        reasons.append(f"STALE_PROCESSING={stale}>{max_stale_processing}")
    if pending > max_pending_events:
        reasons.append(f"PENDING_EVENTS={pending}>{max_pending_events}")
    if failed_events > max_active_failed_events:
        reasons.append(f"FAILED_EVENTS={failed_events}>{max_active_failed_events}")
    if failed_runs > max_active_failed_runs:
        reasons.append(f"FAILED_RUNS={failed_runs}>{max_active_failed_runs}")

    critical = bool(reasons)
    return {
        "health_status": "CRITICAL" if critical else "HEALTHY",
        "readiness_status": "NOT_READY" if critical else "READY",
        "health_reason": "; ".join(reasons) if reasons else "Operational thresholds satisfied",
        "pending_event_count": pending,
        "stale_processing_count": stale,
        "failed_event_count": failed_events,
        "failed_run_count": failed_runs,
    }

def build_operational_health(
    *,
    db_path: Path | None = None,
    max_pending_events: int = 0,
    max_stale_processing: int = 0,
    max_active_failed_events: int = 0,
    max_active_failed_runs: int = 0,
) -> dict[str, pd.DataFrame]:
    snapshot = build_operational_snapshot(db_path=db_path)
    summary = snapshot["summary"].iloc[0]
    evaluation = evaluate_health(
        summary,
        max_pending_events=max_pending_events,
        max_stale_processing=max_stale_processing,
        max_active_failed_events=max_active_failed_events,
        max_active_failed_runs=max_active_failed_runs,
    )

    health_summary = pd.DataFrame([{
        "health_version": HEALTH_VERSION,
        "health_status": evaluation["health_status"],
        "readiness_status": evaluation["readiness_status"],
        "health_reason": evaluation["health_reason"],
        "event_count": _as_int(summary.get("event_count")),
        "pending_event_count": evaluation["pending_event_count"],
        "processing_event_count": _as_int(summary.get("event_processing_count")),
        "stale_processing_count": evaluation["stale_processing_count"],
        "failed_event_count": evaluation["failed_event_count"],
        "quarantined_event_count": _as_int(summary.get("event_quarantined_count")),
        "run_count": _as_int(summary.get("run_count")),
        "running_run_count": _as_int(summary.get("run_running_count")),
        "failed_run_count": evaluation["failed_run_count"],
        "quarantined_run_count": _as_int(summary.get("run_quarantined_count")),
        "task_count": _as_int(summary.get("task_count")),
        "failed_task_count": _as_int(summary.get("task_failed_count")),
        "generated_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
    }])

    checks = pd.DataFrame([
        {"check_id": "PENDING_EVENT_QUEUE", "severity": "CRITICAL",
         "passed": evaluation["pending_event_count"] <= max_pending_events,
         "actual_value": evaluation["pending_event_count"], "threshold": max_pending_events},
        {"check_id": "STALE_PROCESSING_LEASE", "severity": "CRITICAL",
         "passed": evaluation["stale_processing_count"] <= max_stale_processing,
         "actual_value": evaluation["stale_processing_count"], "threshold": max_stale_processing},
        {"check_id": "FAILED_EVENTS", "severity": "CRITICAL",
         "passed": evaluation["failed_event_count"] <= max_active_failed_events,
         "actual_value": evaluation["failed_event_count"], "threshold": max_active_failed_events},
        {"check_id": "FAILED_RUNS", "severity": "CRITICAL",
         "passed": evaluation["failed_run_count"] <= max_active_failed_runs,
         "actual_value": evaluation["failed_run_count"], "threshold": max_active_failed_runs},
    ])
    checks["message"] = checks.apply(
        lambda r: f"{r['check_id']} within threshold" if r["passed"]
        else f"{r['check_id']} exceeds threshold", axis=1
    )

    company = snapshot["company_metrics"].copy()
    if company.empty:
        company["company_health_status"] = pd.Series(dtype="string")
    else:
        company["company_health_status"] = "HEALTHY"
        critical = (
            (company["event_pending_count"] > max_pending_events)
            | (company["event_processing_count"] > 0)
            | (company["event_failed_count"] > max_active_failed_events)
            | (company["run_failed_count"] > max_active_failed_runs)
        )
        company.loc[critical, "company_health_status"] = "CRITICAL"

    return {
        "summary": health_summary,
        "checks": checks,
        "company_health": company,
        "event_status": snapshot["event_status"],
        "run_status": snapshot["run_status"],
        "task_status": snapshot["task_status"],
        "recovery_audit": snapshot["recovery_audit"],
    }

def write_operational_health(
    *,
    db_path: Path | None = None,
    output: Path | None = None,
    max_pending_events: int = 0,
    max_stale_processing: int = 0,
    max_active_failed_events: int = 0,
    max_active_failed_runs: int = 0,
) -> dict[str, Path]:
    output_dir = output or OUTPUT
    output_dir.mkdir(parents=True, exist_ok=True)
    frames = build_operational_health(
        db_path=db_path,
        max_pending_events=max_pending_events,
        max_stale_processing=max_stale_processing,
        max_active_failed_events=max_active_failed_events,
        max_active_failed_runs=max_active_failed_runs,
    )
    paths = {}
    for name, frame in frames.items():
        path = output_dir / f"{name}.parquet"
        frame.to_parquet(path, index=False)
        paths[name] = path
    return paths

def main() -> int:
    paths = write_operational_health()
    summary = pd.read_parquet(paths["summary"]).iloc[0]
    logger.info("=" * 80)
    logger.info("STEP 25 - PLATFORM OPERATIONAL HEALTH / READINESS")
    logger.info("Health version       : %s", HEALTH_VERSION)
    logger.info("Health status        : %s", summary["health_status"])
    logger.info("Readiness status     : %s", summary["readiness_status"])
    logger.info("Pending events       : %s", summary["pending_event_count"])
    logger.info("Stale processing     : %s", summary["stale_processing_count"])
    logger.info("Failed events        : %s", summary["failed_event_count"])
    logger.info("Failed runs          : %s", summary["failed_run_count"])
    logger.info("Reason               : %s", summary["health_reason"])
    logger.info("Output               : %s", OUTPUT)
    logger.info("=" * 80)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
