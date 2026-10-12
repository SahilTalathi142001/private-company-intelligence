from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd

from company_platform.events.event_store import EventStore
from company_platform.run_control.sqlite_run_store import SQLiteRunStore


OBSERVABILITY_VERSION = "24.0.0"

ROOT = Path(__file__).resolve().parents[3]
DATA = ROOT / "data"
CONTROL_DB = DATA / "silver" / "platform" / "control.db"
OUTPUT = DATA / "gold" / "platform_observability"

logger = logging.getLogger("step24_observability")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


def _empty_event_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "event_id",
            "event_type",
            "source_id",
            "company_id",
            "status",
            "retry_count",
            "lease_until",
            "updated_at",
        ]
    )


def _load_events(db_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        return _empty_event_df()

    try:
        events = EventStore(path=str(db_path)).list_events()
    except Exception:
        return _empty_event_df()

    if events.empty:
        return _empty_event_df()

    columns = [
        "event_id",
        "event_type",
        "source_id",
        "company_id",
        "status",
        "retry_count",
        "lease_until",
        "updated_at",
    ]

    for column in columns:
        if column not in events.columns:
            events[column] = None

    return events[columns].copy()


def _load_runs_and_tasks(
    db_path: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not db_path.exists():
        return pd.DataFrame(), pd.DataFrame()

    store = SQLiteRunStore(db_path=str(db_path))

    runs = store.runs()
    tasks = store.tasks()

    return runs, tasks


def _event_metrics(events: pd.DataFrame) -> dict[str, Any]:
    if events.empty:
        return {
            "event_count": 0,
            "event_pending_count": 0,
            "event_processing_count": 0,
            "event_completed_count": 0,
            "event_failed_count": 0,
            "event_quarantined_count": 0,
            "event_retry_count": 0,
            "event_stale_processing_count": 0,
        }

    now_epoch = pd.Timestamp.now(tz="UTC").timestamp()

    statuses = events["status"].astype("string").str.upper()

    lease = pd.to_numeric(events["lease_until"], errors="coerce")
    stale_processing = (
        statuses.eq("PROCESSING")
        & lease.notna()
        & (lease < now_epoch)
    )

    return {
        "event_count": int(len(events)),
        "event_pending_count": int(statuses.eq("PENDING").sum()),
        "event_processing_count": int(statuses.eq("PROCESSING").sum()),
        "event_completed_count": int(statuses.eq("COMPLETED").sum()),
        "event_failed_count": int(statuses.eq("FAILED").sum()),
        "event_quarantined_count": int(statuses.eq("QUARANTINED").sum()),
        "event_retry_count": int(
            pd.to_numeric(events["retry_count"], errors="coerce")
            .fillna(0)
            .sum()
        ),
        "event_stale_processing_count": int(stale_processing.sum()),
    }


def _run_metrics(
    runs: pd.DataFrame,
    tasks: pd.DataFrame,
) -> dict[str, Any]:
    if runs.empty:
        return {
            "run_count": 0,
            "run_running_count": 0,
            "run_success_count": 0,
            "run_failed_count": 0,
            "run_quarantined_count": 0,
            "run_partial_count": 0,
            "run_duration_avg_seconds": None,
            "task_count": int(len(tasks)),
            "task_running_count": int(
                tasks["status"].astype("string").eq("RUNNING").sum()
            )
            if not tasks.empty and "status" in tasks.columns
            else 0,
            "task_failed_count": int(
                tasks["status"].astype("string").eq("FAILED").sum()
            )
            if not tasks.empty and "status" in tasks.columns
            else 0,
            "task_quarantined_count": int(
                tasks["status"].astype("string").eq("QUARANTINED").sum()
            )
            if not tasks.empty and "status" in tasks.columns
            else 0,
        }

    status = runs["status"].astype("string").str.upper()

    durations = pd.Series(dtype="float64")
    if {"started_at_utc", "completed_at_utc"}.issubset(runs.columns):
        started = pd.to_datetime(
            runs["started_at_utc"], errors="coerce", utc=True
        )
        completed = pd.to_datetime(
            runs["completed_at_utc"], errors="coerce", utc=True
        )
        durations = (
            (completed - started)
            .dt.total_seconds()
            .dropna()
        )

    task_status = (
        tasks["status"].astype("string").str.upper()
        if not tasks.empty and "status" in tasks.columns
        else pd.Series(dtype="string")
    )

    return {
        "run_count": int(len(runs)),
        "run_running_count": int(status.eq("RUNNING").sum()),
        "run_success_count": int(status.eq("SUCCESS").sum()),
        "run_failed_count": int(status.eq("FAILED").sum()),
        "run_quarantined_count": int(status.eq("QUARANTINED").sum()),
        "run_partial_count": int(status.eq("PARTIAL").sum()),
        "run_duration_avg_seconds": (
            float(durations.mean()) if not durations.empty else None
        ),
        "task_count": int(len(tasks)),
        "task_running_count": int(task_status.eq("RUNNING").sum()),
        "task_failed_count": int(task_status.eq("FAILED").sum()),
        "task_quarantined_count": int(
            task_status.eq("QUARANTINED").sum()
        ),
    }


def _company_metrics(
    events: pd.DataFrame,
    runs: pd.DataFrame,
) -> pd.DataFrame:
    companies: set[str] = set()

    if not events.empty and "company_id" in events.columns:
        companies.update(
            str(value)
            for value in events["company_id"].dropna()
            if str(value).strip()
        )

    if not runs.empty and "company_id" in runs.columns:
        companies.update(
            str(value)
            for value in runs["company_id"].dropna()
            if str(value).strip()
        )

    rows: list[dict[str, Any]] = []

    for company_id in sorted(companies):
        company_events = (
            events[events["company_id"].astype("string") == company_id]
            if not events.empty
            else pd.DataFrame()
        )
        company_runs = (
            runs[runs["company_id"].astype("string") == company_id]
            if not runs.empty
            else pd.DataFrame()
        )

        event_status = (
            company_events["status"].astype("string").str.upper()
            if not company_events.empty
            else pd.Series(dtype="string")
        )
        run_status = (
            company_runs["status"].astype("string").str.upper()
            if not company_runs.empty
            else pd.Series(dtype="string")
        )

        rows.append(
            {
                "company_id": company_id,
                "event_count": int(len(company_events)),
                "event_pending_count": int(event_status.eq("PENDING").sum()),
                "event_processing_count": int(
                    event_status.eq("PROCESSING").sum()
                ),
                "event_failed_count": int(event_status.eq("FAILED").sum()),
                "event_quarantined_count": int(
                    event_status.eq("QUARANTINED").sum()
                ),
                "run_count": int(len(company_runs)),
                "run_running_count": int(run_status.eq("RUNNING").sum()),
                "run_success_count": int(run_status.eq("SUCCESS").sum()),
                "run_failed_count": int(run_status.eq("FAILED").sum()),
                "run_quarantined_count": int(
                    run_status.eq("QUARANTINED").sum()
                ),
            }
        )

    return pd.DataFrame(rows)


def _recovery_audit(db_path: Path) -> pd.DataFrame:
    columns = [
        "audit_id",
        "task_id",
        "from_status",
        "to_status",
        "reason",
        "created_at_utc",
    ]

    if not db_path.exists():
        return pd.DataFrame(columns=columns)

    with sqlite3.connect(str(db_path)) as conn:
        try:
            df = pd.read_sql_query(
                "SELECT audit_id, task_id, from_status, to_status, "
                "reason, created_at_utc "
                "FROM task_recovery_audit "
                "ORDER BY audit_id",
                conn,
            )
        except Exception:
            return pd.DataFrame(columns=columns)

    return df


def build_operational_snapshot(
    *,
    db_path: Path | None = None,
    output: Path | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Build a read-only operational snapshot from the authoritative control DB.

    No execution state is changed by this function.
    """
    db = db_path or CONTROL_DB
    output_dir = output or OUTPUT

    events = _load_events(db)
    runs, tasks = _load_runs_and_tasks(db)

    event_metrics = _event_metrics(events)
    run_metrics = _run_metrics(runs, tasks)

    summary = pd.DataFrame(
        [
            {
                "observability_version": OBSERVABILITY_VERSION,
                **event_metrics,
                **run_metrics,
                "generated_at_utc": pd.Timestamp.now(
                    tz="UTC"
                ).isoformat(),
            }
        ]
    )

    company = _company_metrics(events, runs)
    if company.empty:
        company = pd.DataFrame(
            columns=[
                "company_id",
                "event_count",
                "event_pending_count",
                "event_processing_count",
                "event_failed_count",
                "event_quarantined_count",
                "run_count",
                "run_running_count",
                "run_success_count",
                "run_failed_count",
                "run_quarantined_count",
            ]
        )

    recovery = _recovery_audit(db)

    event_status = (
        events.groupby(
            ["event_type", "status"],
            dropna=False,
        )
        .size()
        .reset_index(name="event_count")
        if not events.empty
        else pd.DataFrame(
            columns=["event_type", "status", "event_count"]
        )
    )

    run_status = (
        runs.groupby(
            ["trigger_type", "status"],
            dropna=False,
        )
        .size()
        .reset_index(name="run_count")
        if not runs.empty and {"trigger_type", "status"}.issubset(runs.columns)
        else pd.DataFrame(
            columns=["trigger_type", "status", "run_count"]
        )
    )

    task_status = (
        tasks.groupby(
            ["component_id", "status"],
            dropna=False,
        )
        .size()
        .reset_index(name="task_count")
        if not tasks.empty and {"component_id", "status"}.issubset(tasks.columns)
        else pd.DataFrame(
            columns=["component_id", "status", "task_count"]
        )
    )

    return {
        "summary": summary,
        "company_metrics": company,
        "event_status": event_status,
        "run_status": run_status,
        "task_status": task_status,
        "recovery_audit": recovery,
    }


def write_operational_snapshot(
    *,
    db_path: Path | None = None,
    output: Path | None = None,
) -> dict[str, Path]:
    output_dir = output or OUTPUT
    output_dir.mkdir(parents=True, exist_ok=True)

    frames = build_operational_snapshot(
        db_path=db_path,
        output=output_dir,
    )

    paths: dict[str, Path] = {}

    for name, frame in frames.items():
        path = output_dir / f"{name}.parquet"
        frame.to_parquet(path, index=False)
        paths[name] = path

    return paths


def main() -> int:
    paths = write_operational_snapshot()

    summary = pd.read_parquet(paths["summary"]).iloc[0]

    logger.info("=" * 80)
    logger.info("STEP 24 - OPERATIONAL OBSERVABILITY / RUN AUDIT")
    logger.info("Observability version : %s", OBSERVABILITY_VERSION)
    logger.info("Events                : %d", summary["event_count"])
    logger.info("Pending events        : %d", summary["event_pending_count"])
    logger.info("Processing events     : %d", summary["event_processing_count"])
    logger.info("Failed events         : %d", summary["event_failed_count"])
    logger.info("Quarantined events    : %d", summary["event_quarantined_count"])
    logger.info("Stale processing      : %d", summary["event_stale_processing_count"])
    logger.info("Pipeline runs         : %d", summary["run_count"])
    logger.info("Running runs          : %d", summary["run_running_count"])
    logger.info("Failed runs           : %d", summary["run_failed_count"])
    logger.info("Quarantined runs      : %d", summary["run_quarantined_count"])
    logger.info("Tasks                 : %d", summary["task_count"])
    logger.info("Failed tasks          : %d", summary["task_failed_count"])
    logger.info("Recovery audit rows   : %d", len(
        pd.read_parquet(paths["recovery_audit"])
    ))
    logger.info("Output                : %s", OUTPUT)
    logger.info("=" * 80)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
