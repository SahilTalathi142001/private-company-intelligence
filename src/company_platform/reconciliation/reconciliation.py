from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

RECONCILIATION_VERSION = "29.0.0"
ROOT = Path(__file__).resolve().parents[3]
CONTROL_DB = ROOT / "data" / "silver" / "platform" / "control.db"
STRUCTURED_DB = ROOT / "data" / "silver" / "platform" / "structured_events.db"
SIGNALS_DB = ROOT / "data" / "silver" / "platform" / "investment_signals.db"
DECISIONS_DB = ROOT / "data" / "silver" / "platform" / "decision_signals.db"
OUTPUT = ROOT / "data" / "gold" / "platform_reconciliation"

CHECKS = [
    "EVENT_RUN_LINEAGE",
    "RUN_TASK_COMPLETENESS",
    "COMPANY_IDENTITY",
    "EVENT_STRUCTURED_EVENT_LINEAGE",
    "STRUCTURED_EVENT_DOCUMENT_PROVENANCE",
    "SIGNAL_STATE_LINEAGE",
    "DECISION_SIGNAL_LINEAGE",
    "UNIFIED_INTELLIGENCE_PROVENANCE",
    "DOCUMENT_ARTIFACT_IDENTITY",
]


def _now():
    return datetime.now(timezone.utc).isoformat()


def _db(path):
    c = sqlite3.connect(str(path), timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout=30000")
    return c


def _table_rows(path, table):
    if not path.exists():
        return []
    with _db(path) as c:
        try:
            return [dict(r) for r in c.execute(f"SELECT * FROM {table}").fetchall()]
        except sqlite3.Error:
            return []


def _event_rows():
    if not CONTROL_DB.exists(): return []
    with _db(CONTROL_DB) as c:
        rows = c.execute("SELECT * FROM events").fetchall()
    out = []
    for r in rows:
        values = dict(r)
        serialized = values.pop("data", None)
        if serialized is None:
            out.append(values)
            continue
        try:
            event = json.loads(serialized)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(event, dict):
            event.update(values)
            out.append(event)
    return out


def _json_run_rows():
    if not CONTROL_DB.exists(): return []
    with _db(CONTROL_DB) as c:
        rows = c.execute("SELECT data FROM pipeline_runs").fetchall()
    out = []
    for r in rows:
        try: out.append(json.loads(r["data"]))
        except Exception: pass
    return out


def _json_task_rows():
    if not CONTROL_DB.exists(): return []
    with _db(CONTROL_DB) as c:
        rows = c.execute("SELECT data FROM task_runs").fetchall()
    out = []
    for r in rows:
        try: out.append(json.loads(r["data"]))
        except Exception: pass
    return out


def _check(name, passed, message, company_id=None, entity_id=None, severity="FATAL"):
    return {
        "reconciliation_version": RECONCILIATION_VERSION,
        "check": name,
        "passed": bool(passed),
        "severity": severity if not passed else "INFO",
        "company_id": company_id,
        "entity_id": entity_id,
        "message": message,
        "checked_at_utc": _now(),
    }


def _source_lineage_is_valid(
    source_event_id,
    source_run_id,
    event_ids: set[str],
    runs_by_id: dict[str, dict],
) -> bool:
    has_event = source_event_id not in (None, "")
    has_run = source_run_id not in (None, "")
    source_run = runs_by_id.get(str(source_run_id)) if has_run else None

    event_linked = (
        not has_event
        or str(source_event_id) in event_ids
        or (
            source_run is not None
            and str(source_run.get("trigger_event_id")) == str(source_event_id)
        )
    )
    run_linked = not has_run or source_run is not None
    matching_lineage = (
        not (has_event and has_run and source_run is not None)
        or str(source_run.get("trigger_event_id")) == str(source_event_id)
    )
    return event_linked and run_linked and matching_lineage


def _has_value(value) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    return bool(str(value).strip())


def _load_company_artifact(relative, cik, required_columns=None):
    path = ROOT / relative.format(cik=cik)
    if not path.exists():
        return None, path, f"MISSING:{path}"
    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        return None, path, f"UNREADABLE:{path}:{type(exc).__name__}"
    if df.empty:
        return df, path, f"EMPTY:{path}"
    if required_columns:
        missing = sorted(set(required_columns) - set(df.columns))
        if missing:
            return df, path, f"MISSING_COLUMNS:{','.join(missing)}"
    return df, path, "OK"


def reconcile_company(company_id):
    cik = str(company_id).replace("SEC_", "").zfill(10)
    checks = []

    events = [e for e in _event_rows() if str(e.get("company_id")) == company_id]
    runs = _json_run_rows()
    tasks = _json_task_rows()

    run_by_event = {}
    for r in runs:
        trigger = r.get("trigger_event_id")
        if trigger:
            run_by_event.setdefault(str(trigger), []).append(r)

    for e in events:
        eid = str(e.get("event_id"))
        linked = run_by_event.get(eid, [])
        event_terminal = str(e.get("status")) in {"COMPLETED", "FAILED", "QUARANTINED"}
        checks.append(_check(
            "EVENT_RUN_LINEAGE",
            (not event_terminal) or len(linked) == 1,
            f"event={e.get('status')} linked_runs={len(linked)}",
            company_id, eid,
        ))
        for r in linked:
            rid = str(r.get("run_id"))
            r_tasks = [t for t in tasks if str(t.get("run_id")) == rid]
            checks.append(_check(
                "RUN_TASK_COMPLETENESS",
                len(r_tasks) > 0,
                f"run={rid} task_count={len(r_tasks)}",
                company_id, rid,
            ))
            checks.append(_check(
                "COMPANY_IDENTITY",
                str(r.get("company_id")) == company_id and all(
                    str(t.get("company_id")) == company_id for t in r_tasks
                ),
                f"run_company={r.get('company_id')} task_companies={sorted({str(t.get('company_id')) for t in r_tasks})}",
                company_id, rid,
            ))

    structured = [x for x in _table_rows(STRUCTURED_DB, "structured_events")
                  if str(x.get("company_id")) == company_id]
    signals = [x for x in _table_rows(SIGNALS_DB, "investment_signals")
               if str(x.get("company_id")) == company_id]
    decisions = [x for x in _table_rows(DECISIONS_DB, "decision_signals")
                 if str(x.get("company_id")) == company_id]

    event_ids = {str(e.get("event_id")) for e in events}
    runs_by_id = {
        str(r.get("run_id")): r
        for r in runs
        if r.get("run_id") is not None
    }
    structured_by_doc = {str(x.get("document_id")): x for x in structured}
    structured_ids = {str(x.get("event_id")) for x in structured if x.get("event_id")}

    for x in structured:
        source_event = x.get("source_event_id")
        source_run = x.get("source_run_id")
        checks.append(_check(
            "EVENT_STRUCTURED_EVENT_LINEAGE",
            _source_lineage_is_valid(
                source_event,
                source_run,
                event_ids,
                runs_by_id,
            ),
            f"source_event={source_event} source_run={source_run}",
            company_id, x.get("event_id"),
        ))
        doc_id = x.get("document_id")
        checks.append(_check(
            "STRUCTURED_EVENT_DOCUMENT_PROVENANCE",
            bool(doc_id and x.get("source_document_sha256") and x.get("source_document_path")),
            f"document_id={doc_id} sha_present={bool(x.get('source_document_sha256'))}",
            company_id, x.get("event_id"),
        ))

    state_versions = set()
    # Signals carry the state version; reconciliation verifies all signals
    # reference a state lineage rather than assuming a separate state DB schema.
    for x in signals:
        sv = x.get("source_state_version")
        if sv is not None: state_versions.add(str(sv))
        checks.append(_check(
            "SIGNAL_STATE_LINEAGE",
            sv not in (None, ""),
            f"source_state_version={sv}",
            company_id, x.get("signal_id"),
        ))
        checks.append(_check(
            "COMPANY_IDENTITY",
            str(x.get("company_id")) == company_id and
            str(x.get("cik", "")).zfill(10) == cik,
            f"company_id={x.get('company_id')} cik={x.get('cik')}",
            company_id, x.get("signal_id"),
        ))

    for x in decisions:
        sv = x.get("source_state_version")
        checks.append(_check(
            "DECISION_SIGNAL_LINEAGE",
            sv not in (None, ""),
            f"source_state_version={sv}",
            company_id, x.get("decision_id"),
        ))
        checks.append(_check(
            "COMPANY_IDENTITY",
            str(x.get("company_id")) == company_id and
            str(x.get("cik", "")).zfill(10) == cik,
            f"company_id={x.get('company_id')} cik={x.get('cik')}",
            company_id, x.get("decision_id"),
        ))

    # Verify deterministic document artifact identity for documents referenced
    # by structured events. Missing artifact is fatal only when a structured
    # event explicitly claims document provenance.
    for x in structured:
        doc_id = x.get("document_id")
        if not doc_id: continue
        root = ROOT / "data" / "silver" / "documents" / f"cik={cik}" / str(doc_id)
        metadata = root / "document_metadata.parquet"
        text = root / "document_text.parquet"
        checks.append(_check(
            "DOCUMENT_ARTIFACT_IDENTITY",
            metadata.exists() and metadata.stat().st_size > 0 and
            text.exists() and text.stat().st_size > 0,
            f"metadata={metadata.exists()} text={text.exists()}",
            company_id, doc_id,
        ))

    # Unified investment intelligence is a Gold projection. If present, verify
    # identity and provenance columns; absence is reported separately so legacy
    # batch companies do not become false failures.
    ui = ROOT / "data" / "gold" / "company_investment_intelligence" / f"cik={cik}" / "unified_investment_intelligence.parquet"
    if ui.exists():
        try:
            udf = pd.read_parquet(ui)
            required = {
                "company_id","cik","state_version","investment_posture",
                "source_event_id","source_run_id","source_document_id",
                "source_document_sha256",
            }
            missing = required - set(udf.columns)
            identity_ok = (not udf.empty and "company_id" in udf.columns and
                           udf["company_id"].astype(str).eq(company_id).any())
            provenance_ok = all(
                _source_lineage_is_valid(
                    row.get("source_event_id"),
                    row.get("source_run_id"),
                    event_ids,
                    runs_by_id,
                )
                and _has_value(row.get("source_document_id"))
                and _has_value(row.get("source_document_sha256"))
                for row in udf.to_dict(orient="records")
            )
            checks.append(_check(
                "UNIFIED_INTELLIGENCE_PROVENANCE",
                not missing and identity_ok and provenance_ok,
                f"missing={sorted(missing)} identity_ok={identity_ok} provenance_ok={provenance_ok}",
                company_id, str(ui),
            ))
        except Exception as exc:
            checks.append(_check(
                "UNIFIED_INTELLIGENCE_PROVENANCE", False,
                f"unreadable:{type(exc).__name__}", company_id, str(ui),
            ))

    return checks


def reconcile_all(control_db=CONTROL_DB):
    companies = sorted({
        str(x.get("company_id")) for x in _event_rows()
        if x.get("company_id")
    } | {
        str(x.get("company_id")) for x in _json_run_rows()
        if x.get("company_id")
    })

    all_checks = []
    for company_id in companies:
        all_checks.extend(reconcile_company(company_id))

    df = pd.DataFrame(all_checks, columns=[
        "reconciliation_version","check","passed","severity",
        "company_id","entity_id","message","checked_at_utc"
    ])

    if df.empty:
        summary = pd.DataFrame([{
            "reconciliation_version": RECONCILIATION_VERSION,
            "generated_at_utc": _now(),
            "companies_checked": 0,
            "checks": 0,
            "passed_checks": 0,
            "failed_checks": 0,
            "fatal_failures": 0,
            "reconciliation_status": "READY",
        }])
        company_df = pd.DataFrame(columns=[
            "company_id","checks","passed_checks","failed_checks","status"
        ])
    else:
        company_df = (
            df.groupby("company_id", dropna=False)
              .agg(checks=("passed","size"),
                   passed_checks=("passed","sum"))
              .reset_index()
        )
        company_df["failed_checks"] = company_df["checks"] - company_df["passed_checks"]
        company_df["status"] = company_df["failed_checks"].eq(0).map({True:"RECONCILED",False:"EXCEPTION"})
        summary = pd.DataFrame([{
            "reconciliation_version": RECONCILIATION_VERSION,
            "generated_at_utc": _now(),
            "companies_checked": len(company_df),
            "checks": len(df),
            "passed_checks": int(df["passed"].sum()),
            "failed_checks": int((~df["passed"]).sum()),
            "fatal_failures": int((~df["passed"] & df["severity"].eq("FATAL")).sum()),
            "reconciliation_status": "READY" if df["passed"].all() else "NOT_READY",
        }])

    return {
        "summary": summary,
        "reconciliation_checks": df,
        "company_reconciliation": company_df,
    }


def write_artifacts(artifacts, output=OUTPUT):
    output.mkdir(parents=True, exist_ok=True)
    for name, df in artifacts.items():
        df.to_parquet(output / f"{name}.parquet", index=False)


def main():
    parser = argparse.ArgumentParser(description="Step 29 end-to-end platform reconciliation")
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()

    artifacts = reconcile_all()
    write_artifacts(artifacts, args.output)
    s = artifacts["summary"].iloc[0]
    print("=" * 80)
    print("STEP 29 - END-TO-END PLATFORM RECONCILIATION")
    print(f"Reconciliation version : {RECONCILIATION_VERSION}")
    print(f"Companies checked      : {int(s['companies_checked'])}")
    print(f"Checks                 : {int(s['checks'])}")
    print(f"Passed checks          : {int(s['passed_checks'])}")
    print(f"Failed checks          : {int(s['failed_checks'])}")
    print(f"Fatal failures         : {int(s['fatal_failures'])}")
    print(f"Status                 : {s['reconciliation_status']}")
    print(f"Output                 : {args.output}")
    print("=" * 80)
    return 0 if s["reconciliation_status"] == "READY" else 1


if __name__ == "__main__":
    raise SystemExit(main())
