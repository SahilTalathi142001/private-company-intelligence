from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
DATA = ROOT / "data"
CONTROL_DB = DATA / "silver" / "platform" / "control.db"
STRUCTURED_DB = DATA / "silver" / "platform" / "structured_events.db"
REGISTRY = DATA / "silver" / "platform" / "company_registry.parquet"
UNIFIED = DATA / "gold" / "unified_company_intelligence" / "unified_company_view" / "unified_company_view.parquet"
API_VERSION = "36.0.0"


def _json(v: Any) -> Any:
    if isinstance(v, str):
        try: return json.loads(v)
        except Exception: return v
    return v


def _rows(db: Path, table: str) -> list[dict[str, Any]]:
    if not db.exists(): return []
    with sqlite3.connect(db) as con:
        cols = [r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]
        if not cols: return []
        return [dict(zip(cols, row)) for row in con.execute(f"SELECT {','.join(cols)} FROM {table}").fetchall()]


def _normalize(v: Any) -> Any:
    if hasattr(v, "item"):
        try: return v.item()
        except Exception: pass
    if pd.isna(v): return None
    return v


def _registry() -> pd.DataFrame:
    if not REGISTRY.exists():
        return pd.DataFrame()
    df = pd.read_parquet(REGISTRY)
    if "cik" in df:
        df["cik"] = df["cik"].astype(str).str.replace(".0", "", regex=False).str.zfill(10)
    if "status" in df:
        df = df[df["status"].astype(str).str.strip().str.upper().eq("ACTIVE")]
    if "ingestion_enabled" in df:
        enabled = (
            df["ingestion_enabled"]
            .astype(str)
            .str.strip()
            .str.lower()
            .isin({"true", "1", "yes", "y"})
        )
        df = df[enabled]
    return df.reset_index(drop=True)


def _unified() -> pd.DataFrame:
    if not UNIFIED.exists(): return pd.DataFrame()
    df = pd.read_parquet(UNIFIED)
    if "cik" in df: df["cik"] = df["cik"].astype(str).str.zfill(10)
    return df


def _record(df: pd.DataFrame, cik: str) -> dict[str, Any] | None:
    if df.empty or "cik" not in df: return None
    x = df[df["cik"].astype(str).str.zfill(10) == cik]
    if x.empty: return None
    return {k: _normalize(v) for k, v in x.iloc[0].to_dict().items()}


def _structured(cik: str | None = None) -> list[dict[str, Any]]:
    rows = _rows(STRUCTURED_DB, "structured_events")
    if cik:
        rows = [r for r in rows if str(r.get("cik", "")).zfill(10) == cik]
    for r in rows:
        r["facts"] = _json(r.get("facts_json"))
        r["is_test"] = str(r.get("document_id", "")).startswith("TEST-E2E-")
        r["is_production"] = not r["is_test"]
    return rows


def _event_rows(cik: str | None = None) -> list[dict[str, Any]]:
    rows = _rows(CONTROL_DB, "events")
    if cik:
        cid = f"SEC_{cik}"
        rows = [r for r in rows if str(r.get("company_id", "")) == cid or str(r.get("cik", "")).zfill(10) == cik]
    return rows


def _coverage(cik: str) -> dict[str, Any]:
    events = _structured(cik)
    prod = [e for e in events if e["is_production"]]
    test = [e for e in events if e["is_test"]]
    formd = [e for e in prod if str(e.get("event_type", "")).upper() == "SEC_FORM_D_FILING"]
    docs_dir = DATA / "silver" / "documents" / f"cik={cik}"
    processed_docs = len(list(docs_dir.glob("*/document_metadata.parquet"))) if docs_dir.exists() else len({e.get("document_id") for e in prod if e.get("document_id")})
    filing_path = DATA / "silver" / "filings" / f"cik={cik}" / "filings.parquet"
    filing_count = None
    if filing_path.exists():
        try: filing_count = int(len(pd.read_parquet(filing_path)))
        except Exception: pass
    status = "EVENTS_FOUND" if formd else "NO_FORM_D_IN_AVAILABLE_FILINGS"
    return {
        "status": "COMPLETE" if prod else "PARTIAL",
        "sec_filings_in_source_window": filing_count,
        "production_documents_processed": processed_docs,
        "production_structured_events": len(prod),
        "form_d_observations": len(formd),
        "form_d_coverage": status,
        "production_funding_events": len(formd),
        "current_investment_signals": 0,
        "decision_signals": 0,
        "company_state": "NO_DATA",
        "test_structured_events_excluded": len(test),
        "company_registry": "Present" if _record(_registry(), cik) else "Missing",
        "unified_company_view": "Present" if _record(_unified(), cik) else "Missing",
        "coverage_note": (f"{len(formd)} production Form D structured event(s) are present." if formd else "No exact SEC Form D or D/A filing is represented in the processed production event set. This does not prove the company has never raised capital."),
    }


def _enrich(base: dict[str, Any], cik: str) -> dict[str, Any]:
    uv = _record(_unified(), cik) or {}
    events = _structured(cik)
    prod = [e for e in events if e["is_production"]]
    formd = [e for e in prod if str(e.get("event_type", "")).upper() == "SEC_FORM_D_FILING"]
    signals = [e for e in uv.get("investment_signal_types", [])] if isinstance(uv.get("investment_signal_types"), list) else str(uv.get("investment_signal_types") or "").split("|") if uv.get("investment_signal_types") else []
    coverage = _coverage(cik)
    coverage["current_investment_signals"] = int(uv.get("investment_signal_count") or 0)
    coverage["decision_signals"] = 1 if uv.get("event_decision_id") else 0
    coverage["company_state"] = uv.get("company_state_status") or "NO_DATA"
    return {**base, "intelligence": uv, "coverage": coverage, "events": prod, "event_count": len(prod), "form_d_event_count": len(formd)}


def list_companies() -> list[dict[str, Any]]:
    reg = _registry(); uv = _unified()
    if reg.empty: return []
    out=[]
    for _, row in reg.iterrows():
        b={k:_normalize(v) for k,v in row.to_dict().items()}
        cik=str(b.get("cik","")).zfill(10)
        u=_record(uv,cik) or {}
        c=_coverage(cik)
        out.append({**b, "cik":cik, "intelligence":u, "coverage":c})
    return out


def get_company(cik: str) -> dict[str, Any] | None:
    cik=str(cik).replace("SEC_","").zfill(10)
    b=_record(_registry(),cik)
    if not b: return None
    return _enrich(b,cik)


def get_event(event_id: str) -> dict[str, Any] | None:
    for r in _structured():
        if str(r.get("event_id")) == event_id: return r
    for r in _event_rows():
        if str(r.get("event_id")) == event_id or str(r.get("id")) == event_id: return r
    return None


def get_run(run_id: str) -> dict[str, Any] | None:
    rows=_rows(CONTROL_DB,"pipeline_runs")
    for r in rows:
        if str(r.get("run_id") or r.get("id")) == run_id: return r
    return None


def get_operations_summary() -> dict[str, Any]:
    events=_event_rows(); runs=_rows(CONTROL_DB,"pipeline_runs"); tasks=_rows(CONTROL_DB,"task_runs")
    return {"api_version":API_VERSION,"events":{"total":len(events),"pending":sum(str(x.get("status"))=="PENDING" for x in events),"processing":sum(str(x.get("status"))=="PROCESSING" for x in events),"failed":sum(str(x.get("status"))=="FAILED" for x in events),"quarantined":sum(str(x.get("status"))=="QUARANTINED" for x in events)},"runs":{"total":len(runs),"running":sum(str(x.get("status"))=="RUNNING" for x in runs),"failed":sum(str(x.get("status"))=="FAILED" for x in runs)},"tasks":{"total":len(tasks),"failed":sum(str(x.get("status"))=="FAILED" for x in tasks),"blocked":sum(str(x.get("status"))=="BLOCKED" for x in tasks)},"companies":list_companies()}


def get_health(): return {"status":"ok","api_version":API_VERSION}
def get_readiness():
    from company_platform.production_readiness.readiness import evaluate_readiness

    summary, _ = evaluate_readiness()
    if summary.empty:
        raise RuntimeError("Production readiness returned no summary")
    return {key: _normalize(value) for key, value in summary.iloc[0].items()}


def consume_event(event_id: str):
    from company_platform.orchestrator.event_consumer import EventConsumer
    return EventConsumer().consume_event(event_id)


def retry_event(event_id: str, reason: str, actor: str):
    from company_platform.recovery.recovery import apply_retry
    return apply_retry(event_id=event_id, reason=reason, actor=actor)
