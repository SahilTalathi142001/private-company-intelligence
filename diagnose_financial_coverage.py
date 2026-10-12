#!/usr/bin/env python3
"""Read-only diagnostic for CIDM financial-domain coverage.

Compares active company registry CIKs with Bronze SEC inputs, Silver financial
facts, Gold financial statements, and orchestration task history. It never writes
into the repository data directory. Optional CSV/JSON reports are written only
where the user explicitly requests them.
"""
from __future__ import annotations
import argparse
import csv
import json
import sqlite3
from collections import defaultdict, Counter
from pathlib import Path
from typing import Any

import pandas as pd

FINANCIAL_COMPONENTS = {
    "sec_ingestion", "financial_normalization", "quarter_derivation",
    "financial_features", "financial_decision",
}


def normalize_cik(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    if text.startswith("SEC_"):
        text = text[4:]
    return text.zfill(10) if text.isdigit() else text


def read_registry(data: Path) -> tuple[pd.DataFrame, Path]:
    candidates = [data / "silver/platform/company_registry.parquet", data / "silver/company/company.parquet"]
    for path in candidates:
        if not path.is_file():
            continue
        df = pd.read_parquet(path)
        if "cik" not in df.columns:
            raise RuntimeError(f"Registry has no 'cik' column: {path}")
        if "status" in df.columns:
            status = df["status"].astype(str).str.strip().str.upper()
            df = df[status.isin({"ACTIVE", "ENABLED", "PRODUCTION"})].copy()
        if "ingestion_enabled" in df.columns:
            enabled = df["ingestion_enabled"].astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})
            df = df[enabled].copy()
        df["__cik"] = df["cik"].map(normalize_cik)
        return df, path
    raise FileNotFoundError("No readable company registry found in data/silver/platform or data/silver/company")


def safe_read_parquet(path: Path) -> tuple[bool, int | None, str | None]:
    if not path.is_file():
        return False, None, None
    try:
        frame = pd.read_parquet(path)
        return True, int(len(frame)), None
    except Exception as exc:
        return True, None, f"{type(exc).__name__}: {exc}"


def load_task_history(db_path: Path) -> dict[str, dict[str, dict[str, int]]]:
    history: dict[str, dict[str, Counter]] = defaultdict(lambda: defaultdict(Counter))
    if not db_path.is_file():
        return history
    with sqlite3.connect(db_path) as con:
        tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "task_runs" not in tables:
            return history
        cols = {r[1] for r in con.execute("PRAGMA table_info(task_runs)")}
        if "data" in cols and "component_id" in cols:
            query = "SELECT component_id, data FROM task_runs"
            for component, payload in con.execute(query):
                try:
                    obj = json.loads(payload or "{}")
                except Exception:
                    continue
                comp = str(obj.get("component_id") or component or "")
                if comp not in FINANCIAL_COMPONENTS:
                    continue
                cik = normalize_cik(obj.get("company_id") or obj.get("cik") or "")
                if cik.startswith("SEC_"):
                    cik = normalize_cik(cik)
                if not cik:
                    continue
                history[cik][comp][str(obj.get("status") or "UNKNOWN").upper()] += 1
    return history


def company_label(row: dict[str, Any]) -> str:
    for key in ("company_name", "name", "legal_name", "display_name", "ticker"):
        value = row.get(key)
        if value is not None and not pd.isna(value) and str(value).strip():
            return str(value).strip()
    return ""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--repo-root", required=True, help="Repository root containing the data/ directory")
    ap.add_argument("--csv", help="Optional output CSV path")
    ap.add_argument("--json", dest="json_out", help="Optional output JSON path")
    args = ap.parse_args()
    root = Path(args.repo_root).resolve()
    data = root / "data"
    registry, registry_path = read_registry(data)
    tasks = load_task_history(data / "silver/platform/control.db")
    rows: list[dict[str, Any]] = []

    for _, series in registry.iterrows():
        row = series.to_dict()
        cik = normalize_cik(row.get("__cik"))
        if not cik:
            continue
        bronze_facts = data / "bronze/sec/companyfacts" / f"{cik}.json"
        bronze_submissions = data / "bronze/sec/submissions" / f"{cik}.json"
        silver_facts = data / "silver/financial_facts" / f"cik={cik}" / "facts.parquet"
        silver_financial_facts = data / "silver/financial_facts" / f"cik={cik}" / "financial_facts.parquet"
        gold_annual = data / "gold/financials" / f"cik={cik}" / "financials.parquet"
        gold_quarterly = data / "gold/financials_quarterly" / f"cik={cik}" / "financials_quarterly.parquet"
        gold_analytics = data / "gold/financials_analytics" / f"cik={cik}" / "financials_analytics.parquet"

        artifacts = {
            "silver_facts": safe_read_parquet(silver_facts),
            "silver_financial_facts": safe_read_parquet(silver_financial_facts),
            "gold_annual": safe_read_parquet(gold_annual),
            "gold_quarterly": safe_read_parquet(gold_quarterly),
            "gold_analytics": safe_read_parquet(gold_analytics),
        }
        read_errors = {k: v[2] for k, v in artifacts.items() if v[2]}
        silver_present = any(v[0] and v[1] and v[1] > 0 for k, v in artifacts.items() if k.startswith("silver_"))
        gold_present = any(v[0] and v[1] and v[1] > 0 for k, v in artifacts.items() if k.startswith("gold_"))
        silver_file_exists = any(v[0] for k, v in artifacts.items() if k.startswith("silver_"))
        gold_file_exists = any(v[0] for k, v in artifacts.items() if k.startswith("gold_"))
        task_counts = tasks.get(cik, {})
        task_total = sum(sum(counter.values()) for counter in task_counts.values())
        task_statuses = {comp: dict(counter) for comp, counter in task_counts.items()}

        if read_errors:
            diagnosis = "ARTIFACT_READ_ERROR"
        elif gold_present or silver_present:
            diagnosis = "FINANCIAL_DATA_PRESENT_API_LOOKUP_SHOULD_BE_AVAILABLE"
        elif bronze_facts.is_file() and not silver_present:
            diagnosis = "UPSTREAM_NORMALIZATION_NOT_PRODUCED"
        elif silver_file_exists and not gold_present:
            diagnosis = "UPSTREAM_GOLD_FINANCIALS_NOT_PRODUCED"
        elif task_total and not bronze_facts.is_file():
            diagnosis = "UPSTREAM_SOURCE_FETCH_OR_INPUT_GAP"
        elif not bronze_facts.is_file() and task_total == 0:
            diagnosis = "NO_FINANCIAL_SOURCE_INPUT_AND_NO_FINANCIAL_TASK_HISTORY"
        else:
            diagnosis = "UPSTREAM_FINANCIAL_ARTIFACTS_MISSING"

        item = {
            "cik": cik,
            "company_name": company_label(row),
            "registry_status": row.get("status"),
            "ingestion_enabled": row.get("ingestion_enabled"),
            "bronze_companyfacts_exists": bronze_facts.is_file(),
            "bronze_submissions_exists": bronze_submissions.is_file(),
            "silver_facts_exists": artifacts["silver_facts"][0],
            "silver_facts_rows": artifacts["silver_facts"][1],
            "silver_financial_facts_exists": artifacts["silver_financial_facts"][0],
            "silver_financial_facts_rows": artifacts["silver_financial_facts"][1],
            "gold_annual_exists": artifacts["gold_annual"][0],
            "gold_annual_rows": artifacts["gold_annual"][1],
            "gold_quarterly_exists": artifacts["gold_quarterly"][0],
            "gold_quarterly_rows": artifacts["gold_quarterly"][1],
            "gold_analytics_exists": artifacts["gold_analytics"][0],
            "financial_task_count": task_total,
            "financial_task_statuses": json.dumps(task_statuses, sort_keys=True),
            "read_errors": json.dumps(read_errors, sort_keys=True),
            "diagnosis": diagnosis,
        }
        rows.append(item)

    summary = Counter(r["diagnosis"] for r in rows)
    print(json.dumps({
        "repo_root": str(root),
        "data_root": str(data),
        "registry_artifact": str(registry_path.relative_to(root)),
        "active_registry_companies": len(rows),
        "companies_with_any_silver_or_gold_financial_rows": sum(1 for r in rows if r["diagnosis"] == "FINANCIAL_DATA_PRESENT_API_LOOKUP_SHOULD_BE_AVAILABLE"),
        "companies_missing_financial_data": sum(1 for r in rows if r["diagnosis"] != "FINANCIAL_DATA_PRESENT_API_LOOKUP_SHOULD_BE_AVAILABLE"),
        "diagnosis_counts": dict(summary),
        "missing_companies": [{"cik": r["cik"], "company_name": r["company_name"], "diagnosis": r["diagnosis"], "bronze_companyfacts_exists": r["bronze_companyfacts_exists"], "financial_task_count": r["financial_task_count"], "financial_task_statuses": r["financial_task_statuses"]} for r in rows if r["diagnosis"] != "FINANCIAL_DATA_PRESENT_API_LOOKUP_SHOULD_BE_AVAILABLE"],
    }, indent=2, default=str))

    if args.csv:
        out = Path(args.csv).resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["cik", "diagnosis"])
            writer.writeheader()
            writer.writerows(rows)
        print(f"\nCSV report: {out}")
    if args.json_out:
        out = Path(args.json_out).resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(rows, indent=2, default=str), encoding="utf-8")
        print(f"JSON report: {out}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
