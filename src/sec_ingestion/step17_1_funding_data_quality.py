"""
STEP 17.1 - FUNDING & CAPITAL DATA QUALITY & RECONCILIATION
Version: 1.0.0

Purpose
-------
Audit the Step 17 v3.0.1 Funding & Capital Intelligence Gold layer before
downstream use.

Step 17.1 is a DQ gate. It does NOT rebuild or modify Step 17 data.

Validated contracts
-------------------
1. Step/version compatibility
2. Company master identity and configured-company coverage
3. Funding event grain
4. Offering/amendment identity
5. Historical/current reconciliation
6. Capital amount reconciliation
7. Date and numeric integrity
8. Snapshot reconciliation
9. Funding signal integrity
10. Discovery/download/parse observability
11. NO_DATA / zero semantics
12. Funding-stage/source semantics
13. Freshness semantics
14. Lineage/version consistency
15. Promotion gating

Expected source
---------------
data/gold/funding_capital_intelligence/
    funding_discovery/funding_discovery.parquet
    funding_events/funding_events.parquet
    funding_event_history/funding_event_history.parquet
    company_funding_snapshot/company_funding_snapshot.parquet
    funding_signals/funding_signals.parquet
    funding_summary/funding_summary.parquet
    funding_lineage/funding_lineage.parquet
    funding_dataset_history/funding_dataset_history.parquet
    funding_related_persons/funding_related_persons.parquet

Outputs
-------
data/gold/funding_capital_intelligence_dq/
    funding_event_dq/funding_event_dq.parquet
    funding_history_dq/funding_history_dq.parquet
    funding_snapshot_dq/funding_snapshot_dq.parquet
    funding_signal_dq/funding_signal_dq.parquet
    funding_discovery_dq/funding_discovery_dq.parquet
    funding_source_coverage_dq/funding_source_coverage_dq.parquet
    funding_reconciliation_dq/funding_reconciliation_dq.parquet
    funding_dq_exceptions/funding_dq_exceptions.parquet
    funding_dq_summary/funding_dq_summary.parquet
    funding_dq_lineage/funding_dq_lineage.parquet
    funding_promotion/funding_promotion.parquet

Run
---
python ./src/sec_ingestion/step17_1_funding_capital_data_quality.py
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from company_platform.registry.active_company_scope import assert_company_scope
import pandas as pd
from company_platform.registry.active_company_scope import load_active_company_ciks

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args: Any, **kwargs: Any) -> bool:
        return False


# =============================================================================
# CONFIG
# =============================================================================

load_dotenv()

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DATA_ROOT = REPO_ROOT / "data"

STEP17_GOLD_ROOT = DATA_ROOT / "gold" / "funding_capital_intelligence"
DQ_GOLD_ROOT = DATA_ROOT / "gold" / "funding_capital_intelligence_dq"

COMPANY_MASTER_PATH = DATA_ROOT / "silver" / "company" / "company.parquet"

STEP17_VERSION_EXPECTED = "3.0.1"
DQ_VERSION = "1.0.0"

FORM_D_START_QUARTER = os.getenv("FORM_D_START_QUARTER", "2008Q1")
FORM_D_END_QUARTER = os.getenv("FORM_D_END_QUARTER", "")

def configured_companies() -> List[str]:
     return assert_company_scope()





EXPECTED_SOURCE_SCOPE = "SEC_EXEMPT_OFFERING_ONLY"
EXPECTED_EVENT_SOURCE_SCOPE = "SEC_EXEMPT_OFFERING"
EXPECTED_STAGE = "UNKNOWN_SEC_ONLY"
EXPECTED_STAGE_SOURCE = "NOT_AVAILABLE_IN_FORM_D"
EXPECTED_ROUND_SOURCE = "NOT_AVAILABLE_IN_FORM_D"

VALID_FILING_TYPES = {"D", "D/A"}

VALID_EVENT_RESOLUTION_STATUSES = {
    "RESOLVED_ORIGINAL",
    "RESOLVED_ROOT",
    "RESOLVED_ROOT_EXTERNAL",
    "UNRESOLVED_AMENDMENT_PARENT",
    "CYCLE_DETECTED",
    "CHAIN_TOO_LONG",
    "MISSING_ACCESSION",
}

VALID_SIGNAL_TYPES = {
    "RECENT_CAPITAL_ACTIVITY",
    "NO_RECENT_CAPITAL_ACTIVITY",
    "CAPITAL_ACCELERATION",
    "CAPITAL_DECELERATION",
    "CAPITAL_STABLE",
    "CAPITAL_DATA_LIMITED",
    "SEC_CAPITAL_COVERAGE",
    "FUNDING_VELOCITY",
    "FUNDING_STAGE",
    "INVESTOR_COVERAGE",
}

VALID_FUNDING_QUALITY = {
    "COMPLETE",
    "PARTIAL",
    "LIMITED",
    "NO_DATA",
}

VALID_FRESHNESS = {
    "FRESH",
    "STALE",
    "VERY_STALE",
    "UNKNOWN",
}

EXPECTED_DATA_SCOPE = "SEC_EXEMPT_OFFERING_ONLY"


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("step17_1")


# =============================================================================
# HELPERS
# =============================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_cik(value: Any) -> str:
    if value is None:
        return ""
    text = re.sub(r"\D", "", str(value))
    return text.zfill(10) if text else ""
COMPANIES = configured_companies()


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def safe_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None


def parse_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def parse_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def values_equal(left: Any, right: Any) -> bool:
    left_num = pd.to_numeric(pd.Series([left]), errors="coerce").iloc[0]
    right_num = pd.to_numeric(pd.Series([right]), errors="coerce").iloc[0]

    if pd.notna(left_num) or pd.notna(right_num):
        if pd.isna(left_num) and pd.isna(right_num):
            return True
        if pd.isna(left_num) or pd.isna(right_num):
            return False
        return bool(left_num == right_num)

    left_text = clean_text(left)
    right_text = clean_text(right)
    return left_text == right_text


def series_all_null(series: pd.Series) -> bool:
    return series.isna().all()


def ensure_columns(
    df: pd.DataFrame,
    required: Sequence[str],
    artifact_name: str,
    failures: List[str],
) -> bool:
    missing = sorted(set(required) - set(df.columns))
    if missing:
        failures.append(
            f"{artifact_name} missing required columns: {missing}"
        )
        return False
    return True


def load_parquet(
    path: Path,
    artifact_name: str,
    failures: List[str],
) -> pd.DataFrame:
    if not path.exists():
        failures.append(
            f"Required Step 17 artifact not found: {artifact_name} -> {path}"
        )
        return pd.DataFrame()

    try:
        return pd.read_parquet(path)
    except Exception as exc:
        failures.append(
            f"Could not read {artifact_name}: {path} | {exc}"
        )
        return pd.DataFrame()


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def append_exception(
    rows: List[Dict[str, Any]],
    category: str,
    severity: str,
    code: str,
    message: str,
    cik: Any = None,
    funding_event_id: Any = None,
    offering_id: Any = None,
    accession_number: Any = None,
) -> None:
    rows.append({
        "dq_category": category,
        "severity": severity,
        "exception_code": code,
        "message": message,
        "cik": normalize_cik(cik) if cik is not None else None,
        "funding_event_id": clean_text(funding_event_id),
        "offering_id": clean_text(offering_id),
        "accession_number": clean_text(accession_number),
        "dq_version": DQ_VERSION,
        "created_at_utc": utc_now_iso(),
    })


# =============================================================================
# COMPANY MASTER
# =============================================================================

def load_company_master(failures: List[str]) -> pd.DataFrame:
    if not COMPANY_MASTER_PATH.exists():
        failures.append(
            f"Company master not found: {COMPANY_MASTER_PATH}"
        )
        return pd.DataFrame()

    try:
        df = pd.read_parquet(COMPANY_MASTER_PATH)
    except Exception as exc:
        failures.append(f"Could not read company master: {exc}")
        return pd.DataFrame()

    if not ensure_columns(
        df,
        ["cik", "company_name"],
        "Company master",
        failures,
    ):
        return pd.DataFrame()

    out = df[["cik", "company_name"]].copy()
    out["cik"] = out["cik"].map(normalize_cik)
    out["company_name"] = out["company_name"].map(clean_text)

    out = out[out["cik"].ne("")].copy()

    required_ciks = {normalize_cik(c) for c in COMPANIES}

    missing = sorted(required_ciks - set(out["cik"]))
    if missing:
        failures.append(
            f"Company master missing configured CIKs: {missing}"
        )

    conflicts = (
        out[out["company_name"].notna()]
        .groupby("cik")["company_name"]
        .nunique()
    )
    conflicting_ciks = sorted(conflicts[conflicts > 1].index.tolist())
    if conflicting_ciks:
        failures.append(
            f"Company master has conflicting names for CIKs: {conflicting_ciks}"
        )

    out = (
        out[out["cik"].isin(required_ciks)]
        .drop_duplicates(["cik", "company_name"])
        .sort_values("cik")
    )

    return out.reset_index(drop=True)


# =============================================================================
# ARTIFACT PATHS / LOAD
# =============================================================================

def artifact_paths() -> Dict[str, Path]:
    return {
        "discovery": STEP17_GOLD_ROOT / "funding_discovery" / "funding_discovery.parquet",
        "events": STEP17_GOLD_ROOT / "funding_events" / "funding_events.parquet",
        "history": STEP17_GOLD_ROOT / "funding_event_history" / "funding_event_history.parquet",
        "snapshot": STEP17_GOLD_ROOT / "company_funding_snapshot" / "company_funding_snapshot.parquet",
        "signals": STEP17_GOLD_ROOT / "funding_signals" / "funding_signals.parquet",
        "summary": STEP17_GOLD_ROOT / "funding_summary" / "funding_summary.parquet",
        "lineage": STEP17_GOLD_ROOT / "funding_lineage" / "funding_lineage.parquet",
        "dataset_history": STEP17_GOLD_ROOT / "funding_dataset_history" / "funding_dataset_history.parquet",
        "related_persons": STEP17_GOLD_ROOT / "funding_related_persons" / "funding_related_persons.parquet",
    }


def load_artifacts(failures: List[str]) -> Dict[str, pd.DataFrame]:
    frames: Dict[str, pd.DataFrame] = {}

    for name, path in artifact_paths().items():
        # dataset_history and related_persons are additive outputs. Their absence
        # is not fatal when the core Gold artifacts exist.
        optional = name in {"dataset_history", "related_persons"}

        if not path.exists() and optional:
            frames[name] = pd.DataFrame()
            continue

        frames[name] = load_parquet(
            path,
            f"Step 17 {name}",
            failures,
        )

    return frames


# =============================================================================
# VERSION / SUMMARY / LINEAGE CONTRACT
# =============================================================================

def validate_versions(
    summary: pd.DataFrame,
    lineage: pd.DataFrame,
    exceptions: List[Dict[str, Any]],
    failures: List[str],
) -> None:
    for artifact_name, df in [
        ("funding_summary", summary),
        ("funding_lineage", lineage),
    ]:
        if df.empty:
            continue

        if "step_version" in df.columns:
            versions = set(
                df["step_version"].dropna().astype(str)
            )
            bad = versions - {STEP17_VERSION_EXPECTED}
            if bad:
                message = (
                    f"{artifact_name} contains unexpected Step 17 versions: "
                    f"{sorted(bad)}"
                )
                failures.append(message)
                append_exception(
                    exceptions,
                    "VERSION",
                    "FATAL",
                    "STEP17_VERSION_MISMATCH",
                    message,
                )

    if not summary.empty and "step" in summary.columns:
        bad_steps = set(summary["step"].dropna().astype(str)) - {"STEP_17"}
        if bad_steps:
            failures.append(
                f"Unexpected step values in funding_summary: {sorted(bad_steps)}"
            )


# =============================================================================
# EVENT DQ
# =============================================================================

def audit_events(
    events: pd.DataFrame,
    exceptions: List[Dict[str, Any]],
    failures: List[str],
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    required = [
        "funding_event_id",
        "offering_id",
        "cik",
        "filing_type",
        "filing_date",
        "first_sale_date",
        "accession_number",
        "total_amount_sold_usd",
        "capital_amount_usd",
        "capital_source_scope",
        "funding_stage",
        "funding_stage_source",
        "round_label_source",
        "capital_data_quality",
        "event_status",
        "offering_id_resolution_status",
    ]

    if not ensure_columns(events, required, "funding_events", failures):
        return pd.DataFrame()

    work = events.copy()
    work["cik"] = work["cik"].map(normalize_cik)
    work["funding_event_id"] = work["funding_event_id"].map(clean_text)
    work["offering_id"] = work["offering_id"].map(clean_text)
    work["accession_number"] = work["accession_number"].map(clean_text)
    work["filing_type"] = work["filing_type"].map(clean_text).str.upper()

    numeric_cols = [
        "capital_amount_usd",
        "total_amount_sold_usd",
        "total_offering_amount_usd",
        "total_remaining_usd",
        "investor_count",
    ]
    for col in numeric_cols:
        if col in work.columns:
            work[col] = parse_numeric(work[col])

    for col in ["filing_date", "first_sale_date"]:
        work[col] = parse_datetime(work[col])

    for idx, row in work.iterrows():
        status = "PASS"
        messages: List[str] = []

        if not row["cik"]:
            status = "FAIL"
            messages.append("BLANK_CIK")

        if not row["funding_event_id"]:
            status = "FAIL"
            messages.append("BLANK_FUNDING_EVENT_ID")

        if not row["offering_id"]:
            status = "FAIL"
            messages.append("BLANK_OFFERING_ID")

        if not row["accession_number"]:
            status = "FAIL"
            messages.append("BLANK_ACCESSION_NUMBER")

        if row["filing_type"] not in VALID_FILING_TYPES:
            status = "FAIL"
            messages.append("INVALID_FILING_TYPE")

        if row["capital_amount_usd"] is not None:
            if pd.notna(row["capital_amount_usd"]) and row["capital_amount_usd"] < 0:
                status = "FAIL"
                messages.append("NEGATIVE_CAPITAL_AMOUNT")

        if (
            pd.notna(row["investor_count"])
            and row["investor_count"] < 0
        ):
            status = "FAIL"
            messages.append("NEGATIVE_INVESTOR_COUNT")

        # Null-safe capital alignment.
        if not values_equal(
            row["capital_amount_usd"],
            row["total_amount_sold_usd"],
        ):
            status = "FAIL"
            messages.append("CAPITAL_NOT_EQUAL_TO_TOTAL_AMOUNT_SOLD")

        # Source semantics.
        if clean_text(row["capital_source_scope"]) != EXPECTED_EVENT_SOURCE_SCOPE:
            status = "FAIL"
            messages.append("INVALID_CAPITAL_SOURCE_SCOPE")

        if clean_text(row["funding_stage"]) != EXPECTED_STAGE:
            status = "FAIL"
            messages.append("UNAUTHORIZED_FUNDING_STAGE")

        if clean_text(row["funding_stage_source"]) != EXPECTED_STAGE_SOURCE:
            status = "FAIL"
            messages.append("INVALID_FUNDING_STAGE_SOURCE")

        if clean_text(row["round_label_source"]) != EXPECTED_ROUND_SOURCE:
            status = "FAIL"
            messages.append("INVALID_ROUND_LABEL_SOURCE")

        if clean_text(row["offering_id_resolution_status"]) not in VALID_EVENT_RESOLUTION_STATUSES:
            status = "FAIL"
            messages.append("INVALID_OFFERING_RESOLUTION_STATUS")

        if row["filing_type"] == "D" and row["accession_number"]:
            if row["offering_id"] != row["accession_number"]:
                status = "FAIL"
                messages.append("ORIGINAL_D_OFFERING_ID_MISMATCH")

        if row["filing_type"] == "D/A":
            previous = clean_text(row.get("previous_accession_number"))
            if not previous:
                status = "FAIL"
                messages.append("AMENDMENT_MISSING_PREVIOUS_ACCESSION")

        resolution = clean_text(row["offering_id_resolution_status"])
        if resolution in {
            "UNRESOLVED_AMENDMENT_PARENT",
            "CYCLE_DETECTED",
            "CHAIN_TOO_LONG",
            "MISSING_ACCESSION",
        }:
            status = "FAIL"
            messages.append("INVALID_AMENDMENT_LINEAGE")

        if status == "FAIL":
            for msg in messages:
                append_exception(
                    exceptions,
                    "EVENT",
                    "FATAL",
                    msg,
                    f"Current funding event failed DQ: {msg}",
                    cik=row["cik"],
                    funding_event_id=row["funding_event_id"],
                    offering_id=row["offering_id"],
                    accession_number=row["accession_number"],
                )
            failures.append(
                f"Funding event DQ failure at row {idx}: {messages}"
            )

        rows.append({
            "cik": row["cik"],
            "funding_event_id": row["funding_event_id"],
            "offering_id": row["offering_id"],
            "accession_number": row["accession_number"],
            "event_dq_status": status,
            "event_dq_reason": "|".join(messages) if messages else "PASS",
            "dq_version": DQ_VERSION,
        })

    # Global current-event grain.
    if work["funding_event_id"].duplicated().any():
        duplicate_ids = (
            work.loc[
                work["funding_event_id"].duplicated(keep=False),
                "funding_event_id",
            ]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        message = (
            f"Duplicate current funding_event_id values: {duplicate_ids}"
        )
        failures.append(message)
        append_exception(
            exceptions,
            "EVENT_GRAIN",
            "FATAL",
            "DUPLICATE_FUNDING_EVENT_ID",
            message,
        )

    offering_conflicts = (
        work.dropna(subset=["cik", "offering_id", "funding_event_id"])
        .groupby(["cik", "offering_id"])["funding_event_id"]
        .nunique()
    )
    if (offering_conflicts > 1).any():
        message = "A CIK/offering_id maps to multiple funding_event_id values."
        failures.append(message)
        append_exception(
            exceptions,
            "EVENT_GRAIN",
            "FATAL",
            "OFFERING_EVENT_ID_CONFLICT",
            message,
        )

    return pd.DataFrame(rows)


# =============================================================================
# HISTORY DQ
# =============================================================================

def audit_history(
    history: pd.DataFrame,
    current_events: pd.DataFrame,
    exceptions: List[Dict[str, Any]],
    failures: List[str],
) -> pd.DataFrame:
    required = [
        "funding_event_id",
        "offering_id",
        "cik",
        "filing_type",
        "filing_date",
        "accession_number",
        "is_amendment",
        "previous_accession_number",
        "offering_id_resolution_status",
    ]

    if not ensure_columns(history, required, "funding_event_history", failures):
        return pd.DataFrame()

    work = history.copy()
    work["cik"] = work["cik"].map(normalize_cik)
    for col in [
        "funding_event_id",
        "offering_id",
        "accession_number",
        "previous_accession_number",
    ]:
        work[col] = work[col].map(clean_text)
    work["filing_type"] = work["filing_type"].map(clean_text).str.upper()
    work["filing_date"] = parse_datetime(work["filing_date"])

    results: List[Dict[str, Any]] = []

    # Immutable filing key.
    if work["accession_number"].duplicated().any():
        message = "Historical Form D history contains duplicate accession numbers."
        failures.append(message)
        append_exception(
            exceptions,
            "HISTORY",
            "FATAL",
            "DUPLICATE_ACCESSION",
            message,
        )

    for idx, row in work.iterrows():
        status = "PASS"
        reasons: List[str] = []

        if row["filing_type"] not in VALID_FILING_TYPES:
            status = "FAIL"
            reasons.append("INVALID_FILING_TYPE")

        if row["filing_type"] == "D":
            if row["offering_id"] != row["accession_number"]:
                status = "FAIL"
                reasons.append("ORIGINAL_D_OFFERING_ID_MISMATCH")

        if row["filing_type"] == "D/A":
            if not row["previous_accession_number"]:
                status = "FAIL"
                reasons.append("AMENDMENT_MISSING_PREVIOUS_ACCESSION")

            if row["offering_id"] == row["accession_number"]:
                # This can be legitimate only when the parent is unresolved,
                # but the canonicalization contract should expose that state.
                resolution = clean_text(row["offering_id_resolution_status"])
                if resolution not in {
                    "UNRESOLVED_AMENDMENT_PARENT",
                    "CYCLE_DETECTED",
                    "CHAIN_TOO_LONG",
                    "MISSING_ACCESSION",
                }:
                    status = "FAIL"
                    reasons.append("AMENDMENT_NOT_RESOLVED_TO_ROOT")

        if clean_text(row["offering_id_resolution_status"]) not in VALID_EVENT_RESOLUTION_STATUSES:
            status = "FAIL"
            reasons.append("INVALID_OFFERING_RESOLUTION_STATUS")

        if status == "FAIL":
            for reason in reasons:
                append_exception(
                    exceptions,
                    "HISTORY",
                    "FATAL",
                    reason,
                    f"Historical filing failed DQ: {reason}",
                    cik=row["cik"],
                    funding_event_id=row["funding_event_id"],
                    offering_id=row["offering_id"],
                    accession_number=row["accession_number"],
                )
            failures.append(
                f"History DQ failure at row {idx}: {reasons}"
            )

        results.append({
            "cik": row["cik"],
            "funding_event_id": row["funding_event_id"],
            "offering_id": row["offering_id"],
            "accession_number": row["accession_number"],
            "history_dq_status": status,
            "history_dq_reason": "|".join(reasons) if reasons else "PASS",
            "dq_version": DQ_VERSION,
        })

    # Every current event must have history for its accession/offering.
    if not current_events.empty:
        current_ids = set(
            current_events["funding_event_id"].dropna().astype(str)
        )
        history_ids = set(
            work["funding_event_id"].dropna().astype(str)
        )
        missing_history = sorted(current_ids - history_ids)
        if missing_history:
            message = (
                f"Current funding events missing from history: {missing_history}"
            )
            failures.append(message)
            append_exception(
                exceptions,
                "RECONCILIATION",
                "FATAL",
                "CURRENT_EVENT_MISSING_HISTORY",
                message,
            )

    return pd.DataFrame(results)


# =============================================================================
# SNAPSHOT DQ
# =============================================================================

def audit_snapshot(
    snapshot: pd.DataFrame,
    current_events: pd.DataFrame,
    company_master: pd.DataFrame,
    exceptions: List[Dict[str, Any]],
    failures: List[str],
) -> pd.DataFrame:
    required = [
        "cik",
        "company_name",
        "funding_event_count",
        "total_sec_capital_raised_usd",
        "latest_funding_date",
        "latest_capital_amount_usd",
        "funding_stage",
        "funding_stage_source",
        "capital_data_scope",
        "funding_data_quality",
        "funding_freshness_status",
    ]

    if not ensure_columns(
        snapshot,
        required,
        "company_funding_snapshot",
        failures,
    ):
        return pd.DataFrame()

    work = snapshot.copy()
    work["cik"] = work["cik"].map(normalize_cik)
    work["company_name"] = work["company_name"].map(clean_text)
    work["funding_event_count"] = pd.to_numeric(
        work["funding_event_count"], errors="coerce"
    )
    work["total_sec_capital_raised_usd"] = pd.to_numeric(
        work["total_sec_capital_raised_usd"], errors="coerce"
    )
    work["latest_capital_amount_usd"] = pd.to_numeric(
        work["latest_capital_amount_usd"], errors="coerce"
    )
    work["latest_funding_date"] = parse_datetime(
        work["latest_funding_date"]
    )

    rows: List[Dict[str, Any]] = []

    if work["cik"].duplicated().any():
        message = "Company funding snapshot is not one row per CIK."
        failures.append(message)
        append_exception(
            exceptions,
            "SNAPSHOT_GRAIN",
            "FATAL",
            "DUPLICATE_SNAPSHOT_CIK",
            message,
        )

    master_ciks = set(company_master["cik"]) if not company_master.empty else set()
    snapshot_ciks = set(work["cik"])

    missing_ciks = sorted(master_ciks - snapshot_ciks)
    extra_ciks = sorted(snapshot_ciks - master_ciks)

    if missing_ciks:
        message = f"Configured CIKs missing from funding snapshot: {missing_ciks}"
        failures.append(message)
        append_exception(
            exceptions,
            "SNAPSHOT_COVERAGE",
            "FATAL",
            "SNAPSHOT_MISSING_COMPANIES",
            message,
        )

    if extra_ciks:
        message = f"Funding snapshot contains non-configured CIKs: {extra_ciks}"
        failures.append(message)
        append_exception(
            exceptions,
            "SNAPSHOT_COVERAGE",
            "FATAL",
            "SNAPSHOT_EXTRA_COMPANIES",
            message,
        )

    event_counts = (
        current_events.groupby("cik").size().to_dict()
        if not current_events.empty else {}
    )

    capital_totals = {}
    latest_by_cik: Dict[str, pd.Series] = {}

    if not current_events.empty:
        ce = current_events.copy()
        ce["cik"] = ce["cik"].map(normalize_cik)
        ce["capital_amount_usd"] = parse_numeric(
            ce["capital_amount_usd"]
        )
        ce["first_sale_date"] = parse_datetime(
            ce["first_sale_date"]
        )
        ce["filing_date"] = parse_datetime(
            ce["filing_date"]
        )

        for cik, group in ce.groupby("cik"):
            capital_totals[cik] = group["capital_amount_usd"].sum(
                min_count=1
            )
            ordered = group.sort_values(
                ["first_sale_date", "filing_date", "funding_event_id"],
                na_position="last",
            )
            latest_by_cik[cik] = ordered.iloc[-1]

    for _, row in work.iterrows():
        cik = row["cik"]
        status = "PASS"
        reasons: List[str] = []

        expected_count = int(event_counts.get(cik, 0))

        actual_snapshot_count = (
            int(row["funding_event_count"])
            if pd.notna(row["funding_event_count"])
            else -1
        )
        if actual_snapshot_count != expected_count:
            status = "FAIL"
            reasons.append("SNAPSHOT_EVENT_COUNT_MISMATCH")

        quality = clean_text(row["funding_data_quality"])
        if quality not in VALID_FUNDING_QUALITY:
            status = "FAIL"
            reasons.append("INVALID_FUNDING_DATA_QUALITY")

        freshness = clean_text(row["funding_freshness_status"])
        if freshness not in VALID_FRESHNESS:
            status = "FAIL"
            reasons.append("INVALID_FRESHNESS_STATUS")

        # Strong NO_DATA contract.
        if expected_count == 0:
            for col in [
                "total_sec_capital_raised_usd",
                "latest_funding_date",
                "latest_capital_amount_usd",
                "latest_funding_event_id",
                "latest_offering_amount_usd",
                "latest_investor_count",
                "first_funding_date",
                "last_funding_date",
                "funding_profile_as_of_date",
            ]:
                if col in work.columns:
                    value = row.get(col)
                    if value is not None and not pd.isna(value):
                        status = "FAIL"
                        reasons.append(f"NO_DATA_WITH_{col.upper()}")

            if quality != "NO_DATA":
                status = "FAIL"
                reasons.append("ZERO_EVENTS_MUST_BE_NO_DATA")

            if freshness != "UNKNOWN":
                status = "FAIL"
                reasons.append("ZERO_EVENTS_MUST_HAVE_UNKNOWN_FRESHNESS")

        else:
            if quality == "NO_DATA":
                status = "FAIL"
                reasons.append("OBSERVED_EVENTS_CANNOT_BE_NO_DATA")

            expected_total = capital_totals.get(cik)
            if not values_equal(
                row["total_sec_capital_raised_usd"],
                expected_total,
            ):
                status = "FAIL"
                reasons.append("SNAPSHOT_CAPITAL_TOTAL_MISMATCH")

            latest = latest_by_cik.get(cik)
            if latest is not None:
                if not values_equal(
                    row["latest_capital_amount_usd"],
                    latest.get("capital_amount_usd"),
                ):
                    status = "FAIL"
                    reasons.append("SNAPSHOT_LATEST_CAPITAL_MISMATCH")

                snap_date = row["latest_funding_date"]
                latest_date = latest.get("first_sale_date")
                if pd.isna(snap_date) and pd.notna(latest_date):
                    status = "FAIL"
                    reasons.append("SNAPSHOT_LATEST_DATE_MISMATCH")
                elif pd.notna(snap_date) and pd.notna(latest_date):
                    if pd.Timestamp(snap_date) != pd.Timestamp(latest_date):
                        status = "FAIL"
                        reasons.append("SNAPSHOT_LATEST_DATE_MISMATCH")

        if clean_text(row["capital_data_scope"]) != EXPECTED_DATA_SCOPE:
            status = "FAIL"
            reasons.append("INVALID_CAPITAL_DATA_SCOPE")

        if clean_text(row["funding_stage"]) != EXPECTED_STAGE:
            status = "FAIL"
            reasons.append("SNAPSHOT_STAGE_NOT_UNKNOWN_SEC_ONLY")

        if clean_text(row["funding_stage_source"]) != EXPECTED_STAGE_SOURCE:
            status = "FAIL"
            reasons.append("SNAPSHOT_STAGE_SOURCE_INVALID")

        if status == "FAIL":
            for reason in reasons:
                append_exception(
                    exceptions,
                    "SNAPSHOT",
                    "FATAL",
                    reason,
                    f"Company funding snapshot failed DQ: {reason}",
                    cik=cik,
                )
            failures.append(
                f"Snapshot DQ failure for {cik}: {reasons}"
            )

        rows.append({
            "cik": cik,
            "snapshot_dq_status": status,
            "snapshot_dq_reason": "|".join(reasons) if reasons else "PASS",
            "dq_version": DQ_VERSION,
        })

    return pd.DataFrame(rows)


# =============================================================================
# SIGNAL DQ
# =============================================================================

def audit_signals(
    signals: pd.DataFrame,
    snapshot: pd.DataFrame,
    exceptions: List[Dict[str, Any]],
    failures: List[str],
) -> pd.DataFrame:
    required = [
        "cik",
        "signal_type",
        "signal_value",
        "signal_unit",
        "signal_direction",
        "signal_strength",
        "source_scope",
        "signal_version",
    ]

    if not ensure_columns(
        signals,
        required,
        "funding_signals",
        failures,
    ):
        return pd.DataFrame()

    work = signals.copy()
    work["cik"] = work["cik"].map(normalize_cik)
    work["signal_type"] = work["signal_type"].map(clean_text)

    rows: List[Dict[str, Any]] = []

    if work[["cik", "signal_type"]].duplicated().any():
        message = "Duplicate CIK/signal_type pairs found in funding_signals."
        failures.append(message)
        append_exception(
            exceptions,
            "SIGNAL",
            "FATAL",
            "DUPLICATE_SIGNAL_KEY",
            message,
        )

    invalid_types = set(work["signal_type"].dropna()) - VALID_SIGNAL_TYPES
    if invalid_types:
        message = f"Invalid funding signal types: {sorted(invalid_types)}"
        failures.append(message)
        append_exception(
            exceptions,
            "SIGNAL",
            "FATAL",
            "INVALID_SIGNAL_TYPE",
            message,
        )

    bad_versions = (
        set(work["signal_version"].dropna().astype(str))
        - {STEP17_VERSION_EXPECTED}
    )
    if bad_versions:
        message = f"Funding signals contain unexpected versions: {sorted(bad_versions)}"
        failures.append(message)
        append_exception(
            exceptions,
            "SIGNAL",
            "FATAL",
            "SIGNAL_VERSION_MISMATCH",
            message,
        )

    if clean_text_column_exists(work, "source_scope"):
        invalid_scope = set(
            work["source_scope"].dropna().astype(str)
        ) - {EXPECTED_SOURCE_SCOPE}
        if invalid_scope:
            message = f"Invalid funding signal source scopes: {sorted(invalid_scope)}"
            failures.append(message)
            append_exception(
                exceptions,
                "SIGNAL",
                "FATAL",
                "INVALID_SIGNAL_SOURCE_SCOPE",
                message,
            )

    expected_ciks = set(snapshot["cik"].map(normalize_cik)) if not snapshot.empty else set()
    signal_ciks = set(work["cik"].dropna())
    missing_signal_ciks = sorted(expected_ciks - signal_ciks)

    if missing_signal_ciks:
        message = f"Companies missing funding signals: {missing_signal_ciks}"
        failures.append(message)
        append_exception(
            exceptions,
            "SIGNAL_COVERAGE",
            "FATAL",
            "SIGNAL_COMPANY_COVERAGE_GAP",
            message,
        )

    for _, row in work.iterrows():
        status = "PASS"
        reasons: List[str] = []

        if clean_text(row["signal_type"]) == "FUNDING_STAGE":
            value = clean_text(row["signal_value"])
            if value != EXPECTED_STAGE:
                status = "FAIL"
                reasons.append("FUNDING_STAGE_SIGNAL_NOT_UNKNOWN_SEC_ONLY")

        if clean_text(row["signal_type"]) == "INVESTOR_COVERAGE":
            # Form D investor count is a count, not named investor identity.
            # A missing count may legitimately be represented as null.
            numeric_value = pd.to_numeric(
                pd.Series([row["signal_value"]]),
                errors="coerce",
            ).iloc[0]
            if pd.notna(numeric_value) and numeric_value < 0:
                status = "FAIL"
                reasons.append("NEGATIVE_INVESTOR_COUNT_SIGNAL")

        if clean_text(row["source_scope"]) != EXPECTED_SOURCE_SCOPE:
            status = "FAIL"
            reasons.append("INVALID_SIGNAL_SOURCE_SCOPE")

        if status == "FAIL":
            append_exception(
                exceptions,
                "SIGNAL",
                "FATAL",
                "|".join(reasons),
                f"Funding signal failed DQ: {'|'.join(reasons)}",
                cik=row["cik"],
            )
            failures.append(
                f"Signal DQ failure for {row['cik']}: {reasons}"
            )

        rows.append({
            "cik": row["cik"],
            "signal_type": row["signal_type"],
            "signal_dq_status": status,
            "signal_dq_reason": "|".join(reasons) if reasons else "PASS",
            "dq_version": DQ_VERSION,
        })

    return pd.DataFrame(rows)


def clean_text_column_exists(df: pd.DataFrame, column: str) -> bool:
    return column in df.columns


# =============================================================================
# DISCOVERY / SOURCE COVERAGE DQ
# =============================================================================

def audit_discovery(
    discovery: pd.DataFrame,
    exceptions: List[Dict[str, Any]],
    failures: List[str],
    warnings: List[str],
) -> pd.DataFrame:
    required = [
        "dataset_quarter",
        "dataset_url",
        "download_status",
        "parse_status",
        "target_event_rows",
    ]

    if not ensure_columns(
        discovery,
        required,
        "funding_discovery",
        failures,
    ):
        return pd.DataFrame()

    work = discovery.copy()
    work["dataset_quarter"] = work["dataset_quarter"].map(clean_text).str.upper()

    rows: List[Dict[str, Any]] = []

    if work["dataset_quarter"].duplicated().any():
        message = "Funding dataset discovery contains duplicate quarter keys."
        failures.append(message)
        append_exception(
            exceptions,
            "DISCOVERY",
            "FATAL",
            "DUPLICATE_DATASET_QUARTER",
            message,
        )

    invalid_download = set(
        work["download_status"].dropna().astype(str)
    ) - {"DOWNLOADED", "CACHED_BRONZE", "FAILED", "PENDING"}

    if invalid_download:
        message = f"Invalid download_status values: {sorted(invalid_download)}"
        failures.append(message)
        append_exception(
            exceptions,
            "DISCOVERY",
            "FATAL",
            "INVALID_DOWNLOAD_STATUS",
            message,
        )

    invalid_parse = set(
        work["parse_status"].dropna().astype(str)
    ) - {"PARSED", "FAILED", "NOT_ATTEMPTED"}
    if invalid_parse:
        message = f"Invalid parse_status values: {sorted(invalid_parse)}"
        failures.append(message)
        append_exception(
            exceptions,
            "DISCOVERY",
            "FATAL",
            "INVALID_PARSE_STATUS",
            message,
        )

    download_failures = int(
        work["download_status"].eq("FAILED").sum()
    )
    parse_failures = int(
        work["parse_status"].eq("FAILED").sum()
    )

    if download_failures:
        message = (
            f"{download_failures} Form D dataset download failure(s) detected."
        )
        warnings.append(message)
        append_exception(
            exceptions,
            "DISCOVERY",
            "WARNING",
            "DATASET_DOWNLOAD_FAILURE",
            message,
        )

    if parse_failures:
        message = (
            f"{parse_failures} Form D dataset parse failure(s) detected."
        )
        warnings.append(message)
        append_exception(
            exceptions,
            "DISCOVERY",
            "WARNING",
            "DATASET_PARSE_FAILURE",
            message,
        )

    target_rows = pd.to_numeric(
        work["target_event_rows"],
        errors="coerce",
    ).fillna(0)

    for _, row in work.iterrows():
        rows.append({
            "dataset_quarter": row["dataset_quarter"],
            "download_status": row["download_status"],
            "parse_status": row["parse_status"],
            "target_event_rows": pd.to_numeric(
                pd.Series([row["target_event_rows"]]),
                errors="coerce",
            ).iloc[0],
            "discovery_dq_status": (
                "FAIL"
                if row["download_status"] == "FAILED"
                or row["parse_status"] == "FAILED"
                else "PASS"
            ),
            "dq_version": DQ_VERSION,
        })

    return pd.DataFrame(rows)


def audit_source_coverage(
    discovery: pd.DataFrame,
    events: pd.DataFrame,
    snapshot: pd.DataFrame,
    exceptions: List[Dict[str, Any]],
    warnings: List[str],
) -> pd.DataFrame:
    configured = {normalize_cik(c) for c in COMPANIES}

    observed = (
        set(events["cik"].map(normalize_cik).dropna())
        if not events.empty and "cik" in events.columns
        else set()
    )

    rows = []
    for cik in sorted(configured):
        event_count = (
            int(
                (events["cik"].map(normalize_cik) == cik).sum()
            )
            if not events.empty and "cik" in events.columns
            else 0
        )

        rows.append({
            "cik": cik,
            "configured": True,
            "observed_form_d_event": event_count > 0,
            "funding_event_count": event_count,
            "source_coverage_status": (
                "OBSERVED"
                if event_count > 0
                else "NO_DATA"
            ),
            "source_coverage_note": (
                "At least one SEC Form D offering event observed."
                if event_count > 0
                else "No SEC Form D event observed; this is not evidence of zero funding."
            ),
            "dq_version": DQ_VERSION,
        })

    if not observed:
        warnings.append(
            "No configured company has an observed SEC Form D event in the current Step 17 dataset window."
        )

    return pd.DataFrame(rows)


# =============================================================================
# RECONCILIATION DQ
# =============================================================================

def audit_reconciliation(
    events: pd.DataFrame,
    history: pd.DataFrame,
    snapshot: pd.DataFrame,
    summary: pd.DataFrame,
    lineage: pd.DataFrame,
    exceptions: List[Dict[str, Any]],
    failures: List[str],
) -> pd.DataFrame:
    checks: List[Dict[str, Any]] = []

    def add_check(
        check_name: str,
        status: str,
        expected: Any = None,
        actual: Any = None,
        message: str = "",
    ) -> None:
        checks.append({
            "check_name": check_name,
            "status": status,
            "expected": expected,
            "actual": actual,
            "message": message,
            "dq_version": DQ_VERSION,
        })

    # Current/history row counts.
    current_count = len(events)
    history_count = len(history)

    if current_count > history_count:
        message = (
            f"Current event count {current_count} exceeds history count {history_count}."
        )
        failures.append(message)
        append_exception(
            exceptions,
            "RECONCILIATION",
            "FATAL",
            "CURRENT_GT_HISTORY",
            message,
        )
        add_check(
            "CURRENT_EVENT_COUNT_LE_HISTORY",
            "FAIL",
            f"current <= history ({history_count})",
            current_count,
            message,
        )
    else:
        add_check(
            "CURRENT_EVENT_COUNT_LE_HISTORY",
            "PASS",
            f"current <= history ({history_count})",
            current_count,
        )

    # Current event IDs must exist in history.
    if not events.empty and not history.empty:
        current_ids = set(
            events["funding_event_id"].dropna().astype(str)
        )
        history_ids = set(
            history["funding_event_id"].dropna().astype(str)
        )
        missing = sorted(current_ids - history_ids)
        status = "FAIL" if missing else "PASS"

        add_check(
            "CURRENT_EVENTS_PRESENT_IN_HISTORY",
            status,
            "all current funding_event_id values",
            missing if missing else "none",
            "Current canonical events must have historical filing lineage.",
        )

        if missing:
            message = f"Current events missing from history: {missing}"
            failures.append(message)
            append_exception(
                exceptions,
                "RECONCILIATION",
                "FATAL",
                "CURRENT_EVENT_MISSING_HISTORY",
                message,
            )

    # Snapshot event counts.
    if not snapshot.empty:
        event_counts = (
            events.groupby("cik").size().to_dict()
            if not events.empty else {}
        )

        mismatches = []
        for _, row in snapshot.iterrows():
            cik = normalize_cik(row["cik"])
            actual = int(event_counts.get(cik, 0))
            expected = int(row["funding_event_count"]) if pd.notna(
                row["funding_event_count"]
            ) else -1
            if actual != expected:
                mismatches.append({
                    "cik": cik,
                    "snapshot": expected,
                    "events": actual,
                })

        add_check(
            "SNAPSHOT_EVENT_COUNT_RECONCILIATION",
            "FAIL" if mismatches else "PASS",
            "snapshot count == current event count by CIK",
            mismatches if mismatches else "all matched",
        )

        if mismatches:
            message = f"Snapshot event-count mismatches: {mismatches}"
            failures.append(message)
            append_exception(
                exceptions,
                "RECONCILIATION",
                "FATAL",
                "SNAPSHOT_EVENT_COUNT_MISMATCH",
                message,
            )

    # Snapshot capital totals.
    if not snapshot.empty:
        event_totals = {}
        if not events.empty:
            x = events.copy()
            x["cik"] = x["cik"].map(normalize_cik)
            x["capital_amount_usd"] = parse_numeric(
                x["capital_amount_usd"]
            )
            event_totals = (
                x.groupby("cik")["capital_amount_usd"]
                .sum(min_count=1)
                .to_dict()
            )

        mismatches = []
        for _, row in snapshot.iterrows():
            cik = normalize_cik(row["cik"])
            actual = event_totals.get(cik)
            expected = row["total_sec_capital_raised_usd"]
            if not values_equal(expected, actual):
                mismatches.append({
                    "cik": cik,
                    "snapshot": expected,
                    "events": actual,
                })

        add_check(
            "SNAPSHOT_CAPITAL_RECONCILIATION",
            "FAIL" if mismatches else "PASS",
            "snapshot total == sum(current event capital)",
            mismatches if mismatches else "all matched",
        )

        if mismatches:
            message = f"Snapshot capital mismatches: {mismatches}"
            failures.append(message)
            append_exception(
                exceptions,
                "RECONCILIATION",
                "FATAL",
                "SNAPSHOT_CAPITAL_MISMATCH",
                message,
            )

    # Summary reconciliation.
    if not summary.empty:
        row = summary.iloc[0]
        checks_to_compare = [
            ("current_funding_events", len(events)),
            ("historical_form_d_filings", len(history)),
            ("discovered_form_d_datasets", None),
        ]

        for column, actual in checks_to_compare:
            if column not in summary.columns:
                continue
            expected = row[column]
            if actual is None:
                continue
            try:
                matched = int(expected) == int(actual)
            except Exception:
                matched = False

            add_check(
                f"SUMMARY_{column.upper()}",
                "PASS" if matched else "FAIL",
                actual,
                expected,
            )

            if not matched:
                message = (
                    f"Summary {column}={expected} does not match actual {actual}."
                )
                failures.append(message)
                append_exception(
                    exceptions,
                    "RECONCILIATION",
                    "FATAL",
                    f"SUMMARY_{column.upper()}_MISMATCH",
                    message,
                )

    # Lineage reconciliation.
    if not lineage.empty:
        row = lineage.iloc[0]

        comparisons = [
            ("current_event_rows", len(events)),
            ("historical_filing_rows", len(history)),
            ("snapshot_rows", len(snapshot)),
            ("signal_rows", None),
            ("discovered_dataset_rows", None),
        ]

        for column, actual in comparisons:
            if column not in lineage.columns or actual is None:
                continue
            try:
                matched = int(row[column]) == int(actual)
            except Exception:
                matched = False

            add_check(
                f"LINEAGE_{column.upper()}",
                "PASS" if matched else "FAIL",
                actual,
                row[column],
            )

            if not matched:
                message = (
                    f"Lineage {column}={row[column]} does not match actual {actual}."
                )
                failures.append(message)
                append_exception(
                    exceptions,
                    "RECONCILIATION",
                    "FATAL",
                    f"LINEAGE_{column.upper()}_MISMATCH",
                    message,
                )

    result = pd.DataFrame(checks)

    # These diagnostic fields intentionally contain counts, labels and
    # structured mismatch details. Store them as text so PyArrow does not
    # infer one object column from incompatible Python types.
    def serialize_diagnostic(value: Any) -> Optional[str]:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        if isinstance(value, (dict, list, tuple, set)):
            return json.dumps(value, sort_keys=True, default=str)
        return str(value)

    for column in ["expected", "actual"]:
        if column in result.columns:
            result[column] = result[column].map(serialize_diagnostic)

    return result


# =============================================================================
# FRESHNESS / SEMANTIC DQ
# =============================================================================

def audit_freshness_semantics(
    snapshot: pd.DataFrame,
    exceptions: List[Dict[str, Any]],
    failures: List[str],
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    if snapshot.empty:
        return pd.DataFrame()

    for _, row in snapshot.iterrows():
        cik = normalize_cik(row["cik"])
        quality = clean_text(row.get("funding_data_quality"))
        freshness = clean_text(row.get("funding_freshness_status"))
        profile_date = row.get("funding_profile_as_of_date")

        status = "PASS"
        reason = "PASS"

        if quality == "NO_DATA":
            if freshness != "UNKNOWN":
                status = "FAIL"
                reason = "NO_DATA_REQUIRES_UNKNOWN_FRESHNESS"
        else:
            if pd.isna(profile_date):
                status = "FAIL"
                reason = "OBSERVED_DATA_MISSING_PROFILE_DATE"
            elif freshness == "UNKNOWN":
                status = "FAIL"
                reason = "OBSERVED_DATA_CANNOT_HAVE_UNKNOWN_FRESHNESS"

        if status == "FAIL":
            failures.append(
                f"Freshness semantic failure for {cik}: {reason}"
            )
            append_exception(
                exceptions,
                "FRESHNESS",
                "FATAL",
                reason,
                f"Funding freshness contract failed for {cik}.",
                cik=cik,
            )

        rows.append({
            "cik": cik,
            "freshness_dq_status": status,
            "freshness_dq_reason": reason,
            "dq_version": DQ_VERSION,
        })

    return pd.DataFrame(rows)


# =============================================================================
# EXPLICIT SOURCE LIMITATION
# =============================================================================

def build_expected_source_warning(
    exceptions: List[Dict[str, Any]],
    warnings: List[str],
) -> None:
    message = (
        "SEC Form D does not represent complete private-company funding history. "
        "Step 17 capital metrics are SEC exempt-offering observations only."
    )
    warnings.append(message)
    append_exception(
        exceptions,
        "SOURCE_COVERAGE",
        "WARNING",
        "SEC_FORM_D_INCOMPLETE_FUNDING_COVERAGE",
        message,
    )


# =============================================================================
# SUMMARY / PROMOTION
# =============================================================================

def build_dq_summary(
    artifacts: Dict[str, pd.DataFrame],
    failures: List[str],
    warnings: List[str],
    exceptions: List[Dict[str, Any]],
) -> pd.DataFrame:
    events = artifacts["events"]
    history = artifacts["history"]
    snapshot = artifacts["snapshot"]
    signals = artifacts["signals"]
    discovery = artifacts["discovery"]

    fatal_count = sum(
        1 for x in exceptions if x.get("severity") == "FATAL"
    )
    warning_count = sum(
        1 for x in exceptions if x.get("severity") == "WARNING"
    )

    overall_status = (
        "FAIL"
        if failures
        else ("WARNING" if warnings else "PASS")
    )

    return pd.DataFrame([{
        "step": "STEP_17_1",
        "step_name": "Funding & Capital Data Quality & Reconciliation",
        "dq_version": DQ_VERSION,
        "expected_step17_version": STEP17_VERSION_EXPECTED,
        "overall_dq_status": overall_status,
        "promotion_decision": "BLOCKED" if failures else "PROMOTE",
        "configured_companies": len(COMPANIES),
        "current_funding_events": len(events),
        "historical_form_d_filings": len(history),
        "snapshot_rows": len(snapshot),
        "funding_signal_rows": len(signals),
        "discovered_form_d_datasets": len(discovery),
        "fatal_exception_count": fatal_count,
        "warning_exception_count": warning_count,
        "fatal_failure_count": len(failures),
        "warning_count": len(warnings),
        "source_scope": EXPECTED_SOURCE_SCOPE,
        "sec_form_d_coverage_limitation": True,
        "generated_at_utc": utc_now_iso(),
    }])


def build_dq_lineage(
    artifacts: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    return pd.DataFrame([{
        "step": "STEP_17_1",
        "dq_version": DQ_VERSION,
        "source_step": "STEP_17",
        "source_step_version": STEP17_VERSION_EXPECTED,
        "source_root": str(STEP17_GOLD_ROOT),
        "dq_output_root": str(DQ_GOLD_ROOT),
        "company_master_source": str(COMPANY_MASTER_PATH),
        "form_d_start_quarter": FORM_D_START_QUARTER,
        "form_d_end_quarter": FORM_D_END_QUARTER or "LATEST_AVAILABLE",
        "current_event_rows": len(artifacts["events"]),
        "historical_filing_rows": len(artifacts["history"]),
        "snapshot_rows": len(artifacts["snapshot"]),
        "signal_rows": len(artifacts["signals"]),
        "discovery_rows": len(artifacts["discovery"]),
        "dataset_history_rows": len(artifacts["dataset_history"]),
        "related_person_rows": len(artifacts["related_persons"]),
        "generated_at_utc": utc_now_iso(),
    }])


def build_promotion(
    summary: pd.DataFrame,
    failures: List[str],
    warnings: List[str],
) -> pd.DataFrame:
    status = summary.iloc[0]["overall_dq_status"] if not summary.empty else "FAIL"
    decision = "BLOCKED" if failures else "PROMOTE"

    return pd.DataFrame([{
        "step": "STEP_17_1",
        "dq_version": DQ_VERSION,
        "source_step": "STEP_17",
        "source_step_version": STEP17_VERSION_EXPECTED,
        "dq_status": status,
        "promotion_decision": decision,
        "fatal_failure_count": len(failures),
        "warning_count": len(warnings),
        "promotion_contract": (
            "PROMOTE when no fatal DQ exceptions; retain explicit SEC Form D coverage warning."
        ),
        "generated_at_utc": utc_now_iso(),
    }])


# =============================================================================
# WRITE OUTPUTS
# =============================================================================

def write_outputs(
    event_dq: pd.DataFrame,
    history_dq: pd.DataFrame,
    snapshot_dq: pd.DataFrame,
    signal_dq: pd.DataFrame,
    discovery_dq: pd.DataFrame,
    source_coverage_dq: pd.DataFrame,
    reconciliation_dq: pd.DataFrame,
    exceptions_df: pd.DataFrame,
    summary: pd.DataFrame,
    lineage: pd.DataFrame,
    promotion: pd.DataFrame,
) -> None:
    write_parquet(
        event_dq,
        DQ_GOLD_ROOT / "funding_event_dq" / "funding_event_dq.parquet",
    )
    write_parquet(
        history_dq,
        DQ_GOLD_ROOT / "funding_history_dq" / "funding_history_dq.parquet",
    )
    write_parquet(
        snapshot_dq,
        DQ_GOLD_ROOT / "funding_snapshot_dq" / "funding_snapshot_dq.parquet",
    )
    write_parquet(
        signal_dq,
        DQ_GOLD_ROOT / "funding_signal_dq" / "funding_signal_dq.parquet",
    )
    write_parquet(
        discovery_dq,
        DQ_GOLD_ROOT / "funding_discovery_dq" / "funding_discovery_dq.parquet",
    )
    write_parquet(
        source_coverage_dq,
        DQ_GOLD_ROOT
        / "funding_source_coverage_dq"
        / "funding_source_coverage_dq.parquet",
    )
    write_parquet(
        reconciliation_dq,
        DQ_GOLD_ROOT
        / "funding_reconciliation_dq"
        / "funding_reconciliation_dq.parquet",
    )
    write_parquet(
        exceptions_df,
        DQ_GOLD_ROOT
        / "funding_dq_exceptions"
        / "funding_dq_exceptions.parquet",
    )
    write_parquet(
        summary,
        DQ_GOLD_ROOT
        / "funding_dq_summary"
        / "funding_dq_summary.parquet",
    )
    write_parquet(
        lineage,
        DQ_GOLD_ROOT
        / "funding_dq_lineage"
        / "funding_dq_lineage.parquet",
    )
    write_parquet(
        promotion,
        DQ_GOLD_ROOT
        / "funding_promotion"
        / "funding_promotion.parquet",
    )


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    logger.info("=" * 72)
    logger.info("STEP 17.1 - FUNDING & CAPITAL DATA QUALITY & RECONCILIATION")
    logger.info("Version: %s", DQ_VERSION)
    logger.info("=" * 72)
    logger.info("Expected Step 17 version: %s", STEP17_VERSION_EXPECTED)
    logger.info("Step 17 source: %s", STEP17_GOLD_ROOT)
    logger.info("DQ output: %s", DQ_GOLD_ROOT)

    failures: List[str] = []
    warnings: List[str] = []
    exceptions: List[Dict[str, Any]] = []

    company_master = load_company_master(failures)
    artifacts = load_artifacts(failures)

    logger.info(
        "Loaded events rows=%d",
        len(artifacts["events"]),
    )
    logger.info(
        "Loaded history rows=%d",
        len(artifacts["history"]),
    )
    logger.info(
        "Loaded snapshot rows=%d",
        len(artifacts["snapshot"]),
    )
    logger.info(
        "Loaded signals rows=%d",
        len(artifacts["signals"]),
    )
    logger.info(
        "Loaded discovery rows=%d",
        len(artifacts["discovery"]),
    )
    logger.info(
        "Loaded summary rows=%d",
        len(artifacts["summary"]),
    )
    logger.info(
        "Loaded lineage rows=%d",
        len(artifacts["lineage"]),
    )

    validate_versions(
        artifacts["summary"],
        artifacts["lineage"],
        exceptions,
        failures,
    )

    event_dq = audit_events(
        artifacts["events"],
        exceptions,
        failures,
    )

    history_dq = audit_history(
        artifacts["history"],
        artifacts["events"],
        exceptions,
        failures,
    )

    snapshot_dq = audit_snapshot(
        artifacts["snapshot"],
        artifacts["events"],
        company_master,
        exceptions,
        failures,
    )

    signal_dq = audit_signals(
        artifacts["signals"],
        artifacts["snapshot"],
        exceptions,
        failures,
    )

    discovery_dq = audit_discovery(
        artifacts["discovery"],
        exceptions,
        failures,
        warnings,
    )

    source_coverage_dq = audit_source_coverage(
        artifacts["discovery"],
        artifacts["events"],
        artifacts["snapshot"],
        exceptions,
        warnings,
    )

    reconciliation_dq = audit_reconciliation(
        artifacts["events"],
        artifacts["history"],
        artifacts["snapshot"],
        artifacts["summary"],
        artifacts["lineage"],
        exceptions,
        failures,
    )

    freshness_dq = audit_freshness_semantics(
        artifacts["snapshot"],
        exceptions,
        failures,
    )

    build_expected_source_warning(
        exceptions,
        warnings,
    )

    # Ensure warnings do not become fatal promotion blockers.
    # Any actual contract violation is already represented in failures.
    exceptions_df = pd.DataFrame(
        exceptions,
        columns=[
            "dq_category",
            "severity",
            "exception_code",
            "message",
            "cik",
            "funding_event_id",
            "offering_id",
            "accession_number",
            "dq_version",
            "created_at_utc",
        ],
    )

    summary = build_dq_summary(
        artifacts,
        failures,
        warnings,
        exceptions,
    )

    lineage = build_dq_lineage(artifacts)

    promotion = build_promotion(
        summary,
        failures,
        warnings,
    )

    write_outputs(
        event_dq,
        history_dq,
        snapshot_dq,
        signal_dq,
        discovery_dq,
        source_coverage_dq,
        reconciliation_dq,
        exceptions_df,
        summary,
        lineage,
        promotion,
    )

    logger.info("")
    logger.info("STEP 17.1 COMPLETE")
    logger.info(
        "Current funding events : %d",
        len(artifacts["events"]),
    )
    logger.info(
        "Historical filings     : %d",
        len(artifacts["history"]),
    )
    logger.info(
        "Configured companies   : %d",
        len(COMPANIES),
    )
    logger.info(
        "DQ fatal exceptions    : %d",
        len(failures),
    )
    logger.info(
        "DQ warnings            : %d",
        len(warnings),
    )
    logger.info(
        "Promotion decision     : %s",
        "BLOCKED" if failures else "PROMOTE",
    )
    logger.info(
        "Overall DQ status      : %s",
        summary.iloc[0]["overall_dq_status"] if not summary.empty else "FAIL",
    )

    if failures:
        for failure in failures:
            logger.error("FAIL: %s", failure)
        raise RuntimeError(
            "STEP 17.1 DQ failed. Funding Gold is not DQ-cleared."
        )

    for warning in warnings:
        logger.warning("WARNING: %s", warning)

    logger.info(
        "STEP 17.1 PASS: Funding Gold is DQ-cleared with explicit "
        "SEC Form D source-coverage limitation."
    )


if __name__ == "__main__":
    main()
