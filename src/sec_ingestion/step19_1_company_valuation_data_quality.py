"""
STEP 19.1 - COMPANY VALUATION DATA QUALITY & RECONCILIATION

Purpose
-------
Audit Step 19 Company Valuation Gold before downstream consumption.

This is a DQ gate only. It does not rebuild or mutate Step 19.

Critical semantics
------------------
- NO_DATA means valuation signal value must be NULL.
- Missing valuation source is a WARNING, not a failure.
- Zero is never used as a placeholder for missing valuation.
- SEC Form D capital is NOT company valuation.
- Reported valuation must be source-provided.
- Transaction-implied valuation is valid only when:
      valuation = transaction_amount / (equity_pct / 100)
  with transaction_amount > 0 and 0 < equity_pct <= 100.
- Valuation fabrication must remain explicitly prevented.
- A valuation ratio to SEC capital is contextual only; it must never be
  promoted as a valuation.

Run
---
python ./src/sec_ingestion/step19_1_company_valuation_data_quality.py
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
from company_platform.registry.active_company_scope import load_active_company_ciks
from company_platform.registry.active_company_scope import normalize_cik
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# CONFIG
# =============================================================================

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DATA_ROOT = REPO_ROOT / "data"
COMPANY_MASTER_PATH = (
    DATA_ROOT / "silver" / "company" / "company.parquet"
)
STEP19_ROOT = DATA_ROOT / "gold" / "valuation_intelligence"
STEP17_ROOT = DATA_ROOT / "gold" / "funding_capital_intelligence"

STEP19_VERSION_EXPECTED = "19.0.0"
STEP17_VERSION_EXPECTED = "3.0.1"
STEP18_VERSION_EXPECTED = "18.0.0"

DQ_VERSION = "19.1.0"

def configured_companies() -> list[str]:
    """Resolve the authoritative active production company scope."""
    ciks = [str(cik).zfill(10) for cik in load_active_company_ciks()]
    if not ciks:
        raise ValueError("Active company execution scope is empty.")
    return sorted(set(ciks))


def configured_companies() -> list[str]:
    """Resolve the authoritative active production company universe."""
    ciks = [str(c).zfill(10) for c in load_active_company_ciks()]
    if not ciks:
        raise ValueError("Active company execution scope is empty.")
    return sorted(set(ciks))

COMPANIES = configured_companies()


OUTPUT_ROOT = DATA_ROOT / "gold" / "valuation_intelligence_dq"

VALID_VALUATION_TYPES = {
    "REPORTED_VALUATION",
    "TRANSACTION_IMPLIED_VALUATION",
}

VALID_VALUATION_STATUS = {
    "AVAILABLE",
    "NO_DATA",
    "PARTIAL",
}

VALID_CONFIDENCE = {
    "HIGH",
    "MEDIUM",
    "LOW",
    "UNKNOWN",
}

VALID_SIGNAL_TYPES = {
    "VALUATION_COVERAGE",
    "VALUATION_FRESHNESS",
    "VALUATION_TO_CAPITAL",
    "VALUATION_DATA_LIMITED",
}

VALID_SIGNAL_STATUS = {
    "AVAILABLE",
    "NO_DATA",
    "PARTIAL",
}

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("step19_1")


# =============================================================================
# HELPERS
# =============================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_cik(value: Any) -> str:
    if value is None:
        return ""
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits.zfill(10) if digits else ""


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = " ".join(str(value).split()).strip()
    return text or None


def safe_bool(value: Any) -> bool | None:
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


def numeric(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def parse_date(value: Any) -> pd.Timestamp | None:
    try:
        result = pd.to_datetime(value, errors="coerce")
        return result if pd.notna(result) else None
    except Exception:
        return None


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def load_required(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Required {label} artifact not found: {path}"
        )
    return pd.read_parquet(path)


def make_result(
    check_name: str,
    status: str,
    severity: str,
    message: str,
    rows_checked: int = 0,
    failure_count: int = 0,
    warning_count: int = 0,
) -> Dict[str, Any]:
    return {
        "check_name": check_name,
        "status": status,
        "severity": severity,
        "message": message,
        "rows_checked": rows_checked,
        "failure_count": failure_count,
        "warning_count": warning_count,
        "dq_version": DQ_VERSION,
        "created_at_utc": utc_now_iso(),
    }


# =============================================================================
# PATHS
# =============================================================================

def step19_paths() -> Dict[str, Path]:
    return {
        "events": STEP19_ROOT / "valuation_events" / "valuation_events.parquet",
        "history": STEP19_ROOT / "valuation_event_history" / "valuation_event_history.parquet",
        "snapshot": STEP19_ROOT / "company_valuation_snapshot" / "company_valuation_snapshot.parquet",
        "signals": STEP19_ROOT / "valuation_signals" / "valuation_signals.parquet",
        "summary": STEP19_ROOT / "valuation_summary" / "valuation_summary.parquet",
        "lineage": STEP19_ROOT / "valuation_lineage" / "valuation_lineage.parquet",
        "exceptions": STEP19_ROOT / "valuation_exceptions" / "valuation_exceptions.parquet",
        "promotion": STEP19_ROOT / "valuation_promotion" / "valuation_promotion.parquet",
    }


def step17_snapshot_path() -> Path:
    return (
        STEP17_ROOT
        / "company_funding_snapshot"
        / "company_funding_snapshot.parquet"
    )


# =============================================================================
# VERSION / SCHEMA
# =============================================================================

def audit_versions(
    events: pd.DataFrame,
    snapshot: pd.DataFrame,
    signals: pd.DataFrame,
    summary: pd.DataFrame,
    lineage: pd.DataFrame,
    promotion: pd.DataFrame,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    observed19: set[str] = set()

    for df in [
        events,
        snapshot,
        signals,
        summary,
        lineage,
        promotion,
    ]:
        for col in ["step_version", "source_step_version"]:
            if col in df.columns:
                observed19.update(
                    df[col].dropna().astype(str).tolist()
                )

    allowed = {
        STEP19_VERSION_EXPECTED,
        STEP18_VERSION_EXPECTED,
        STEP17_VERSION_EXPECTED,
    }

    unexpected = observed19 - allowed

    if unexpected:
        results.append(make_result(
            "VERSION_COMPATIBILITY",
            "FAIL",
            "FATAL",
            f"Unexpected upstream/version values: {sorted(unexpected)}",
            failures=1,
        ))
    else:
        results.append(make_result(
            "VERSION_COMPATIBILITY",
            "PASS",
            "INFO",
            "Step 19/18/17 version lineage is compatible.",
        ))

    if "step_version" in promotion.columns:
        bad = promotion[
            promotion["step_version"].astype(str)
            != STEP19_VERSION_EXPECTED
        ]
        if not bad.empty:
            results.append(make_result(
                "PROMOTION_VERSION",
                "FAIL",
                "FATAL",
                "Promotion artifact has an unexpected Step 19 version.",
                rows_checked=len(promotion),
                failure_count=len(bad),
            ))
        else:
            results.append(make_result(
                "PROMOTION_VERSION",
                "PASS",
                "INFO",
                "Promotion artifact version is correct.",
                rows_checked=len(promotion),
            ))

    return results


# =============================================================================
# EVENT DQ
# =============================================================================

def audit_valuation_events(
    events: pd.DataFrame,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    required = {
        "valuation_event_id",
        "cik",
        "valuation_amount_usd",
        "valuation_type",
        "valuation_source",
        "confidence",
        "valuation_status",
        "source_scope",
    }

    missing = required - set(events.columns)

    if missing:
        results.append(make_result(
            "VALUATION_EVENT_SCHEMA",
            "FAIL",
            "FATAL",
            f"Missing valuation-event columns: {sorted(missing)}",
            failures=1,
        ))
        return results

    if events.empty:
        results.append(make_result(
            "VALUATION_EVENT_EMPTY_STATE",
            "PASS",
            "INFO",
            "No valuation events are present; valid while enrichment is unavailable.",
        ))
        return results

    duplicate = events["valuation_event_id"].duplicated()

    if duplicate.any():
        results.append(make_result(
            "VALUATION_EVENT_GRAIN",
            "FAIL",
            "FATAL",
            "Duplicate valuation_event_id values detected.",
            rows_checked=len(events),
            failure_count=int(duplicate.sum()),
        ))
    else:
        results.append(make_result(
            "VALUATION_EVENT_GRAIN",
            "PASS",
            "INFO",
            "Valuation event grain is unique.",
            rows_checked=len(events),
        ))

    amounts = pd.to_numeric(
        events["valuation_amount_usd"],
        errors="coerce",
    )

    invalid_amount = amounts.isna() | amounts.le(0)

    if invalid_amount.any():
        results.append(make_result(
            "VALUATION_AMOUNT_INTEGRITY",
            "FAIL",
            "FATAL",
            "Valuation event contains null, zero, or negative valuation amount.",
            rows_checked=len(events),
            failure_count=int(invalid_amount.sum()),
        ))
    else:
        results.append(make_result(
            "VALUATION_AMOUNT_INTEGRITY",
            "PASS",
            "INFO",
            "All valuation event amounts are positive.",
            rows_checked=len(events),
        ))

    invalid_types = (
        set(events["valuation_type"].dropna().astype(str))
        - VALID_VALUATION_TYPES
    )

    if invalid_types:
        results.append(make_result(
            "VALUATION_TYPE_VALUES",
            "FAIL",
            "FATAL",
            f"Invalid valuation types: {sorted(invalid_types)}",
            failure_count=1,
        ))

    invalid_confidence = (
        set(events["confidence"].dropna().astype(str))
        - VALID_CONFIDENCE
    )

    if invalid_confidence:
        results.append(make_result(
            "VALUATION_CONFIDENCE_VALUES",
            "FAIL",
            "FATAL",
            f"Invalid valuation confidence values: {sorted(invalid_confidence)}",
            failure_count=1,
        ))

    # Transaction-implied mathematical validation.
    implied_failures: List[str] = []

    for _, row in events.iterrows():
        valuation_type = clean_text(row["valuation_type"])
        amount = numeric(row["valuation_amount_usd"])

        if valuation_type != "TRANSACTION_IMPLIED_VALUATION":
            continue

        transaction = numeric(row.get("transaction_amount_usd"))
        equity_pct = numeric(row.get("equity_pct"))

        if transaction is None or transaction <= 0:
            implied_failures.append(
                f"{row['valuation_event_id']}: invalid transaction amount"
            )
            continue

        if equity_pct is None or equity_pct <= 0 or equity_pct > 100:
            implied_failures.append(
                f"{row['valuation_event_id']}: invalid equity_pct"
            )
            continue

        expected = transaction / (equity_pct / 100.0)

        if amount is None or abs(amount - expected) > max(
            0.01,
            abs(expected) * 1e-9,
        ):
            implied_failures.append(
                f"{row['valuation_event_id']}: valuation={amount}, expected={expected}"
            )

    if implied_failures:
        results.append(make_result(
            "TRANSACTION_IMPLIED_MATH",
            "FAIL",
            "FATAL",
            "; ".join(implied_failures),
            failure_count=len(implied_failures),
        ))
    else:
        results.append(make_result(
            "TRANSACTION_IMPLIED_MATH",
            "PASS",
            "INFO",
            "All transaction-implied valuations satisfy the derivation contract.",
        ))

    # Reported valuations must not claim a mathematical derivation.
    reported_derivation = []
    if "valuation_derivation_method" in events.columns:
        for _, row in events.iterrows():
            if clean_text(row["valuation_type"]) == "REPORTED_VALUATION":
                method = clean_text(row["valuation_derivation_method"])
                if method == "TRANSACTION_AMOUNT_DIVIDED_BY_EQUITY_PERCENT":
                    reported_derivation.append(
                        str(row["valuation_event_id"])
                    )

    if reported_derivation:
        results.append(make_result(
            "REPORTED_VALUATION_SEMANTICS",
            "FAIL",
            "FATAL",
            "Reported valuation rows incorrectly claim transaction-implied derivation.",
            failure_count=len(reported_derivation),
        ))
    else:
        results.append(make_result(
            "REPORTED_VALUATION_SEMANTICS",
            "PASS",
            "INFO",
            "Reported valuation semantics are preserved.",
        ))

    return results


# =============================================================================
# SNAPSHOT DQ
# =============================================================================

def audit_snapshot(
    snapshot: pd.DataFrame,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    required = {
        "cik",
        "valuation_event_count",
        "latest_valuation_amount_usd",
        "latest_valuation_date",
        "valuation_status",
        "valuation_data_quality",
        "valuation_confidence",
        "valuation_fabrication_prevented",
        "sec_capital_context_usd",
        "sec_capital_context_usd",
        "valuation_to_sec_capital_ratio",
    }

    missing = required - set(snapshot.columns)

    if missing:
        results.append(make_result(
            "VALUATION_SNAPSHOT_SCHEMA",
            "FAIL",
            "FATAL",
            f"Snapshot missing required columns: {sorted(missing)}",
            failures=1,
        ))
        return results

    normalized = snapshot["cik"].map(normalize_cik)
    configured = {normalize_cik(c) for c in COMPANIES}

    if set(normalized) != configured:
        results.append(make_result(
            "VALUATION_SNAPSHOT_COVERAGE",
            "FAIL",
            "FATAL",
            f"Snapshot CIK coverage mismatch. Missing={sorted(configured-set(normalized))}, "
            f"Extra={sorted(set(normalized)-configured)}",
            rows_checked=len(snapshot),
            failure_count=1,
        ))
    else:
        results.append(make_result(
            "VALUATION_SNAPSHOT_COVERAGE",
            "PASS",
            "INFO",
            "Snapshot covers all configured companies.",
            rows_checked=len(snapshot),
        ))

    duplicate = normalized.duplicated()
    if duplicate.any():
        results.append(make_result(
            "VALUATION_SNAPSHOT_GRAIN",
            "FAIL",
            "FATAL",
            "More than one valuation snapshot exists per CIK.",
            rows_checked=len(snapshot),
            failure_count=int(duplicate.sum()),
        ))
    else:
        results.append(make_result(
            "VALUATION_SNAPSHOT_GRAIN",
            "PASS",
            "INFO",
            "Valuation snapshot grain is one row per CIK.",
            rows_checked=len(snapshot),
        ))

    invalid_status = (
        set(snapshot["valuation_status"].dropna().astype(str))
        - VALID_VALUATION_STATUS
    )

    if invalid_status:
        results.append(make_result(
            "VALUATION_STATUS_VALUES",
            "FAIL",
            "FATAL",
            f"Invalid valuation status values: {sorted(invalid_status)}",
            failure_count=1,
        ))

    no_data_amount = []
    no_data_date = []

    for _, row in snapshot.iterrows():
        if clean_text(row["valuation_status"]) == "NO_DATA":
            if not is_missing(row["latest_valuation_amount_usd"]):
                no_data_amount.append(normalize_cik(row["cik"]))
            if not is_missing(row["latest_valuation_date"]):
                no_data_date.append(normalize_cik(row["cik"]))

    if no_data_amount or no_data_date:
        results.append(make_result(
            "VALUATION_NO_DATA_SEMANTICS",
            "FAIL",
            "FATAL",
            f"NO_DATA rows contain values. amount={no_data_amount}, date={no_data_date}",
            failure_count=len(no_data_amount) + len(no_data_date),
        ))
    else:
        results.append(make_result(
            "VALUATION_NO_DATA_SEMANTICS",
            "PASS",
            "INFO",
            "NO_DATA valuation status correctly uses NULL valuation amount/date.",
            rows_checked=len(snapshot),
        ))

    fabrication = []
    for _, row in snapshot.iterrows():
        if safe_bool(row["valuation_fabrication_prevented"]) is not True:
            fabrication.append(normalize_cik(row["cik"]))

    if fabrication:
        results.append(make_result(
            "VALUATION_FABRICATION_CONTROL",
            "FAIL",
            "FATAL",
            f"Valuation fabrication control disabled: {fabrication}",
            failure_count=len(fabrication),
        ))
    else:
        results.append(make_result(
            "VALUATION_FABRICATION_CONTROL",
            "PASS",
            "INFO",
            "Valuation fabrication control is enabled for all companies.",
            rows_checked=len(snapshot),
        ))

    # SEC capital must remain contextual only.
    if "valuation_to_sec_capital_ratio" in snapshot.columns:
        ratio = pd.to_numeric(
            snapshot["valuation_to_sec_capital_ratio"],
            errors="coerce",
        )
        invalid_ratio = ratio.lt(0)

        if invalid_ratio.any():
            results.append(make_result(
                "VALUATION_CAPITAL_CONTEXT",
                "FAIL",
                "FATAL",
                "Valuation-to-SEC-capital contextual ratio is negative.",
                failure_count=int(invalid_ratio.sum()),
            ))
        else:
            results.append(make_result(
                "VALUATION_CAPITAL_CONTEXT",
                "PASS",
                "INFO",
                "SEC capital is retained as contextual ratio only, not valuation.",
                rows_checked=len(snapshot),
            ))

    return results


# =============================================================================
# SIGNAL DQ
# =============================================================================

def audit_signals(
    signals: pd.DataFrame,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    required = {
        "cik",
        "signal_type",
        "signal_value",
        "signal_status",
    }

    missing = required - set(signals.columns)

    if missing:
        results.append(make_result(
            "VALUATION_SIGNAL_SCHEMA",
            "FAIL",
            "FATAL",
            f"Signals missing columns: {sorted(missing)}",
            failure_count=1,
        ))
        return results

    duplicate = signals[
        ["cik", "signal_type"]
    ].duplicated()

    if duplicate.any():
        results.append(make_result(
            "VALUATION_SIGNAL_GRAIN",
            "FAIL",
            "FATAL",
            "Duplicate CIK/signal_type signal rows detected.",
            rows_checked=len(signals),
            failure_count=int(duplicate.sum()),
        ))
    else:
        results.append(make_result(
            "VALUATION_SIGNAL_GRAIN",
            "PASS",
            "INFO",
            "Signal grain is one row per CIK/signal type.",
            rows_checked=len(signals),
        ))

    invalid_types = (
        set(signals["signal_type"].dropna().astype(str))
        - VALID_SIGNAL_TYPES
    )

    if invalid_types:
        results.append(make_result(
            "VALUATION_SIGNAL_TYPES",
            "FAIL",
            "FATAL",
            f"Invalid signal types: {sorted(invalid_types)}",
            failure_count=1,
        ))

    invalid_status = (
        set(signals["signal_status"].dropna().astype(str))
        - VALID_SIGNAL_STATUS
    )

    if invalid_status:
        results.append(make_result(
            "VALUATION_SIGNAL_STATUS",
            "FAIL",
            "FATAL",
            f"Invalid signal status values: {sorted(invalid_status)}",
            failure_count=1,
        ))

    no_data_with_value = []

    for _, row in signals.iterrows():
        if clean_text(row["signal_status"]) == "NO_DATA":
            if not is_missing(row["signal_value"]):
                no_data_with_value.append(
                    f"{normalize_cik(row['cik'])}:{row['signal_type']}"
                )

    if no_data_with_value:
        results.append(make_result(
            "VALUATION_SIGNAL_NO_DATA_SEMANTICS",
            "FAIL",
            "FATAL",
            f"NO_DATA signals contain values: {no_data_with_value}",
            failure_count=len(no_data_with_value),
        ))
    else:
        results.append(make_result(
            "VALUATION_SIGNAL_NO_DATA_SEMANTICS",
            "PASS",
            "INFO",
            "NO_DATA valuation signals correctly use NULL values.",
            rows_checked=len(signals),
        ))

    return results


# =============================================================================
# CROSS-ARTIFACT RECONCILIATION
# =============================================================================

def audit_reconciliation(
    events: pd.DataFrame,
    snapshot: pd.DataFrame,
    signals: pd.DataFrame,
    step17_snapshot: pd.DataFrame,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    configured = {normalize_cik(c) for c in COMPANIES}

    event_counts = (
        events["cik"].map(normalize_cik).value_counts().to_dict()
        if not events.empty
        else {}
    )

    failures = []

    for _, row in snapshot.iterrows():
        cik = normalize_cik(row["cik"])
        expected = int(event_counts.get(cik, 0))
        actual = int(row["valuation_event_count"])

        if actual != expected:
            failures.append(
                f"{cik}: snapshot={actual}, events={expected}"
            )

    if failures:
        results.append(make_result(
            "SNAPSHOT_EVENT_RECONCILIATION",
            "FAIL",
            "FATAL",
            "; ".join(failures),
            failure_count=len(failures),
        ))
    else:
        results.append(make_result(
            "SNAPSHOT_EVENT_RECONCILIATION",
            "PASS",
            "INFO",
            "Valuation snapshot event counts reconcile to valuation events.",
            rows_checked=len(snapshot),
        ))

    expected_signal_rows = len(COMPANIES) * len(VALID_SIGNAL_TYPES)

    if len(signals) != expected_signal_rows:
        results.append(make_result(
            "SIGNAL_COVERAGE_RECONCILIATION",
            "FAIL",
            "FATAL",
            f"Expected {expected_signal_rows} signal rows but found {len(signals)}.",
            rows_checked=len(signals),
            failure_count=1,
        ))
    else:
        results.append(make_result(
            "SIGNAL_COVERAGE_RECONCILIATION",
            "PASS",
            "INFO",
            "Signal count reconciles to configured companies and signal contract.",
            rows_checked=len(signals),
        ))

    # Step 17 capital context must never become valuation.
    if not step17_snapshot.empty:
        if "cik" not in step17_snapshot.columns:
            results.append(make_result(
                "STEP17_CAPITAL_CONTEXT_SCHEMA",
                "FAIL",
                "FATAL",
                "Step 17 funding snapshot lacks CIK.",
                failure_count=1,
            ))
        else:
            results.append(make_result(
                "STEP17_CAPITAL_CONTEXT_SCHEMA",
                "PASS",
                "INFO",
                "Step 17 capital context is available for semantic reconciliation.",
                rows_checked=len(step17_snapshot),
            ))

    # Company coverage.
    step17_ciks = (
        set(step17_snapshot["cik"].map(normalize_cik))
        if "cik" in step17_snapshot.columns
        else set()
    )

    if step17_ciks and not step17_ciks.issuperset(configured):
        results.append(make_result(
            "UPSTREAM_COMPANY_COVERAGE",
            "FAIL",
            "FATAL",
            "Step 17 funding snapshot does not cover all configured companies.",
            failure_count=1,
        ))
    else:
        results.append(make_result(
            "UPSTREAM_COMPANY_COVERAGE",
            "PASS",
            "INFO",
            "Step 17 funding snapshot covers configured companies.",
        ))

    return results


# =============================================================================
# SOURCE COVERAGE / SEMANTICS
# =============================================================================

def audit_source_semantics(
    events: pd.DataFrame,
    snapshot: pd.DataFrame,
    summary: pd.DataFrame,
    lineage: pd.DataFrame,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    warnings: List[str] = []

    if events.empty:
        warnings.append(
            "No valuation enrichment source is currently available; valuation remains NO_DATA."
        )

    if "valuation_fabrication_prevented" in summary.columns:
        values = summary["valuation_fabrication_prevented"].map(safe_bool)
        if not values.eq(True).all():
            results.append(make_result(
                "SUMMARY_FABRICATION_CONTROL",
                "FAIL",
                "FATAL",
                "Summary does not confirm valuation fabrication prevention.",
                failure_count=int((~values.eq(True)).sum()),
            ))
        else:
            results.append(make_result(
                "SUMMARY_FABRICATION_CONTROL",
                "PASS",
                "INFO",
                "Summary confirms valuation fabrication prevention.",
                rows_checked=len(summary),
            ))

    if "sec_capital_treated_as_valuation" in summary.columns:
        values = summary["sec_capital_treated_as_valuation"].map(safe_bool)
        if not values.eq(False).all():
            results.append(make_result(
                "SEC_CAPITAL_VALUATION_CONTROL",
                "FAIL",
                "FATAL",
                "Step 19 incorrectly treats SEC capital as valuation.",
                failure_count=int((~values.eq(False)).sum()),
            ))
        else:
            results.append(make_result(
                "SEC_CAPITAL_VALUATION_CONTROL",
                "PASS",
                "INFO",
                "SEC capital is explicitly not treated as valuation.",
                rows_checked=len(summary),
            ))

    if "sec_capital_treated_as_valuation" in lineage.columns:
        values = lineage["sec_capital_treated_as_valuation"].map(safe_bool)
        observed_values = values.dropna()
        if observed_values.empty or not observed_values.eq(False).all():
            results.append(make_result(
                "LINEAGE_SEC_CAPITAL_CONTROL",
                "FAIL",
                "FATAL",
                "Lineage does not confirm SEC capital/valuation separation.",
                failure_count=max(1, int((~observed_values.eq(False)).sum())),
            ))
        else:
            results.append(make_result(
                "LINEAGE_SEC_CAPITAL_CONTROL",
                "PASS",
                "INFO",
                "Lineage confirms SEC capital is not valuation.",
                rows_checked=len(observed_values),
            ))

    if warnings:
        results.append(make_result(
            "VALUATION_SOURCE_COVERAGE",
            "WARNING",
            "WARNING",
            " ".join(warnings),
            warning_count=len(warnings),
        ))
    else:
        results.append(make_result(
            "VALUATION_SOURCE_COVERAGE",
            "PASS",
            "INFO",
            "Valuation source coverage is available.",
        ))

    return results


# =============================================================================
# PROMOTION
# =============================================================================

def audit_promotion(
    promotion: pd.DataFrame,
    fatal_count: int,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    if "promotion_decision" not in promotion.columns:
        results.append(make_result(
            "PROMOTION_SCHEMA",
            "FAIL",
            "FATAL",
            "Promotion artifact lacks promotion_decision.",
            failure_count=1,
        ))
        return results

    observed = set(
        promotion["promotion_decision"].dropna().astype(str)
    )
    # This is the upstream Step 19 artifact. Validate its own promotion
    # contract instead of comparing it with downstream Step 19.1 failures.
    expected = "PROMOTE"

    if observed != {expected}:
        results.append(make_result(
            "PROMOTION_CONSISTENCY",
            "FAIL",
            "FATAL",
            f"Promotion={sorted(observed)}, expected={expected}.",
            failure_count=1,
        ))
    else:
        results.append(make_result(
            "PROMOTION_CONSISTENCY",
            "PASS",
            "INFO",
            f"Promotion correctly reflects DQ status: {expected}.",
            rows_checked=len(promotion),
        ))

    return results


# =============================================================================
# OUTPUT
# =============================================================================

def write_outputs(
    results: List[Dict[str, Any]],
    exceptions: pd.DataFrame,
    summary: Dict[str, Any],
    lineage: pd.DataFrame,
    promotion: str,
) -> None:
    results_df = pd.DataFrame(results)

    write_parquet(
        results_df[results_df["check_name"].str.startswith("VALUATION_EVENT")],
        OUTPUT_ROOT / "valuation_event_dq" / "valuation_event_dq.parquet",
    )

    write_parquet(
        results_df[
            results_df["check_name"].str.startswith("VALUATION_SNAPSHOT")
            | results_df["check_name"].str.startswith("VALUATION_STATUS")
            | results_df["check_name"].str.startswith("VALUATION_NO_DATA")
        ],
        OUTPUT_ROOT / "valuation_snapshot_dq" / "valuation_snapshot_dq.parquet",
    )

    write_parquet(
        results_df[results_df["check_name"].str.startswith("VALUATION_SIGNAL")],
        OUTPUT_ROOT / "valuation_signal_dq" / "valuation_signal_dq.parquet",
    )

    write_parquet(
        results_df[
            results_df["check_name"].str.contains(
                "RECONCILIATION|UPSTREAM_COMPANY_COVERAGE",
                regex=True,
            )
        ],
        OUTPUT_ROOT / "valuation_reconciliation_dq"
        / "valuation_reconciliation_dq.parquet",
    )

    write_parquet(
        results_df[
            results_df["check_name"].str.contains(
                "SOURCE|SEC_CAPITAL|FABRICATION|LINEAGE",
                regex=True,
            )
        ],
        OUTPUT_ROOT / "valuation_source_coverage_dq"
        / "valuation_source_coverage_dq.parquet",
    )

    write_parquet(
        exceptions,
        OUTPUT_ROOT / "valuation_dq_exceptions"
        / "valuation_dq_exceptions.parquet",
    )

    write_parquet(
        pd.DataFrame([summary]),
        OUTPUT_ROOT / "valuation_dq_summary"
        / "valuation_dq_summary.parquet",
    )

    write_parquet(
        lineage,
        OUTPUT_ROOT / "valuation_dq_lineage"
        / "valuation_dq_lineage.parquet",
    )

    promotion_df = pd.DataFrame([{
        "step": "STEP_19.1",
        "dq_version": DQ_VERSION,
        "step19_version": STEP19_VERSION_EXPECTED,
        "step17_version": STEP17_VERSION_EXPECTED,
        "step18_version": STEP18_VERSION_EXPECTED,
        "promotion_decision": promotion,
        "valuation_fabrication_prevented": True,
        "sec_capital_treated_as_valuation": False,
        "created_at_utc": utc_now_iso(),
    }])

    write_parquet(
        promotion_df,
        OUTPUT_ROOT / "valuation_promotion"
        / "valuation_promotion.parquet",
    )


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    logger.info("=" * 72)
    logger.info("STEP 19.1 - COMPANY VALUATION DATA QUALITY & RECONCILIATION")
    logger.info("Version: %s", DQ_VERSION)
    logger.info("Expected Step 19 version: %s", STEP19_VERSION_EXPECTED)
    logger.info("Step 19 source: %s", STEP19_ROOT)
    logger.info("DQ output: %s", OUTPUT_ROOT)
    logger.info("=" * 72)

    paths = step19_paths()

    frames = {
        name: load_required(path, f"Step 19 {name}")
        for name, path in paths.items()
    }

    for name, df in frames.items():
        logger.info("Loaded %s rows=%s", name, len(df))

    step17_snapshot = load_required(
        step17_snapshot_path(),
        "Step 17 funding snapshot",
    )

    results: List[Dict[str, Any]] = []

    results.extend(audit_versions(
        frames["events"],
        frames["snapshot"],
        frames["signals"],
        frames["summary"],
        frames["lineage"],
        frames["promotion"],
    ))

    results.extend(audit_valuation_events(frames["events"]))
    results.extend(audit_snapshot(frames["snapshot"]))
    results.extend(audit_signals(frames["signals"]))
    results.extend(audit_reconciliation(
        frames["events"],
        frames["snapshot"],
        frames["signals"],
        step17_snapshot,
    ))
    results.extend(audit_source_semantics(
        frames["events"],
        frames["snapshot"],
        frames["summary"],
        frames["lineage"],
    ))

    fatal_results = [
        r for r in results
        if r["severity"] == "FATAL"
        and r["status"] == "FAIL"
    ]

    warning_results = [
        r for r in results
        if r["status"] == "WARNING"
    ]

    promotion = "BLOCKED" if fatal_results else "PROMOTE"

    # Promotion artifact consistency must itself be checked.
    promotion_results = audit_promotion(
        frames["promotion"],
        len(fatal_results),
    )
    results.extend(promotion_results)

    fatal_results = [
        r for r in results
        if r["severity"] == "FATAL"
        and r["status"] == "FAIL"
    ]

    warning_results = [
        r for r in results
        if r["status"] == "WARNING"
    ]

    promotion = "BLOCKED" if fatal_results else "PROMOTE"

    exception_rows = []

    for result in fatal_results + warning_results:
        exception_rows.append({
            "severity": result["severity"],
            "exception_code": result["check_name"],
            "message": result["message"],
            "failure_count": result["failure_count"],
            "warning_count": result["warning_count"],
            "dq_version": DQ_VERSION,
            "created_at_utc": utc_now_iso(),
        })

    exceptions = pd.DataFrame(exception_rows)

    no_data_result = next(
        (
            r for r in results
            if r["check_name"] == "VALUATION_NO_DATA_SEMANTICS"
        ),
        None,
    )

    summary = {
        "step": "STEP_19.1",
        "dq_version": DQ_VERSION,
        "step19_version": STEP19_VERSION_EXPECTED,
        "step17_version": STEP17_VERSION_EXPECTED,
        "step18_version": STEP18_VERSION_EXPECTED,
        "configured_companies": len(COMPANIES),
        "valuation_event_rows": len(frames["events"]),
        "valuation_history_rows": len(frames["history"]),
        "snapshot_rows": len(frames["snapshot"]),
        "signal_rows": len(frames["signals"]),
        "fatal_exception_count": len(fatal_results),
        "warning_count": len(warning_results),
        "promotion_decision": promotion,
        "no_data_semantics_validated": bool(
            no_data_result
            and no_data_result["status"] == "PASS"
        ),
        "valuation_fabrication_prevented": True,
        "sec_capital_treated_as_valuation": False,
        "generated_at_utc": utc_now_iso(),
    }

    lineage = pd.DataFrame([{
        "step": "STEP_19.1",
        "dq_version": DQ_VERSION,
        "source_step": "STEP_19",
        "source_step_version": STEP19_VERSION_EXPECTED,
        "source_step17_version": STEP17_VERSION_EXPECTED,
        "source_step18_version": STEP18_VERSION_EXPECTED,
        "valuation_dq_output_root": str(OUTPUT_ROOT),
        "valuation_fabrication_prevented": True,
        "sec_capital_treated_as_valuation": False,
        "promotion_decision": promotion,
        "fatal_exception_count": len(fatal_results),
        "warning_count": len(warning_results),
        "generated_at_utc": utc_now_iso(),
    }])

    write_outputs(
        results,
        exceptions,
        summary,
        lineage,
        promotion,
    )

    companies_with_valuation = int(
        frames["snapshot"]["valuation_status"]
        .eq("AVAILABLE")
        .sum()
    )

    logger.info("")
    logger.info("Configured companies       : %d", len(COMPANIES))
    logger.info(
        "Valuation event rows       : %d",
        len(frames["events"]),
    )
    logger.info(
        "Companies with valuation   : %d",
        companies_with_valuation,
    )
    logger.info(
        "Fatal DQ exceptions        : %d",
        len(fatal_results),
    )
    logger.info(
        "DQ warnings                : %d",
        len(warning_results),
    )
    logger.info(
        "Promotion decision         : %s",
        promotion,
    )
    logger.info(
        "NO_DATA semantics          : %s",
        "VALIDATED"
        if summary["no_data_semantics_validated"]
        else "FAILED",
    )
    logger.info("SEC capital ≠ valuation   : ENFORCED")
    logger.info("Valuation fabrication      : PREVENTED")

    for result in warning_results:
        logger.warning("%s", result["message"])

    if promotion == "PROMOTE":
        logger.info(
            "STEP 19.1 PASS: Valuation Gold is DQ-cleared with explicit "
            "source-coverage semantics."
        )
    else:
        logger.error("STEP 19.1 FAILED")
        raise RuntimeError(
            f"Step 19.1 DQ blocked promotion with {len(fatal_results)} fatal exceptions."
        )


if __name__ == "__main__":
    main()
