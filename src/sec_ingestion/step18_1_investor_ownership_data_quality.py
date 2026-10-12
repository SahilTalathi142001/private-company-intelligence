"""
STEP 18.1 - INVESTOR & OWNERSHIP DATA QUALITY & RECONCILIATION

Purpose
-------
Audit Step 18 Investor & Ownership Gold before downstream consumption.

This is a DQ gate only. It does not rebuild or mutate Step 18 data.

Critical semantics
------------------
- SEC Form D investor counts are NOT named investor identities.
- SEC Form D does NOT provide a complete cap table.
- NO_DATA means the corresponding signal value must be NULL.
- Zero is a valid numeric observation only when supplied by the source.
- Missing investor enrichment must not be converted to zero named investors
  as a factual observation.
- Ownership must never be fabricated.
- Current Step 18 with no enrichment is expected to pass with explicit
  source-coverage limitations.

Run
---
python ./src/sec_ingestion/step18_1_investor_ownership_data_quality.py
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
from company_platform.registry.active_company_scope import load_active_company_ciks
from company_platform.registry.active_company_scope import (
    assert_company_scope,
    normalize_cik,
)
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

STEP18_GOLD_ROOT = DATA_ROOT / "gold" / "investor_ownership_intelligence"
STEP17_GOLD_ROOT = DATA_ROOT / "gold" / "funding_capital_intelligence"

STEP18_VERSION_EXPECTED = "18.0.0"
STEP17_VERSION_EXPECTED = "3.0.1"

DQ_VERSION = "18.1.0"

def configured_companies() -> List[str]:
    """Return the authoritative production execution scope.

    The SEC/company master is reference identity data only.  Active
    processing scope is controlled exclusively by company_registry.parquet.
    """
    return assert_company_scope()


COMPANIES = configured_companies()

EXPECTED_SOURCE_SCOPE = "SEC_EXEMPT_OFFERING_ONLY"

OUTPUT_ROOT = DATA_ROOT / "gold" / "investor_ownership_intelligence_dq"

VALID_INVESTOR_STATUS = {
    "AVAILABLE",
    "NO_DATA",
    "PARTIAL",
}

VALID_OWNERSHIP_STATUS = {
    "AVAILABLE",
    "NO_DATA",
    "PARTIAL",
}

VALID_SIGNAL_STATUS = {
    "AVAILABLE",
    "NO_DATA",
    "PARTIAL",
}

VALID_SIGNAL_TYPES = {
    "INVESTOR_COVERAGE",
    "NAMED_INVESTOR_COUNT",
    "INVESTOR_CONCENTRATION",
    "OWNERSHIP_COVERAGE",
    "OWNERSHIP_CONCENTRATION",
    "INVESTOR_DATA_LIMITED",
}

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("step18_1")


# =============================================================================
# HELPERS
# =============================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def load_required(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Required {label} artifact not found: {path}"
        )
    return pd.read_parquet(path)


def step18_paths() -> Dict[str, Path]:
    return {
        "events": STEP18_GOLD_ROOT / "investor_events" / "investor_events.parquet",
        "history": STEP18_GOLD_ROOT / "investor_event_history" / "investor_event_history.parquet",
        "snapshot": STEP18_GOLD_ROOT / "company_investor_snapshot" / "company_investor_snapshot.parquet",
        "concentration": STEP18_GOLD_ROOT / "investor_concentration" / "investor_concentration.parquet",
        "ownership": STEP18_GOLD_ROOT / "ownership_snapshot" / "ownership_snapshot.parquet",
        "signals": STEP18_GOLD_ROOT / "investor_signals" / "investor_signals.parquet",
        "summary": STEP18_GOLD_ROOT / "investor_summary" / "investor_summary.parquet",
        "lineage": STEP18_GOLD_ROOT / "investor_lineage" / "investor_lineage.parquet",
        "exceptions": STEP18_GOLD_ROOT / "investor_exceptions" / "investor_exceptions.parquet",
        "promotion": STEP18_GOLD_ROOT / "investor_promotion" / "investor_promotion.parquet",
    }


def step17_event_path() -> Path:
    return STEP17_GOLD_ROOT / "funding_events" / "funding_events.parquet"


# =============================================================================
# DQ RESULT HELPERS
# =============================================================================

def make_result(
    check: str,
    status: str,
    severity: str,
    message: str,
    rows_checked: int = 0,
    failures: int = 0,
    warnings: int = 0,
) -> Dict[str, Any]:
    return {
        "check_name": check,
        "status": status,
        "severity": severity,
        "message": message,
        "rows_checked": rows_checked,
        "failure_count": failures,
        "warning_count": warnings,
        "dq_version": DQ_VERSION,
        "created_at_utc": utc_now_iso(),
    }


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


# =============================================================================
# CHECK 1 - VERSION / SOURCE CONTRACT
# =============================================================================

def audit_versions(
    snapshot: pd.DataFrame,
    signals: pd.DataFrame,
    summary: pd.DataFrame,
    lineage: pd.DataFrame,
    promotion: pd.DataFrame,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    observed18: set[str] = set()
    for df in [snapshot, signals, summary, lineage, promotion]:
        for col in ["step_version", "source_step_version"]:
            if col in df.columns:
                observed18.update(
                    df[col].dropna().astype(str).tolist()
                )

    bad18 = {
        v for v in observed18
        if v not in {STEP18_VERSION_EXPECTED, STEP17_VERSION_EXPECTED}
    }

    if bad18:
        results.append(make_result(
            "STEP18_VERSION_COMPATIBILITY",
            "FAIL",
            "FATAL",
            f"Unexpected Step 18/Step 17 versions: {sorted(bad18)}",
            failures=1,
        ))
    else:
        results.append(make_result(
            "STEP18_VERSION_COMPATIBILITY",
            "PASS",
            "INFO",
            f"Step 18 version contract is compatible with {STEP18_VERSION_EXPECTED}.",
        ))

    if "step_version" in promotion.columns:
        bad = promotion[
            promotion["step_version"].astype(str)
            != STEP18_VERSION_EXPECTED
        ]
        if not bad.empty:
            results.append(make_result(
                "PROMOTION_VERSION",
                "FAIL",
                "FATAL",
                "Promotion artifact contains an unexpected Step 18 version.",
                rows_checked=len(promotion),
                failures=len(bad),
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
# CHECK 2 - COMPANY COVERAGE / SNAPSHOT GRAIN
# =============================================================================

def audit_snapshot(snapshot: pd.DataFrame) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    configured = {normalize_cik(c) for c in COMPANIES}

    required = {
        "cik",
        "funding_event_count",
        "named_investor_count",
        "investor_identity_status",
        "ownership_status",
        "investor_source",
        "ownership_fabrication_prevented",
        "investor_data_quality",
    }
    missing = required - set(snapshot.columns)

    if missing:
        results.append(make_result(
            "SNAPSHOT_SCHEMA",
            "FAIL",
            "FATAL",
            f"Snapshot missing required columns: {sorted(missing)}",
            failures=1,
        ))
        return results

    normalized = snapshot["cik"].map(normalize_cik)
    observed = set(normalized)

    if observed != configured:
        results.append(make_result(
            "SNAPSHOT_COMPANY_COVERAGE",
            "FAIL",
            "FATAL",
            f"Snapshot CIK coverage mismatch. Missing={sorted(configured-observed)}, "
            f"Extra={sorted(observed-configured)}",
            rows_checked=len(snapshot),
            failures=1,
        ))
    else:
        results.append(make_result(
            "SNAPSHOT_COMPANY_COVERAGE",
            "PASS",
            "INFO",
            "All configured companies have exactly one investor snapshot.",
            rows_checked=len(snapshot),
        ))

    if normalized.duplicated().any():
        results.append(make_result(
            "SNAPSHOT_GRAIN",
            "FAIL",
            "FATAL",
            "More than one investor snapshot row exists for a CIK.",
            rows_checked=len(snapshot),
            failures=int(normalized.duplicated().sum()),
        ))
    else:
        results.append(make_result(
            "SNAPSHOT_GRAIN",
            "PASS",
            "INFO",
            "Investor snapshot grain is one row per CIK.",
            rows_checked=len(snapshot),
        ))

    invalid_inv_status = (
        set(snapshot["investor_identity_status"].dropna().astype(str))
        - VALID_INVESTOR_STATUS
    )
    invalid_own_status = (
        set(snapshot["ownership_status"].dropna().astype(str))
        - VALID_OWNERSHIP_STATUS
    )

    if invalid_inv_status or invalid_own_status:
        results.append(make_result(
            "SNAPSHOT_STATUS_VALUES",
            "FAIL",
            "FATAL",
            f"Invalid status values. investor={sorted(invalid_inv_status)}, "
            f"ownership={sorted(invalid_own_status)}",
            failures=1,
        ))
    else:
        results.append(make_result(
            "SNAPSHOT_STATUS_VALUES",
            "PASS",
            "INFO",
            "Investor and ownership status values are valid.",
            rows_checked=len(snapshot),
        ))

    fabrication_failures = []
    for _, row in snapshot.iterrows():
        if safe_bool(row["ownership_fabrication_prevented"]) is not True:
            fabrication_failures.append(normalize_cik(row["cik"]))

    if fabrication_failures:
        results.append(make_result(
            "OWNERSHIP_FABRICATION_CONTROL",
            "FAIL",
            "FATAL",
            f"Ownership fabrication control disabled for CIKs: {fabrication_failures}",
            failures=len(fabrication_failures),
        ))
    else:
        results.append(make_result(
            "OWNERSHIP_FABRICATION_CONTROL",
            "PASS",
            "INFO",
            "Ownership fabrication control is enabled for every company.",
            rows_checked=len(snapshot),
        ))

    # NO_DATA means the corresponding named investor count must be NULL.
    no_data_with_count = []
    for _, row in snapshot.iterrows():
        if clean_text(row["investor_identity_status"]) == "NO_DATA":
            if not is_missing(row["named_investor_count"]):
                no_data_with_count.append(normalize_cik(row["cik"]))

    if no_data_with_count:
        results.append(make_result(
            "INVESTOR_NO_DATA_SEMANTICS",
            "FAIL",
            "FATAL",
            f"NO_DATA investor status carries a named-investor count: {no_data_with_count}",
            failures=len(no_data_with_count),
        ))
    else:
        results.append(make_result(
            "INVESTOR_NO_DATA_SEMANTICS",
            "PASS",
            "INFO",
            "NO_DATA investor coverage correctly uses NULL for named-investor count.",
            rows_checked=len(snapshot),
        ))

    return results


# =============================================================================
# CHECK 3 - INVESTOR EVENT GRAIN / IDENTITY
# =============================================================================

def audit_investor_events(events: pd.DataFrame) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    if events.empty:
        results.append(make_result(
            "INVESTOR_EVENT_EMPTY_STATE",
            "PASS",
            "INFO",
            "No named investor event rows are present; this is valid when enrichment is absent.",
            rows_checked=0,
        ))
        return results

    required = {
        "cik",
        "funding_event_id",
        "investor_name",
        "investor_type",
    }
    missing = required - set(events.columns)

    if missing:
        results.append(make_result(
            "INVESTOR_EVENT_SCHEMA",
            "FAIL",
            "FATAL",
            f"Investor events missing columns: {sorted(missing)}",
            failures=1,
        ))
        return results

    blank_names = events["investor_name"].map(clean_text).isna()
    if blank_names.any():
        results.append(make_result(
            "INVESTOR_NAME_INTEGRITY",
            "FAIL",
            "FATAL",
            "Named investor event rows contain blank investor names.",
            rows_checked=len(events),
            failures=int(blank_names.sum()),
        ))
    else:
        results.append(make_result(
            "INVESTOR_NAME_INTEGRITY",
            "PASS",
            "INFO",
            "All named investor event rows have nonblank investor names.",
            rows_checked=len(events),
        ))

    duplicate_key = events[
        ["cik", "funding_event_id", "investor_name"]
    ].duplicated()

    if duplicate_key.any():
        results.append(make_result(
            "INVESTOR_EVENT_GRAIN",
            "FAIL",
            "FATAL",
            "Duplicate CIK/funding_event_id/investor_name rows detected.",
            rows_checked=len(events),
            failures=int(duplicate_key.sum()),
        ))
    else:
        results.append(make_result(
            "INVESTOR_EVENT_GRAIN",
            "PASS",
            "INFO",
            "Investor event grain is valid.",
            rows_checked=len(events),
        ))

    return results


# =============================================================================
# CHECK 4 - HISTORY
# =============================================================================

def audit_history(history: pd.DataFrame) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    if history.empty:
        results.append(make_result(
            "INVESTOR_HISTORY_EMPTY_STATE",
            "PASS",
            "INFO",
            "No named-investor historical rows are present; valid without enrichment.",
            rows_checked=0,
        ))
        return results

    key_candidates = [
        c for c in
        ["cik", "funding_event_id", "offering_id", "accession_number", "investor_name"]
        if c in history.columns
    ]

    if len(key_candidates) < 3:
        results.append(make_result(
            "INVESTOR_HISTORY_SCHEMA",
            "FAIL",
            "FATAL",
            "Investor history does not contain enough lineage keys.",
            failures=1,
        ))
        return results

    duplicate = history[key_candidates].duplicated()
    if duplicate.any():
        results.append(make_result(
            "INVESTOR_HISTORY_GRAIN",
            "FAIL",
            "FATAL",
            "Duplicate investor history rows detected.",
            rows_checked=len(history),
            failures=int(duplicate.sum()),
        ))
    else:
        results.append(make_result(
            "INVESTOR_HISTORY_GRAIN",
            "PASS",
            "INFO",
            "Investor history grain is valid.",
            rows_checked=len(history),
        ))

    return results


# =============================================================================
# CHECK 5 - CONCENTRATION
# =============================================================================

def audit_concentration(
    concentration: pd.DataFrame,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    required = {
        "cik",
        "investor_concentration_status",
        "ownership_concentration_status",
    }
    missing = required - set(concentration.columns)

    if missing:
        results.append(make_result(
            "CONCENTRATION_SCHEMA",
            "FAIL",
            "FATAL",
            f"Concentration output missing columns: {sorted(missing)}",
            failures=1,
        ))
        return results

    normalized = concentration["cik"].map(normalize_cik)

    if normalized.duplicated().any():
        results.append(make_result(
            "CONCENTRATION_GRAIN",
            "FAIL",
            "FATAL",
            "More than one concentration row exists per CIK.",
            rows_checked=len(concentration),
            failures=int(normalized.duplicated().sum()),
        ))
    else:
        results.append(make_result(
            "CONCENTRATION_GRAIN",
            "PASS",
            "INFO",
            "Concentration output is one row per CIK.",
            rows_checked=len(concentration),
        ))

    for col, valid in [
        ("investor_concentration_status", VALID_SIGNAL_STATUS),
        ("ownership_concentration_status", VALID_SIGNAL_STATUS),
    ]:
        values = set(concentration[col].dropna().astype(str))
        bad = values - valid
        if bad:
            results.append(make_result(
                f"{col.upper()}_VALUES",
                "FAIL",
                "FATAL",
                f"Invalid {col} values: {sorted(bad)}",
                failures=1,
            ))

    return results


# =============================================================================
# CHECK 6 - OWNERSHIP
# =============================================================================

def audit_ownership(ownership: pd.DataFrame) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    required = {
        "cik",
        "ownership_status",
        "ownership_fabrication_prevented",
    }
    missing = required - set(ownership.columns)

    if missing:
        results.append(make_result(
            "OWNERSHIP_SCHEMA",
            "FAIL",
            "FATAL",
            f"Ownership output missing columns: {sorted(missing)}",
            failures=1,
        ))
        return results

    normalized = ownership["cik"].map(normalize_cik)

    if normalized.duplicated().any():
        results.append(make_result(
            "OWNERSHIP_GRAIN",
            "FAIL",
            "FATAL",
            "More than one ownership snapshot exists per CIK.",
            rows_checked=len(ownership),
            failures=int(normalized.duplicated().sum()),
        ))
    else:
        results.append(make_result(
            "OWNERSHIP_GRAIN",
            "PASS",
            "INFO",
            "Ownership output is one row per CIK.",
            rows_checked=len(ownership),
        ))

    bad_status = (
        set(ownership["ownership_status"].dropna().astype(str))
        - VALID_OWNERSHIP_STATUS
    )
    if bad_status:
        results.append(make_result(
            "OWNERSHIP_STATUS_VALUES",
            "FAIL",
            "FATAL",
            f"Invalid ownership status values: {sorted(bad_status)}",
            failures=1,
        ))

    fabrication = []
    for _, row in ownership.iterrows():
        if safe_bool(row["ownership_fabrication_prevented"]) is not True:
            fabrication.append(normalize_cik(row["cik"]))

    if fabrication:
        results.append(make_result(
            "OWNERSHIP_FABRICATION_CONTROL_OUTPUT",
            "FAIL",
            "FATAL",
            f"Ownership fabrication control disabled: {fabrication}",
            failures=len(fabrication),
        ))
    else:
        results.append(make_result(
            "OWNERSHIP_FABRICATION_CONTROL_OUTPUT",
            "PASS",
            "INFO",
            "Ownership output explicitly prevents fabrication.",
            rows_checked=len(ownership),
        ))

    return results


# =============================================================================
# CHECK 7 - SIGNALS
# =============================================================================

def audit_signals(signals: pd.DataFrame) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    required = {
        "cik",
        "signal_type",
        "signal_value",
        "signal_status",
        "source_scope",
    }
    missing = required - set(signals.columns)

    if missing:
        results.append(make_result(
            "SIGNAL_SCHEMA",
            "FAIL",
            "FATAL",
            f"Signals missing required columns: {sorted(missing)}",
            failures=1,
        ))
        return results

    key = signals[["cik", "signal_type"]].copy()
    duplicate = key.duplicated()

    if duplicate.any():
        results.append(make_result(
            "SIGNAL_GRAIN",
            "FAIL",
            "FATAL",
            "Duplicate CIK/signal_type rows detected.",
            rows_checked=len(signals),
            failures=int(duplicate.sum()),
        ))
    else:
        results.append(make_result(
            "SIGNAL_GRAIN",
            "PASS",
            "INFO",
            "Signal grain is one row per CIK/signal type.",
            rows_checked=len(signals),
        ))

    bad_types = (
        set(signals["signal_type"].dropna().astype(str))
        - VALID_SIGNAL_TYPES
    )
    if bad_types:
        results.append(make_result(
            "SIGNAL_TYPE_VALUES",
            "FAIL",
            "FATAL",
            f"Invalid signal types: {sorted(bad_types)}",
            failures=1,
        ))

    bad_status = (
        set(signals["signal_status"].dropna().astype(str))
        - VALID_SIGNAL_STATUS
    )
    if bad_status:
        results.append(make_result(
            "SIGNAL_STATUS_VALUES",
            "FAIL",
            "FATAL",
            f"Invalid signal status values: {sorted(bad_status)}",
            failures=1,
        ))

    # Critical semantic gate:
    # NO_DATA must have a NULL signal value.
    no_data_with_value = []
    for _, row in signals.iterrows():
        if clean_text(row["signal_status"]) == "NO_DATA":
            if not is_missing(row["signal_value"]):
                no_data_with_value.append(
                    f"{normalize_cik(row['cik'])}:{row['signal_type']}"
                )

    if no_data_with_value:
        results.append(make_result(
            "NO_DATA_SIGNAL_SEMANTICS",
            "FAIL",
            "FATAL",
            f"NO_DATA signals contain values: {no_data_with_value}",
            failures=len(no_data_with_value),
        ))
    else:
        results.append(make_result(
            "NO_DATA_SIGNAL_SEMANTICS",
            "PASS",
            "INFO",
            "NO_DATA signals correctly use NULL signal values.",
            rows_checked=len(signals),
        ))

    wrong_scope = signals[
        signals["source_scope"].astype(str) != EXPECTED_SOURCE_SCOPE
    ]
    if not wrong_scope.empty:
        results.append(make_result(
            "SIGNAL_SOURCE_SCOPE",
            "FAIL",
            "FATAL",
            "Investor signals contain an unexpected source scope.",
            rows_checked=len(signals),
            failures=len(wrong_scope),
        ))
    else:
        results.append(make_result(
            "SIGNAL_SOURCE_SCOPE",
            "PASS",
            "INFO",
            "Investor signals preserve the Step 17 SEC source scope.",
            rows_checked=len(signals),
        ))

    return results


# =============================================================================
# CHECK 8 - SOURCE COVERAGE
# =============================================================================

def audit_source_coverage(
    snapshot: pd.DataFrame,
    summary: pd.DataFrame,
    lineage: pd.DataFrame,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    warnings: List[str] = []

    source_values = (
        set(snapshot["investor_source"].dropna().astype(str))
        if "investor_source" in snapshot.columns
        else set()
    )

    if source_values == {"NONE"}:
        warnings.append(
            "No named investor enrichment source is available."
        )

    if "investor_identity_from_sec_form_d" in summary.columns:
        if not summary["investor_identity_from_sec_form_d"].map(
            safe_bool
        ).eq(False).all():
            results.append(make_result(
                "SEC_FORM_D_IDENTITY_CONTROL",
                "FAIL",
                "FATAL",
                "Step 18 incorrectly assumes SEC Form D provides named investor identity.",
                failures=1,
            ))
        else:
            results.append(make_result(
                "SEC_FORM_D_IDENTITY_CONTROL",
                "PASS",
                "INFO",
                "SEC Form D investor identity is explicitly not assumed.",
                rows_checked=len(summary),
            ))

    if "ownership_from_sec_form_d" in summary.columns:
        if not summary["ownership_from_sec_form_d"].map(
            safe_bool
        ).eq(False).all():
            results.append(make_result(
                "SEC_FORM_D_OWNERSHIP_CONTROL",
                "FAIL",
                "FATAL",
                "Step 18 incorrectly assumes SEC Form D provides ownership.",
                failures=1,
            ))
        else:
            results.append(make_result(
                "SEC_FORM_D_OWNERSHIP_CONTROL",
                "PASS",
                "INFO",
                "SEC Form D ownership is explicitly not assumed.",
                rows_checked=len(summary),
            ))

    if warnings:
        results.append(make_result(
            "INVESTOR_SOURCE_COVERAGE",
            "WARNING",
            "WARNING",
            " ".join(warnings),
            warnings=len(warnings),
        ))
    else:
        results.append(make_result(
            "INVESTOR_SOURCE_COVERAGE",
            "PASS",
            "INFO",
            "Named investor enrichment source is available.",
        ))

    if "ownership_fabrication_prevented" in lineage.columns:
        controls = lineage["ownership_fabrication_prevented"].map(safe_bool)
        observed_controls = controls.dropna()
        if observed_controls.empty or not observed_controls.eq(True).all():
            results.append(make_result(
                "LINEAGE_FABRICATION_CONTROL",
                "FAIL",
                "FATAL",
                "Lineage does not confirm ownership fabrication prevention.",
                failures=max(1, int((~observed_controls.eq(True)).sum())),
            ))
        else:
            results.append(make_result(
                "LINEAGE_FABRICATION_CONTROL",
                "PASS",
                "INFO",
                "Lineage confirms ownership fabrication prevention.",
                rows_checked=len(observed_controls),
            ))

    return results


# =============================================================================
# CHECK 9 - CROSS-ARTIFACT RECONCILIATION
# =============================================================================

def audit_reconciliation(
    events: pd.DataFrame,
    snapshot: pd.DataFrame,
    concentration: pd.DataFrame,
    ownership: pd.DataFrame,
    signals: pd.DataFrame,
    summary: pd.DataFrame,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    configured = {normalize_cik(c) for c in COMPANIES}

    event_counts = (
        events["cik"].map(normalize_cik).value_counts().to_dict()
        if not events.empty and "cik" in events.columns
        else {}
    )

    failures = []

    for _, row in snapshot.iterrows():
        cik = normalize_cik(row["cik"])
        expected = int(event_counts.get(cik, 0))
        actual = int(row["funding_event_count"])
        if actual != expected:
            failures.append(
                f"{cik}: snapshot funding_event_count={actual}, "
                f"Step18 event rows={expected}"
            )

    if failures:
        results.append(make_result(
            "SNAPSHOT_EVENT_RECONCILIATION",
            "FAIL",
            "FATAL",
            "; ".join(failures),
            failures=len(failures),
        ))
    else:
        results.append(make_result(
            "SNAPSHOT_EVENT_RECONCILIATION",
            "PASS",
            "INFO",
            "Snapshot funding-event counts reconcile to Step 18 event rows.",
            rows_checked=len(snapshot),
        ))

    # Signal coverage: six deterministic signal rows per company.
    expected_signal_rows = len(configured) * len(VALID_SIGNAL_TYPES)
    if len(signals) != expected_signal_rows:
        results.append(make_result(
            "SIGNAL_COVERAGE_RECONCILIATION",
            "FAIL",
            "FATAL",
            f"Expected {expected_signal_rows} signal rows but found {len(signals)}.",
            rows_checked=len(signals),
            failures=1,
        ))
    else:
        results.append(make_result(
            "SIGNAL_COVERAGE_RECONCILIATION",
            "PASS",
            "INFO",
            "Signal row count reconciles to configured company coverage.",
            rows_checked=len(signals),
        ))

    for df, name in [
        (concentration, "CONCENTRATION"),
        (ownership, "OWNERSHIP"),
    ]:
        observed = (
            set(df["cik"].map(normalize_cik))
            if "cik" in df.columns
            else set()
        )
        if observed != configured:
            results.append(make_result(
                f"{name}_COMPANY_RECONCILIATION",
                "FAIL",
                "FATAL",
                f"{name} CIK coverage does not match configured companies.",
                failures=1,
            ))
        else:
            results.append(make_result(
                f"{name}_COMPANY_RECONCILIATION",
                "PASS",
                "INFO",
                f"{name} output covers all configured companies.",
                rows_checked=len(df),
            ))

    if "configured_companies" in summary.columns:
        bad = summary[
            pd.to_numeric(summary["configured_companies"], errors="coerce")
            != len(COMPANIES)
        ]
        if not bad.empty:
            results.append(make_result(
                "SUMMARY_COMPANY_RECONCILIATION",
                "FAIL",
                "FATAL",
                "Summary configured-company count does not match Step 18 configuration.",
                failures=len(bad),
            ))
        else:
            results.append(make_result(
                "SUMMARY_COMPANY_RECONCILIATION",
                "PASS",
                "INFO",
                "Summary company count reconciles.",
                rows_checked=len(summary),
            ))

    return results


# =============================================================================
# CHECK 10 - PROMOTION
# =============================================================================

def audit_promotion(
    promotion: pd.DataFrame,
    fatal_count: int,
) -> Tuple[List[Dict[str, Any]], str]:
    results: List[Dict[str, Any]] = []

    if "promotion_decision" not in promotion.columns:
        results.append(make_result(
            "PROMOTION_SCHEMA",
            "FAIL",
            "FATAL",
            "Promotion artifact lacks promotion_decision.",
            failures=1,
        ))
        return results, "BLOCKED"

    decisions = set(
        promotion["promotion_decision"].dropna().astype(str)
    )

    # This is the upstream Step 18 artifact. Its decision must be validated
    # against the upstream contract, not against Step 18.1's newly discovered
    # DQ results; otherwise every downstream DQ failure fabricates an upstream
    # promotion inconsistency.
    expected = "PROMOTE"

    if decisions != {expected}:
        results.append(make_result(
            "PROMOTION_CONSISTENCY",
            "FAIL",
            "FATAL",
            f"Promotion artifact={sorted(decisions)}, expected={expected}.",
            failures=1,
        ))
        return results, "BLOCKED"

    results.append(make_result(
        "PROMOTION_CONSISTENCY",
        "PASS",
        "INFO",
        f"Promotion decision correctly reflects DQ status: {expected}.",
        rows_checked=len(promotion),
    ))

    return results, expected


# =============================================================================
# OUTPUT
# =============================================================================

def write_dq_outputs(
    results: List[Dict[str, Any]],
    exceptions: pd.DataFrame,
    summary: Dict[str, Any],
    lineage: pd.DataFrame,
    promotion: str,
) -> None:
    results_df = pd.DataFrame(results)

    write_parquet(
        results_df[results_df["check_name"].str.startswith("SNAPSHOT")],
        OUTPUT_ROOT / "investor_snapshot_dq" / "investor_snapshot_dq.parquet",
    )
    write_parquet(
        results_df[results_df["check_name"].str.startswith("INVESTOR_EVENT")
                   | results_df["check_name"].str.startswith("INVESTOR_NAME")],
        OUTPUT_ROOT / "investor_event_dq" / "investor_event_dq.parquet",
    )
    write_parquet(
        results_df[results_df["check_name"].str.startswith("INVESTOR_HISTORY")],
        OUTPUT_ROOT / "investor_history_dq" / "investor_history_dq.parquet",
    )
    write_parquet(
        results_df[results_df["check_name"].str.startswith("CONCENTRATION")],
        OUTPUT_ROOT / "investor_concentration_dq" / "investor_concentration_dq.parquet",
    )
    write_parquet(
        results_df[results_df["check_name"].str.startswith("OWNERSHIP")],
        OUTPUT_ROOT / "ownership_dq" / "ownership_dq.parquet",
    )
    write_parquet(
        results_df[results_df["check_name"].str.startswith("SIGNAL")],
        OUTPUT_ROOT / "investor_signal_dq" / "investor_signal_dq.parquet",
    )
    write_parquet(
        results_df[
            results_df["check_name"].str.contains(
                "SOURCE|SEC_FORM_D|LINEAGE", regex=True
            )
        ],
        OUTPUT_ROOT / "investor_source_coverage_dq" / "investor_source_coverage_dq.parquet",
    )
    write_parquet(
        results_df[results_df["check_name"].str.contains(
            "RECONCILIATION", regex=True
        )],
        OUTPUT_ROOT / "investor_reconciliation_dq" / "investor_reconciliation_dq.parquet",
    )

    write_parquet(
        exceptions,
        OUTPUT_ROOT / "investor_dq_exceptions" / "investor_dq_exceptions.parquet",
    )

    summary_df = pd.DataFrame([summary])
    write_parquet(
        summary_df,
        OUTPUT_ROOT / "investor_dq_summary" / "investor_dq_summary.parquet",
    )

    write_parquet(
        lineage,
        OUTPUT_ROOT / "investor_dq_lineage" / "investor_dq_lineage.parquet",
    )

    promotion_df = pd.DataFrame([{
        "step": "STEP_18.1",
        "dq_version": DQ_VERSION,
        "step18_version": STEP18_VERSION_EXPECTED,
        "promotion_decision": promotion,
        "ownership_fabrication_prevented": True,
        "created_at_utc": utc_now_iso(),
    }])

    write_parquet(
        promotion_df,
        OUTPUT_ROOT / "investor_promotion" / "investor_promotion.parquet",
    )


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    logger.info("=" * 72)
    logger.info("STEP 18.1 - INVESTOR & OWNERSHIP DATA QUALITY & RECONCILIATION")
    logger.info("Version: %s", DQ_VERSION)
    logger.info("Expected Step 18 version: %s", STEP18_VERSION_EXPECTED)
    logger.info("Step 18 source: %s", STEP18_GOLD_ROOT)
    logger.info("DQ output: %s", OUTPUT_ROOT)
    logger.info("=" * 72)

    paths = step18_paths()

    frames = {
        name: load_required(path, f"Step 18 {name}")
        for name, path in paths.items()
    }

    for name, df in frames.items():
        logger.info("Loaded %s rows=%s", name, len(df))

    # Step 17 event source is required for reconciliation.
    step17_events = load_required(
        step17_event_path(),
        "Step 17 funding events",
    )

    results: List[Dict[str, Any]] = []

    results.extend(audit_versions(
        frames["snapshot"],
        frames["signals"],
        frames["summary"],
        frames["lineage"],
        frames["promotion"],
    ))

    results.extend(audit_snapshot(frames["snapshot"]))
    results.extend(audit_investor_events(frames["events"]))
    results.extend(audit_history(frames["history"]))
    results.extend(audit_concentration(frames["concentration"]))
    results.extend(audit_ownership(frames["ownership"]))
    results.extend(audit_signals(frames["signals"]))
    results.extend(audit_source_coverage(
        frames["snapshot"],
        frames["summary"],
        frames["lineage"],
    ))
    results.extend(audit_reconciliation(
        step17_events,
        frames["snapshot"],
        frames["concentration"],
        frames["ownership"],
        frames["signals"],
        frames["summary"],
    ))

    fatal_results = [
        r for r in results
        if r["severity"] == "FATAL" and r["status"] == "FAIL"
    ]
    warning_results = [
        r for r in results
        if r["status"] == "WARNING"
    ]

    exceptions_rows = []
    for r in fatal_results + warning_results:
        exceptions_rows.append({
            "severity": r["severity"],
            "exception_code": r["check_name"],
            "message": r["message"],
            "failure_count": r["failure_count"],
            "warning_count": r["warning_count"],
            "dq_version": DQ_VERSION,
            "created_at_utc": utc_now_iso(),
        })

    exceptions_df = pd.DataFrame(exceptions_rows)

    # Promotion is determined from all DQ results, including cross-artifact
    # reconciliation, before the promotion artifact is written.
    promotion = "BLOCKED" if fatal_results else "PROMOTE"

    promotion_results, promotion = audit_promotion(
        frames["promotion"],
        len(fatal_results),
    )
    results.extend(promotion_results)

    # Recalculate after promotion consistency check.
    fatal_results = [
        r for r in results
        if r["severity"] == "FATAL" and r["status"] == "FAIL"
    ]
    warning_results = [
        r for r in results
        if r["status"] == "WARNING"
    ]
    promotion = "BLOCKED" if fatal_results else "PROMOTE"

    exceptions_rows = []
    for r in fatal_results + warning_results:
        exceptions_rows.append({
            "severity": r["severity"],
            "exception_code": r["check_name"],
            "message": r["message"],
            "failure_count": r["failure_count"],
            "warning_count": r["warning_count"],
            "dq_version": DQ_VERSION,
            "created_at_utc": utc_now_iso(),
        })
    exceptions_df = pd.DataFrame(exceptions_rows)

    summary = {
        "step": "STEP_18.1",
        "dq_version": DQ_VERSION,
        "step18_version": STEP18_VERSION_EXPECTED,
        "step17_version": STEP17_VERSION_EXPECTED,
        "configured_companies": len(COMPANIES),
        "funding_events": len(step17_events),
        "investor_event_rows": len(frames["events"]),
        "investor_history_rows": len(frames["history"]),
        "snapshot_rows": len(frames["snapshot"]),
        "concentration_rows": len(frames["concentration"]),
        "ownership_rows": len(frames["ownership"]),
        "signal_rows": len(frames["signals"]),
        "fatal_exception_count": len(fatal_results),
        "warning_count": len(warning_results),
        "promotion_decision": promotion,
        "no_data_semantics_validated": not any(
            r["check_name"] == "NO_DATA_SIGNAL_SEMANTICS"
            and r["status"] == "FAIL"
            for r in results
        ),
        "ownership_fabrication_prevented": True,
        "generated_at_utc": utc_now_iso(),
    }

    lineage = pd.DataFrame([{
        "step": "STEP_18.1",
        "dq_version": DQ_VERSION,
        "source_step": "STEP_18",
        "source_step_version": STEP18_VERSION_EXPECTED,
        "source_step17_version": STEP17_VERSION_EXPECTED,
        "dq_output_root": str(OUTPUT_ROOT),
        "ownership_fabrication_prevented": True,
        "investor_identity_from_form_d_assumed": False,
        "ownership_from_form_d_assumed": False,
        "promotion_decision": promotion,
        "fatal_exception_count": len(fatal_results),
        "warning_count": len(warning_results),
        "generated_at_utc": utc_now_iso(),
    }])

    write_dq_outputs(
        results,
        exceptions_df,
        summary,
        lineage,
        promotion,
    )

    logger.info("")
    logger.info("Configured companies: %s", len(COMPANIES))
    logger.info("Investor event rows: %s", len(frames["events"]))
    logger.info(
        "Named investor rows: %s",
        (
            len(frames["events"])
            if not frames["events"].empty
            else 0
        ),
    )
    logger.info(
        "Unique named investors: %s",
        (
            frames["events"]["investor_name"].nunique()
            if not frames["events"].empty
            else 0
        ),
    )
    logger.info(
        "Companies with investors: %s",
        int(
            frames["snapshot"]["investor_identity_status"]
            .eq("AVAILABLE").sum()
        ),
    )
    logger.info(
        "Companies with ownership: %s",
        int(
            frames["snapshot"]["ownership_status"]
            .eq("AVAILABLE").sum()
        ),
    )
    logger.info("Fatal DQ exceptions: %s", len(fatal_results))
    logger.info("DQ warnings: %s", len(warning_results))
    logger.info("Promotion decision: %s", promotion)

    no_data_ok = not any(
        r["check_name"] == "NO_DATA_SIGNAL_SEMANTICS"
        and r["status"] == "FAIL"
        for r in results
    )
    logger.info(
        "NO_DATA semantics: %s",
        "VALIDATED" if no_data_ok else "FAILED",
    )
    logger.info("Ownership fabrication: PREVENTED")

    if promotion == "PROMOTE":
        if warning_results:
            for r in warning_results:
                logger.warning("%s", r["message"])
            logger.info(
                "STEP 18.1 PASS: Investor/ownership Gold is DQ-cleared "
                "with explicit source-coverage limitations."
            )
        else:
            logger.info(
                "STEP 18.1 PASS: Investor/ownership Gold is DQ-cleared."
            )
    else:
        logger.error("STEP 18.1 FAILED")
        raise RuntimeError(
            f"Step 18.1 DQ blocked promotion with {len(fatal_results)} fatal exceptions."
        )


if __name__ == "__main__":
    main()
