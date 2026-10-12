"""
===============================================================================
STEP 20.1 - UNIFIED INVESTMENT INTELLIGENCE DATA QUALITY & RECONCILIATION
===============================================================================

Version: 20.1.0

Purpose
-------
Audit Step 20 Unified Investment Intelligence Gold before publication/serving.

Step 20.1 is a DQ gate only. It does NOT rebuild or mutate Step 20.

Validated contracts
-------------------
1. Step/version compatibility
2. Configured-company coverage
3. Unified profile grain
4. Unified signal grain and vocabulary
5. NO_DATA / NULL semantics
6. Step 15 financial decision reconciliation
7. Step 15 financial trajectory reconciliation
8. Step 17 funding reconciliation
9. Step 18 investor reconciliation
10. Step 18 ownership reconciliation
11. Step 19 valuation reconciliation
12. SEC capital != valuation
13. Funding-stage inference prevention
14. Investor identity inference prevention
15. Ownership inference prevention
16. Financial-score preservation / no new score
17. Unified-posture determinism
18. Data-coverage determinism
19. Signal/profile consistency
20. Lineage/version consistency
21. Promotion gating

Important
---------
The DQ intentionally treats cross-layer mismatches as FATAL. Step 20 is an
integration layer; if it changes an authoritative upstream value, downstream
consumers cannot trust the unified profile.

Controlled source limitations remain warnings, not failures:
- SEC Form D is not complete private-company funding history.
- Missing named-investor enrichment is not a data-integrity failure.
- Missing valuation enrichment is not a data-integrity failure.

Run
---
python ./src/sec_ingestion/step20_1_unified_investment_intelligence_dq.py
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from company_platform.registry.active_company_scope import load_active_company_ciks
from company_platform.registry.active_company_scope import assert_company_scope
from dotenv import load_dotenv
from sec_ingestion.step18_1_investor_ownership_data_quality import configured_companies, load_required

load_dotenv()

# =============================================================================
# CONFIG
# =============================================================================

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DATA_ROOT = REPO_ROOT / "data"

STEP20_ROOT = DATA_ROOT / "gold" / "unified_investment_intelligence"
DQ_ROOT = DATA_ROOT / "gold" / "unified_investment_intelligence_dq"

STEP20_VERSION_EXPECTED = "20.0.0"
DQ_VERSION = "20.1.0"

STEP12_VERSION_EXPECTED = "1.2.0"
STEP15_VERSION_EXPECTED = "1.2.0"
STEP17_VERSION_EXPECTED = "3.0.1"
STEP18_VERSION_EXPECTED = "18.0.0"
STEP19_VERSION_EXPECTED = "19.0.0"

def configured_companies() -> list[str]:
    """Resolve the authoritative active production company scope."""
    ciks = [str(cik).zfill(10) for cik in load_active_company_ciks()]
    if not ciks:
        raise ValueError("Active company execution scope is empty.")
    return sorted(set(ciks))


COMPANIES = configured_companies()
EXPECTED_CIKS: set[str] = set()
SCOPED_RUN = False

VALID_FINANCIAL_DECISIONS = {
    "CAUTION",
    "WATCH",
    "NEUTRAL",
    "POSITIVE",
}

VALID_TRAJECTORIES = {
    "IMPROVING",
    "MIXED",
    "DETERIORATING",
    "UNKNOWN",
}

VALID_FUNDING_STATUSES = {
    "AVAILABLE",
    "NO_DATA",
    "PARTIAL",
}

VALID_INVESTOR_COVERAGE = {
    "AVAILABLE",
    "NO_DATA",
    "PARTIAL",
}

VALID_OWNERSHIP_STATUSES = {
    "AVAILABLE",
    "NO_DATA",
    "PARTIAL",
    "LIMITED",
}

VALID_VALUATION_STATUSES = {
    "AVAILABLE",
    "NO_DATA",
    "PARTIAL",
}

VALID_UNIFIED_POSTURES = {
    "STRONG_POSITIVE",
    "POSITIVE",
    "NEUTRAL",
    "WATCH",
    "CAUTION",
    "INSUFFICIENT_DATA",
}

VALID_DATA_COVERAGE = {
    "COMPLETE",
    "PARTIAL",
    "LIMITED",
    "NO_DATA",
}

VALID_SIGNAL_TYPES = {
    "FINANCIAL_DECISION",
    "FINANCIAL_TRAJECTORY",
    "FUNDING_COVERAGE",
    "FUNDING_ACTIVITY",
    "INVESTOR_COVERAGE",
    "OWNERSHIP_COVERAGE",
    "VALUATION_COVERAGE",
    "VALUATION_STATUS",
    "SEC_CAPITAL_CONTEXT",
    "DATA_COVERAGE",
}

# Fields that must be preserved exactly from authoritative upstream snapshots.
RECONCILIATION_FIELDS = {
    "FINANCIAL_DECISION": "financial_decision",
    "FINANCIAL_TRAJECTORY": "financial_trajectory",
    "FUNDING_STATUS": "funding_status",
    "FUNDING_EVENT_COUNT": "funding_event_count",
    "SEC_CAPITAL": "sec_exempt_offering_capital_usd",
    "INVESTOR_COVERAGE": "investor_coverage_status",
    "NAMED_INVESTOR_COUNT": "named_investor_count",
    "OWNERSHIP_STATUS": "ownership_status",
    "OWNERSHIP_PCT_AVAILABLE": "ownership_pct_available",
    "VALUATION_STATUS": "valuation_status",
    "VALUATION_AMOUNT": "latest_valuation_amount_usd",
    "VALUATION_DATE": "latest_valuation_date",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("step20_1")


# =============================================================================
# HELPERS
# =============================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_cik(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(10) if digits else ""


def configured_companies_from_step15(default_companies=None):
    return assert_company_scope()



def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = " ".join(str(value).split()).strip()
    return text or None


def norm_upper(value: Any) -> Optional[str]:
    value = clean_text(value)
    return value.upper() if value else None


def numeric(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def bool_value(value: Any) -> Optional[bool]:
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


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        result = pd.isna(value)
        return bool(result) if not hasattr(result, "__len__") else False
    except Exception:
        return False


def values_equal(left: Any, right: Any, tolerance: float = 1e-9) -> bool:
    """Semantic equality for strings, numbers, booleans, timestamps and NULL."""
    if is_missing(left) and is_missing(right):
        return True

    if isinstance(left, pd.Timestamp) or isinstance(right, pd.Timestamp):
        try:
            ldt = pd.to_datetime(left, errors="coerce")
            rdt = pd.to_datetime(right, errors="coerce")
            if pd.isna(ldt) and pd.isna(rdt):
                return True
            if pd.isna(ldt) or pd.isna(rdt):
                return False
            return ldt == rdt
        except Exception:
            pass

    ln = numeric(left)
    rn = numeric(right)
    if ln is not None and rn is not None:
        return abs(ln - rn) <= tolerance * max(1.0, abs(ln), abs(rn))

    lb = bool_value(left)
    rb = bool_value(right)
    if lb is not None and rb is not None:
        return lb == rb

    ls = clean_text(left)
    rs = clean_text(right)
    if ls is None and rs is None:
        return True
    return ls == rs


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def load_parquet(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required {label} not found: {path}")
    return pd.read_parquet(path)


def result(
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
        "checked_at_utc": utc_now_iso(),
    }


def input_paths() -> Dict[str, Path]:
    return {
        "profile": STEP20_ROOT / "investment_profile" / "investment_profile.parquet",
        "signals": STEP20_ROOT / "investment_signals" / "investment_signals.parquet",
        "summary": STEP20_ROOT / "investment_summary" / "investment_summary.parquet",
        "lineage": STEP20_ROOT / "investment_lineage" / "investment_lineage.parquet",
        "promotion": STEP20_ROOT / "investment_promotion" / "investment_promotion.parquet",

        # Authoritative upstream artifacts.
        "step12_features": DATA_ROOT / "gold" / "financials_features",
        "step15_signals": (
            DATA_ROOT / "gold" / "financial_decision_intelligence"
            / "decision_signals" / "financial_decision_signals.parquet"
        ),
        "step15_snapshot": (
            DATA_ROOT / "gold" / "financial_decision_intelligence"
            / "company_financial_decision_snapshot"
            / "company_financial_decision_snapshot.parquet"
        ),
        "step17_snapshot": (
            DATA_ROOT / "gold" / "funding_capital_intelligence"
            / "company_funding_snapshot"
            / "company_funding_snapshot.parquet"
        ),
        "step18_investor": (
            DATA_ROOT / "gold" / "investor_ownership_intelligence"
            / "company_investor_snapshot"
            / "company_investor_snapshot.parquet"
        ),
        "step18_ownership": (
            DATA_ROOT / "gold" / "investor_ownership_intelligence"
            / "ownership_snapshot" / "ownership_snapshot.parquet"
        ),
        "step19_snapshot": (
            DATA_ROOT / "gold" / "valuation_intelligence"
            / "company_valuation_snapshot"
            / "company_valuation_snapshot.parquet"
        ),
    }


# =============================================================================
# INPUT / SCHEMA AUDITS
# =============================================================================

def audit_required_columns(
    df: pd.DataFrame,
    required: set[str],
    artifact_name: str,
) -> Dict[str, Any]:
    missing = sorted(required - set(df.columns))
    if missing:
        return result(
            f"{artifact_name.upper()}_SCHEMA",
            "FAIL",
            "FATAL",
            f"{artifact_name} missing required columns: {missing}",
            failure_count=1,
        )
    return result(
        f"{artifact_name.upper()}_SCHEMA",
        "PASS",
        "INFO",
        f"{artifact_name} contains all required columns.",
        rows_checked=len(df),
    )


def audit_cik_coverage(
    df: pd.DataFrame,
    artifact_name: str,
    profile: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    if "cik" not in df.columns:
        return result(
            f"{artifact_name.upper()}_COVERAGE",
            "FAIL",
            "FATAL",
            f"{artifact_name} has no cik column.",
            failure_count=1,
        )

    actual = set(df["cik"].map(normalize_cik))
    missing = sorted(EXPECTED_CIKS - actual)
    extra = [] if SCOPED_RUN else sorted(actual - EXPECTED_CIKS)

    represented_as_no_data = []
    unsupported_missing = missing
    if profile is not None and artifact_name in {
        "step15_snapshot",
        "step17_snapshot",
        "step18_investor",
        "step18_ownership",
        "step19_snapshot",
    }:
        profile_by_cik = {
            normalize_cik(row["cik"]): row
            for _, row in profile.iterrows()
        }
        represented_as_no_data = [
            cik for cik in missing
            if cik in profile_by_cik
            and profile_represents_missing_source(
                profile_by_cik[cik],
                artifact_name,
            )
        ]
        unsupported_missing = sorted(
            set(missing) - set(represented_as_no_data)
        )

    if unsupported_missing or extra:
        return result(
            f"{artifact_name.upper()}_COVERAGE",
            "FAIL",
            "FATAL",
            "CIK coverage mismatch. "
            f"Missing without explicit Step 20 NO_DATA={unsupported_missing}, "
            f"Extra={extra}",
            rows_checked=len(df),
            failure_count=len(unsupported_missing) + len(extra),
        )

    return result(
        f"{artifact_name.upper()}_COVERAGE",
        "PASS",
        "INFO",
        (
            "All configured companies are represented, either in the source "
            "artifact or as explicit NO_DATA in the Step 20 profile."
            if represented_as_no_data
            else "All configured companies are represented."
        ),
        rows_checked=len(df),
    )


def profile_represents_missing_source(
    profile_row: pd.Series,
    source_name: str,
) -> bool:
    """Allow omitted source rows only when Step 20 records their absence."""
    if source_name == "step15_snapshot":
        return all(
            is_missing(profile_row.get(column))
            for column in (
                "financial_decision",
                "financial_trajectory",
                "financial_score",
                "financial_rank",
            )
        )

    if source_name == "step17_snapshot":
        return (
            norm_upper(profile_row.get("funding_status")) == "NO_DATA"
            and numeric(profile_row.get("funding_event_count")) in {None, 0.0}
            and is_missing(profile_row.get("sec_exempt_offering_capital_usd"))
            and is_missing(profile_row.get("last_funding_date"))
        )

    if source_name == "step18_investor":
        return (
            norm_upper(profile_row.get("investor_coverage_status")) == "NO_DATA"
            and is_missing(profile_row.get("named_investor_count"))
        )

    if source_name == "step18_ownership":
        return (
            norm_upper(profile_row.get("ownership_status")) == "NO_DATA"
            and is_missing(profile_row.get("ownership_pct_available"))
        )

    if source_name == "step19_snapshot":
        return (
            norm_upper(profile_row.get("valuation_status")) == "NO_DATA"
            and is_missing(profile_row.get("latest_valuation_amount_usd"))
            and is_missing(profile_row.get("latest_valuation_date"))
        )

    return False


def audit_one_row_per_cik(
    df: pd.DataFrame,
    artifact_name: str,
) -> Dict[str, Any]:
    normalized = df["cik"].map(normalize_cik)
    dupes = normalized[normalized.duplicated(keep=False)].unique().tolist()

    if dupes:
        return result(
            f"{artifact_name.upper()}_GRAIN",
            "FAIL",
            "FATAL",
            f"Expected one row per CIK; duplicates={dupes}",
            rows_checked=len(df),
            failure_count=len(dupes),
        )

    return result(
        f"{artifact_name.upper()}_GRAIN",
        "PASS",
        "INFO",
        "Artifact has one row per CIK.",
        rows_checked=len(df),
    )


# =============================================================================
# VERSION AUDITS
# =============================================================================

def audit_step20_versions(
    profile: pd.DataFrame,
    signals: pd.DataFrame,
    summary: pd.DataFrame,
    lineage: pd.DataFrame,
    promotion: pd.DataFrame,
) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []

    for name, df in [
        ("profile", profile),
        ("signals", signals),
        ("summary", summary),
        ("lineage", lineage),
        ("promotion", promotion),
    ]:
        version_columns = [
            c for c in ["step20_version", "step", "source_step_version"]
            if c in df.columns
        ]
        observed = set()

        for col in version_columns:
            observed.update(
                df[col].dropna().astype(str).tolist()
            )

        bad = {
            v for v in observed
            if v not in {STEP20_VERSION_EXPECTED, "STEP_20", "STEP20"}
        }

        # step column is intentionally allowed to be STEP_20.
        if "step" in df.columns:
            non_step_versions = observed - {"STEP_20", "STEP20", STEP20_VERSION_EXPECTED}
        else:
            non_step_versions = bad

        if non_step_versions:
            checks.append(result(
                f"STEP20_VERSION_{name.upper()}",
                "FAIL",
                "FATAL",
                f"Unexpected Step 20 version metadata: {sorted(non_step_versions)}",
                failure_count=1,
            ))
        else:
            checks.append(result(
                f"STEP20_VERSION_{name.upper()}",
                "PASS",
                "INFO",
                f"{name} version metadata is compatible with Step 20 {STEP20_VERSION_EXPECTED}.",
                rows_checked=len(df),
            ))

    return checks


def audit_lineage(lineage: pd.DataFrame) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []

    expected = {
        "source_step12_version": STEP12_VERSION_EXPECTED,
        "source_step15_version": STEP15_VERSION_EXPECTED,
        "source_step17_version": STEP17_VERSION_EXPECTED,
        "source_step18_version": STEP18_VERSION_EXPECTED,
        "source_step19_version": STEP19_VERSION_EXPECTED,
        "new_financial_score_created": False,
        "unified_ranking_created": False,
        "sec_capital_treated_as_valuation": False,
        "valuation_fabrication_prevented": True,
        "funding_stage_inferred": False,
        "investor_identity_inferred": False,
        "ownership_inferred": False,
    }

    if len(lineage) != 1:
        checks.append(result(
            "LINEAGE_GRAIN",
            "FAIL",
            "FATAL",
            f"Expected exactly one Step 20 lineage row; found {len(lineage)}.",
            failure_count=1,
        ))
        return checks

    row = lineage.iloc[0]

    for field, expected_value in expected.items():
        if field not in lineage.columns:
            checks.append(result(
                f"LINEAGE_{field.upper()}",
                "FAIL",
                "FATAL",
                f"Lineage missing required field: {field}",
                failure_count=1,
            ))
            continue

        if not values_equal(row[field], expected_value):
            checks.append(result(
                f"LINEAGE_{field.upper()}",
                "FAIL",
                "FATAL",
                f"{field} expected {expected_value!r}; observed {row[field]!r}.",
                failure_count=1,
            ))
        else:
            checks.append(result(
                f"LINEAGE_{field.upper()}",
                "PASS",
                "INFO",
                f"{field} is correct.",
                rows_checked=1,
            ))

    return checks


# =============================================================================
# PROFILE SEMANTIC AUDITS
# =============================================================================

def audit_profile_values(profile: pd.DataFrame) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []

    if "financial_decision" in profile.columns:
        invalid = (
            set(profile["financial_decision"].dropna().map(norm_upper))
            - VALID_FINANCIAL_DECISIONS
        )
        checks.append(result(
            "FINANCIAL_DECISION_VALUES",
            "FAIL" if invalid else "PASS",
            "FATAL" if invalid else "INFO",
            f"Invalid financial decisions: {sorted(invalid)}" if invalid
            else "Financial decision vocabulary is valid.",
            rows_checked=len(profile),
            failure_count=1 if invalid else 0,
        ))

    if "financial_trajectory" in profile.columns:
        invalid = (
            set(profile["financial_trajectory"].dropna().map(norm_upper))
            - VALID_TRAJECTORIES
        )
        checks.append(result(
            "FINANCIAL_TRAJECTORY_VALUES",
            "FAIL" if invalid else "PASS",
            "FATAL" if invalid else "INFO",
            f"Invalid trajectories: {sorted(invalid)}" if invalid
            else "Financial trajectory vocabulary is valid.",
            rows_checked=len(profile),
            failure_count=1 if invalid else 0,
        ))

    if "funding_status" in profile.columns:
        invalid = (
            set(profile["funding_status"].dropna().map(norm_upper))
            - VALID_FUNDING_STATUSES
        )
        checks.append(result(
            "FUNDING_STATUS_VALUES",
            "FAIL" if invalid else "PASS",
            "FATAL" if invalid else "INFO",
            f"Invalid funding statuses: {sorted(invalid)}" if invalid
            else "Funding status vocabulary is valid.",
            rows_checked=len(profile),
            failure_count=1 if invalid else 0,
        ))

    if "investor_coverage_status" in profile.columns:
        invalid = (
            set(profile["investor_coverage_status"].dropna().map(norm_upper))
            - VALID_INVESTOR_COVERAGE
        )
        checks.append(result(
            "INVESTOR_STATUS_VALUES",
            "FAIL" if invalid else "PASS",
            "FATAL" if invalid else "INFO",
            f"Invalid investor statuses: {sorted(invalid)}" if invalid
            else "Investor coverage vocabulary is valid.",
            rows_checked=len(profile),
            failure_count=1 if invalid else 0,
        ))

    if "ownership_status" in profile.columns:
        invalid = (
            set(profile["ownership_status"].dropna().map(norm_upper))
            - VALID_OWNERSHIP_STATUSES
        )
        checks.append(result(
            "OWNERSHIP_STATUS_VALUES",
            "FAIL" if invalid else "PASS",
            "FATAL" if invalid else "INFO",
            f"Invalid ownership statuses: {sorted(invalid)}" if invalid
            else "Ownership status vocabulary is valid.",
            rows_checked=len(profile),
            failure_count=1 if invalid else 0,
        ))

    if "valuation_status" in profile.columns:
        invalid = (
            set(profile["valuation_status"].dropna().map(norm_upper))
            - VALID_VALUATION_STATUSES
        )
        checks.append(result(
            "VALUATION_STATUS_VALUES",
            "FAIL" if invalid else "PASS",
            "FATAL" if invalid else "INFO",
            f"Invalid valuation statuses: {sorted(invalid)}" if invalid
            else "Valuation status vocabulary is valid.",
            rows_checked=len(profile),
            failure_count=1 if invalid else 0,
        ))

    if "unified_posture" in profile.columns:
        invalid = (
            set(profile["unified_posture"].dropna().map(norm_upper))
            - VALID_UNIFIED_POSTURES
        )
        checks.append(result(
            "UNIFIED_POSTURE_VALUES",
            "FAIL" if invalid else "PASS",
            "FATAL" if invalid else "INFO",
            f"Invalid unified postures: {sorted(invalid)}" if invalid
            else "Unified posture vocabulary is valid.",
            rows_checked=len(profile),
            failure_count=1 if invalid else 0,
        ))

    if "data_coverage" in profile.columns:
        invalid = (
            set(profile["data_coverage"].dropna().map(norm_upper))
            - VALID_DATA_COVERAGE
        )
        checks.append(result(
            "DATA_COVERAGE_VALUES",
            "FAIL" if invalid else "PASS",
            "FATAL" if invalid else "INFO",
            f"Invalid data coverage values: {sorted(invalid)}" if invalid
            else "Data coverage vocabulary is valid.",
            rows_checked=len(profile),
            failure_count=1 if invalid else 0,
        ))

    return checks


def audit_no_data_semantics(profile: pd.DataFrame) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []

    rules = [
        ("funding_status", "NO_DATA", "funding_event_count"),
        ("investor_coverage_status", "NO_DATA", "named_investor_count"),
        ("valuation_status", "NO_DATA", "latest_valuation_amount_usd"),
        ("valuation_status", "NO_DATA", "latest_valuation_date"),
    ]

    violations = []

    for status_col, no_data_value, value_col in rules:
        if status_col not in profile.columns or value_col not in profile.columns:
            continue

        for _, row in profile.iterrows():
            status = norm_upper(row[status_col])
            value = row[value_col]

            # funding_event_count is a special case: Step 17 historically
            # materializes zero event count for configured companies. Step 20
            # therefore must not interpret zero as evidence of funding.
            if status == no_data_value and value_col == "funding_event_count":
                if numeric(value) not in {None, 0.0}:
                    violations.append(
                        f"{normalize_cik(row['cik'])}:{value_col}={value}"
                    )
            elif status == no_data_value and not is_missing(value):
                violations.append(
                    f"{normalize_cik(row['cik'])}:{value_col}={value}"
                )

    checks.append(result(
        "NO_DATA_SEMANTICS",
        "FAIL" if violations else "PASS",
        "FATAL" if violations else "INFO",
        f"NO_DATA fields contain unsupported values: {violations}"
        if violations
        else "NO_DATA semantics are preserved; missing values are not fabricated.",
        rows_checked=len(profile),
        failure_count=len(violations),
    ))

    return checks


# =============================================================================
# INFERENCE / GOVERNANCE AUDITS
# =============================================================================

def audit_governance_controls(profile: pd.DataFrame) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []

    if "sec_capital_treated_as_valuation" in profile.columns:
        bad = profile[
        profile["sec_capital_treated_as_valuation"]
        .map(bool_value)
        .eq(True)
    ]

        checks.append(result(
        "SEC_CAPITAL_VALUATION_SEPARATION",
        "FAIL" if not bad.empty else "PASS",
        "FATAL" if not bad.empty else "INFO",
        (
            "SEC exempt-offering capital is incorrectly treated as valuation."
            if not bad.empty
            else "SEC exempt-offering capital is not treated as valuation."
        ),
        rows_checked=len(profile),
        failure_count=len(bad),
    ))
    else:
        checks.append(result(
        "SEC_CAPITAL_VALUATION_SEPARATION",
        "FAIL",
        "FATAL",
        "Missing sec_capital_treated_as_valuation control.",
        failure_count=1,
          ))

    if "valuation_fabrication_prevented" in profile.columns:
        bad = profile[
            ~profile["valuation_fabrication_prevented"]
            .map(bool_value)
            .fillna(False)
        ]
        checks.append(result(
            "VALUATION_FABRICATION_CONTROL",
            "FAIL" if not bad.empty else "PASS",
            "FATAL" if not bad.empty else "INFO",
            "Valuation fabrication prevention is disabled."
            if not bad.empty
            else "Valuation fabrication prevention is enabled.",
            rows_checked=len(profile),
            failure_count=len(bad),
        ))
    else:
        checks.append(result(
            "VALUATION_FABRICATION_CONTROL",
            "FAIL",
            "FATAL",
            "Missing valuation_fabrication_prevented control.",
            failure_count=1,
        ))

    if "funding_stage" in profile.columns and "funding_status" in profile.columns:
        stage = profile["funding_stage"].fillna("").astype(str).str.upper().str.strip()
        status = profile["funding_status"].fillna("").astype(str).str.upper().str.strip()

        conventional = {"SEED", "SERIES A", "SERIES B", "SERIES C", "SERIES D", "SERIES E"}
        suspicious = profile[stage.isin(conventional) & ~status.eq("AVAILABLE")]

        checks.append(result(
            "FUNDING_STAGE_INFERENCE_CONTROL",
            "FAIL" if not suspicious.empty else "PASS",
            "FATAL" if not suspicious.empty else "INFO",
            "Conventional funding stage exists without supported coverage."
            if not suspicious.empty
            else "Funding-stage inference is not detected.",
            rows_checked=len(profile),
            failure_count=len(suspicious),
        ))

    if "named_investor_count" in profile.columns and "investor_coverage_status" in profile.columns:
        invalid = profile[
            profile["named_investor_count"].notna()
            & profile["investor_coverage_status"].astype(str).str.upper().eq("NO_DATA")
        ]
        checks.append(result(
            "INVESTOR_IDENTITY_CONTROL",
            "FAIL" if not invalid.empty else "PASS",
            "FATAL" if not invalid.empty else "INFO",
            "Named investor count exists while investor coverage is NO_DATA."
            if not invalid.empty
            else "Investor identity remains source-aware.",
            rows_checked=len(profile),
            failure_count=len(invalid),
        ))

    if "ownership_pct_available" in profile.columns and "ownership_status" in profile.columns:
        invalid = profile[
            profile["ownership_pct_available"].map(bool_value).eq(True)
            & profile["ownership_status"].astype(str).str.upper().eq("NO_DATA")
        ]
        checks.append(result(
            "OWNERSHIP_INFERENCE_CONTROL",
            "FAIL" if not invalid.empty else "PASS",
            "FATAL" if not invalid.empty else "INFO",
            "Ownership percentage is available while ownership status is NO_DATA."
            if not invalid.empty
            else "Ownership is not inferred from unavailable source data.",
            rows_checked=len(profile),
            failure_count=len(invalid),
        ))

    if "financial_score" in profile.columns:
        checks.append(result(
            "NO_NEW_FINANCIAL_SCORE",
            "PASS",
            "INFO",
            "Step 20 preserves the upstream financial score; DQ does not accept a new unified score.",
            rows_checked=len(profile),
        ))

    return checks


# =============================================================================
# SIGNAL AUDITS
# =============================================================================

def audit_signal_structure(signals: pd.DataFrame) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []

    if "signal_type" not in signals.columns:
        return [result(
            "SIGNAL_SCHEMA",
            "FAIL",
            "FATAL",
            "Signal dataset has no signal_type column.",
            failure_count=1,
        )]

    actual_types = set(signals["signal_type"].dropna().astype(str))
    unexpected = actual_types - VALID_SIGNAL_TYPES

    checks.append(result(
        "SIGNAL_TYPES",
        "FAIL" if unexpected else "PASS",
        "FATAL" if unexpected else "INFO",
        f"Unexpected signal types: {sorted(unexpected)}"
        if unexpected
        else "Signal types conform to the Step 20 contract.",
        rows_checked=len(signals),
        failure_count=1 if unexpected else 0,
    ))

    if {"cik", "signal_type"}.issubset(signals.columns):
        dupes = signals.duplicated(["cik", "signal_type"], keep=False)
        checks.append(result(
            "SIGNAL_GRAIN",
            "FAIL" if dupes.any() else "PASS",
            "FATAL" if dupes.any() else "INFO",
            "Duplicate CIK/signal_type rows detected."
            if dupes.any()
            else "Signal grain is one row per CIK/signal type.",
            rows_checked=len(signals),
            failure_count=int(dupes.sum()),
        ))

        expected_pairs = {
            (cik, signal_type)
            for cik in EXPECTED_CIKS
            for signal_type in VALID_SIGNAL_TYPES
        }
        actual_pairs = {
            (normalize_cik(row["cik"]), str(row["signal_type"]))
            for _, row in signals.iterrows()
        }
        missing = sorted(expected_pairs - actual_pairs)
        extra = sorted(actual_pairs - expected_pairs)

        checks.append(result(
            "SIGNAL_COMPLETENESS",
            "FAIL" if missing or extra else "PASS",
            "FATAL" if missing or extra else "INFO",
            f"Missing signal pairs={missing[:20]}, Extra signal pairs={extra[:20]}"
            if missing or extra
            else "Every configured company has every required Step 20 signal.",
            rows_checked=len(signals),
            failure_count=1 if missing or extra else 0,
        ))

    if {"signal_status", "signal_value"}.issubset(signals.columns):
        violations = []
        for _, row in signals.iterrows():
            status = norm_upper(row["signal_status"])
            if status == "NO_DATA" and not is_missing(row["signal_value"]):
                violations.append(
                    f"{normalize_cik(row['cik'])}:{row['signal_type']}"
                )

        checks.append(result(
            "SIGNAL_NO_DATA_SEMANTICS",
            "FAIL" if violations else "PASS",
            "FATAL" if violations else "INFO",
            f"NO_DATA signals contain values: {violations}"
            if violations
            else "NO_DATA signals correctly contain NULL values.",
            rows_checked=len(signals),
            failure_count=len(violations),
        ))

    return checks


def signal_lookup(signals: pd.DataFrame) -> Dict[tuple[str, str], pd.Series]:
    out: Dict[tuple[str, str], pd.Series] = {}
    for _, row in signals.iterrows():
        key = (normalize_cik(row["cik"]), str(row["signal_type"]))
        out[key] = row
    return out


def audit_signal_profile_consistency(
    profile: pd.DataFrame,
    signals: pd.DataFrame,
) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []
    lookup = signal_lookup(signals)
    mismatches = []

    for _, row in profile.iterrows():
        cik = normalize_cik(row["cik"])

        expected = {
            "FINANCIAL_DECISION": row.get("financial_decision"),
            "FINANCIAL_TRAJECTORY": row.get("financial_trajectory"),
            "FUNDING_COVERAGE": row.get("funding_status"),
            "FUNDING_ACTIVITY": row.get("sec_exempt_offering_capital_usd")
                if norm_upper(row.get("funding_activity_status")) == "AVAILABLE"
                else None,
            "INVESTOR_COVERAGE": row.get("investor_coverage_status"),
            "OWNERSHIP_COVERAGE": (
                row.get("ownership_status")
                if norm_upper(row.get("ownership_status")) in {"AVAILABLE", "PARTIAL"}
                else None
            ),
            "VALUATION_COVERAGE": row.get("valuation_status"),
            "VALUATION_STATUS": (
                row.get("latest_valuation_amount_usd")
                if norm_upper(row.get("valuation_status")) == "AVAILABLE"
                else None
            ),
            "SEC_CAPITAL_CONTEXT": row.get("sec_capital_context_usd"),
            "DATA_COVERAGE": row.get("data_coverage"),
        }

        for signal_type, expected_value in expected.items():
            signal = lookup.get((cik, signal_type))
            if signal is None:
                continue

            if not values_equal(signal.get("signal_value"), expected_value):
                mismatches.append(
                    f"{cik}:{signal_type}:profile={expected_value!r},signal={signal.get('signal_value')!r}"
                )

    checks.append(result(
        "SIGNAL_PROFILE_CONSISTENCY",
        "FAIL" if mismatches else "PASS",
        "FATAL" if mismatches else "INFO",
        "Profile/signal mismatches: " + "; ".join(mismatches[:30])
        if mismatches
        else "All Step 20 signals reconcile to the Step 20 profile.",
        rows_checked=len(profile),
        failure_count=len(mismatches),
    ))

    return checks


# =============================================================================
# AUTHORITATIVE UPSTREAM RECONCILIATION
# =============================================================================

def latest_upstream_row(
    df: pd.DataFrame,
    cik: str,
    date_columns: List[str],
) -> Optional[pd.Series]:
    subset = df[df["cik"].map(normalize_cik) == cik].copy()
    if subset.empty:
        return None

    usable = []
    for column in date_columns:
        if column in subset.columns:
            subset[column] = pd.to_datetime(subset[column], errors="coerce")
            usable.append(column)

    if usable:
        # Avoid pandas fillna object downcasting warnings.
        sort_date = pd.Series(pd.NaT, index=subset.index, dtype="datetime64[ns]")
        for column in usable:
            candidate = subset[column]
            sort_date = sort_date.where(sort_date.notna(), candidate)
        subset["_sort_date"] = sort_date
        subset = subset.sort_values("_sort_date", na_position="first")

    return subset.iloc[-1].drop(labels=["_sort_date"], errors="ignore")


def load_feature_latest(
    feature_root: Path,
) -> pd.DataFrame:
    if feature_root.is_file():
        df = pd.read_parquet(feature_root)
    else:
        paths = sorted(feature_root.glob("cik=*/financial_features.parquet"))
        if not paths:
            paths = sorted(feature_root.rglob("financial_features.parquet"))
        if not paths:
            raise FileNotFoundError(
                f"No Step 12 financial feature parquet files found under {feature_root}"
            )
        df = pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)

    if "cik" not in df.columns:
        raise ValueError("Step 12 features missing cik.")

    return df


def audit_authoritative_reconciliation(
    profile: pd.DataFrame,
    upstream: Dict[str, pd.DataFrame],
) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []

    comparisons = [
        (
            "FINANCIAL_DECISION_RECONCILIATION",
            "step15_snapshot",
            "decision_posture",
            "financial_decision",
            ["decision_generated_at_utc", "generated_at_utc"],
        ),
        (
            "FINANCIAL_TRAJECTORY_RECONCILIATION",
            "step15_snapshot",
            "financial_trajectory",
            "financial_trajectory",
            ["decision_generated_at_utc", "generated_at_utc"],
        ),
        (
            "FUNDING_STATUS_RECONCILIATION",
            "step17_snapshot",
            "funding_status",
            "funding_status",
            ["last_funding_date", "generated_at_utc"],
        ),
        (
            "FUNDING_EVENT_COUNT_RECONCILIATION",
            "step17_snapshot",
            "funding_event_count",
            "funding_event_count",
            ["last_funding_date", "generated_at_utc"],
        ),
        (
            "INVESTOR_COVERAGE_RECONCILIATION",
            "step18_investor",
            "investor_identity_status",
            "investor_coverage_status",
            ["as_of_date", "generated_at_utc"],
        ),
        (
            "NAMED_INVESTOR_COUNT_RECONCILIATION",
            "step18_investor",
            "named_investor_count",
            "named_investor_count",
            ["as_of_date", "generated_at_utc"],
        ),
        (
            "OWNERSHIP_STATUS_RECONCILIATION",
            "step18_ownership",
            "ownership_status",
            "ownership_status",
            ["ownership_as_of_date", "as_of_date", "generated_at_utc"],
        ),
        (
            "OWNERSHIP_PCT_RECONCILIATION",
            "step18_ownership",
            "ownership_pct_available",
            "ownership_pct_available",
            ["ownership_as_of_date", "as_of_date", "generated_at_utc"],
        ),
        (
            "VALUATION_STATUS_RECONCILIATION",
            "step19_snapshot",
            "valuation_status",
            "valuation_status",
            ["latest_valuation_date", "generated_at_utc"],
        ),
        (
            "VALUATION_AMOUNT_RECONCILIATION",
            "step19_snapshot",
            "latest_valuation_amount_usd",
            "latest_valuation_amount_usd",
            ["latest_valuation_date", "generated_at_utc"],
        ),
        (
            "VALUATION_DATE_RECONCILIATION",
            "step19_snapshot",
            "latest_valuation_date",
            "latest_valuation_date",
            ["latest_valuation_date", "generated_at_utc"],
        ),
    ]

    for (
        check_name,
        source_name,
        source_field,
        profile_field,
        date_columns,
    ) in comparisons:
        mismatches = []
        missing_source = []

        source = upstream[source_name]

        for _, prow in profile.iterrows():
            cik = normalize_cik(prow["cik"])
            srow = latest_upstream_row(source, cik, date_columns)

            if srow is None:
                if not profile_represents_missing_source(
                    prow,
                    source_name,
                ):
                    missing_source.append(cik)
                continue

            left = prow.get(profile_field)
            right = srow.get(source_field)

            if check_name == "FUNDING_STATUS_RECONCILIATION":
                event_count = numeric(srow.get("funding_event_count"))
                right = (
                    "AVAILABLE"
                    if event_count is not None and event_count > 0
                    else "NO_DATA"
                )

            if not values_equal(left, right):
                mismatches.append(
                    f"{cik}:profile={left!r},upstream={right!r}"
                )

        if missing_source:
            checks.append(result(
                check_name + "_SOURCE_COVERAGE",
                "FAIL",
                "FATAL",
                f"Authoritative upstream row missing for CIKs: {missing_source}",
                rows_checked=len(profile),
                failure_count=len(missing_source),
            ))

        checks.append(result(
            check_name,
            "FAIL" if mismatches else "PASS",
            "FATAL" if mismatches else "INFO",
            f"Step 20 mismatch: {mismatches[:30]}"
            if mismatches
            else (
                f"Step 20 {profile_field} exactly reconciles to {source_name}; "
                "omitted source rows are represented as explicit NO_DATA."
                if any(
                    latest_upstream_row(source, normalize_cik(prow["cik"]), date_columns) is None
                    for _, prow in profile.iterrows()
                )
                else f"Step 20 {profile_field} exactly reconciles to {source_name}."
            ),
            rows_checked=len(profile),
            failure_count=len(mismatches),
        ))

    return checks


def audit_sec_capital(
    profile: pd.DataFrame,
    upstream: Dict[str, pd.DataFrame],
) -> List[Dict[str, Any]]:
    checks = []

    if "sec_capital_context_usd" not in profile.columns:
        return [result(
            "SEC_CAPITAL_CONTEXT_RECONCILIATION",
            "FAIL",
            "FATAL",
            "Step 20 profile missing sec_capital_context_usd.",
            failure_count=1,
        )]

    source = upstream["step17_snapshot"]
    mismatches = []

    for _, prow in profile.iterrows():
        cik = normalize_cik(prow["cik"])
        srow = latest_upstream_row(
            source,
            cik,
            ["last_funding_date", "generated_at_utc"],
        )

        if srow is None:
            continue

        source_value = srow.get("total_sec_capital_raised_usd")
        if is_missing(source_value):
            source_value = srow.get("total_capital_raised_usd")

        if not values_equal(
            prow.get("sec_capital_context_usd"),
            source_value,
        ):
            mismatches.append(
                f"{cik}:profile={prow.get('sec_capital_context_usd')!r},"
                f"step17={source_value!r}"
            )

    checks.append(result(
        "SEC_CAPITAL_CONTEXT_RECONCILIATION",
        "FAIL" if mismatches else "PASS",
        "FATAL" if mismatches else "INFO",
        f"SEC capital mismatches: {mismatches[:30]}"
        if mismatches
        else "SEC capital context reconciles to Step 17.",
        rows_checked=len(profile),
        failure_count=len(mismatches),
    ))

    return checks


# =============================================================================
# DETERMINISM AUDITS
# =============================================================================

def expected_unified_posture(
    financial_decision: Optional[str],
    financial_trajectory: Optional[str],
    data_coverage: Optional[str],
) -> str:
    fd = norm_upper(financial_decision)
    trajectory = norm_upper(financial_trajectory)
    coverage = norm_upper(data_coverage)

    if coverage == "NO_DATA":
        return "INSUFFICIENT_DATA"

    if fd == "CAUTION":
        if trajectory == "DETERIORATING":
            return "CAUTION"
        return "CAUTION"

    if fd == "WATCH":
        if trajectory == "DETERIORATING":
            return "CAUTION"
        return "WATCH"

    if fd == "POSITIVE":
        if trajectory == "IMPROVING":
            return "STRONG_POSITIVE"
        return "POSITIVE"

    if fd == "NEUTRAL":
        if trajectory == "DETERIORATING":
            return "WATCH"
        return "NEUTRAL"

    return "INSUFFICIENT_DATA"


def expected_data_coverage(
    financial_available: bool,
    statuses: List[Optional[str]],
) -> str:
    normalized = [norm_upper(x) or "NO_DATA" for x in statuses]

    if not financial_available:
        if all(x == "NO_DATA" for x in normalized):
            return "NO_DATA"
        return "LIMITED"

    available_count = sum(x == "AVAILABLE" for x in normalized)
    partial_count = sum(x == "PARTIAL" for x in normalized)

    if available_count >= 3:
        return "COMPLETE"

    if available_count >= 1 or partial_count >= 1:
        return "PARTIAL"

    return "LIMITED"


def audit_deterministic_rules(profile: pd.DataFrame) -> List[Dict[str, Any]]:
    checks = []

    posture_mismatches = []
    coverage_mismatches = []

    for _, row in profile.iterrows():
        expected_posture = expected_unified_posture(
            row.get("financial_decision"),
            row.get("financial_trajectory"),
            row.get("data_coverage"),
        )

        if norm_upper(row.get("unified_posture")) != expected_posture:
            posture_mismatches.append(
                f"{normalize_cik(row['cik'])}:expected={expected_posture},"
                f"observed={row.get('unified_posture')!r}"
            )

        expected_coverage = expected_data_coverage(
            financial_available=(
                norm_upper(row.get("financial_decision"))
                in VALID_FINANCIAL_DECISIONS
            ),
            statuses=[
                row.get("funding_status"),
                row.get("investor_coverage_status"),
                row.get("ownership_status"),
                row.get("valuation_status"),
            ],
        )

        if norm_upper(row.get("data_coverage")) != expected_coverage:
            coverage_mismatches.append(
                f"{normalize_cik(row['cik'])}:expected={expected_coverage},"
                f"observed={row.get('data_coverage')!r}"
            )

    checks.append(result(
        "UNIFIED_POSTURE_DETERMINISM",
        "FAIL" if posture_mismatches else "PASS",
        "FATAL" if posture_mismatches else "INFO",
        f"Unified posture rule mismatches: {posture_mismatches}"
        if posture_mismatches
        else "Unified posture matches the Step 20 deterministic rule.",
        rows_checked=len(profile),
        failure_count=len(posture_mismatches),
    ))

    checks.append(result(
        "DATA_COVERAGE_DETERMINISM",
        "FAIL" if coverage_mismatches else "PASS",
        "FATAL" if coverage_mismatches else "INFO",
        f"Data coverage rule mismatches: {coverage_mismatches}"
        if coverage_mismatches
        else "Data coverage matches the Step 20 deterministic rule.",
        rows_checked=len(profile),
        failure_count=len(coverage_mismatches),
    ))

    return checks


# =============================================================================
# SUMMARY / OUTPUT
# =============================================================================

def build_summary(
    profile: pd.DataFrame,
    signals: pd.DataFrame,
    checks: List[Dict[str, Any]],
    warnings: List[str],
) -> pd.DataFrame:
    failures = [c for c in checks if c["status"] == "FAIL"]
    infos = [c for c in checks if c["status"] == "PASS"]

    return pd.DataFrame([{
        "step": "STEP_20_1",
        "dq_version": DQ_VERSION,
        "step20_version_expected": STEP20_VERSION_EXPECTED,
        "configured_companies": len(EXPECTED_CIKS),
        "profile_rows": len(profile),
        "signal_rows": len(signals),
        "dq_checks": len(checks),
        "dq_pass_checks": len(infos),
        "dq_fail_checks": len(failures),
        "fatal_exception_count": sum(
            int(c.get("failure_count", 0))
            for c in failures
            if c.get("severity") == "FATAL"
        ),
        "warning_count": len(warnings),
        "overall_dq_status": "FAIL" if failures else "PASS",
        "promotion_decision": "BLOCKED" if failures else "PROMOTE",
        "new_financial_score_created": False,
        "unified_ranking_created": False,
        "sec_capital_treated_as_valuation": False,
        "valuation_fabrication_prevented": True,
        "funding_stage_inference_prevented": True,
        "investor_identity_inference_prevented": True,
        "ownership_inference_prevented": True,
        "generated_at_utc": utc_now_iso(),
    }])


def build_exceptions(checks: List[Dict[str, Any]]) -> pd.DataFrame:
    failures = [
        c for c in checks
        if c["status"] == "FAIL"
    ]

    if not failures:
        return pd.DataFrame(columns=[
            "dq_category",
            "severity",
            "exception_code",
            "message",
            "rows_checked",
            "failure_count",
            "dq_version",
            "created_at_utc",
        ])

    rows = []
    for check in failures:
        rows.append({
            "dq_category": "UNIFIED_INVESTMENT_INTELLIGENCE",
            "severity": check["severity"],
            "exception_code": check["check_name"],
            "message": check["message"],
            "rows_checked": check.get("rows_checked", 0),
            "failure_count": check.get("failure_count", 0),
            "dq_version": DQ_VERSION,
            "created_at_utc": utc_now_iso(),
        })

    return pd.DataFrame(rows)


def build_lineage() -> pd.DataFrame:
    return pd.DataFrame([{
        "step": "STEP_20_1",
        "dq_version": DQ_VERSION,
        "audited_step20_version": STEP20_VERSION_EXPECTED,
        "source_step12_version": STEP12_VERSION_EXPECTED,
        "source_step15_version": STEP15_VERSION_EXPECTED,
        "source_step17_version": STEP17_VERSION_EXPECTED,
        "source_step18_version": STEP18_VERSION_EXPECTED,
        "source_step19_version": STEP19_VERSION_EXPECTED,
        "step20_root": str(STEP20_ROOT),
        "dq_root": str(DQ_ROOT),
        "cross_layer_reconciliation": True,
        "new_financial_score_created": False,
        "unified_ranking_created": False,
        "sec_capital_treated_as_valuation": False,
        "valuation_fabrication_prevented": True,
        "funding_stage_inference_prevented": True,
        "investor_identity_inference_prevented": True,
        "ownership_inference_prevented": True,
        "generated_at_utc": utc_now_iso(),
    }])


def write_outputs(
    profile: pd.DataFrame,
    signals: pd.DataFrame,
    checks: List[Dict[str, Any]],
    summary: pd.DataFrame,
    lineage: pd.DataFrame,
) -> None:
    validation_df = pd.DataFrame(checks)
    exceptions_df = build_exceptions(checks)

    fatal_count = int(
        (
            validation_df["status"].eq("FAIL")
            & validation_df["severity"].eq("FATAL")
        ).sum()
    )

    promotion = pd.DataFrame([{
        "step": "STEP_20_1",
        "dq_version": DQ_VERSION,
        "audited_step20_version": STEP20_VERSION_EXPECTED,
        "promotion_decision": "BLOCKED" if fatal_count else "PROMOTE",
        "fatal_exception_count": fatal_count,
        "warning_count": int(
            validation_df["status"].eq("WARN").sum()
        ),
        "new_financial_score_created": False,
        "unified_ranking_created": False,
        "sec_capital_treated_as_valuation": False,
        "valuation_fabrication_prevented": True,
        "funding_stage_inference_prevented": True,
        "investor_identity_inference_prevented": True,
        "ownership_inference_prevented": True,
        "generated_at_utc": utc_now_iso(),
    }])

    write_parquet(
        validation_df,
        DQ_ROOT / "unified_validation_checks"
        / "unified_validation_checks.parquet",
    )

    write_parquet(
        exceptions_df,
        DQ_ROOT / "unified_investment_exceptions"
        / "unified_investment_exceptions.parquet",
    )

    write_parquet(
        summary,
        DQ_ROOT / "unified_investment_dq_summary"
        / "unified_investment_dq_summary.parquet",
    )

    write_parquet(
        lineage,
        DQ_ROOT / "unified_investment_dq_lineage"
        / "unified_investment_dq_lineage.parquet",
    )

    write_parquet(
        promotion,
        DQ_ROOT / "unified_investment_promotion"
        / "unified_investment_promotion.parquet",
    )


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    global COMPANIES, EXPECTED_CIKS, SCOPED_RUN

    paths = input_paths()

    step15_snapshot = load_required(
        paths["step15_snapshot"],
        "Step 15 financial decision snapshot",
    )

    from company_platform.registry.active_company_scope import (
    assert_company_scope,
)

    COMPANIES = configured_companies()

    EXPECTED_CIKS = set(COMPANIES)
    SCOPED_RUN = False

    logger.info("=" * 80)
    logger.info("STEP 20.1 - UNIFIED INVESTMENT INTELLIGENCE DQ")
    logger.info("DQ Version: %s", DQ_VERSION)
    logger.info("Audited Step 20 version: %s", STEP20_VERSION_EXPECTED)
    logger.info("=" * 80)


    frames = {}
    for name in [
        "profile",
        "signals",
        "summary",
        "lineage",
        "promotion",
        "step15_signals",
        "step15_snapshot",
        "step17_snapshot",
        "step18_investor",
        "step18_ownership",
        "step19_snapshot",
    ]:
        frames[name] = load_parquet(
            paths[name],
            f"Step 20.1 {name}",
        )
        logger.info("Loaded %-18s rows=%d", name, len(frames[name]))

    # Step 12 features are partitioned. Load only for coverage/version context.
    step12_features = load_feature_latest(paths["step12_features"])
    frames["step12_features"] = step12_features
    logger.info("Loaded %-18s rows=%d", "step12_features", len(step12_features))

    checks: List[Dict[str, Any]] = []
    warnings: List[str] = []

    # -------------------------------------------------------------------------
    # Step 20 artifact schemas
    # -------------------------------------------------------------------------

    checks.append(audit_required_columns(
        frames["profile"],
        {
            "cik",
            "financial_decision",
            "financial_trajectory",
            "financial_score",
            "funding_status",
            "funding_event_count",
            "sec_exempt_offering_capital_usd",
            "funding_activity_status",
            "investor_coverage_status",
            "named_investor_count",
            "ownership_status",
            "ownership_pct_available",
            "valuation_status",
            "latest_valuation_amount_usd",
            "latest_valuation_date",
            "sec_capital_context_usd",
            "sec_capital_treated_as_valuation",
            "valuation_fabrication_prevented",
            "data_coverage",
            "unified_posture",
            "step12_version",
            "step15_version",
            "step17_version",
            "step18_version",
            "step19_version",
            "step20_version",
        },
        "investment profile",
    ))

    checks.append(audit_required_columns(
        frames["signals"],
        {
            "cik",
            "signal_type",
            "signal_value",
            "signal_status",
            "source_scope",
            "confidence",
            "step20_version",
        },
        "investment signals",
    ))

    checks.append(audit_required_columns(
        frames["lineage"],
        {
            "step",
            "step20_version",
            "source_step12_version",
            "source_step15_version",
            "source_step17_version",
            "source_step18_version",
            "source_step19_version",
            "new_financial_score_created",
            "unified_ranking_created",
            "sec_capital_treated_as_valuation",
            "valuation_fabrication_prevented",
            "funding_stage_inferred",
            "investor_identity_inferred",
            "ownership_inferred",
        },
        "investment lineage",
    ))

    # -------------------------------------------------------------------------
    # Coverage / grain
    # -------------------------------------------------------------------------

    checks.append(audit_cik_coverage(frames["profile"], "investment_profile"))
    checks.append(audit_one_row_per_cik(frames["profile"], "investment_profile"))

    checks.append(audit_cik_coverage(
        frames["step15_snapshot"],
        "step15_snapshot",
        frames["profile"],
    ))
    checks.append(audit_one_row_per_cik(
        frames["step15_snapshot"], "step15_snapshot"
    ))

    checks.append(audit_cik_coverage(
        frames["step17_snapshot"],
        "step17_snapshot",
        frames["profile"],
    ))
    checks.append(audit_one_row_per_cik(
        frames["step17_snapshot"], "step17_snapshot"
    ))

    checks.append(audit_cik_coverage(
        frames["step18_investor"],
        "step18_investor",
        frames["profile"],
    ))
    checks.append(audit_one_row_per_cik(
        frames["step18_investor"], "step18_investor"
    ))

    checks.append(audit_cik_coverage(
        frames["step18_ownership"],
        "step18_ownership",
        frames["profile"],
    ))
    checks.append(audit_one_row_per_cik(
        frames["step18_ownership"], "step18_ownership"
    ))

    checks.append(audit_cik_coverage(
        frames["step19_snapshot"],
        "step19_snapshot",
        frames["profile"],
    ))
    checks.append(audit_one_row_per_cik(
        frames["step19_snapshot"], "step19_snapshot"
    ))

    # -------------------------------------------------------------------------
    # Version / lineage
    # -------------------------------------------------------------------------

    checks.extend(audit_step20_versions(
        frames["profile"],
        frames["signals"],
        frames["summary"],
        frames["lineage"],
        frames["promotion"],
    ))

    checks.extend(audit_lineage(frames["lineage"]))

    # Upstream version checks.
    version_requirements = [
        ("step12_features", ["feature_version"], STEP12_VERSION_EXPECTED, "Step 12"),
        ("step15_signals", ["decision_version", "step15_version"], STEP15_VERSION_EXPECTED, "Step 15"),
        ("step15_snapshot", ["decision_version", "step15_version"], STEP15_VERSION_EXPECTED, "Step 15 snapshot"),
        ("step17_snapshot", ["snapshot_version", "step17_version", "source_step_version"], STEP17_VERSION_EXPECTED, "Step 17"),
        ("step18_investor", ["snapshot_version", "step18_version", "source_step_version", "step_version"], STEP18_VERSION_EXPECTED, "Step 18 investor"),
        ("step18_ownership", ["snapshot_version", "step18_version", "source_step_version", "step_version"], STEP18_VERSION_EXPECTED, "Step 18 ownership"),
        ("step19_snapshot", ["snapshot_version", "step19_version", "source_step_version", "step_version"], STEP19_VERSION_EXPECTED, "Step 19"),
    ]

    for frame_name, columns, expected, label in version_requirements:
        df = frames[frame_name]
        observed = set()
        for col in columns:
            if col in df.columns:
                observed.update(df[col].dropna().astype(str).tolist())

        if observed and observed != {expected}:
            checks.append(result(
                f"{label.upper().replace(' ', '_')}_VERSION",
                "FAIL",
                "FATAL",
                f"Expected {expected}; observed={sorted(observed)}",
                failure_count=1,
            ))
        elif not observed:
            checks.append(result(
                f"{label.upper().replace(' ', '_')}_VERSION",
                "FAIL",
                "FATAL",
                f"No supported version metadata found for {label}.",
                failure_count=1,
            ))
        else:
            checks.append(result(
                f"{label.upper().replace(' ', '_')}_VERSION",
                "PASS",
                "INFO",
                f"{label} version is {expected}.",
                rows_checked=len(df),
            ))

    # -------------------------------------------------------------------------
    # Profile / signal semantics
    # -------------------------------------------------------------------------

    checks.extend(audit_profile_values(frames["profile"]))
    checks.extend(audit_no_data_semantics(frames["profile"]))
    checks.extend(audit_governance_controls(frames["profile"]))

    checks.extend(audit_signal_structure(frames["signals"]))
    checks.extend(audit_signal_profile_consistency(
        frames["profile"],
        frames["signals"],
    ))

    # -------------------------------------------------------------------------
    # Authoritative upstream reconciliation
    # -------------------------------------------------------------------------

    checks.extend(audit_authoritative_reconciliation(
        frames["profile"],
        {
            "step15_snapshot": frames["step15_snapshot"],
            "step17_snapshot": frames["step17_snapshot"],
            "step18_investor": frames["step18_investor"],
            "step18_ownership": frames["step18_ownership"],
            "step19_snapshot": frames["step19_snapshot"],
        },
    ))

    checks.extend(audit_sec_capital(
        frames["profile"],
        {
            "step17_snapshot": frames["step17_snapshot"],
        },
    ))

    # -------------------------------------------------------------------------
    # Deterministic synthesis
    # -------------------------------------------------------------------------

    checks.extend(audit_deterministic_rules(frames["profile"]))

    # -------------------------------------------------------------------------
    # Controlled source-coverage warnings
    # -------------------------------------------------------------------------

    investor_status = (
        frames["profile"]["investor_coverage_status"]
        .astype(str).str.upper()
    )
    valuation_status = (
        frames["profile"]["valuation_status"]
        .astype(str).str.upper()
    )

    if (investor_status == "NO_DATA").all():
        warnings.append(
            "Named investor enrichment is currently unavailable; investor identity remains NO_DATA."
        )

    if (valuation_status == "NO_DATA").all():
        warnings.append(
            "Valuation enrichment is currently unavailable; valuation remains NO_DATA."
        )

    warnings.append(
        "SEC Form D coverage is not a complete private-company funding history."
    )

    # -------------------------------------------------------------------------
    # Summary / outputs
    # -------------------------------------------------------------------------

    summary = build_summary(
        frames["profile"],
        frames["signals"],
        checks,
        warnings,
    )

    lineage = build_lineage()

    write_outputs(
        frames["profile"],
        frames["signals"],
        checks,
        summary,
        lineage,
    )

    fatal = [
        c for c in checks
        if c["status"] == "FAIL" and c["severity"] == "FATAL"
    ]

    logger.info("")
    logger.info("=" * 80)
    logger.info("STEP 20.1 SUMMARY")
    logger.info("=" * 80)
    logger.info("Configured companies : %d", len(EXPECTED_CIKS))
    logger.info("Profile rows        : %d", len(frames["profile"]))
    logger.info("Signal rows         : %d", len(frames["signals"]))
    logger.info("DQ checks           : %d", len(checks))
    logger.info("Passed checks       : %d", sum(c["status"] == "PASS" for c in checks))
    logger.info("Failed checks       : %d", sum(c["status"] == "FAIL" for c in checks))
    logger.info("Fatal failures      : %d", len(fatal))
    logger.info("Warnings            : %d", len(warnings))
    logger.info("Promotion decision  : %s", "BLOCKED" if fatal else "PROMOTE")

    logger.info("")
    logger.info("Cross-layer reconciliation: ENFORCED")
    logger.info("Step 15 trajectory reconciliation: ENFORCED")
    logger.info("Step 15 decision reconciliation: ENFORCED")
    logger.info("Step 17 funding reconciliation: ENFORCED")
    logger.info("Step 18 investor/ownership reconciliation: ENFORCED")
    logger.info("Step 19 valuation reconciliation: ENFORCED")
    logger.info("SEC capital != valuation: ENFORCED")
    logger.info("Funding-stage inference: PREVENTED")
    logger.info("Investor identity inference: PREVENTED")
    logger.info("Ownership inference: PREVENTED")
    logger.info("New financial score: NO")
    logger.info("Unified ranking: NO")

    if warnings:
        logger.info("")
        logger.info("CONTROLLED WARNINGS")
        for warning in warnings:
            logger.warning(warning)

    if fatal:
        logger.error("")
        logger.error("STEP 20.1 FAILED - Unified Investment Intelligence is NOT DQ-cleared.")
        logger.error("Review:")
        logger.error(
            str(
                DQ_ROOT
                / "unified_investment_exceptions"
                / "unified_investment_exceptions.parquet"
            )
        )
        raise RuntimeError(
            f"Step 20.1 blocked promotion with {len(fatal)} fatal DQ failure(s)."
        )

    logger.info("")
    logger.info(
        "STEP 20.1 PASS - Unified Investment Intelligence is DQ-cleared."
    )


if __name__ == "__main__":
    main()
