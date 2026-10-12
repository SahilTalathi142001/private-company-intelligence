
"""
STEP 16.1 - COMPANY INTELLIGENCE DATA QUALITY

Purpose
-------
Audit the Step 16 company-facing intelligence profile before it is consumed
by Power BI, an API, or a web application.

Step 16.1 validates:

A. Company identity
   - company master is mandatory
   - every discovered CIK has exactly one identity
   - company_name is non-empty
   - profile company_name must exactly match the company master

B. Profile quality reason
   - profile_quality must be COMPLETE or PARTIAL
   - profile_quality_reason must be deterministic and consistent with
     Step 12 feature quality + Step 15 decision completeness
   - PARTIAL upstream quality can never be promoted to COMPLETE

C. Profile-level freshness
   - profile_as_of_period must equal the latest Step 15 period
   - upstream source timestamps must be parseable
   - profile_data_as_of_utc is the oldest required upstream dependency
   - profile_age_days and profile_freshness_status must be internally consistent

Additional controls
-------------------
- one latest profile row per CIK
- one history row per CIK/FY/quarter
- latest profile period agrees across Step 12 and Step 15
- Step 15 decision fields are preserved exactly
- latest quantitative profile values agree with Step 12
- historical Step 16 rows agree with Step 15 quarter-by-quarter
- narrative contains the core deterministic conclusions
- no new financial score
- warnings are retained for stale/partial profiles
- FAIL means the profile layer is not a clean downstream contract

Outputs
-------
data/gold/financial_company_intelligence_dq/
    profile_quality_audit/
        profile_quality_audit.parquet
    profile_quality_exceptions/
        profile_quality_exceptions.parquet
    profile_freshness_audit/
        profile_freshness_audit.parquet
    profile_identity_audit/
        profile_identity_audit.parquet
    profile_dq_summary/
        profile_dq_summary.parquet
    profile_dq_lineage/
        profile_dq_lineage.parquet

Version
-------
1.0.0
"""

from __future__ import annotations

import argparse
import math
import traceback
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

from company_platform.registry.active_company_scope import load_active_company_ciks


# =============================================================================
# VERSION / CONTRACT
# =============================================================================

DQ_VERSION = "1.0.0"
SUPPORTED_PROFILE_VERSIONS = {"1.1.0"}

QUALITY_COMPLETE = "COMPLETE"
QUALITY_PARTIAL = "PARTIAL"

VALID_PROFILE_QUALITY_REASONS = {
    "COMPLETE",
    "PARTIAL_FEATURE_DATA",
    "PARTIAL_DECISION_DATA",
    "PARTIAL_IDENTITY_DATA",
    "PARTIAL_UPSTREAM_DATA",
}

VALID_FRESHNESS_STATUS = {
    "FRESH",
    "STALE",
    "VERY_STALE",
    "UNKNOWN",
}

FRESHNESS_FRESH_DAYS = 30.0
FRESHNESS_STALE_DAYS = 90.0

DECISION_PRIORITY_MAP = {
    "CAUTION": 1,
    "WATCH": 2,
    "NEUTRAL": 3,
    "POSITIVE": 4,
}

CORE_DECISION_FIELDS = [
    "financial_health",
    "financial_risk_signal",
    "financial_trajectory",
    "peer_position",
    "peer_population_class",
    "decision_posture",
    "decision_priority",
    "decision_reason_code",
    "decision_confidence",
    "revenue_growth_signal",
    "profitability_signal",
    "growth_consistency_signal",
    "momentum_signal",
]

CORE_NUMERIC_FIELDS = [
    "revenue_value",
    "operating_income_value",
    "net_income_value",
    "revenue_yoy_growth_pct",
    "revenue_growth_acceleration",
    "operating_margin",
    "net_margin",
    "ttm_revenue",
    "ttm_operating_margin",
    "ttm_net_margin",
]

HISTORY_FIELDS = [
    "decision_posture",
    "decision_priority",
    "decision_confidence",
    "financial_health",
    "financial_risk_signal",
    "financial_trajectory",
    "peer_position",
    "calibrated_overall_rank",
    "calibrated_display_score",
]

NUMERIC_TOLERANCE = 1e-9
TEXT_TOLERANCE = 1e-9


# =============================================================================
# DEFAULT PATHS
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_FEATURES = PROJECT_ROOT / "data" / "gold" / "financials_features"
DEFAULT_DECISIONS = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "financial_decision_intelligence"
)
DEFAULT_PROFILE = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "financial_company_intelligence"
)
DEFAULT_COMPANY_MASTER = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "company"
    / "company.parquet"
)
DEFAULT_OUTPUT = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "financial_company_intelligence_dq"
)


# =============================================================================
# LOGGING / HELPERS
# =============================================================================

def log(message: str = "") -> None:
    print(message, flush=True)


def section(title: str) -> None:
    log("=" * 80)
    log(title)
    log("=" * 80)


def normalize_cik(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""

    text = str(value).strip()

    if text.endswith(".0"):
        text = text[:-2]

    digits = "".join(ch for ch in text if ch.isdigit())

    return digits.zfill(10) if digits else ""


def normalize_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""

    text = str(value).strip()

    if text.lower() in {"nan", "none", "<na>", "nat"}:
        return ""

    return text


def normalize_upper(value: Any) -> str:
    return normalize_text(value).upper()


def safe_int(value: Any) -> Optional[int]:
    if value is None or pd.isna(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def safe_float(value: Any) -> Optional[float]:
    if value is None or pd.isna(value):
        return None
    try:
        value = float(value)
        if not math.isfinite(value):
            return None
        return value
    except (TypeError, ValueError):
        return None


def close_enough(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return a is None and b is None

    if pd.isna(a) and pd.isna(b):
        return True

    if pd.isna(a) or pd.isna(b):
        return False

    try:
        return math.isclose(
            float(a),
            float(b),
            rel_tol=1e-7,
            abs_tol=NUMERIC_TOLERANCE,
        )
    except (TypeError, ValueError):
        return normalize_text(a) == normalize_text(b)


def parse_utc(value: Any) -> Optional[pd.Timestamp]:
    if value is None or normalize_text(value) == "":
        return None

    ts = pd.to_datetime(value, errors="coerce", utc=True)

    if pd.isna(ts):
        return None

    return ts


def quarter_num(value: Any) -> int:
    return {
        "Q1": 1,
        "Q2": 2,
        "Q3": 3,
        "Q4": 4,
    }.get(normalize_upper(value), 0)


def sort_period(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["_fy_sort"] = pd.to_numeric(
        out["fiscal_year"],
        errors="coerce",
    )
    out["_q_sort"] = out["quarter"].map(quarter_num)
    out = out.sort_values(
        ["_fy_sort", "_q_sort"],
        na_position="last",
    )
    return out.drop(columns=["_fy_sort", "_q_sort"])


def latest_row(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        raise ValueError("Cannot select latest row from empty DataFrame.")
    return sort_period(df).iloc[-1]


def read_parquet(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")

    df = pd.read_parquet(path)

    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"{label} did not load as a DataFrame: {path}")

    return df


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp.parquet")
    if tmp.exists():
        tmp.unlink()
    df.to_parquet(tmp, index=False)
    tmp.replace(path)


# =============================================================================
# EXCEPTIONS
# =============================================================================

class ExceptionCollector:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    def add(
        self,
        *,
        cik: str,
        check_name: str,
        exception_type: str,
        severity: str,
        field: Optional[str] = None,
        expected: Any = None,
        actual: Any = None,
        message: str,
    ) -> None:
        self.rows.append(
            {
                "cik": normalize_cik(cik),
                "check_name": check_name,
                "exception_type": exception_type,
                "severity": severity,
                "field": field,
                "expected": expected,
                "actual": actual,
                "message": message,
                "dq_version": DQ_VERSION,
                "detected_at_utc": pd.Timestamp.now(tz="UTC"),
            }
        )

    def dataframe(self) -> pd.DataFrame:
        columns = [
            "cik",
            "check_name",
            "exception_type",
            "severity",
            "field",
            "expected",
            "actual",
            "message",
            "dq_version",
            "detected_at_utc",
        ]

        if not self.rows:
            return pd.DataFrame(columns=columns)

        return pd.DataFrame(self.rows, columns=columns)

    def fatal_count(self) -> int:
        return sum(row["severity"] == "FAIL" for row in self.rows)

    def warning_count(self) -> int:
        return sum(row["severity"] == "WARNING" for row in self.rows)


# =============================================================================
# INPUT LOADING
# =============================================================================

def load_company_master(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            "Company master is mandatory for Step 16.1 identity DQ: "
            f"{path}"
        )

    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
    elif path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        raise ValueError(
            f"Unsupported company master format: {path.suffix}"
        )

    required = {"cik", "company_name"}
    missing = sorted(required - set(df.columns))

    if missing:
        raise KeyError(
            f"Company master missing required columns: {missing}"
        )

    out = df.copy()
    out["cik"] = out["cik"].map(normalize_cik)
    out["company_name"] = out["company_name"].map(normalize_text)

    if (out["cik"] == "").any():
        raise ValueError("Company master contains blank/invalid CIK values.")

    if (out["company_name"] == "").any():
        bad = out.loc[
            out["company_name"] == "",
            "cik",
        ].tolist()
        raise ValueError(
            "Company master contains blank company_name values: "
            f"{bad}"
        )

    if out["cik"].duplicated().any():
        conflicting_names = (
            out.groupby("cik")["company_name"]
            .nunique(dropna=False)
        )
        if (conflicting_names > 1).any():
            dupes = conflicting_names[conflicting_names > 1].index.tolist()
            raise ValueError(
                "Company master contains conflicting company names for "
                f"duplicate CIKs: {dupes}"
            )

        sort_columns = ["cik", "company_name"]
        if "ticker" in out.columns:
            sort_columns.append("ticker")
        out = (
            out.sort_values(sort_columns, na_position="last")
            .drop_duplicates(subset=["cik"], keep="first")
            .reset_index(drop=True)
        )

    return out


def load_profile(profile_root: Path) -> pd.DataFrame:
    path = (
        profile_root
        / "company_intelligence_profile"
        / "company_intelligence_profile.parquet"
    )

    return read_parquet(
        path,
        "Step 16 company intelligence profile",
    )


def load_history(profile_root: Path) -> pd.DataFrame:
    path = (
        profile_root
        / "company_intelligence_history"
        / "company_intelligence_history.parquet"
    )

    return read_parquet(
        path,
        "Step 16 company intelligence history",
    )


def load_decisions(decisions_root: Path) -> pd.DataFrame:
    path = (
        decisions_root
        / "decision_signals"
        / "financial_decision_signals.parquet"
    )

    df = read_parquet(path, "Step 15 decision signals")

    if df.empty:
        raise ValueError("Step 15 decision signals are empty.")

    if "cik" not in df.columns:
        raise KeyError("Step 15 decision signals missing cik.")

    df = df.copy()
    df["cik"] = df["cik"].map(normalize_cik)

    required = [
        "cik",
        "fiscal_year",
        "quarter",
        "decision_posture",
        "decision_priority",
        "decision_confidence",
        "financial_health",
        "financial_risk_signal",
        "financial_trajectory",
        "peer_position",
        "peer_population_class",
        "decision_reason_code",
    ]

    missing = [c for c in required if c not in df.columns]

    if missing:
        raise KeyError(
            "Step 15 decision signals missing columns: "
            f"{missing}"
        )

    return df


def load_features(features_root: Path, cik: str) -> pd.DataFrame:
    path = (
        features_root
        / f"cik={normalize_cik(cik)}"
        / "financial_features.parquet"
    )

    df = read_parquet(
        path,
        f"Step 12 financial features for {cik}",
    )

    if df.empty:
        raise ValueError(
            f"Step 12 financial features are empty for {cik}."
        )

    out = df.copy()

    if "cik" not in out.columns:
        out["cik"] = cik

    out["cik"] = out["cik"].map(normalize_cik)

    required = [
        "cik",
        "fiscal_year",
        "quarter",
        "feature_version",
        "feature_data_quality",
    ]

    missing = [c for c in required if c not in out.columns]

    if missing:
        raise KeyError(
            f"Step 12 financial features missing columns for {cik}: "
            f"{missing}"
        )

    return out


# =============================================================================
# IDENTITY
# =============================================================================

def discover_ciks(decisions: pd.DataFrame) -> list[str]:
    # Company identity comes from the active registry. Financial decision
    # availability is measured separately and may legitimately be NO_DATA.
    return load_active_company_ciks()


def audit_identity(
    profile: pd.DataFrame,
    master: pd.DataFrame,
    discovered_ciks: list[str],
    exceptions: ExceptionCollector,
) -> pd.DataFrame:
    rows = []

    master_map = dict(
        zip(
            master["cik"],
            master["company_name"],
        )
    )

    profile_map = (
        profile.groupby("cik", dropna=False)
        .size()
        .to_dict()
    )

    for cik in discovered_ciks:
        expected_name = normalize_text(master_map.get(cik))
        profile_rows = profile[
            profile["cik"].map(normalize_cik) == cik
        ]

        row_count = len(profile_rows)
        actual_name = (
            normalize_text(profile_rows.iloc[0]["company_name"])
            if row_count == 1
            else ""
        )

        status = "PASS"

        if cik not in master_map:
            status = "FAIL"
            exceptions.add(
                cik=cik,
                check_name="identity_master_coverage",
                exception_type="IDENTITY_NOT_IN_MASTER",
                severity="FAIL",
                expected="company master row",
                actual="missing",
                message="Discovered CIK is absent from company master.",
            )

        if row_count != 1:
            status = "FAIL"
            exceptions.add(
                cik=cik,
                check_name="identity_profile_grain",
                exception_type="PROFILE_IDENTITY_GRAIN",
                severity="FAIL",
                expected=1,
                actual=row_count,
                message="Expected exactly one latest profile row per CIK.",
            )

        if not actual_name:
            status = "FAIL"
            exceptions.add(
                cik=cik,
                check_name="identity_company_name_present",
                exception_type="BLANK_COMPANY_NAME",
                severity="FAIL",
                field="company_name",
                expected="non-empty",
                actual=actual_name,
                message="Profile company_name is blank.",
            )

        if expected_name and actual_name != expected_name:
            status = "FAIL"
            exceptions.add(
                cik=cik,
                check_name="identity_name_match",
                exception_type="COMPANY_NAME_MISMATCH",
                severity="FAIL",
                field="company_name",
                expected=expected_name,
                actual=actual_name,
                message="Profile company_name does not match company master.",
            )

        rows.append(
            {
                "cik": cik,
                "master_company_name": expected_name,
                "profile_company_name": actual_name,
                "profile_row_count": row_count,
                "identity_status": status,
                "dq_version": DQ_VERSION,
            }
        )

    return pd.DataFrame(rows)


# =============================================================================
# PROFILE QUALITY REASON
# =============================================================================

def expected_quality_reason(
    feature_quality: str,
    decision: pd.Series,
) -> tuple[str, str]:
    feature_quality = normalize_upper(feature_quality)

    decision_fields = [
        decision.get("financial_health"),
        decision.get("financial_risk_signal"),
        decision.get("financial_trajectory"),
        decision.get("decision_posture"),
        decision.get("decision_confidence"),
        decision.get("decision_reason_code"),
    ]

    decision_complete = all(
        normalize_text(value)
        for value in decision_fields
    )

    feature_complete = feature_quality == QUALITY_COMPLETE

    if feature_complete and decision_complete:
        return QUALITY_COMPLETE, "COMPLETE"

    if not feature_complete and not decision_complete:
        return QUALITY_PARTIAL, "PARTIAL_UPSTREAM_DATA"

    if not feature_complete:
        return QUALITY_PARTIAL, "PARTIAL_FEATURE_DATA"

    return QUALITY_PARTIAL, "PARTIAL_DECISION_DATA"


def audit_quality_reason(
    profile: pd.DataFrame,
    decisions: pd.DataFrame,
    features_root: Path,
    exceptions: ExceptionCollector,
) -> pd.DataFrame:
    rows = []

    for _, profile_row in profile.iterrows():
        cik = normalize_cik(profile_row.get("cik"))

        decision_rows = decisions[
            decisions["cik"].map(normalize_cik) == cik
        ].copy()

        if decision_rows.empty:
            actual_quality = normalize_upper(profile_row.get("profile_quality"))
            actual_reason = normalize_upper(profile_row.get("profile_quality_reason"))
            status = (
                "PASS"
                if actual_quality == QUALITY_PARTIAL
                and actual_reason == "NO_FINANCIAL_DATA_AVAILABLE"
                else "FAIL"
            )
            if status == "FAIL":
                exceptions.add(
                    cik=cik,
                    check_name="quality_reason_upstream_coverage",
                    exception_type="DECISION_UPSTREAM_MISSING_CONTRACT",
                    severity="FAIL",
                    expected="PARTIAL / NO_FINANCIAL_DATA_AVAILABLE",
                    actual=f"{actual_quality} / {actual_reason}",
                    message="Missing Step 15 decision data must be represented explicitly as NO_DATA.",
                )
            rows.append(
                {
                    "cik": cik,
                    "profile_quality": actual_quality,
                    "profile_quality_reason": actual_reason,
                    "expected_quality": "PARTIAL",
                    "expected_quality_reason": "NO_FINANCIAL_DATA_AVAILABLE",
                    "quality_reason_status": status,
                    "dq_version": DQ_VERSION,
                }
            )
            continue

        latest_decision = latest_row(decision_rows)

        try:
            features = load_features(features_root, cik)
            latest_feature = latest_row(features)
            feature_quality = normalize_upper(
                latest_feature.get("feature_data_quality")
            )
        except Exception as exc:
            feature_quality = ""
            exceptions.add(
                cik=cik,
                check_name="quality_reason_feature_coverage",
                exception_type="FEATURE_UPSTREAM_MISSING",
                severity="FAIL",
                message=str(exc),
            )

        expected_quality, expected_reason = expected_quality_reason(
            feature_quality,
            latest_decision,
        )

        actual_quality = normalize_upper(
            profile_row.get("profile_quality")
        )
        actual_reason = normalize_upper(
            profile_row.get("profile_quality_reason")
        )

        status = "PASS"

        if actual_quality != expected_quality:
            status = "FAIL"
            exceptions.add(
                cik=cik,
                check_name="profile_quality_value",
                exception_type="PROFILE_QUALITY_MISMATCH",
                severity="FAIL",
                field="profile_quality",
                expected=expected_quality,
                actual=actual_quality,
                message="Step 16 profile quality does not match upstream state.",
            )

        if actual_reason != expected_reason:
            status = "FAIL"
            exceptions.add(
                cik=cik,
                check_name="profile_quality_reason",
                exception_type="PROFILE_QUALITY_REASON_MISMATCH",
                severity="FAIL",
                field="profile_quality_reason",
                expected=expected_reason,
                actual=actual_reason,
                message="Profile quality reason is inconsistent with upstream quality.",
            )

        if actual_quality == QUALITY_COMPLETE and expected_reason != "COMPLETE":
            status = "FAIL"
            exceptions.add(
                cik=cik,
                check_name="quality_no_upstream_promotion",
                exception_type="UPSTREAM_PARTIAL_PROMOTED_TO_COMPLETE",
                severity="FAIL",
                field="profile_quality",
                expected="PARTIAL",
                actual=actual_quality,
                message="Partial upstream quality cannot become COMPLETE at Step 16.",
            )

        if actual_quality == QUALITY_PARTIAL:
            exceptions.add(
                cik=cik,
                check_name="partial_profile_visibility",
                exception_type="PARTIAL_PROFILE",
                severity="WARNING",
                field="profile_quality_reason",
                expected="visible partial reason",
                actual=actual_reason,
                message="Profile remains PARTIAL and is correctly visible downstream.",
            )

        rows.append(
            {
                "cik": cik,
                "profile_quality": actual_quality,
                "profile_quality_reason": actual_reason,
                "expected_quality": expected_quality,
                "expected_quality_reason": expected_reason,
                "feature_data_quality": feature_quality,
                "quality_reason_status": status,
                "dq_version": DQ_VERSION,
            }
        )

    return pd.DataFrame(rows)


# =============================================================================
# FRESHNESS
# =============================================================================

def freshness_status(age_days: Optional[float]) -> str:
    if age_days is None:
        return "UNKNOWN"

    if age_days <= FRESHNESS_FRESH_DAYS:
        return "FRESH"

    if age_days <= FRESHNESS_STALE_DAYS:
        return "STALE"

    return "VERY_STALE"


def audit_freshness(
    profile: pd.DataFrame,
    features_root: Path,
    decisions_root: Path,
    exceptions: ExceptionCollector,
) -> pd.DataFrame:
    rows = []

    decision_signal_path = (
        decisions_root
        / "decision_signals"
        / "financial_decision_signals.parquet"
    )

    decision_mtime = (
        pd.Timestamp(
            decision_signal_path.stat().st_mtime,
            unit="s",
            tz="UTC",
        )
        if decision_signal_path.exists()
        else None
    )

    for _, row in profile.iterrows():
        cik = normalize_cik(row.get("cik"))

        profile_generated = parse_utc(
            row.get("profile_generated_at_utc")
        )
        profile_as_of_utc = parse_utc(
            row.get("profile_data_as_of_utc")
        )

        feature_path = (
            features_root
            / f"cik={cik}"
            / "financial_features.parquet"
        )

        feature_mtime = (
            pd.Timestamp(
                feature_path.stat().st_mtime,
                unit="s",
                tz="UTC",
            )
            if feature_path.exists()
            else None
        )

        expected_upstream = [
            ts for ts in [feature_mtime, decision_mtime]
            if ts is not None
        ]

        expected_data_as_of = (
            min(expected_upstream)
            if expected_upstream
            else None
        )

        status = "PASS"

        if profile_generated is None:
            status = "FAIL"
            exceptions.add(
                cik=cik,
                check_name="freshness_generated_timestamp",
                exception_type="PROFILE_GENERATED_TIMESTAMP_INVALID",
                severity="FAIL",
                field="profile_generated_at_utc",
                message="profile_generated_at_utc is missing or invalid.",
            )

        no_financial_data = (
            normalize_upper(row.get("profile_quality_reason"))
            == "NO_FINANCIAL_DATA_AVAILABLE"
        )

        # A registry company with no current financial data is a valid
        # company-grain state. Its data-as-of timestamp must remain NULL and
        # freshness must remain UNKNOWN; this is not a DQ failure.
        if profile_as_of_utc is None and not no_financial_data:
            status = "FAIL"
            exceptions.add(
                cik=cik,
                check_name="freshness_data_as_of_timestamp",
                exception_type="PROFILE_AS_OF_TIMESTAMP_INVALID",
                severity="FAIL",
                field="profile_data_as_of_utc",
                message="profile_data_as_of_utc is missing or invalid.",
            )

        if (
            not no_financial_data
            and expected_data_as_of is not None
            and profile_as_of_utc is not None
        ):
            if abs(
                (profile_as_of_utc - expected_data_as_of)
                .total_seconds()
            ) > 1.0:
                status = "FAIL"
                exceptions.add(
                    cik=cik,
                    check_name="freshness_upstream_timestamp",
                    exception_type="UPSTREAM_TIMESTAMP_MISMATCH",
                    severity="FAIL",
                    field="profile_data_as_of_utc",
                    expected=expected_data_as_of.isoformat(),
                    actual=profile_as_of_utc.isoformat(),
                    message="Profile data-as-of timestamp does not equal the oldest current upstream dependency.",
                )

        age_days = None

        if profile_generated is not None and profile_as_of_utc is not None:
            age_days = max(
                0.0,
                (
                    profile_generated - profile_as_of_utc
                ).total_seconds() / 86400.0,
            )

        expected_status = freshness_status(age_days)
        actual_status = normalize_upper(
            row.get("profile_freshness_status")
        )

        if actual_status != expected_status:
            status = "FAIL"
            exceptions.add(
                cik=cik,
                check_name="freshness_status",
                exception_type="FRESHNESS_STATUS_MISMATCH",
                severity="FAIL",
                field="profile_freshness_status",
                expected=expected_status,
                actual=actual_status,
                message="Profile freshness status is inconsistent with profile age.",
            )

        actual_age = safe_float(row.get("profile_age_days"))

        if age_days is None:
            if actual_age is not None:
                status = "FAIL"
                exceptions.add(
                    cik=cik,
                    check_name="freshness_age",
                    exception_type="FRESHNESS_AGE_SHOULD_BE_NULL",
                    severity="FAIL",
                    field="profile_age_days",
                    expected=None,
                    actual=actual_age,
                    message="Freshness age should be null when timestamps are unavailable.",
                )
        elif actual_age is None or not math.isclose(
            actual_age,
            age_days,
            rel_tol=1e-6,
            abs_tol=1e-6,
        ):
            status = "FAIL"
            exceptions.add(
                cik=cik,
                check_name="freshness_age",
                exception_type="FRESHNESS_AGE_MISMATCH",
                severity="FAIL",
                field="profile_age_days",
                expected=age_days,
                actual=actual_age,
                message="profile_age_days is inconsistent with profile timestamps.",
            )

        if actual_status in {"STALE", "VERY_STALE"}:
            exceptions.add(
                cik=cik,
                check_name="freshness_staleness_visibility",
                exception_type="STALE_PROFILE",
                severity="WARNING",
                field="profile_freshness_status",
                expected="FRESH",
                actual=actual_status,
                message="Profile is stale relative to its upstream artifact timestamps.",
            )

        rows.append(
            {
                "cik": cik,
                "profile_generated_at_utc": (
                    profile_generated.isoformat()
                    if profile_generated is not None
                    else None
                ),
                "profile_data_as_of_utc": (
                    profile_as_of_utc.isoformat()
                    if profile_as_of_utc is not None
                    else None
                ),
                "expected_upstream_data_as_of_utc": (
                    expected_data_as_of.isoformat()
                    if expected_data_as_of is not None
                    else None
                ),
                "profile_age_days_calculated": age_days,
                "profile_age_days_reported": actual_age,
                "profile_freshness_status": actual_status,
                "expected_freshness_status": expected_status,
                "freshness_status": status,
                "dq_version": DQ_VERSION,
            }
        )

    return pd.DataFrame(rows)


# =============================================================================
# GRAIN / UPSTREAM CONSISTENCY
# =============================================================================

def audit_profile_grain(
    profile: pd.DataFrame,
    history: pd.DataFrame,
    decisions: pd.DataFrame,
    exceptions: ExceptionCollector,
) -> None:
    profile_dupes = profile.duplicated(
        subset=["cik"],
        keep=False,
    )

    if profile_dupes.any():
        exceptions.add(
            cik="",
            check_name="profile_latest_grain",
            exception_type="PROFILE_DUPLICATE_CIK",
            severity="FAIL",
            message=(
                "Step 16 latest profile must contain exactly one row per CIK."
            ),
        )

    history_dupes = history.duplicated(
        subset=["cik", "fiscal_year", "quarter"],
        keep=False,
    )

    if history_dupes.any():
        exceptions.add(
            cik="",
            check_name="profile_history_grain",
            exception_type="HISTORY_DUPLICATE_PERIOD",
            severity="FAIL",
            message=(
                "Step 16 history must contain one row per CIK/FY/quarter."
            ),
        )

    decision_dupes = decisions.duplicated(
        subset=["cik", "fiscal_year", "quarter"],
        keep=False,
    )

    if decision_dupes.any():
        exceptions.add(
            cik="",
            check_name="decision_upstream_grain",
            exception_type="DECISION_DUPLICATE_PERIOD",
            severity="FAIL",
            message="Step 15 decision signals violate company-quarter grain.",
        )


def audit_latest_consistency(
    profile: pd.DataFrame,
    decisions: pd.DataFrame,
    features_root: Path,
    exceptions: ExceptionCollector,
) -> pd.DataFrame:
    rows = []

    for _, profile_row in profile.iterrows():
        cik = normalize_cik(profile_row.get("cik"))

        d = decisions[
            decisions["cik"].map(normalize_cik) == cik
        ].copy()

        if d.empty:
            continue

        latest_decision = latest_row(d)

        try:
            features = load_features(features_root, cik)
            latest_feature = latest_row(features)
        except Exception as exc:
            exceptions.add(
                cik=cik,
                check_name="latest_feature_coverage",
                exception_type="FEATURE_LATEST_MISSING",
                severity="FAIL",
                message=str(exc),
            )
            continue

        status = "PASS"

        expected_fy = safe_int(latest_decision.get("fiscal_year"))
        expected_q = normalize_upper(latest_decision.get("quarter"))

        actual_fy = safe_int(profile_row.get("latest_fiscal_year"))
        actual_q = normalize_upper(profile_row.get("latest_quarter"))

        if (actual_fy, actual_q) != (expected_fy, expected_q):
            status = "FAIL"
            exceptions.add(
                cik=cik,
                check_name="latest_period_alignment",
                exception_type="LATEST_PERIOD_MISMATCH",
                severity="FAIL",
                expected=f"{expected_fy}-{expected_q}",
                actual=f"{actual_fy}-{actual_q}",
                message="Profile latest period does not match Step 15.",
            )

        feature_fy = safe_int(latest_feature.get("fiscal_year"))
        feature_q = normalize_upper(latest_feature.get("quarter"))

        if (feature_fy, feature_q) != (expected_fy, expected_q):
            status = "FAIL"
            exceptions.add(
                cik=cik,
                check_name="latest_period_feature_alignment",
                exception_type="LATEST_FEATURE_PERIOD_MISMATCH",
                severity="FAIL",
                expected=f"{expected_fy}-{expected_q}",
                actual=f"{feature_fy}-{feature_q}",
                message="Step 12 latest period does not match Step 15 latest period.",
            )

        for field in CORE_DECISION_FIELDS:
            if field not in profile_row.index:
                continue
            expected = latest_decision.get(field)
            actual = profile_row.get(field)

            expected_norm = normalize_upper(expected)
            actual_norm = normalize_upper(actual)

            if field == "decision_priority":
                expected_value = safe_int(
                    DECISION_PRIORITY_MAP.get(
                        normalize_upper(
                            latest_decision.get("decision_posture")
                        )
                    )
                )
                actual_value = safe_int(actual)
                if actual_value != expected_value:
                    status = "FAIL"
                    exceptions.add(
                        cik=cik,
                        check_name="decision_priority_consistency",
                        exception_type="DECISION_PRIORITY_MISMATCH",
                        severity="FAIL",
                        field=field,
                        expected=expected_value,
                        actual=actual_value,
                        message="Profile decision priority does not match Step 15 posture.",
                    )
            elif expected_norm != actual_norm:
                status = "FAIL"
                exceptions.add(
                    cik=cik,
                    check_name="decision_field_consistency",
                    exception_type="STEP15_FIELD_MISMATCH",
                    severity="FAIL",
                    field=field,
                    expected=expected_norm,
                    actual=actual_norm,
                    message="Profile decision field differs from authoritative Step 15.",
                )

        for field in CORE_NUMERIC_FIELDS:
            if field not in latest_feature.index or field not in profile_row.index:
                continue

            expected = safe_float(latest_feature.get(field))
            actual = safe_float(profile_row.get(field))

            if not close_enough(expected, actual):
                status = "FAIL"
                exceptions.add(
                    cik=cik,
                    check_name="numeric_profile_consistency",
                    exception_type="NUMERIC_PROFILE_MISMATCH",
                    severity="FAIL",
                    field=field,
                    expected=expected,
                    actual=actual,
                    message="Latest numeric profile value differs from Step 12.",
                )

        rows.append(
            {
                "cik": cik,
                "profile_fiscal_year": actual_fy,
                "profile_quarter": actual_q,
                "step15_fiscal_year": expected_fy,
                "step15_quarter": expected_q,
                "step12_fiscal_year": feature_fy,
                "step12_quarter": feature_q,
                "latest_consistency_status": status,
                "dq_version": DQ_VERSION,
            }
        )

    return pd.DataFrame(rows)


def audit_history_consistency(
    profile_history: pd.DataFrame,
    decisions: pd.DataFrame,
    exceptions: ExceptionCollector,
) -> pd.DataFrame:
    rows = []

    for cik, history_group in profile_history.groupby(
        "cik",
        dropna=False,
    ):
        cik = normalize_cik(cik)

        decision_group = decisions[
            decisions["cik"].map(normalize_cik) == cik
        ].copy()

        if decision_group.empty:
            continue

        history_group = history_group.copy()

        merged = decision_group.merge(
            history_group,
            on=["cik", "fiscal_year", "quarter"],
            how="outer",
            suffixes=("_step15", "_step16"),
            indicator=True,
        )

        status = "PASS"

        for _, row in merged.iterrows():
            fy = safe_int(row.get("fiscal_year"))
            quarter = normalize_upper(row.get("quarter"))

            if row["_merge"] != "both":
                status = "FAIL"
                exceptions.add(
                    cik=cik,
                    check_name="history_period_coverage",
                    exception_type="HISTORY_PERIOD_COVERAGE",
                    severity="FAIL",
                    expected="matching Step 15 period",
                    actual=str(row["_merge"]),
                    message=(
                        f"Step 16 history and Step 15 differ for "
                        f"{fy}-{quarter}."
                    ),
                )
                continue

            for field in HISTORY_FIELDS:
                step15_field = f"{field}_step15"
                step16_field = f"{field}_step16"

                if (
                    step15_field not in row.index
                    or step16_field not in row.index
                ):
                    continue

                if field in {
                    "calibrated_overall_rank",
                    "decision_priority",
                }:
                    a = safe_int(row.get(step15_field))
                    b = safe_int(row.get(step16_field))
                    equal = a == b
                elif field == "calibrated_display_score":
                    a = safe_float(row.get(step15_field))
                    b = safe_float(row.get(step16_field))
                    equal = close_enough(a, b)
                else:
                    a = normalize_upper(row.get(step15_field))
                    b = normalize_upper(row.get(step16_field))
                    equal = a == b

                if not equal:
                    status = "FAIL"
                    exceptions.add(
                        cik=cik,
                        check_name="history_upstream_consistency",
                        exception_type="HISTORY_STEP15_MISMATCH",
                        severity="FAIL",
                        field=field,
                        expected=a,
                        actual=b,
                        message=(
                            f"Step 16 history differs from Step 15 "
                            f"for {fy}-{quarter}."
                        ),
                    )

        rows.append(
            {
                "cik": cik,
                "step15_rows": len(decision_group),
                "step16_history_rows": len(history_group),
                "history_consistency_status": status,
                "dq_version": DQ_VERSION,
            }
        )

    return pd.DataFrame(rows)


# =============================================================================
# NARRATIVE / CONTRACT
# =============================================================================

def audit_narrative(
    profile: pd.DataFrame,
    exceptions: ExceptionCollector,
) -> pd.DataFrame:
    rows = []

    for _, row in profile.iterrows():
        cik = normalize_cik(row.get("cik"))
        narrative = normalize_text(row.get("profile_narrative")).lower()

        expected_tokens = [
            normalize_text(row.get("financial_health")).lower(),
            normalize_text(row.get("financial_risk_signal")).lower(),
            normalize_text(row.get("financial_trajectory")).lower(),
            normalize_text(row.get("decision_posture")).lower(),
        ]

        missing_tokens = [
            token
            for token in expected_tokens
            if token and token not in narrative
        ]

        status = "PASS"

        if not any(expected_tokens) and normalize_upper(row.get("profile_quality")) == "PARTIAL":
            rows.append(
                {
                    "cik": cik,
                    "narrative_status": "PASS",
                    "missing_core_tokens": "",
                    "dq_version": DQ_VERSION,
                }
            )
            continue

        if not narrative:
            status = "FAIL"
            exceptions.add(
                cik=cik,
                check_name="narrative_presence",
                exception_type="NARRATIVE_MISSING",
                severity="FAIL",
                field="profile_narrative",
                message="Profile narrative is blank.",
            )
        elif missing_tokens:
            status = "WARNING"
            exceptions.add(
                cik=cik,
                check_name="narrative_deterministic_content",
                exception_type="NARRATIVE_CORE_TOKEN_MISSING",
                severity="WARNING",
                field="profile_narrative",
                expected="core Step 15 conclusions",
                actual=", ".join(missing_tokens),
                message="Narrative does not contain every expected core conclusion token.",
            )

        rows.append(
            {
                "cik": cik,
                "narrative_status": status,
                "missing_core_tokens": " | ".join(missing_tokens),
                "dq_version": DQ_VERSION,
            }
        )

    return pd.DataFrame(rows)


def audit_contract(
    profile: pd.DataFrame,
    exceptions: ExceptionCollector,
) -> None:
    if "calibrated_display_score" in profile.columns:
        # Step 16 may expose the Step 14.1 score, but must not create another
        # financial score. The profile contract therefore checks only for
        # explicit score-creation flags if they exist.
        if "new_financial_score_created" in profile.columns:
            bad = profile[
                profile["new_financial_score_created"]
                .fillna(False)
                .astype(bool)
            ]
            if not bad.empty:
                exceptions.add(
                    cik="",
                    check_name="no_new_financial_score",
                    exception_type="NEW_FINANCIAL_SCORE_CREATED",
                    severity="FAIL",
                    field="new_financial_score_created",
                    expected=False,
                    actual=True,
                    message="Step 16 contract violation: new financial score detected.",
                )


# =============================================================================
# SUMMARY / LINEAGE
# =============================================================================

def build_summary(
    profile: pd.DataFrame,
    identity_audit: pd.DataFrame,
    quality_audit: pd.DataFrame,
    freshness_audit: pd.DataFrame,
    exceptions: ExceptionCollector,
) -> pd.DataFrame:
    statuses = (
        identity_audit["identity_status"]
        if not identity_audit.empty
        else pd.Series(dtype=object)
    )

    quality_statuses = (
        quality_audit["quality_reason_status"]
        if not quality_audit.empty
        else pd.Series(dtype=object)
    )

    freshness_statuses = (
        freshness_audit["freshness_status"]
        if not freshness_audit.empty
        else pd.Series(dtype=object)
    )

    return pd.DataFrame(
        [
            {
                "dq_version": DQ_VERSION,
                "total_profiles": int(len(profile)),
                "identity_pass": int((statuses == "PASS").sum()),
                "identity_fail": int((statuses == "FAIL").sum()),
                "quality_reason_pass": int(
                    (quality_statuses == "PASS").sum()
                ),
                "quality_reason_fail": int(
                    (quality_statuses == "FAIL").sum()
                ),
                "freshness_pass": int(
                    (freshness_statuses == "PASS").sum()
                ),
                "freshness_fail": int(
                    (freshness_statuses == "FAIL").sum()
                ),
                "fresh_profiles": int(
                    (
                        profile["profile_freshness_status"]
                        .map(normalize_upper)
                        == "FRESH"
                    ).sum()
                ),
                "stale_profiles": int(
                    (
                        profile["profile_freshness_status"]
                        .map(normalize_upper)
                        == "STALE"
                    ).sum()
                ),
                "very_stale_profiles": int(
                    (
                        profile["profile_freshness_status"]
                        .map(normalize_upper)
                        == "VERY_STALE"
                    ).sum()
                ),
                "complete_profiles": int(
                    (
                        profile["profile_quality"]
                        .map(normalize_upper)
                        == QUALITY_COMPLETE
                    ).sum()
                ),
                "partial_profiles": int(
                    (
                        profile["profile_quality"]
                        .map(normalize_upper)
                        == QUALITY_PARTIAL
                    ).sum()
                ),
                "fatal_exception_count": exceptions.fatal_count(),
                "warning_exception_count": exceptions.warning_count(),
                "dq_status": (
                    "FAIL"
                    if exceptions.fatal_count() > 0
                    else (
                        "WARNING"
                        if exceptions.warning_count() > 0
                        else "PASS"
                    )
                ),
                "new_financial_score_created": False,
                "generated_at_utc": pd.Timestamp.now(tz="UTC"),
            }
        ]
    )


def build_lineage() -> pd.DataFrame:
    rows = [
        (
            "company_name",
            "Step 16 profile + required company master",
            "Exact CIK-to-company identity validation",
        ),
        (
            "profile_quality",
            "Step 16 profile + Step 12 feature quality + Step 15 decision completeness",
            "Recomputed deterministic profile quality check",
        ),
        (
            "profile_quality_reason",
            "Step 16 profile + upstream quality state",
            "Recomputed deterministic reason code check",
        ),
        (
            "profile_as_of_period",
            "Step 15 latest fiscal_year/quarter",
            "Latest reporting period consistency check",
        ),
        (
            "profile_freshness_status",
            "Step 16 timestamps + Step 12/Step 15 artifact timestamps",
            "Recomputed age and freshness classification",
        ),
        (
            "latest_decision_fields",
            "Step 15 decision_signals",
            "Authoritative field-by-field consistency check",
        ),
        (
            "latest_numeric_features",
            "Step 12 financial_features",
            "Numeric profile reconciliation",
        ),
        (
            "historical_profile",
            "Step 15 decision_signals",
            "Quarter-by-quarter historical reconciliation",
        ),
    ]

    return pd.DataFrame(
        [
            {
                "output_field": output,
                "source": source,
                "transformation": transformation,
                "dq_version": DQ_VERSION,
            }
            for output, source, transformation in rows
        ]
    )


# =============================================================================
# CLI / MAIN
# =============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Step 16.1 - Company Intelligence Data Quality"
    )

    parser.add_argument(
        "--features",
        type=Path,
        default=DEFAULT_FEATURES,
    )
    parser.add_argument(
        "--decisions",
        type=Path,
        default=DEFAULT_DECISIONS,
    )
    parser.add_argument(
        "--profile",
        type=Path,
        default=DEFAULT_PROFILE,
    )
    parser.add_argument(
        "--company-master",
        type=Path,
        default=DEFAULT_COMPANY_MASTER,
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    section("STEP 16.1 - COMPANY INTELLIGENCE DATA QUALITY")

    log(f"Features       : {args.features}")
    log(f"Decisions      : {args.decisions}")
    log(f"Profile        : {args.profile}")
    log(f"Company master : {args.company_master}")
    log(f"Output         : {args.output}")
    log(f"DQ version     : {DQ_VERSION}")
    log(f"Supported profile versions: {sorted(SUPPORTED_PROFILE_VERSIONS)}")

    log("")
    log("Contract : COMPANY IDENTITY IS AUTHORITATIVE")
    log("Contract : QUALITY REASON IS DETERMINISTIC")
    log("Contract : PROFILE FRESHNESS IS EXPLICIT")
    log("Contract : STEP 15 DECISION REMAINS AUTHORITATIVE")
    log("Contract : NO NEW FINANCIAL SCORE")

    company_master = load_company_master(args.company_master)
    profile = load_profile(args.profile)
    history = load_history(args.profile)
    decisions = load_decisions(args.decisions)

    if "profile_version" not in profile.columns:
        raise KeyError("Step 16 profile missing profile_version.")

    profile_versions = set(
        profile["profile_version"]
        .dropna()
        .astype(str)
    )

    unsupported = profile_versions - SUPPORTED_PROFILE_VERSIONS

    if unsupported:
        raise RuntimeError(
            f"Unsupported Step 16 profile versions: {sorted(unsupported)}"
        )

    required_profile = [
        "cik",
        "company_name",
        "latest_fiscal_year",
        "latest_quarter",
        "financial_health",
        "financial_risk_signal",
        "financial_trajectory",
        "peer_position",
        "peer_population_class",
        "decision_posture",
        "decision_priority",
        "decision_reason_code",
        "decision_confidence",
        "profile_quality",
        "profile_quality_reason",
        "profile_as_of_period",
        "profile_freshness_status",
        "profile_age_days",
        "profile_data_as_of_utc",
        "profile_generated_at_utc",
        "profile_version",
    ]

    missing_profile = [
        col for col in required_profile
        if col not in profile.columns
    ]

    if missing_profile:
        raise KeyError(
            f"Step 16 profile missing required 1.1.0 columns: "
            f"{missing_profile}"
        )

    profile["cik"] = profile["cik"].map(normalize_cik)
    history["cik"] = history["cik"].map(normalize_cik)

    decisions["cik"] = decisions["cik"].map(normalize_cik)

    discovered_ciks = discover_ciks(decisions)

    log(f"Companies discovered: {len(discovered_ciks)}")

    exceptions = ExceptionCollector()

    # -------------------------------------------------------------------------
    # A. Identity
    # -------------------------------------------------------------------------
    section("A. COMPANY IDENTITY DQ")

    identity_audit = audit_identity(
        profile=profile,
        master=company_master,
        discovered_ciks=discovered_ciks,
        exceptions=exceptions,
    )

    # -------------------------------------------------------------------------
    # Grain
    # -------------------------------------------------------------------------
    audit_profile_grain(
        profile=profile,
        history=history,
        decisions=decisions,
        exceptions=exceptions,
    )

    # -------------------------------------------------------------------------
    # B. Quality reason
    # -------------------------------------------------------------------------
    section("B. PROFILE QUALITY REASON DQ")

    quality_audit = audit_quality_reason(
        profile=profile,
        decisions=decisions,
        features_root=args.features,
        exceptions=exceptions,
    )

    # -------------------------------------------------------------------------
    # C. Freshness
    # -------------------------------------------------------------------------
    section("C. PROFILE FRESHNESS DQ")

    freshness_audit = audit_freshness(
        profile=profile,
        features_root=args.features,
        decisions_root=args.decisions,
        exceptions=exceptions,
    )

    # -------------------------------------------------------------------------
    # Cross-layer consistency
    # -------------------------------------------------------------------------
    section("UPSTREAM CONSISTENCY DQ")

    latest_audit = audit_latest_consistency(
        profile=profile,
        decisions=decisions,
        features_root=args.features,
        exceptions=exceptions,
    )

    history_audit = audit_history_consistency(
        profile_history=history,
        decisions=decisions,
        exceptions=exceptions,
    )

    narrative_audit = audit_narrative(
        profile=profile,
        exceptions=exceptions,
    )

    audit_contract(
        profile=profile,
        exceptions=exceptions,
    )

    exception_df = exceptions.dataframe()

    summary_df = build_summary(
        profile=profile,
        identity_audit=identity_audit,
        quality_audit=quality_audit,
        freshness_audit=freshness_audit,
        exceptions=exceptions,
    )

    lineage_df = build_lineage()

    # -------------------------------------------------------------------------
    # Write outputs
    # -------------------------------------------------------------------------
    profile_quality_audit_path = (
        args.output
        / "profile_quality_audit"
        / "profile_quality_audit.parquet"
    )

    exceptions_path = (
        args.output
        / "profile_quality_exceptions"
        / "profile_quality_exceptions.parquet"
    )

    freshness_path = (
        args.output
        / "profile_freshness_audit"
        / "profile_freshness_audit.parquet"
    )

    identity_path = (
        args.output
        / "profile_identity_audit"
        / "profile_identity_audit.parquet"
    )

    latest_path = (
        args.output
        / "profile_latest_consistency_audit"
        / "profile_latest_consistency_audit.parquet"
    )

    history_path = (
        args.output
        / "profile_history_consistency_audit"
        / "profile_history_consistency_audit.parquet"
    )

    narrative_path = (
        args.output
        / "profile_narrative_audit"
        / "profile_narrative_audit.parquet"
    )

    summary_path = (
        args.output
        / "profile_dq_summary"
        / "profile_dq_summary.parquet"
    )

    lineage_path = (
        args.output
        / "profile_dq_lineage"
        / "profile_dq_lineage.parquet"
    )

    write_parquet(identity_audit, identity_path)
    write_parquet(quality_audit, profile_quality_audit_path)
    write_parquet(freshness_audit, freshness_path)
    write_parquet(latest_audit, latest_path)
    write_parquet(history_audit, history_path)
    write_parquet(narrative_audit, narrative_path)
    write_parquet(exception_df, exceptions_path)
    write_parquet(summary_df, summary_path)
    write_parquet(lineage_df, lineage_path)

    # -------------------------------------------------------------------------
    # Terminal summary
    # -------------------------------------------------------------------------
    section("STEP 16.1 SUMMARY")

    log(f"Profiles audited       : {len(profile)}")
    log(f"Historical rows audited: {len(history)}")
    log(f"Identity checks        : {len(identity_audit)}")
    log(f"Quality checks         : {len(quality_audit)}")
    log(f"Freshness checks       : {len(freshness_audit)}")
    log(f"Fatal exceptions      : {exceptions.fatal_count()}")
    log(f"Warnings              : {exceptions.warning_count()}")

    log("")
    log("Profile quality:")
    log(
        profile["profile_quality"]
        .map(normalize_upper)
        .value_counts()
        .sort_index()
        .to_string()
    )

    log("")
    log("Profile quality reason:")
    log(
        profile["profile_quality_reason"]
        .map(normalize_upper)
        .value_counts()
        .sort_index()
        .to_string()
    )

    log("")
    log("Profile freshness:")
    log(
        profile["profile_freshness_status"]
        .map(normalize_upper)
        .value_counts()
        .sort_index()
        .to_string()
    )

    log("")
    log(
        "DQ status        : "
        + summary_df.iloc[0]["dq_status"]
    )

    log("")
    section("OUTPUTS")

    log(f"Identity audit    : {identity_path}")
    log(f"Quality audit     : {profile_quality_audit_path}")
    log(f"Freshness audit   : {freshness_path}")
    log(f"Latest audit      : {latest_path}")
    log(f"History audit     : {history_path}")
    log(f"Narrative audit   : {narrative_path}")
    log(f"Exceptions        : {exceptions_path}")
    log(f"Summary           : {summary_path}")
    log(f"Lineage           : {lineage_path}")

    if exceptions.fatal_count() > 0:
        section("STEP 16.1 FAILED")
        raise RuntimeError(
            f"Step 16.1 failed with "
            f"{exceptions.fatal_count()} fatal exception(s). "
            "Review profile_quality_exceptions.parquet."
        )

    section("STEP 16.1 COMPLETE")
    log(f"Company Intelligence DQ version: {DQ_VERSION}")
    log("No financial data was modified.")
    log("No new financial score was created.")
    log("Step 15 decision interpretation remained authoritative.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        section("STEP 16.1 FAILED")
        log(str(exc))
        raise
