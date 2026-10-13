"""
STEP 13.1 - FINANCIAL INTELLIGENCE QUALITY AUDIT

Purpose
-------
Independently audits Step 13 financial intelligence without modifying
Steps 10, 11, 12, 12.1 or 13.

Contracts
---------
1. Step 12.1 must PASS.
2. Feature version must be supported.
3. Step 13 signal grain: one row per CIK + fiscal_year + quarter.
4. Step 13 snapshot grain: one row per CIK.
5. Scores must be finite and in [0, 100].
6. Enumerated signal values must be valid.
7. Intelligence signals must be consistent with the underlying Step 12
   features where the source feature is available.
8. Confidence must be consistent with upstream feature quality.
9. Snapshot period must equal the latest intelligence period for the CIK.
10. Lineage/version columns must be present and consistent where supplied.
11. Legitimate source-data incompleteness, non-consecutive history and
    insufficient history are contextual conditions; they do not fail the
    audit by themselves.
12. Genuine contradictions or structural violations are FATAL.
13. The script writes audit artifacts even when the audit fails.

Output
------
financial_intelligence_validation/
    intelligence_validation_summary/
        intelligence_validation_summary.parquet
    company_intelligence_summary/
        company_intelligence_summary.parquet
    intelligence_exceptions/
        intelligence_exceptions.parquet
    intelligence_lineage/
        intelligence_lineage.parquet
"""

from __future__ import annotations

import argparse
import math
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


# =============================================================================
# CONTRACT / VERSION
# =============================================================================

AUDIT_VERSION = "1.0.0"
SUPPORTED_INTELLIGENCE_VERSIONS = {"1.0.0"}
SUPPORTED_FEATURE_VERSIONS = {"1.2.0"}

EXPECTED_SIGNAL_ROWS_TOTAL = 296
EXPECTED_SNAPSHOT_ROWS_TOTAL = 5

REQUIRED_SIGNAL_COLUMNS = {
    "cik",
    "fiscal_year",
    "quarter",
    "financial_score",
    "financial_health",
    "financial_risk_signal",
    "financial_signal_confidence",
    "revenue_growth_signal",
    "profitability_signal",
    "growth_consistency_signal",
    "momentum_signal",
}

REQUIRED_SNAPSHOT_COLUMNS = {
    "cik",
    "fiscal_year",
    "quarter",
    "financial_score",
    "financial_health",
    "financial_risk_signal",
    "financial_signal_confidence",
}

# These are the values produced by Step 13 v1.0.0.
VALID_HEALTH = {
    "INSUFFICIENT_DATA",
    "MIXED",
    "HEALTHY",
    "STRONG",
    "EXCELLENT",
    "WEAK",
}

VALID_RISK = {
    "LOW",
    "MODERATE",
    "ELEVATED",
    "HIGH",
}

VALID_CONFIDENCE = {
    "HIGH",
    "MEDIUM",
    "LOW",
}

VALID_REVENUE_SIGNALS = {
    "SOURCE_DATA_MISSING",
    "INSUFFICIENT_HISTORY",
    "NO_SIGNAL",
    "CONTRACTING",
    "STABLE",
    "DECELERATING",
    "ACCELERATING",
    "STRONG_ACCELERATION",
}

VALID_PROFITABILITY_SIGNALS = {
    "SOURCE_DATA_MISSING",
    "INSUFFICIENT_HISTORY",
    "NO_SIGNAL",
    "CONTRACTING",
    "STABLE",
    "WEAK",
    "EXPANDING",
    "STRONG_EXPANSION",
}

VALID_CONSISTENCY_SIGNALS = {
    "SOURCE_DATA_MISSING",
    "INSUFFICIENT_HISTORY",
    "MIXED",
    "CONSISTENT",
    "INCONSISTENT",
    "VOLATILE",
}

VALID_MOMENTUM_SIGNALS = {
    "SOURCE_DATA_MISSING",
    "INSUFFICIENT_HISTORY",
    "NEGATIVE",
    "STRONG_NEGATIVE",
    "NEUTRAL",
    "POSITIVE",
    "STRONG_POSITIVE",
}

# Approximate score bands are intentionally centralized. If Step 13 changes
# these bands, update the contract here rather than silently accepting drift.
HEALTH_SCORE_BANDS = {
    "MIXED": (45.0, 59.999999),
    "HEALTHY": (60.0, 74.999999),
    "STRONG": (75.0, 89.999999),
    "EXCELLENT": (90.0, 100.0),
}

# Step 13's confidence policy:
# - COMPLETE features support HIGH confidence.
# - SOURCE_DATA_INCOMPLETE supports MEDIUM/LOW, never HIGH.
# - NON_CONSECUTIVE / INSUFFICIENT_HISTORY should not be HIGH when the
#   current row itself depends on unavailable history.
#
# The audit deliberately does not require MEDIUM/LOW for every incomplete
# historical row because Step 13 can legitimately classify a row using
# currently available metrics.


# =============================================================================
# PATHS
# =============================================================================

_PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_FEATURES = (
    _PROJECT_ROOT
    / "data"
    / "gold"
    / "financials_features"
)

DEFAULT_VALIDATION = (
    _PROJECT_ROOT
    / "data"
    / "gold"
    / "financials_feature_validation"
)

DEFAULT_INTELLIGENCE = (
    _PROJECT_ROOT
    / "data"
    / "gold"
    / "financial_intelligence"
)

DEFAULT_OUTPUT = (
    _PROJECT_ROOT
    / "data"
    / "gold"
    / "financial_intelligence_validation"
)


# =============================================================================
# LOGGING
# =============================================================================

def log(message: str = "") -> None:
    print(message, flush=True)


def section(title: str) -> None:
    log("=" * 80)
    log(title)
    log("=" * 80)


# =============================================================================
# IO
# =============================================================================

def read_parquet(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        raise RuntimeError(f"Unable to read {label}: {path}: {exc}") from exc


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def find_company_feature_files(features_root: Path) -> dict[str, Path]:
    result: dict[str, Path] = {}
    if not features_root.exists():
        raise FileNotFoundError(f"Features directory not found: {features_root}")

    for path in sorted(features_root.glob("cik=*/financial_features.parquet")):
        cik = path.parent.name.split("=", 1)[-1].strip()
        if cik:
            result[cik] = path

    if not result:
        raise FileNotFoundError(
            f"No company feature parquet files found under {features_root}"
        )

    return result


def locate_intelligence_files(intelligence_root: Path) -> tuple[Path, Path, Path]:
    signal = (
        intelligence_root
        / "financial_signals"
        / "financial_signals.parquet"
    )
    snapshot = (
        intelligence_root
        / "company_financial_snapshot"
        / "company_financial_snapshot.parquet"
    )
    summary = (
        intelligence_root
        / "financial_intelligence_summary"
        / "financial_intelligence_summary.parquet"
    )
    return signal, snapshot, summary


def locate_validation_files(validation_root: Path) -> tuple[Path, Path]:
    company = (
        validation_root
        / "company_feature_summary"
        / "company_feature_summary.parquet"
    )
    summary = (
        validation_root
        / "feature_validation_summary"
        / "feature_validation_summary.parquet"
    )
    return company, summary


# =============================================================================
# GENERAL HELPERS
# =============================================================================

def clean_cik(series: pd.Series) -> pd.Series:
    return (
        series.astype("string")
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .str.zfill(10)
    )


def normalize_quarter(series: pd.Series) -> pd.Series:
    return series.astype("string").str.upper().str.strip()


def add_period_key(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["cik"] = clean_cik(out["cik"])
    out["fiscal_year"] = pd.to_numeric(
        out["fiscal_year"], errors="coerce"
    ).astype("Int64")
    out["quarter"] = normalize_quarter(out["quarter"])
    out["_period_key"] = (
        out["fiscal_year"].astype("string")
        + "-"
        + out["quarter"]
    )
    return out


def is_finite_number(value: Any) -> bool:
    try:
        return value is not None and math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def close_enough(a: Any, b: Any, tolerance: float = 1e-9) -> bool:
    if pd.isna(a) or pd.isna(b):
        return False
    try:
        return math.isclose(
            float(a),
            float(b),
            rel_tol=1e-7,
            abs_tol=tolerance,
        )
    except (TypeError, ValueError):
        return False


def get_first_existing(row: pd.Series, names: list[str]) -> Any:
    for name in names:
        if name in row.index and pd.notna(row[name]):
            return row[name]
    return np.nan


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
        fiscal_year: Any = pd.NA,
        quarter: Any = pd.NA,
        exception_type: str,
        severity: str,
        field: str | None = None,
        expected: Any = None,
        actual: Any = None,
        message: str,
    ) -> None:
        self.rows.append(
            {
                "cik": cik,
                "fiscal_year": fiscal_year,
                "quarter": quarter,
                "exception_type": exception_type,
                "severity": severity,
                "field": field,
                "expected": expected,
                "actual": actual,
                "message": message,
                "audit_version": AUDIT_VERSION,
                "detected_at_utc": datetime.now(timezone.utc),
            }
        )

    def dataframe(self) -> pd.DataFrame:
        columns = [
            "cik",
            "fiscal_year",
            "quarter",
            "exception_type",
            "severity",
            "field",
            "expected",
            "actual",
            "message",
            "audit_version",
            "detected_at_utc",
        ]
        if not self.rows:
            return pd.DataFrame(columns=columns)
        result = pd.DataFrame(self.rows, columns=columns)
        for column in ["expected", "actual"]:
            result[column] = result[column].map(
                lambda value: repr(value)
                if isinstance(value, (list, tuple, set, dict))
                else value
            )
        return result

    def fatal_count(self) -> int:
        return sum(x["severity"] == "FAIL" for x in self.rows)

    def warning_count(self) -> int:
        return sum(x["severity"] == "WARNING" for x in self.rows)


# =============================================================================
# STEP 12.1 GATE
# =============================================================================

def load_validation_gate(validation_root: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    company_path, summary_path = locate_validation_files(validation_root)

    company_df = read_parquet(
        company_path,
        "Step 12.1 company audit summary",
    )
    summary_df = read_parquet(
        summary_path,
        "Step 12.1 validation summary",
    )

    if company_df.empty:
        raise RuntimeError("Step 12.1 company audit summary is empty.")
    if summary_df.empty:
        raise RuntimeError("Step 12.1 validation summary is empty.")

    if "audit_status" not in company_df.columns:
        raise ValueError(
            "Step 12.1 company summary does not contain audit_status."
        )

    statuses = (
        company_df["audit_status"]
        .astype("string")
        .str.upper()
        .dropna()
        .unique()
        .tolist()
    )

    if any(status != "PASS" for status in statuses):
        raise RuntimeError(
            f"Step 12.1 feature-quality gate failed. "
            f"Observed statuses: {statuses}"
        )

    if "fatal_exception_rows" in company_df.columns:
        fatal = pd.to_numeric(
            company_df["fatal_exception_rows"],
            errors="coerce",
        ).fillna(0)
        if (fatal > 0).any():
            raise RuntimeError(
                "Step 12.1 feature-quality gate failed: "
                "fatal_exception_rows > 0."
            )

    return company_df, summary_df


# =============================================================================
# FEATURE NORMALIZATION
# =============================================================================

def load_features(path: Path, cik: str) -> pd.DataFrame:
    df = read_parquet(path, f"Step 12 financial features for {cik}")
    if df.empty:
        raise ValueError(f"Feature dataset is empty for {cik}.")

    df = add_period_key(df)

    required = {
        "cik",
        "fiscal_year",
        "quarter",
        "feature_data_quality",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Feature dataset for {cik} missing columns: {sorted(missing)}"
        )

    if "feature_version" in df.columns:
        versions = (
            df["feature_version"]
            .dropna()
            .astype("string")
            .str.strip()
            .unique()
            .tolist()
        )
        unsupported = [
            v for v in versions if v not in SUPPORTED_FEATURE_VERSIONS
        ]
        if unsupported:
            raise ValueError(
                f"Unsupported feature version(s) for {cik}: {unsupported}"
            )

    return df


# =============================================================================
# STRUCTURAL VALIDATION
# =============================================================================

def validate_columns(
    df: pd.DataFrame,
    required: set[str],
    label: str,
    collector: ExceptionCollector,
    cik: str,
) -> bool:
    missing = sorted(required - set(df.columns))
    if missing:
        collector.add(
            cik=cik,
            exception_type="MISSING_REQUIRED_COLUMNS",
            severity="FAIL",
            field=label,
            expected=", ".join(sorted(required)),
            actual=", ".join(df.columns.astype(str).tolist()),
            message=f"{label} is missing required columns: {missing}",
        )
        return False
    return True


def validate_grain(
    df: pd.DataFrame,
    keys: list[str],
    *,
    label: str,
    collector: ExceptionCollector,
    cik: str,
) -> None:
    if df.empty:
        collector.add(
            cik=cik,
            exception_type="EMPTY_DATASET",
            severity="FAIL",
            field=label,
            message=f"{label} is empty.",
        )
        return

    dup_mask = df.duplicated(keys, keep=False)
    if dup_mask.any():
        duplicates = (
            df.loc[dup_mask, keys]
            .value_counts()
            .reset_index(name="row_count")
        )
        for _, row in duplicates.iterrows():
            collector.add(
                cik=cik,
                fiscal_year=row.get("fiscal_year", pd.NA),
                quarter=row.get("quarter", pd.NA),
                exception_type="DUPLICATE_GRAIN",
                severity="FAIL",
                field=label,
                expected="1 row per grain",
                actual=int(row["row_count"]),
                message=(
                    f"{label} contains duplicate grain "
                    f"for {keys}."
                ),
            )


def validate_signal_schema(
    signals: pd.DataFrame,
    collector: ExceptionCollector,
    cik: str,
) -> bool:
    return validate_columns(
        signals,
        REQUIRED_SIGNAL_COLUMNS,
        "financial_signals",
        collector,
        cik,
    )


def validate_snapshot_schema(
    snapshot: pd.DataFrame,
    collector: ExceptionCollector,
    cik: str,
) -> bool:
    return validate_columns(
        snapshot,
        REQUIRED_SNAPSHOT_COLUMNS,
        "company_financial_snapshot",
        collector,
        cik,
    )


# =============================================================================
# ENUM / SCORE VALIDATION
# =============================================================================

def validate_enum_column(
    df: pd.DataFrame,
    column: str,
    valid_values: set[str],
    *,
    collector: ExceptionCollector,
    cik: str,
) -> None:
    if column not in df.columns:
        return

    values = (
        df[column]
        .dropna()
        .astype("string")
        .str.strip()
        .str.upper()
    )

    invalid_mask = ~values.isin(valid_values)

    for idx in values.index[invalid_mask]:
        row = df.loc[idx]
        collector.add(
            cik=cik,
            fiscal_year=row.get("fiscal_year", pd.NA),
            quarter=row.get("quarter", pd.NA),
            exception_type="INVALID_ENUM_VALUE",
            severity="FAIL",
            field=column,
            expected=sorted(valid_values),
            actual=row.get(column),
            message=f"Invalid {column} value.",
        )


def validate_score(
    df: pd.DataFrame,
    *,
    collector: ExceptionCollector,
    cik: str,
) -> None:
    if "financial_score" not in df.columns:
        return

    numeric = pd.to_numeric(df["financial_score"], errors="coerce")

    for idx in df.index:
        value = numeric.loc[idx]
        row = df.loc[idx]

        if pd.isna(value):
            if str(row.get("financial_health", "")).upper() == "INSUFFICIENT_DATA":
                continue
            collector.add(
                cik=cik,
                fiscal_year=row.get("fiscal_year", pd.NA),
                quarter=row.get("quarter", pd.NA),
                exception_type="INVALID_FINANCIAL_SCORE",
                severity="FAIL",
                field="financial_score",
                expected="finite numeric value in [0, 100]",
                actual=row.get("financial_score"),
                message="Financial score is null or non-numeric.",
            )
            continue

        if not np.isfinite(float(value)) or not 0 <= float(value) <= 100:
            collector.add(
                cik=cik,
                fiscal_year=row.get("fiscal_year", pd.NA),
                quarter=row.get("quarter", pd.NA),
                exception_type="FINANCIAL_SCORE_OUT_OF_RANGE",
                severity="FAIL",
                field="financial_score",
                expected="[0, 100]",
                actual=float(value),
                message="Financial score is outside the allowed range.",
            )


def validate_health_score_alignment(
    df: pd.DataFrame,
    *,
    collector: ExceptionCollector,
    cik: str,
) -> None:
    if not {"financial_score", "financial_health"} <= set(df.columns):
        return

    scores = pd.to_numeric(df["financial_score"], errors="coerce")

    for idx in df.index:
        row = df.loc[idx]
        score = scores.loc[idx]
        health = str(row["financial_health"]).upper() if pd.notna(
            row["financial_health"]
        ) else ""

        if pd.isna(score) or health not in HEALTH_SCORE_BANDS:
            continue

        low, high = HEALTH_SCORE_BANDS[health]
        if not (low <= float(score) <= high):
            collector.add(
                cik=cik,
                fiscal_year=row.get("fiscal_year", pd.NA),
                quarter=row.get("quarter", pd.NA),
                exception_type="HEALTH_SCORE_MISMATCH",
                severity="FAIL",
                field="financial_health",
                expected=f"{health} score range [{low}, {high}]",
                actual=float(score),
                message=(
                    "financial_health does not match the configured "
                    "financial_score band."
                ),
            )


# =============================================================================
# FEATURE / INTELLIGENCE SEMANTIC VALIDATION
# =============================================================================

def validate_revenue_signal(row: pd.Series) -> tuple[bool, str]:
    growth = get_first_existing(
        row,
        ["revenue_yoy_growth_pct", "revenue_growth_yoy_pct"],
    )
    acceleration = get_first_existing(
        row,
        ["revenue_growth_acceleration"],
    )
    signal = str(row.get("revenue_growth_signal", "")).upper()

    if signal in {
        "SOURCE_DATA_MISSING",
        "INSUFFICIENT_HISTORY",
        "NO_SIGNAL",
    }:
        return True, ""

    if pd.isna(growth):
        return False, "Revenue signal exists but revenue growth is missing."

    # Step 13's semantics:
    # acceleration positive -> accelerating; negative -> decelerating.
    # Strong acceleration is reserved for clearly positive acceleration.
    if signal == "STRONG_ACCELERATION":
        if pd.isna(acceleration) or float(acceleration) <= 0:
            return False, (
                "STRONG_ACCELERATION requires positive "
                "revenue_growth_acceleration."
            )
    elif signal == "ACCELERATING":
        if pd.isna(acceleration) or float(acceleration) <= 0:
            return False, (
                "ACCELERATING requires positive "
                "revenue_growth_acceleration."
            )
    elif signal == "DECELERATING":
        if pd.isna(acceleration) or float(acceleration) >= 0:
            return False, (
                "DECELERATING requires negative "
                "revenue_growth_acceleration."
            )
    return True, ""


def validate_profitability_signal(row: pd.Series) -> tuple[bool, str]:
    margin = get_first_existing(
        row,
        ["operating_margin"],
    )
    margin_change = get_first_existing(
        row,
        [
            "operating_margin_yoy_change",
            "operating_margin_qoq_change",
        ],
    )
    signal = str(row.get("profitability_signal", "")).upper()

    if signal in {
        "SOURCE_DATA_MISSING",
        "INSUFFICIENT_HISTORY",
        "NO_SIGNAL",
    }:
        return True, ""

    if pd.isna(margin):
        return False, "Profitability signal exists but operating margin is missing."

    if signal in {"EXPANDING", "STRONG_EXPANSION"}:
        if pd.notna(margin_change) and float(margin_change) < 0:
            return False, (
                f"{signal} conflicts with negative "
                "operating_margin_qoq_change."
            )

    if signal == "CONTRACTING":
        if pd.notna(margin_change) and float(margin_change) > 0:
            return False, (
                "CONTRACTING conflicts with positive "
                "operating_margin_qoq_change."
            )

    return True, ""


def validate_consistency_signal(row: pd.Series) -> tuple[bool, str]:
    signal = str(
        row.get("growth_consistency_signal", "")
    ).upper()

    if signal in {
        "SOURCE_DATA_MISSING",
        "INSUFFICIENT_HISTORY",
        "MIXED",
        "CONSISTENT",
        "INCONSISTENT",
        "VOLATILE",
        "NO_SIGNAL",
        "",
        "NAN",
    }:
        return True, ""

    return False, "Unknown growth consistency signal."


def validate_momentum_signal(row: pd.Series) -> tuple[bool, str]:
    signal = str(row.get("momentum_signal", "")).upper()

    if signal in {
        "SOURCE_DATA_MISSING",
        "INSUFFICIENT_HISTORY",
        "NEGATIVE",
        "STRONG_NEGATIVE",
        "NEUTRAL",
        "POSITIVE",
        "STRONG_POSITIVE",
        "NO_SIGNAL",
        "",
        "NAN",
    }:
        return True, ""

    return False, "Unknown momentum signal."


def validate_signal_feature_consistency(
    signals: pd.DataFrame,
    features: pd.DataFrame,
    collector: ExceptionCollector,
    cik: str,
) -> None:
    feature = add_period_key(features)
    sig = add_period_key(signals)

    # Avoid duplicate joins even if a bad feature file contains duplicates.
    feature_lookup = feature.drop_duplicates(
        "_period_key",
        keep="first",
    ).set_index("_period_key")

    for idx, row in sig.iterrows():
        key = row["_period_key"]
        if key not in feature_lookup.index:
            collector.add(
                cik=cik,
                fiscal_year=row.get("fiscal_year", pd.NA),
                quarter=row.get("quarter", pd.NA),
                exception_type="MISSING_FEATURE_LINEAGE",
                severity="FAIL",
                field="_period_key",
                expected="matching Step 12 feature row",
                actual=key,
                message="No matching Step 12 feature row exists.",
            )
            continue

        feature_row = feature_lookup.loc[key]
        combined = pd.concat(
            [
                feature_row.rename("_feature"),
                row.rename("_signal"),
            ],
            axis=0,
        )

        # Build a normal row-like object from the two sources.
        check = row.copy()
        for col in feature_row.index:
            if col not in check.index or pd.isna(check[col]):
                check[col] = feature_row[col]

        ok, msg = validate_revenue_signal(check)
        if not ok:
            collector.add(
                cik=cik,
                fiscal_year=row.get("fiscal_year", pd.NA),
                quarter=row.get("quarter", pd.NA),
                exception_type="REVENUE_SIGNAL_CONTRADICTION",
                severity="FAIL",
                field="revenue_growth_signal",
                actual=row.get("revenue_growth_signal"),
                message=msg,
            )

        ok, msg = validate_profitability_signal(check)
        if not ok:
            collector.add(
                cik=cik,
                fiscal_year=row.get("fiscal_year", pd.NA),
                quarter=row.get("quarter", pd.NA),
                exception_type="PROFITABILITY_SIGNAL_CONTRADICTION",
                severity="FAIL",
                field="profitability_signal",
                actual=row.get("profitability_signal"),
                message=msg,
            )

        ok, msg = validate_consistency_signal(check)
        if not ok:
            collector.add(
                cik=cik,
                fiscal_year=row.get("fiscal_year", pd.NA),
                quarter=row.get("quarter", pd.NA),
                exception_type="CONSISTENCY_SIGNAL_INVALID",
                severity="FAIL",
                field="growth_consistency_signal",
                actual=row.get("growth_consistency_signal"),
                message=msg,
            )

        ok, msg = validate_momentum_signal(check)
        if not ok:
            collector.add(
                cik=cik,
                fiscal_year=row.get("fiscal_year", pd.NA),
                quarter=row.get("quarter", pd.NA),
                exception_type="MOMENTUM_SIGNAL_INVALID",
                severity="FAIL",
                field="momentum_signal",
                actual=row.get("momentum_signal"),
                message=msg,
            )


# =============================================================================
# CONFIDENCE / QUALITY VALIDATION
# =============================================================================

def validate_confidence_alignment(
    signals: pd.DataFrame,
    features: pd.DataFrame,
    collector: ExceptionCollector,
    cik: str,
) -> None:
    feature = add_period_key(features)
    sig = add_period_key(signals)

    feature_lookup = feature.drop_duplicates(
        "_period_key",
        keep="first",
    ).set_index("_period_key")

    for _, row in sig.iterrows():
        key = row["_period_key"]
        if key not in feature_lookup.index:
            continue

        f = feature_lookup.loc[key]
        confidence = (
            str(row.get("financial_signal_confidence", "")).upper()
        )
        quality = (
            str(f.get("feature_data_quality", "")).upper()
        )

        # A source-incomplete feature row cannot claim HIGH confidence.
        if quality == "SOURCE_DATA_INCOMPLETE" and confidence == "HIGH":
            collector.add(
                cik=cik,
                fiscal_year=row.get("fiscal_year", pd.NA),
                quarter=row.get("quarter", pd.NA),
                exception_type="CONFIDENCE_OVERSTATED",
                severity="FAIL",
                field="financial_signal_confidence",
                expected="MEDIUM or LOW",
                actual=confidence,
                message=(
                    "Step 13 marks a source-incomplete feature row HIGH "
                    "confidence."
                ),
            )

        # If all critical feature inputs are absent, a confident score is
        # invalid. This is a genuine intelligence-contract failure.
        critical = [
            f.get("revenue_value", np.nan),
            f.get("operating_margin", np.nan),
            f.get("ttm_revenue", np.nan),
        ]
        available = sum(pd.notna(x) for x in critical)

        if available == 0 and confidence in {"HIGH", "MEDIUM"}:
            collector.add(
                cik=cik,
                fiscal_year=row.get("fiscal_year", pd.NA),
                quarter=row.get("quarter", pd.NA),
                exception_type="CONFIDENCE_WITHOUT_EVIDENCE",
                severity="FAIL",
                field="financial_signal_confidence",
                expected="LOW or no intelligence",
                actual=confidence,
                message=(
                    "Confidence is non-low despite all core financial "
                    "evidence being unavailable."
                ),
            )


# =============================================================================
# RISK / SIGNAL SANITY
# =============================================================================

def validate_risk_sanity(
    signals: pd.DataFrame,
    features: pd.DataFrame,
    collector: ExceptionCollector,
    cik: str,
) -> None:
    feature = add_period_key(features)
    sig = add_period_key(signals)

    feature_lookup = feature.drop_duplicates(
        "_period_key",
        keep="first",
    ).set_index("_period_key")

    for _, row in sig.iterrows():
        key = row["_period_key"]
        if key not in feature_lookup.index:
            continue

        f = feature_lookup.loc[key]
        risk = str(row.get("financial_risk_signal", "")).upper()

        growth = get_first_existing(
            f,
            ["revenue_yoy_growth_pct", "revenue_growth_yoy_pct"],
        )
        margin_change = get_first_existing(
            f,
            ["operating_margin_qoq_change"],
        )

        # Do not force a particular risk classification from a single metric.
        # Instead, reject only impossible claims: LOW risk while the feature
        # layer explicitly marks the current row as source-incomplete and the
        # intelligence has no usable core evidence.
        if risk == "LOW":
            core_available = sum(
                pd.notna(
                    get_first_existing(
                        f,
                        [
                            "revenue_value",
                            "operating_margin",
                            "ttm_revenue",
                            "ttm_operating_margin",
                        ],
                    )
                )
                for _ in [0]
            )
            health = str(row.get("financial_health", "")).upper()
            score_missing = pd.isna(row.get("financial_score"))
            if core_available == 0 and health != "INSUFFICIENT_DATA" and not score_missing:
                collector.add(
                    cik=cik,
                    fiscal_year=row.get("fiscal_year", pd.NA),
                    quarter=row.get("quarter", pd.NA),
                    exception_type="RISK_WITHOUT_EVIDENCE",
                    severity="FAIL",
                    field="financial_risk_signal",
                    expected="non-low risk or insufficient intelligence",
                    actual=risk,
                    message="LOW financial risk has no core financial evidence.",
                )


# =============================================================================
# SNAPSHOT VALIDATION
# =============================================================================

def validate_snapshot(
    signals: pd.DataFrame,
    snapshot: pd.DataFrame,
    collector: ExceptionCollector,
    cik: str,
) -> None:
    sig = add_period_key(signals)
    snap = add_period_key(snapshot)

    if len(snap) != 1:
        return

    srow = snap.iloc[0]

    latest_year = sig["fiscal_year"].max()
    latest_rows = sig.loc[
        sig["fiscal_year"].eq(latest_year)
    ].copy()

    if latest_rows.empty:
        return

    latest_rows["_quarter_num"] = (
        latest_rows["quarter"]
        .str.extract(r"Q(\d)", expand=False)
        .astype("Int64")
    )
    latest = latest_rows.sort_values(
        ["fiscal_year", "_quarter_num"],
        na_position="last",
    ).iloc[-1]

    for field in ["fiscal_year", "quarter"]:
        if str(srow.get(field)) != str(latest.get(field)):
            collector.add(
                cik=cik,
                fiscal_year=srow.get("fiscal_year", pd.NA),
                quarter=srow.get("quarter", pd.NA),
                exception_type="SNAPSHOT_PERIOD_MISMATCH",
                severity="FAIL",
                field=field,
                expected=latest.get(field),
                actual=srow.get(field),
                message=(
                    "Company snapshot does not point to the latest "
                    "intelligence period."
                ),
            )

    for field in [
        "financial_score",
        "financial_health",
        "financial_risk_signal",
        "financial_signal_confidence",
    ]:
        if field in latest.index and field in srow.index:
            a = latest[field]
            b = srow[field]
            if pd.isna(a) and pd.isna(b):
                continue
            if str(a) != str(b):
                if field == "financial_score":
                    same = close_enough(a, b)
                else:
                    same = False

                if not same:
                    collector.add(
                        cik=cik,
                        fiscal_year=srow.get("fiscal_year", pd.NA),
                        quarter=srow.get("quarter", pd.NA),
                        exception_type="SNAPSHOT_VALUE_MISMATCH",
                        severity="FAIL",
                        field=field,
                        expected=a,
                        actual=b,
                        message=(
                            "Company snapshot value does not match the "
                            "latest signal row."
                        ),
                    )


# =============================================================================
# VERSION / LINEAGE
# =============================================================================

def validate_versions(
    signals: pd.DataFrame,
    snapshot: pd.DataFrame,
    *,
    collector: ExceptionCollector,
    cik: str,
) -> None:
    for label, df, supported in [
        (
            "financial_signals",
            signals,
            SUPPORTED_INTELLIGENCE_VERSIONS,
        ),
        (
            "company_financial_snapshot",
            snapshot,
            SUPPORTED_INTELLIGENCE_VERSIONS,
        ),
    ]:
        version_columns = [
            c
            for c in [
                "intelligence_version",
                "feature_version",
            ]
            if c in df.columns
        ]

        for col in version_columns:
            values = (
                df[col]
                .dropna()
                .astype("string")
                .str.strip()
                .unique()
                .tolist()
            )

            if col == "intelligence_version":
                allowed = SUPPORTED_INTELLIGENCE_VERSIONS
            else:
                allowed = SUPPORTED_FEATURE_VERSIONS

            unsupported = [v for v in values if v not in allowed]

            if unsupported:
                collector.add(
                    cik=cik,
                    exception_type="UNSUPPORTED_VERSION",
                    severity="FAIL",
                    field=col,
                    expected=sorted(allowed),
                    actual=unsupported,
                    message=(
                        f"{label} contains unsupported {col} value(s)."
                    ),
                )


# =============================================================================
# PER-COMPANY AUDIT
# =============================================================================

def audit_company(
    cik: str,
    feature_path: Path,
    signals: pd.DataFrame,
    snapshot: pd.DataFrame,
    collector: ExceptionCollector,
) -> dict[str, Any]:
    features = load_features(feature_path, cik)

    sig = add_period_key(signals.loc[signals["cik"].eq(cik)].copy())
    snap = add_period_key(snapshot.loc[snapshot["cik"].eq(cik)].copy())

    company_exceptions_before = len(collector.rows)

    validate_signal_schema(sig, collector, cik)
    validate_snapshot_schema(snap, collector, cik)

    if REQUIRED_SIGNAL_COLUMNS <= set(sig.columns):
        validate_grain(
            sig,
            ["cik", "fiscal_year", "quarter"],
            label="financial_signals",
            collector=collector,
            cik=cik,
        )

        validate_enum_column(
            sig,
            "financial_health",
            VALID_HEALTH,
            collector=collector,
            cik=cik,
        )
        validate_enum_column(
            sig,
            "financial_risk_signal",
            VALID_RISK,
            collector=collector,
            cik=cik,
        )
        validate_enum_column(
            sig,
            "financial_signal_confidence",
            VALID_CONFIDENCE,
            collector=collector,
            cik=cik,
        )
        validate_enum_column(
            sig,
            "revenue_growth_signal",
            VALID_REVENUE_SIGNALS,
            collector=collector,
            cik=cik,
        )
        validate_enum_column(
            sig,
            "profitability_signal",
            VALID_PROFITABILITY_SIGNALS,
            collector=collector,
            cik=cik,
        )
        validate_enum_column(
            sig,
            "growth_consistency_signal",
            VALID_CONSISTENCY_SIGNALS,
            collector=collector,
            cik=cik,
        )
        validate_enum_column(
            sig,
            "momentum_signal",
            VALID_MOMENTUM_SIGNALS,
            collector=collector,
            cik=cik,
        )

        validate_score(
            sig,
            collector=collector,
            cik=cik,
        )
        validate_health_score_alignment(
            sig,
            collector=collector,
            cik=cik,
        )

        validate_signal_feature_consistency(
            sig,
            features,
            collector,
            cik,
        )

        validate_confidence_alignment(
            sig,
            features,
            collector,
            cik,
        )

        validate_risk_sanity(
            sig,
            features,
            collector,
            cik,
        )

        validate_versions(
            sig,
            snap,
            collector=collector,
            cik=cik,
        )

    if REQUIRED_SNAPSHOT_COLUMNS <= set(snap.columns):
        validate_grain(
            snap,
            ["cik"],
            label="company_financial_snapshot",
            collector=collector,
            cik=cik,
        )
        validate_score(
            snap,
            collector=collector,
            cik=cik,
        )
        validate_health_score_alignment(
            snap,
            collector=collector,
            cik=cik,
        )
        validate_snapshot(
            sig,
            snap,
            collector,
            cik,
        )

    company_exceptions = collector.rows[company_exceptions_before:]
    fatal = sum(
        1 for x in company_exceptions if x["severity"] == "FAIL"
    )
    warning = sum(
        1 for x in company_exceptions if x["severity"] == "WARNING"
    )

    quality_counts = (
        features["feature_data_quality"]
        .astype("string")
        .str.upper()
        .value_counts()
        .to_dict()
    )

    return {
        "cik": cik,
        "feature_rows": len(features),
        "signal_rows": len(sig),
        "snapshot_rows": len(snap),
        "complete_feature_rows": int(
            quality_counts.get("COMPLETE", 0)
        ),
        "partial_feature_rows": int(
            quality_counts.get("PARTIAL", 0)
        ),
        "source_incomplete_feature_rows": int(
            quality_counts.get("SOURCE_DATA_INCOMPLETE", 0)
        ),
        "non_consecutive_feature_rows": int(
            quality_counts.get("NON_CONSECUTIVE_DATA", 0)
        ),
        "insufficient_history_feature_rows": int(
            quality_counts.get("INSUFFICIENT_HISTORY", 0)
        ),
        "high_confidence_rows": int(
            (sig["financial_signal_confidence"] == "HIGH").sum()
            if "financial_signal_confidence" in sig.columns
            else 0
        ),
        "medium_confidence_rows": int(
            (sig["financial_signal_confidence"] == "MEDIUM").sum()
            if "financial_signal_confidence" in sig.columns
            else 0
        ),
        "low_confidence_rows": int(
            (sig["financial_signal_confidence"] == "LOW").sum()
            if "financial_signal_confidence" in sig.columns
            else 0
        ),
        "fatal_exception_rows": fatal,
        "warning_exception_rows": warning,
        "audit_status": "FAIL" if fatal else (
            "WARNING" if warning else "PASS"
        ),
        "audit_version": AUDIT_VERSION,
        "intelligence_version": (
            sorted(
                set(
                    sig["intelligence_version"]
                    .dropna()
                    .astype(str)
                )
            )[0]
            if "intelligence_version" in sig.columns
            and not sig["intelligence_version"].dropna().empty
            else "UNKNOWN"
        ),
    }


# =============================================================================
# MAIN
# =============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Step 13.1 - Financial Intelligence Quality Audit"
    )
    parser.add_argument(
        "--features",
        type=Path,
        default=DEFAULT_FEATURES,
    )
    parser.add_argument(
        "--validation",
        type=Path,
        default=DEFAULT_VALIDATION,
    )
    parser.add_argument(
        "--intelligence",
        type=Path,
        default=DEFAULT_INTELLIGENCE,
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    section("STEP 13.1 - FINANCIAL INTELLIGENCE QUALITY AUDIT")
    log(f"Features            : {args.features}")
    log(f"Validation          : {args.validation}")
    log(f"Intelligence        : {args.intelligence}")
    log(f"Output              : {args.output}")
    log(f"Audit version       : {AUDIT_VERSION}")
    log(
        f"Supported intelligence: "
        f"{sorted(SUPPORTED_INTELLIGENCE_VERSIONS)}"
    )
    log(
        f"Supported features    : "
        f"{sorted(SUPPORTED_FEATURE_VERSIONS)}"
    )

    # -------------------------------------------------------------------------
    # Step 12.1 gate
    # -------------------------------------------------------------------------
    log("Checking Step 12.1 feature-quality gate...")
    _, _ = load_validation_gate(args.validation)
    log("Step 12.1 gate       : PASS")

    # -------------------------------------------------------------------------
    # Load Step 13 outputs
    # -------------------------------------------------------------------------
    signal_path, snapshot_path, summary_path = locate_intelligence_files(
        args.intelligence
    )

    signals = read_parquet(
        signal_path,
        "Step 13 financial signals",
    )
    snapshot = read_parquet(
        snapshot_path,
        "Step 13 company financial snapshot",
    )

    # Summary is loaded to verify that Step 13 produced the expected artifact.
    intelligence_summary = read_parquet(
        summary_path,
        "Step 13 financial intelligence summary",
    )

    if signals.empty:
        raise RuntimeError("Step 13 financial signals are empty.")
    if snapshot.empty:
        raise RuntimeError("Step 13 company snapshot is empty.")
    if intelligence_summary.empty:
        raise RuntimeError(
            "Step 13 financial intelligence summary is empty."
        )

    signals["cik"] = clean_cik(signals["cik"])
    snapshot["cik"] = clean_cik(snapshot["cik"])
    for published_column, canonical_column in [
        ("latest_fiscal_year", "fiscal_year"),
        ("latest_quarter", "quarter"),
    ]:
        if canonical_column not in snapshot.columns:
            if published_column not in snapshot.columns:
                continue
            snapshot[canonical_column] = snapshot[published_column]

    companies = find_company_feature_files(args.features)
    discovered_ciks = sorted(companies.keys())

    section(f"Companies discovered: {len(discovered_ciks)}")

    if len(discovered_ciks) == 0:
        raise RuntimeError("No companies discovered.")

    # Global structural checks.
    global_collector = ExceptionCollector()

    validate_columns(
        signals,
        REQUIRED_SIGNAL_COLUMNS,
        "financial_signals",
        global_collector,
        "GLOBAL",
    )
    validate_columns(
        snapshot,
        REQUIRED_SNAPSHOT_COLUMNS,
        "company_financial_snapshot",
        global_collector,
        "GLOBAL",
    )

    if len(signals) != EXPECTED_SIGNAL_ROWS_TOTAL:
        global_collector.add(
            cik="GLOBAL",
            exception_type="UNEXPECTED_SIGNAL_ROW_COUNT",
            severity="WARNING",
            field="financial_signals",
            expected=EXPECTED_SIGNAL_ROWS_TOTAL,
            actual=len(signals),
            message=(
                "Signal row count differs from the current fixture "
                "expectation. This is a warning, not a contract failure."
            ),
        )

    if len(snapshot) != EXPECTED_SNAPSHOT_ROWS_TOTAL:
        global_collector.add(
            cik="GLOBAL",
            exception_type="UNEXPECTED_SNAPSHOT_ROW_COUNT",
            severity="WARNING",
            field="company_financial_snapshot",
            expected=EXPECTED_SNAPSHOT_ROWS_TOTAL,
            actual=len(snapshot),
            message=(
                "Snapshot row count differs from the current fixture "
                "expectation. This is a warning, not a contract failure."
            ),
        )

    missing_signal_ciks = sorted(
        set(discovered_ciks) - set(signals["cik"].dropna().unique())
    )
    extra_signal_ciks = sorted(
        set(signals["cik"].dropna().unique()) - set(discovered_ciks)
    )

    for cik in missing_signal_ciks:
        global_collector.add(
            cik=cik,
            exception_type="MISSING_COMPANY_SIGNAL_DATA",
            severity="FAIL",
            message="Company feature data exists but Step 13 signals do not.",
        )

    for cik in extra_signal_ciks:
        global_collector.add(
            cik=cik,
            exception_type="ORPHAN_SIGNAL_COMPANY",
            severity="FAIL",
            message=(
                "Step 13 contains a CIK that is not present in the "
                "Step 12 feature population."
            ),
        )

    company_summaries: list[dict[str, Any]] = []
    all_exceptions = global_collector.rows.copy()

    for cik in discovered_ciks:
        section(f"PROCESSING CIK: {cik}")

        feature_path = companies[cik]
        company_signals = signals.loc[
            signals["cik"].eq(cik)
        ].copy()
        company_snapshot = snapshot.loc[
            snapshot["cik"].eq(cik)
        ].copy()

        log(f"Feature rows        : loading {feature_path}")
        log(f"Signal rows         : {len(company_signals)}")
        log(f"Snapshot rows       : {len(company_snapshot)}")

        collector = ExceptionCollector()

        try:
            summary = audit_company(
                cik,
                feature_path,
                company_signals,
                company_snapshot,
                collector,
            )

            company_summaries.append(summary)

            log(
                f"Feature rows        : {summary['feature_rows']}"
            )
            log(
                f"Signal rows         : {summary['signal_rows']}"
            )
            log(
                f"Snapshot rows       : {summary['snapshot_rows']}"
            )
            log(
                f"COMPLETE             : "
                f"{summary['complete_feature_rows']}"
            )
            log(
                f"PARTIAL              : "
                f"{summary['partial_feature_rows']}"
            )
            log(
                f"SOURCE_DATA_INCOMPLETE: "
                f"{summary['source_incomplete_feature_rows']}"
            )
            log(
                f"NON_CONSECUTIVE      : "
                f"{summary['non_consecutive_feature_rows']}"
            )
            log(
                f"INSUFFICIENT_HISTORY : "
                f"{summary['insufficient_history_feature_rows']}"
            )
            log(
                f"HIGH confidence      : "
                f"{summary['high_confidence_rows']}"
            )
            log(
                f"MEDIUM confidence    : "
                f"{summary['medium_confidence_rows']}"
            )
            log(
                f"LOW confidence       : "
                f"{summary['low_confidence_rows']}"
            )
            log(
                f"Fatal exceptions     : "
                f"{summary['fatal_exception_rows']}"
            )
            log(
                f"Warnings             : "
                f"{summary['warning_exception_rows']}"
            )
            log(f"AUDIT STATUS         : {summary['audit_status']}")

        except Exception as exc:
            collector.add(
                cik=cik,
                exception_type="AUDIT_EXECUTION_ERROR",
                severity="FAIL",
                message=str(exc),
            )

            company_summaries.append(
                {
                    "cik": cik,
                    "feature_rows": pd.NA,
                    "signal_rows": len(company_signals),
                    "snapshot_rows": len(company_snapshot),
                    "complete_feature_rows": pd.NA,
                    "partial_feature_rows": pd.NA,
                    "source_incomplete_feature_rows": pd.NA,
                    "non_consecutive_feature_rows": pd.NA,
                    "insufficient_history_feature_rows": pd.NA,
                    "high_confidence_rows": pd.NA,
                    "medium_confidence_rows": pd.NA,
                    "low_confidence_rows": pd.NA,
                    "fatal_exception_rows": 1,
                    "warning_exception_rows": 0,
                    "audit_status": "FAIL",
                    "audit_version": AUDIT_VERSION,
                    "intelligence_version": "UNKNOWN",
                }
            )

            log(f"AUDIT EXECUTION ERROR: {exc}")
            traceback.print_exc()

        all_exceptions.extend(collector.rows)

    # -------------------------------------------------------------------------
    # Global duplicate checks after company processing
    # -------------------------------------------------------------------------
    if REQUIRED_SIGNAL_COLUMNS <= set(signals.columns):
        duplicate_mask = signals.duplicated(
            ["cik", "fiscal_year", "quarter"],
            keep=False,
        )
        if duplicate_mask.any():
            dupes = (
                signals.loc[
                    duplicate_mask,
                    ["cik", "fiscal_year", "quarter"],
                ]
                .value_counts()
                .reset_index(name="row_count")
            )
            for _, row in dupes.iterrows():
                all_exceptions.append(
                    {
                        "cik": row["cik"],
                        "fiscal_year": row["fiscal_year"],
                        "quarter": row["quarter"],
                        "exception_type": "GLOBAL_SIGNAL_DUPLICATE_GRAIN",
                        "severity": "FAIL",
                        "field": "financial_signals",
                        "expected": "1",
                        "actual": int(row["row_count"]),
                        "message": (
                            "Global Step 13 signal grain contains duplicates."
                        ),
                        "audit_version": AUDIT_VERSION,
                        "detected_at_utc": datetime.now(timezone.utc),
                    }
                )

    if REQUIRED_SNAPSHOT_COLUMNS <= set(snapshot.columns):
        duplicate_snapshot = snapshot.duplicated(
            ["cik"],
            keep=False,
        )
        if duplicate_snapshot.any():
            for cik in sorted(
                snapshot.loc[duplicate_snapshot, "cik"].unique()
            ):
                all_exceptions.append(
                    {
                        "cik": cik,
                        "fiscal_year": pd.NA,
                        "quarter": pd.NA,
                        "exception_type": "GLOBAL_SNAPSHOT_DUPLICATE_GRAIN",
                        "severity": "FAIL",
                        "field": "company_financial_snapshot",
                        "expected": "1",
                        "actual": int(
                            snapshot.loc[
                                snapshot["cik"].eq(cik)
                            ].shape[0]
                        ),
                        "message": (
                            "Global Step 13 snapshot grain contains duplicates."
                        ),
                        "audit_version": AUDIT_VERSION,
                        "detected_at_utc": datetime.now(timezone.utc),
                    }
                )

    # -------------------------------------------------------------------------
    # Build outputs
    # -------------------------------------------------------------------------
    exception_df = pd.DataFrame(all_exceptions)

    if exception_df.empty:
        exception_df = ExceptionCollector().dataframe()
    else:
        # Ensure deterministic column order.
        desired = [
            "cik",
            "fiscal_year",
            "quarter",
            "exception_type",
            "severity",
            "field",
            "expected",
            "actual",
            "message",
            "audit_version",
            "detected_at_utc",
        ]
        for col in desired:
            if col not in exception_df.columns:
                exception_df[col] = pd.NA
        exception_df = exception_df[desired]
        for col in ["expected", "actual"]:
            exception_df[col] = exception_df[col].astype("string")

    company_df = pd.DataFrame(company_summaries)

    fatal_count = int(
        (exception_df["severity"] == "FAIL").sum()
        if not exception_df.empty
        else 0
    )
    warning_count = int(
        (exception_df["severity"] == "WARNING").sum()
        if not exception_df.empty
        else 0
    )

    total_signal_rows = int(len(signals))
    total_snapshot_rows = int(len(snapshot))

    validation_summary = pd.DataFrame(
        [
            {
                "audit_version": AUDIT_VERSION,
                "intelligence_version": (
                    sorted(
                        set(
                            signals["intelligence_version"]
                            .dropna()
                            .astype(str)
                        )
                    )[0]
                    if "intelligence_version" in signals.columns
                    and not signals["intelligence_version"].dropna().empty
                    else "UNKNOWN"
                ),
                "feature_version": (
                    sorted(
                        set(
                            signals["feature_version"]
                            .dropna()
                            .astype(str)
                        )
                    )[0]
                    if "feature_version" in signals.columns
                    and not signals["feature_version"].dropna().empty
                    else "1.2.0"
                ),
                "validation_rows": total_signal_rows,
                "snapshot_rows": total_snapshot_rows,
                "companies_processed": len(company_summaries),
                "pass_rows": total_signal_rows if fatal_count == 0 else 0,
                "warning_rows": warning_count,
                "fail_rows": fatal_count,
                "fatal_exceptions": fatal_count,
                "warning_exceptions": warning_count,
                "audit_status": (
                    "FAIL"
                    if fatal_count > 0
                    else "WARNING"
                    if warning_count > 0
                    else "PASS"
                ),
                "audited_at_utc": datetime.now(timezone.utc),
            }
        ]
    )

    lineage_df = pd.DataFrame(
        [
            {
                "cik": cik,
                "feature_path": str(companies[cik]),
                "intelligence_signals_path": str(signal_path),
                "intelligence_snapshot_path": str(snapshot_path),
                "step12_1_validation_path": str(args.validation),
                "feature_version": (
                    company_df.loc[
                        company_df["cik"].eq(cik),
                        "intelligence_version",
                    ].iloc[0]
                    if not company_df.empty
                    and "intelligence_version" in company_df.columns
                    and (company_df["cik"] == cik).any()
                    else "UNKNOWN"
                ),
                "intelligence_version": (
                    (
                        signals.loc[
                            signals["cik"].eq(cik),
                            "intelligence_version",
                        ]
                        .dropna()
                        .astype(str)
                        .iloc[0]
                    )
                    if "intelligence_version" in signals.columns
                    and not signals.loc[
                        signals["cik"].eq(cik),
                        "intelligence_version",
                    ].dropna().empty
                    else "UNKNOWN"
                ),
                "audit_version": AUDIT_VERSION,
                "lineage_status": (
                    "PASS"
                    if not any(
                        e["cik"] == cik and e["severity"] == "FAIL"
                        for e in all_exceptions
                    )
                    else "FAIL"
                ),
                "audited_at_utc": datetime.now(timezone.utc),
            }
            for cik in discovered_ciks
        ]
    )

    # -------------------------------------------------------------------------
    # Persist
    # -------------------------------------------------------------------------
    summary_path = (
        args.output
        / "intelligence_validation_summary"
        / "intelligence_validation_summary.parquet"
    )
    company_summary_path = (
        args.output
        / "company_intelligence_summary"
        / "company_intelligence_summary.parquet"
    )
    exception_path = (
        args.output
        / "intelligence_exceptions"
        / "intelligence_exceptions.parquet"
    )
    lineage_path = (
        args.output
        / "intelligence_lineage"
        / "intelligence_lineage.parquet"
    )

    write_parquet(validation_summary, summary_path)
    write_parquet(company_df, company_summary_path)
    write_parquet(exception_df, exception_path)
    write_parquet(lineage_df, lineage_path)

    # -------------------------------------------------------------------------
    # Final console output
    # -------------------------------------------------------------------------
    section("STEP 13.1 SUMMARY")
    log(f"Total validation rows : {total_signal_rows}")
    log(f"Total snapshot rows   : {total_snapshot_rows}")
    log(f"PASS                  : {total_signal_rows if fatal_count == 0 else 0}")
    log(f"WARNING               : {warning_count}")
    log(f"FAIL                  : {fatal_count}")
    log(f"Fatal exceptions      : {fatal_count}")
    log(f"Warning exceptions    : {warning_count}")

    section("COMPANY INTELLIGENCE AUDIT SUMMARY")
    if not company_df.empty:
        display_cols = [
            "cik",
            "feature_rows",
            "signal_rows",
            "snapshot_rows",
            "complete_feature_rows",
            "source_incomplete_feature_rows",
            "non_consecutive_feature_rows",
            "insufficient_history_feature_rows",
            "high_confidence_rows",
            "medium_confidence_rows",
            "low_confidence_rows",
            "fatal_exception_rows",
            "warning_exception_rows",
            "audit_status",
        ]
        display_cols = [
            c for c in display_cols if c in company_df.columns
        ]
        log(company_df[display_cols].to_string(index=False))

    section("OUTPUTS")
    log(str(summary_path))
    log(str(company_summary_path))
    log(str(exception_path))
    log(str(lineage_path))

    section("STEP 13.1 COMPLETE" if fatal_count == 0 else "STEP 13.1 FAILED")

    if fatal_count == 0:
        log("All companies passed the financial intelligence data contract.")
        log(
            "Legitimate upstream source-data incompleteness, "
            "non-consecutive history and insufficient history remain "
            "contextual conditions and do not fail the audit."
        )
    else:
        log(
            f"Financial intelligence audit failed with "
            f"{fatal_count} fatal exception(s). "
            f"Review intelligence_exceptions.parquet."
        )
        raise RuntimeError(
            f"Step 13.1 failed with {fatal_count} fatal exception(s)."
        )


if __name__ == "__main__":
    main()
