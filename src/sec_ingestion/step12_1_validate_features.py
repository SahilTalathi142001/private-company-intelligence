"""
STEP 12.1 - FINANCIAL FEATURE QUALITY AUDIT

Read-only production audit for Step 12 financial features.

Inputs:
  data/gold/financials_quarterly_final/cik=<CIK>/financials_quarterly.parquet
  data/gold/financials_analytics/cik=<CIK>/financials_analytics.parquet
  data/gold/financials_features/cik=<CIK>/financial_features.parquet

Outputs:
  data/gold/financials_feature_validation/
    feature_validation_summary/feature_validation_summary.parquet
    company_feature_summary/company_feature_summary.parquet
    feature_exceptions/feature_exceptions.parquet
    feature_lineage/feature_lineage.parquet

The audit never modifies or repairs Steps 10-12.
Legitimate source-data incompleteness is reported, not treated as failure.
Structural/data-contract violations fail the process.
"""

from __future__ import annotations

import re
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


# ============================================================================
# CONFIGURATION
# ============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

FINAL_GOLD_DIR = PROJECT_ROOT / "data" / "gold" / "financials_quarterly_final"
ANALYTICS_DIR = PROJECT_ROOT / "data" / "gold" / "financials_analytics"
FEATURES_DIR = PROJECT_ROOT / "data" / "gold" / "financials_features"

OUTPUT_DIR = PROJECT_ROOT / "data" / "gold" / "financials_feature_validation"

SUMMARY_DIR = OUTPUT_DIR / "feature_validation_summary"
COMPANY_SUMMARY_DIR = OUTPUT_DIR / "company_feature_summary"
EXCEPTIONS_DIR = OUTPUT_DIR / "feature_exceptions"
LINEAGE_DIR = OUTPUT_DIR / "feature_lineage"

AUDIT_VERSION = "1.0.0"
SUPPORTED_FEATURE_VERSIONS = {"1.2.0"}

ROLLING_QUARTERS = 4
ABS_TOLERANCE = 1e-9
REL_TOLERANCE = 1e-12

KEY_COLUMNS = ["cik", "fiscal_year", "quarter"]

METRICS = ["revenue", "operating_income", "net_income"]

GOLD_VALUE_COLUMNS = {
    "revenue": "revenue_value_gold",
    "operating_income": "operating_income_value_gold",
    "net_income": "net_income_value_gold",
}

FEATURE_VALUE_COLUMNS = {
    "revenue": "revenue_value",
    "operating_income": "operating_income_value",
    "net_income": "net_income_value",
}

EXPECTED_FEATURE_COLUMNS = [
    "cik", "fiscal_year", "quarter", "quarter_index", "feature_version",
    "revenue_value", "operating_income_value", "net_income_value",
    "revenue_qoq_growth_pct", "revenue_yoy_growth_pct",
    "revenue_growth_acceleration",
    "operating_margin", "net_margin",
    "operating_margin_qoq_change", "operating_margin_yoy_change",
    "net_margin_qoq_change", "net_margin_yoy_change",
    "ttm_revenue", "ttm_operating_income", "ttm_net_income",
    "ttm_operating_margin", "ttm_net_margin",
    "ttm_revenue_status", "ttm_operating_income_status",
    "ttm_net_income_status",
    "revenue_growth_4q_avg", "operating_margin_4q_avg",
    "net_margin_4q_avg",
    "revenue_growth_4q_status", "operating_margin_4q_status",
    "net_margin_4q_status",
    "revenue_growth_volatility_4q",
    "operating_margin_volatility_4q",
    "net_margin_volatility_4q",
    "revenue_growth_volatility_4q_status",
    "operating_margin_volatility_4q_status",
    "net_margin_volatility_4q_status",
    "revenue_growth_trend", "operating_margin_trend", "net_margin_trend",
    "revenue_positive_growth_quarters_4q",
    "revenue_growth_consistent_4q_flag",
    "operating_margin_expansion_quarters_4q",
    "net_margin_expansion_quarters_4q",
    "quarter_contiguous_with_previous",
    "ttm_revenue_available",
    "ttm_operating_income_available",
    "ttm_net_income_available",
    "feature_data_quality",
    "feature_generated_at_utc",
]

VALID_TTM_STATUSES = {
    "VALID",
    "SOURCE_DATA_MISSING",
    "NON_CONSECUTIVE_QUARTERS",
    "INSUFFICIENT_HISTORY",
}

QUALITY_VALUES = {
    "COMPLETE",
    "PARTIAL",
    "SOURCE_DATA_INCOMPLETE",
    "NON_CONSECUTIVE_DATA",
    "INSUFFICIENT_HISTORY",
}

QUARTER_ORDER = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}


# ============================================================================
# LOGGING / HELPERS
# ============================================================================

def log(message: str = "") -> None:
    print(message)


def section(title: str) -> None:
    log("=" * 80)
    log(title)
    log("=" * 80)


def safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def ensure_columns(df: pd.DataFrame, required: Sequence[str], name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"{name} missing required columns: {missing}\n"
            f"Available columns: {list(df.columns)}"
        )


def values_match(left, right) -> bool:
    if pd.isna(left) and pd.isna(right):
        return True
    if pd.isna(left) or pd.isna(right):
        return False
    try:
        return bool(
            np.isclose(
                float(left),
                float(right),
                atol=ABS_TOLERANCE,
                rtol=REL_TOLERANCE,
            )
        )
    except (TypeError, ValueError):
        return str(left) == str(right)


def is_semver(value) -> bool:
    return isinstance(value, str) and bool(
        re.fullmatch(r"\d+\.\d+\.\d+", value.strip())
    )


def normalize_quarter(value) -> Optional[str]:
    if pd.isna(value):
        return None
    value = str(value).strip().upper()
    if value in QUARTER_ORDER:
        return value
    if value.startswith("Q") and value[1:].isdigit():
        candidate = f"Q{int(value[1:])}"
        if candidate in QUARTER_ORDER:
            return candidate
    return None


def quarter_index(fiscal_year, quarter) -> Optional[int]:
    if pd.isna(fiscal_year):
        return None
    q = normalize_quarter(quarter)
    if q is None:
        return None
    try:
        return int(fiscal_year) * 4 + QUARTER_ORDER[q]
    except (TypeError, ValueError):
        return None


def normalize_keys(df: pd.DataFrame, name: str) -> pd.DataFrame:
    result = df.copy()
    ensure_columns(result, KEY_COLUMNS, name)

    result["cik"] = (
        result["cik"].astype("string").str.strip().str.zfill(10)
    )
    result["fiscal_year"] = pd.to_numeric(
        result["fiscal_year"], errors="coerce"
    ).astype("Int64")
    result["quarter"] = result["quarter"].map(normalize_quarter).astype("string")

    result["_calculated_quarter_index"] = pd.array(
        [
            quarter_index(y, q)
            for y, q in zip(result["fiscal_year"], result["quarter"])
        ],
        dtype="Int64",
    )
    return result


def is_consecutive(indexes: Sequence) -> bool:
    if len(indexes) != ROLLING_QUARTERS:
        return False
    try:
        values = [int(x) for x in indexes]
    except (TypeError, ValueError):
        return False
    return all(values[i] - values[i - 1] == 1 for i in range(1, len(values)))


def make_exception(
    cik: str,
    exception_type: str,
    exception_code: str,
    severity: str,
    fiscal_year=None,
    quarter=None,
    feature_name: Optional[str] = None,
    source_layer: Optional[str] = None,
    source_metric: Optional[str] = None,
    message: str = "",
    expected_value=None,
    observed_value=None,
) -> Dict:
    return {
        "cik": cik,
        "fiscal_year": fiscal_year,
        "quarter": quarter,
        "feature_name": feature_name,
        "exception_type": exception_type,
        "exception_code": exception_code,
        "severity": severity,
        "source_layer": source_layer,
        "source_metric": source_metric,
        "message": message,
        "expected_value": expected_value,
        "observed_value": observed_value,
        "audit_version": AUDIT_VERSION,
    }


def atomic_write(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.stem + ".tmp" + path.suffix)
    if tmp.exists():
        tmp.unlink()
    df.to_parquet(tmp, index=False)
    tmp.replace(path)


# ============================================================================
# LOADING / DISCOVERY
# ============================================================================

def discover_companies() -> List[str]:
    if not FEATURES_DIR.exists():
        return []
    return sorted(
        {
            p.name.replace("cik=", "", 1).strip().zfill(10)
            for p in FEATURES_DIR.glob("cik=*")
            if p.is_dir()
        }
    )


def load_company(root: Path, cik: str, filename: str) -> pd.DataFrame:
    path = root / f"cik={cik}" / filename
    if not path.exists():
        raise FileNotFoundError(f"Required dataset not found: {path}")
    df = pd.read_parquet(path)
    if df.empty:
        raise ValueError(f"Dataset is empty: {path}")
    return df


# ============================================================================
# EXCEPTION AUDITS
# ============================================================================

def audit_feature_schema(features: pd.DataFrame, cik: str) -> List[Dict]:
    exceptions = []
    missing = [c for c in EXPECTED_FEATURE_COLUMNS if c not in features.columns]

    if missing:
        exceptions.append(
            make_exception(
                cik,
                "SCHEMA_ERROR",
                "FEATURE_COLUMNS_MISSING",
                "FATAL",
                feature_name=None,
                source_layer="financials_features",
                message=f"Missing Step 12 feature columns: {missing}",
            )
        )
    return exceptions


def audit_key_integrity(
    df: pd.DataFrame,
    name: str,
    cik: str,
) -> List[Dict]:
    exceptions = []

    if df[KEY_COLUMNS].isna().any(axis=1).any():
        for _, row in df.loc[
            df[KEY_COLUMNS].isna().any(axis=1), KEY_COLUMNS
        ].iterrows():
            exceptions.append(
                make_exception(
                    cik,
                    "SCHEMA_ERROR",
                    "NULL_COMPANY_QUARTER_KEY",
                    "FATAL",
                    row.get("fiscal_year"),
                    row.get("quarter"),
                    source_layer=name,
                    message=f"{name} contains a null company-quarter key.",
                )
            )

    invalid = ~df["quarter"].isin(list(QUARTER_ORDER))
    if invalid.any():
        for _, row in df.loc[invalid, KEY_COLUMNS].iterrows():
            exceptions.append(
                make_exception(
                    cik,
                    "SCHEMA_ERROR",
                    "INVALID_QUARTER",
                    "FATAL",
                    row.get("fiscal_year"),
                    row.get("quarter"),
                    source_layer=name,
                    message=f"{name} contains an invalid quarter.",
                )
            )

    return exceptions


def audit_feature_grain(features: pd.DataFrame, cik: str) -> List[Dict]:
    exceptions = []
    duplicates = features[
        features.duplicated(KEY_COLUMNS, keep=False)
    ]

    if not duplicates.empty:
        grouped = (
            duplicates.groupby(KEY_COLUMNS, dropna=False)
            .size()
            .reset_index(name="row_count")
        )

        for _, row in grouped.iterrows():
            exceptions.append(
                make_exception(
                    cik,
                    "GRAIN_ERROR",
                    "FEATURE_DUPLICATE_COMPANY_QUARTER",
                    "FATAL",
                    row["fiscal_year"],
                    row["quarter"],
                    source_layer="financials_features",
                    message="More than one Step 12 row exists at company-quarter grain.",
                    observed_value=int(row["row_count"]),
                )
            )
    return exceptions


def audit_quarter_index(features: pd.DataFrame, cik: str) -> List[Dict]:
    exceptions = []
    for _, row in features.iterrows():
        expected = row["_calculated_quarter_index"]
        actual = row["quarter_index"]

        if pd.isna(expected):
            continue

        if pd.isna(actual) or int(actual) != int(expected):
            exceptions.append(
                make_exception(
                    cik,
                    "GRAIN_ERROR",
                    "QUARTER_INDEX_MISMATCH",
                    "FATAL",
                    row["fiscal_year"],
                    row["quarter"],
                    "quarter_index",
                    "financials_features",
                    message="quarter_index does not match fiscal_year/quarter.",
                    expected_value=int(expected),
                    observed_value=actual,
                )
            )
    return exceptions


def audit_version(features: pd.DataFrame, cik: str) -> List[Dict]:
    exceptions = []
    versions = (
        features["feature_version"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    for version in versions:
        if not is_semver(version):
            exceptions.append(
                make_exception(
                    cik,
                    "SCHEMA_ERROR",
                    "INVALID_FEATURE_VERSION",
                    "FATAL",
                    feature_name="feature_version",
                    source_layer="financials_features",
                    message=f"Invalid semantic version: {version}",
                    observed_value=version,
                )
            )
        elif version not in SUPPORTED_FEATURE_VERSIONS:
            exceptions.append(
                make_exception(
                    cik,
                    "SCHEMA_ERROR",
                    "UNSUPPORTED_FEATURE_VERSION",
                    "FATAL",
                    feature_name="feature_version",
                    source_layer="financials_features",
                    message=(
                        f"Unsupported Step 12 version {version}; "
                        f"supported={sorted(SUPPORTED_FEATURE_VERSIONS)}"
                    ),
                    observed_value=version,
                )
            )

    if len(versions) != 1:
        exceptions.append(
            make_exception(
                cik,
                "SCHEMA_ERROR",
                "MULTIPLE_FEATURE_VERSIONS",
                "FATAL",
                feature_name="feature_version",
                source_layer="financials_features",
                message="A company feature dataset must contain exactly one feature version.",
                observed_value=",".join(sorted(versions)),
            )
        )
    return exceptions


def build_gold_quarterly(gold: pd.DataFrame, cik: str) -> pd.DataFrame:
    ensure_columns(
        gold,
        ["cik", "metric", "fiscal_year", "quarter", "value"],
        "Final Gold",
    )

    result = normalize_keys(gold, "Final Gold")
    result["metric"] = (
        result["metric"].astype("string").str.strip().str.lower()
    )
    result["value"] = safe_numeric(result["value"])

    parts = []
    for metric in METRICS:
        subset = result[result["metric"].eq(metric)].copy()
        if subset.empty:
            continue

        if subset.duplicated(KEY_COLUMNS, keep=False).any():
            raise ValueError(
                f"Final Gold has duplicate {metric} rows at company-quarter grain for {cik}."
            )

        parts.append(
            subset[KEY_COLUMNS + ["value"]].rename(
                columns={"value": GOLD_VALUE_COLUMNS[metric]}
            )
        )

    if not parts:
        raise ValueError(f"Final Gold contains none of {METRICS} for {cik}.")

    quarterly = parts[0]
    for part in parts[1:]:
        quarterly = quarterly.merge(
            part,
            on=KEY_COLUMNS,
            how="outer",
            validate="one_to_one",
        )

    return quarterly.sort_values(KEY_COLUMNS).reset_index(drop=True)


def normalize_analytics(analytics: pd.DataFrame, cik: str) -> pd.DataFrame:
    result = normalize_keys(analytics, "Financial Analytics")
    if result.duplicated(KEY_COLUMNS, keep=False).any():
        raise ValueError(
            f"Financial Analytics has duplicate company-quarter rows for {cik}."
        )
    return result.sort_values(KEY_COLUMNS).reset_index(drop=True)


def audit_cross_layer_keys(
    gold: pd.DataFrame,
    analytics: pd.DataFrame,
    features: pd.DataFrame,
    cik: str,
) -> List[Dict]:
    exceptions = []

    def keys(df):
        return set(
            tuple(x)
            for x in df[KEY_COLUMNS].itertuples(index=False, name=None)
        )

    feature_keys = keys(features)

    for name, dataset in [
        ("financials_quarterly_final", gold),
        ("financials_analytics", analytics),
    ]:
        source_keys = keys(dataset)

        for fy, q in sorted(feature_keys - source_keys):
            exceptions.append(
                make_exception(
                    cik,
                    "LINEAGE_ERROR",
                    "FEATURE_KEY_MISSING_UPSTREAM",
                    "FATAL",
                    fy,
                    q,
                    source_layer=name,
                    message=f"Step 12 contains {fy} {q}, absent from {name}.",
                )
            )

    return exceptions


# ============================================================================
# BASE VALUE / SOURCE LINEAGE
# ============================================================================

def audit_base_values(
    features: pd.DataFrame,
    gold_quarterly: pd.DataFrame,
    cik: str,
) -> Tuple[pd.DataFrame, List[Dict]]:
    feature_columns = [
        column
        for column in features.columns
        if column not in GOLD_VALUE_COLUMNS.values()
    ]
    merged = features[feature_columns].merge(
        gold_quarterly,
        on=KEY_COLUMNS,
        how="left",
        validate="one_to_one",
    )

    exceptions = []

    for metric in METRICS:
        source_col = GOLD_VALUE_COLUMNS[metric]
        feature_col = FEATURE_VALUE_COLUMNS[metric]

        for _, row in merged.iterrows():
            source = row[source_col]
            feature = row[feature_col]

            if pd.isna(source) and pd.isna(feature):
                continue

            if pd.isna(source) and not pd.isna(feature):
                exceptions.append(
                    make_exception(
                        cik,
                        "LINEAGE_ERROR",
                        "FEATURE_VALUE_WITHOUT_SOURCE",
                        "FATAL",
                        row["fiscal_year"],
                        row["quarter"],
                        feature_col,
                        "financials_quarterly_final",
                        metric,
                        f"{feature_col} has a value while Final Gold {metric} is missing.",
                        np.nan,
                        feature,
                    )
                )
            elif not pd.isna(source) and pd.isna(feature):
                exceptions.append(
                    make_exception(
                        cik,
                        "LINEAGE_ERROR",
                        "FEATURE_VALUE_MISSING_SOURCE_AVAILABLE",
                        "FATAL",
                        row["fiscal_year"],
                        row["quarter"],
                        feature_col,
                        "financials_quarterly_final",
                        metric,
                        f"{feature_col} is missing while Final Gold {metric} is available.",
                        source,
                        np.nan,
                    )
                )
            elif not values_match(feature, source):
                exceptions.append(
                    make_exception(
                        cik,
                        "FEATURE_MISMATCH",
                        "BASE_VALUE_MISMATCH",
                        "FATAL",
                        row["fiscal_year"],
                        row["quarter"],
                        feature_col,
                        "financials_quarterly_final",
                        metric,
                        f"{feature_col} does not equal Final Gold {metric}.",
                        source,
                        feature,
                    )
                )

    for metric in METRICS:
        merged[f"{metric}_source_available"] = (
            merged[GOLD_VALUE_COLUMNS[metric]].notna()
        )

    return merged, exceptions


# ============================================================================
# FEATURE ARITHMETIC
# ============================================================================

def audit_margins(features: pd.DataFrame, cik: str) -> List[Dict]:
    exceptions = []

    checks = [
        (
            "operating_margin",
            "operating_income_value",
            "revenue_value",
            "OPERATING_MARGIN_MISMATCH",
        ),
        (
            "net_margin",
            "net_income_value",
            "revenue_value",
            "NET_MARGIN_MISMATCH",
        ),
    ]

    for margin, numerator, denominator, code in checks:
        for _, row in features.iterrows():
            n = row[numerator]
            d = row[denominator]
            actual = row[margin]

            if pd.isna(n) or pd.isna(d) or pd.isna(actual):
                continue
            if abs(float(d)) <= ABS_TOLERANCE:
                continue

            expected = float(n) / float(d)

            if not values_match(actual, expected):
                exceptions.append(
                    make_exception(
                        cik,
                        "FEATURE_MISMATCH",
                        code,
                        "FATAL",
                        row["fiscal_year"],
                        row["quarter"],
                        margin,
                        "financials_features",
                        None,
                        f"{margin} does not equal {numerator}/{denominator}.",
                        expected,
                        actual,
                    )
                )

    return exceptions


def audit_growth(features: pd.DataFrame, cik: str) -> List[Dict]:
    exceptions = []
    df = features.sort_values("quarter_index").reset_index(drop=True)

    for i, row in df.iterrows():
        if i > 0:
            prev = df.iloc[i - 1]
            contiguous = (
                int(row["quarter_index"]) - int(prev["quarter_index"]) == 1
            )

            current = row["revenue_value"]
            previous = prev["revenue_value"]
            actual = row["revenue_qoq_growth_pct"]

            if (
                contiguous
                and not pd.isna(current)
                and not pd.isna(previous)
                and abs(float(previous)) > ABS_TOLERANCE
            ):
                expected = float(current) / float(previous) - 1.0
                if pd.isna(actual) or not values_match(actual, expected):
                    exceptions.append(
                        make_exception(
                            cik,
                            "FEATURE_MISMATCH",
                            "REVENUE_QOQ_MISMATCH",
                            "FATAL",
                            row["fiscal_year"],
                            row["quarter"],
                            "revenue_qoq_growth_pct",
                            "financials_features",
                            "revenue",
                            "QoQ revenue growth is incorrect.",
                            expected,
                            actual,
                        )
                    )
            elif not contiguous and not pd.isna(actual):
                exceptions.append(
                    make_exception(
                        cik,
                        "FEATURE_MISMATCH",
                        "REVENUE_QOQ_ON_NON_CONSECUTIVE_DATA",
                        "FATAL",
                        row["fiscal_year"],
                        row["quarter"],
                        "revenue_qoq_growth_pct",
                        "financials_features",
                        "revenue",
                        "QoQ growth exists across a non-consecutive boundary.",
                        None,
                        actual,
                    )
                )

        if i >= 4:
            prior = df.iloc[i - 4]
            contiguous = (
                int(row["quarter_index"]) - int(prior["quarter_index"]) == 4
            )

            current = row["revenue_value"]
            previous = prior["revenue_value"]
            actual = row["revenue_yoy_growth_pct"]

            if (
                contiguous
                and not pd.isna(current)
                and not pd.isna(previous)
                and abs(float(previous)) > ABS_TOLERANCE
            ):
                expected = float(current) / float(previous) - 1.0
                if pd.isna(actual) or not values_match(actual, expected):
                    exceptions.append(
                        make_exception(
                            cik,
                            "FEATURE_MISMATCH",
                            "REVENUE_YOY_MISMATCH",
                            "FATAL",
                            row["fiscal_year"],
                            row["quarter"],
                            "revenue_yoy_growth_pct",
                            "financials_features",
                            "revenue",
                            "YoY revenue growth is incorrect.",
                            expected,
                            actual,
                        )
                    )

        if i > 0:
            prev = df.iloc[i - 1]
            contiguous = (
                int(row["quarter_index"]) - int(prev["quarter_index"]) == 1
            )
            current_growth = row["revenue_yoy_growth_pct"]
            previous_growth = prev["revenue_yoy_growth_pct"]
            actual = row["revenue_growth_acceleration"]

            if (
                contiguous
                and not pd.isna(current_growth)
                and not pd.isna(previous_growth)
            ):
                expected = float(current_growth) - float(previous_growth)
                if pd.isna(actual) or not values_match(actual, expected):
                    exceptions.append(
                        make_exception(
                            cik,
                            "FEATURE_MISMATCH",
                            "REVENUE_GROWTH_ACCELERATION_MISMATCH",
                            "FATAL",
                            row["fiscal_year"],
                            row["quarter"],
                            "revenue_growth_acceleration",
                            "financials_features",
                            "revenue",
                            "Growth acceleration is incorrect.",
                            expected,
                            actual,
                        )
                    )

    return exceptions


# ============================================================================
# TTM / ROLLING
# ============================================================================

def expected_ttm_status(
    df: pd.DataFrame,
    i: int,
    metric: str,
) -> str:
    if i < ROLLING_QUARTERS - 1:
        return "INSUFFICIENT_HISTORY"

    window = df.iloc[i - ROLLING_QUARTERS + 1:i + 1]

    if not is_consecutive(window["quarter_index"].tolist()):
        return "NON_CONSECUTIVE_QUARTERS"

    if window[FEATURE_VALUE_COLUMNS[metric]].isna().any():
        return "SOURCE_DATA_MISSING"

    return "VALID"


def audit_ttm(features: pd.DataFrame, cik: str) -> List[Dict]:
    exceptions = []
    df = features.sort_values("quarter_index").reset_index(drop=True)

    for metric in METRICS:
        value_col = FEATURE_VALUE_COLUMNS[metric]
        ttm_col = f"ttm_{metric}"
        status_col = f"ttm_{metric}_status"

        if ttm_col not in df.columns:
            continue

        for i, row in df.iterrows():
            expected_status = expected_ttm_status(df, i, metric)
            actual_status = row[status_col]
            actual_ttm = row[ttm_col]

            if actual_status != expected_status:
                exceptions.append(
                    make_exception(
                        cik,
                        "FEATURE_MISMATCH",
                        "TTM_STATUS_MISMATCH",
                        "FATAL",
                        row["fiscal_year"],
                        row["quarter"],
                        status_col,
                        "financials_features",
                        metric,
                        "TTM status disagrees with source availability/history.",
                        expected_status,
                        actual_status,
                    )
                )

            if expected_status == "VALID":
                window = df.iloc[i - 3:i + 1]
                expected_value = float(
                    safe_numeric(window[value_col]).sum()
                )

                if pd.isna(actual_ttm) or not values_match(
                    actual_ttm, expected_value
                ):
                    exceptions.append(
                        make_exception(
                            cik,
                            "FEATURE_MISMATCH",
                            "TTM_VALUE_MISMATCH",
                            "FATAL",
                            row["fiscal_year"],
                            row["quarter"],
                            ttm_col,
                            "financials_features",
                            metric,
                            "TTM does not equal the sum of four consecutive quarters.",
                            expected_value,
                            actual_ttm,
                        )
                    )
            else:
                if not pd.isna(actual_ttm):
                    exceptions.append(
                        make_exception(
                            cik,
                            "FEATURE_MISMATCH",
                            "TTM_VALUE_WITH_INVALID_WINDOW",
                            "FATAL",
                            row["fiscal_year"],
                            row["quarter"],
                            ttm_col,
                            "financials_features",
                            metric,
                            "TTM value exists while its four-quarter window is invalid.",
                            None,
                            actual_ttm,
                        )
                    )

            availability_col = f"ttm_{metric}_available"
            if availability_col in df.columns:
                expected_available = not pd.isna(actual_ttm)
                actual_available = row[availability_col]
                if (
                    not pd.isna(actual_available)
                    and bool(actual_available) != expected_available
                ):
                    exceptions.append(
                        make_exception(
                            cik,
                            "FEATURE_MISMATCH",
                            "TTM_AVAILABILITY_FLAG_MISMATCH",
                            "FATAL",
                            row["fiscal_year"],
                            row["quarter"],
                            availability_col,
                            "financials_features",
                            metric,
                            "TTM availability flag does not match TTM value.",
                            expected_available,
                            actual_available,
                        )
                    )

    return exceptions


def audit_ttm_margins(features: pd.DataFrame, cik: str) -> List[Dict]:
    exceptions = []
    checks = [
        (
            "ttm_operating_margin",
            "ttm_operating_income",
            "ttm_revenue",
            "TTM_OPERATING_MARGIN_MISMATCH",
        ),
        (
            "ttm_net_margin",
            "ttm_net_income",
            "ttm_revenue",
            "TTM_NET_MARGIN_MISMATCH",
        ),
    ]

    for margin, numerator, denominator, code in checks:
        for _, row in features.iterrows():
            n, d, actual = row[numerator], row[denominator], row[margin]
            if pd.isna(n) or pd.isna(d) or pd.isna(actual):
                continue
            if abs(float(d)) <= ABS_TOLERANCE:
                continue

            expected = float(n) / float(d)
            if not values_match(actual, expected):
                exceptions.append(
                    make_exception(
                        cik,
                        "FEATURE_MISMATCH",
                        code,
                        "FATAL",
                        row["fiscal_year"],
                        row["quarter"],
                        margin,
                        "financials_features",
                        None,
                        f"{margin} is mathematically inconsistent with TTM values.",
                        expected,
                        actual,
                    )
                )
    return exceptions


def audit_rolling(
    features: pd.DataFrame,
    cik: str,
    source_column: str,
    output_column: str,
    status_column: str,
    operation: str,
) -> List[Dict]:
    exceptions = []
    if not all(c in features.columns for c in [
        source_column, output_column, status_column
    ]):
        return exceptions

    df = features.sort_values("quarter_index").reset_index(drop=True)

    for i, row in df.iterrows():
        if i < 3:
            expected_status = "INSUFFICIENT_HISTORY"
            expected_value = np.nan
        else:
            window = df.iloc[i - 3:i + 1]

            if not is_consecutive(window["quarter_index"].tolist()):
                expected_status = "NON_CONSECUTIVE_QUARTERS"
                expected_value = np.nan
            elif window[source_column].isna().any():
                expected_status = "SOURCE_DATA_MISSING"
                expected_value = np.nan
            else:
                expected_status = "VALID"
                values = safe_numeric(window[source_column])
                expected_value = (
                    float(values.mean())
                    if operation == "mean"
                    else float(values.std(ddof=0))
                )

        actual_status = row[status_column]
        actual_value = row[output_column]

        if actual_status != expected_status:
            exceptions.append(
                make_exception(
                    cik,
                    "FEATURE_MISMATCH",
                    "ROLLING_STATUS_MISMATCH",
                    "FATAL",
                    row["fiscal_year"],
                    row["quarter"],
                    status_column,
                    "financials_features",
                    None,
                    "Rolling feature status is incorrect.",
                    expected_status,
                    actual_status,
                )
            )

        if expected_status == "VALID":
            if pd.isna(actual_value) or not values_match(
                actual_value, expected_value
            ):
                exceptions.append(
                    make_exception(
                        cik,
                        "FEATURE_MISMATCH",
                        "ROLLING_VALUE_MISMATCH",
                        "FATAL",
                        row["fiscal_year"],
                        row["quarter"],
                        output_column,
                        "financials_features",
                        None,
                        f"{output_column} is incorrect.",
                        expected_value,
                        actual_value,
                    )
                )
        elif not pd.isna(actual_value):
            exceptions.append(
                make_exception(
                    cik,
                    "FEATURE_MISMATCH",
                    "ROLLING_VALUE_WITH_INVALID_WINDOW",
                    "FATAL",
                    row["fiscal_year"],
                    row["quarter"],
                    output_column,
                    "financials_features",
                    None,
                    f"{output_column} exists for an invalid rolling window.",
                    None,
                    actual_value,
                )
            )

    return exceptions


# ============================================================================
# QUALITY CONTRACT
# ============================================================================

def expected_feature_quality(row: pd.Series) -> str:
    statuses = [
        row["ttm_revenue_status"],
        row["ttm_operating_income_status"],
        row["ttm_net_income_status"],
    ]

    if "NON_CONSECUTIVE_QUARTERS" in statuses:
        return "NON_CONSECUTIVE_DATA"
    if "SOURCE_DATA_MISSING" in statuses:
        return "SOURCE_DATA_INCOMPLETE"
    if all(x == "VALID" for x in statuses):
        return "COMPLETE"
    if any(x == "VALID" for x in statuses):
        return "PARTIAL"
    return "INSUFFICIENT_HISTORY"


def audit_quality(features: pd.DataFrame, cik: str) -> List[Dict]:
    exceptions = []

    for _, row in features.iterrows():
        actual = row["feature_data_quality"]
        expected = expected_feature_quality(row)

        if actual not in QUALITY_VALUES:
            exceptions.append(
                make_exception(
                    cik,
                    "SCHEMA_ERROR",
                    "INVALID_FEATURE_DATA_QUALITY",
                    "FATAL",
                    row["fiscal_year"],
                    row["quarter"],
                    "feature_data_quality",
                    "financials_features",
                    message="Unrecognized feature_data_quality value.",
                    expected_value=",".join(sorted(QUALITY_VALUES)),
                    observed_value=actual,
                )
            )
        elif actual != expected:
            exceptions.append(
                make_exception(
                    cik,
                    "FEATURE_MISMATCH",
                    "FEATURE_QUALITY_CLASSIFICATION_MISMATCH",
                    "FATAL",
                    row["fiscal_year"],
                    row["quarter"],
                    "feature_data_quality",
                    "financials_features",
                    message="Feature quality does not agree with TTM component statuses.",
                    expected_value=expected,
                    observed_value=actual,
                )
            )

    return exceptions


# ============================================================================
# LINEAGE / SUMMARY
# ============================================================================

def build_lineage(
    features: pd.DataFrame,
    gold_quarterly: pd.DataFrame,
    analytics: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        c for c in [
            "cik", "fiscal_year", "quarter", "quarter_index",
            "feature_version",
            "revenue_value", "operating_income_value", "net_income_value",
            "revenue_qoq_growth_pct", "revenue_yoy_growth_pct",
            "revenue_growth_acceleration",
            "operating_margin", "net_margin",
            "ttm_revenue", "ttm_operating_income", "ttm_net_income",
            "ttm_operating_margin", "ttm_net_margin",
            "ttm_revenue_status", "ttm_operating_income_status",
            "ttm_net_income_status",
            "feature_data_quality",
        ]
        if c in features.columns
    ]

    lineage = features[columns].copy()

    lineage = lineage.merge(
        gold_quarterly,
        on=KEY_COLUMNS,
        how="left",
        validate="one_to_one",
    )

    analytics_columns = [
        c for c in [
            "cik", "fiscal_year", "quarter",
            "revenue_value", "revenue_qoq_growth_pct",
            "revenue_yoy_growth_pct", "operating_margin", "net_margin"
        ]
        if c in analytics.columns
    ]

    analytics_view = analytics[analytics_columns].copy()
    analytics_view = analytics_view.rename(
        columns={
            c: f"analytics_{c}"
            for c in analytics_columns
            if c not in KEY_COLUMNS
        }
    )

    lineage = lineage.merge(
        analytics_view,
        on=KEY_COLUMNS,
        how="left",
        validate="one_to_one",
    )

    for metric in METRICS:
        lineage[f"{metric}_source_available"] = (
            lineage[GOLD_VALUE_COLUMNS[metric]].notna()
        )

    lineage["source_final_gold"] = "financials_quarterly_final"
    lineage["source_analytics"] = "financials_analytics"
    lineage["source_features"] = "financials_features"
    lineage["audit_version"] = AUDIT_VERSION
    lineage["audit_generated_at_utc"] = pd.Timestamp.utcnow()

    return lineage.sort_values(KEY_COLUMNS).reset_index(drop=True)


def build_validation_rows(
    features: pd.DataFrame,
    exceptions: List[Dict],
    cik: str,
) -> pd.DataFrame:
    exception_df = pd.DataFrame(exceptions)
    rows = []

    for _, row in features.iterrows():
        if exception_df.empty:
            row_exceptions = exception_df
        else:
            row_exceptions = exception_df[
                exception_df["cik"].eq(cik)
                & exception_df["fiscal_year"].eq(row["fiscal_year"])
                & exception_df["quarter"].eq(row["quarter"])
            ]

        if row_exceptions.empty:
            status = "PASS"
        elif row_exceptions["severity"].eq("FATAL").any():
            status = "FAIL"
        else:
            status = "WARNING"

        rows.append({
            "cik": cik,
            "fiscal_year": row["fiscal_year"],
            "quarter": row["quarter"],
            "quarter_index": row["quarter_index"],
            "feature_version": row["feature_version"],
            "feature_data_quality": row["feature_data_quality"],
            "ttm_revenue_status": row["ttm_revenue_status"],
            "ttm_operating_income_status": row["ttm_operating_income_status"],
            "ttm_net_income_status": row["ttm_net_income_status"],
            "ttm_revenue_available": row["ttm_revenue_available"],
            "ttm_operating_income_available": row["ttm_operating_income_available"],
            "ttm_net_income_available": row["ttm_net_income_available"],
            "exception_count": len(row_exceptions),
            "fatal_exception_count": (
                int(row_exceptions["severity"].eq("FATAL").sum())
                if not row_exceptions.empty else 0
            ),
            "warning_exception_count": (
                int(row_exceptions["severity"].eq("WARNING").sum())
                if not row_exceptions.empty else 0
            ),
            "validation_status": status,
            "audit_version": AUDIT_VERSION,
        })

    return pd.DataFrame(rows)


def build_company_summary(
    cik: str,
    gold: pd.DataFrame,
    analytics: pd.DataFrame,
    features: pd.DataFrame,
    exceptions: List[Dict],
) -> Dict:
    ex = pd.DataFrame(exceptions)

    quality = (
        features["feature_data_quality"]
        .value_counts()
        .to_dict()
    )

    fatal = (
        int(ex["severity"].eq("FATAL").sum())
        if not ex.empty else 0
    )
    warning = (
        int(ex["severity"].eq("WARNING").sum())
        if not ex.empty else 0
    )

    return {
        "cik": cik,
        "gold_rows": len(gold),
        "analytics_rows": len(analytics),
        "feature_rows": len(features),
        "gold_unique_company_quarters": (
            gold[KEY_COLUMNS].drop_duplicates().shape[0]
        ),
        "analytics_unique_company_quarters": (
            analytics[KEY_COLUMNS].drop_duplicates().shape[0]
        ),
        "feature_unique_company_quarters": (
            features[KEY_COLUMNS].drop_duplicates().shape[0]
        ),
        "ttm_revenue_valid_rows": int(
            features["ttm_revenue_status"].eq("VALID").sum()
        ),
        "ttm_operating_income_valid_rows": int(
            features["ttm_operating_income_status"].eq("VALID").sum()
        ),
        "ttm_net_income_valid_rows": int(
            features["ttm_net_income_status"].eq("VALID").sum()
        ),
        "ttm_revenue_source_missing_rows": int(
            features["ttm_revenue_status"].eq("SOURCE_DATA_MISSING").sum()
        ),
        "ttm_operating_income_source_missing_rows": int(
            features["ttm_operating_income_status"].eq("SOURCE_DATA_MISSING").sum()
        ),
        "ttm_net_income_source_missing_rows": int(
            features["ttm_net_income_status"].eq("SOURCE_DATA_MISSING").sum()
        ),
        "complete_feature_rows": int(quality.get("COMPLETE", 0)),
        "partial_feature_rows": int(quality.get("PARTIAL", 0)),
        "source_incomplete_rows": int(
            quality.get("SOURCE_DATA_INCOMPLETE", 0)
        ),
        "non_consecutive_rows": int(
            quality.get("NON_CONSECUTIVE_DATA", 0)
        ),
        "insufficient_history_rows": int(
            quality.get("INSUFFICIENT_HISTORY", 0)
        ),
        "fatal_exception_rows": fatal,
        "warning_exception_rows": warning,
        "audit_status": "FAIL" if fatal else "PASS",
        "audit_version": AUDIT_VERSION,
    }


# ============================================================================
# COMPANY PROCESSING
# ============================================================================

def process_company(cik: str):
    gold = load_company(
        FINAL_GOLD_DIR, cik, "financials_quarterly.parquet"
    )
    analytics = load_company(
        ANALYTICS_DIR, cik, "financials_analytics.parquet"
    )
    features = load_company(
        FEATURES_DIR, cik, "financial_features.parquet"
    )

    log(f"Final Gold rows: {len(gold)}")
    log(f"Analytics rows: {len(analytics)}")
    log(f"Feature rows: {len(features)}")

    gold = normalize_keys(gold, "Final Gold")
    analytics = normalize_keys(analytics, "Financial Analytics")
    features = normalize_keys(features, "Financial Features")

    exceptions: List[Dict] = []

    exceptions += audit_feature_schema(features, cik)
    exceptions += audit_key_integrity(
        gold, "financials_quarterly_final", cik
    )
    exceptions += audit_key_integrity(
        analytics, "financials_analytics", cik
    )
    exceptions += audit_key_integrity(
        features, "financials_features", cik
    )
    exceptions += audit_feature_grain(features, cik)
    exceptions += audit_quarter_index(features, cik)
    exceptions += audit_version(features, cik)

    structural_codes = {
        "FEATURE_COLUMNS_MISSING",
        "NULL_COMPANY_QUARTER_KEY",
        "INVALID_QUARTER",
        "FEATURE_DUPLICATE_COMPANY_QUARTER",
        "QUARTER_INDEX_MISMATCH",
        "INVALID_FEATURE_VERSION",
        "UNSUPPORTED_FEATURE_VERSION",
        "MULTIPLE_FEATURE_VERSIONS",
    }

    if any(
        e["severity"] == "FATAL"
        and e["exception_code"] in structural_codes
        for e in exceptions
    ):
        raise RuntimeError(
            f"Structural Step 12 feature contract failure for {cik}."
        )

    gold_quarterly = build_gold_quarterly(gold, cik)
    analytics = normalize_analytics(analytics, cik)

    exceptions += audit_cross_layer_keys(
        gold, analytics, features, cik
    )

    _, base_exceptions = audit_base_values(
        features, gold_quarterly, cik
    )
    exceptions += base_exceptions

    exceptions += audit_margins(features, cik)
    exceptions += audit_growth(features, cik)
    exceptions += audit_ttm(features, cik)
    exceptions += audit_ttm_margins(features, cik)
    exceptions += audit_quality(features, cik)

    for source, output, status in [
        (
            "revenue_yoy_growth_pct",
            "revenue_growth_4q_avg",
            "revenue_growth_4q_status",
        ),
        (
            "operating_margin",
            "operating_margin_4q_avg",
            "operating_margin_4q_status",
        ),
        (
            "net_margin",
            "net_margin_4q_avg",
            "net_margin_4q_status",
        ),
    ]:
        exceptions += audit_rolling(
            features, cik, source, output, status, "mean"
        )

    for source, output, status in [
        (
            "revenue_yoy_growth_pct",
            "revenue_growth_volatility_4q",
            "revenue_growth_volatility_4q_status",
        ),
        (
            "operating_margin",
            "operating_margin_volatility_4q",
            "operating_margin_volatility_4q_status",
        ),
        (
            "net_margin",
            "net_margin_volatility_4q",
            "net_margin_volatility_4q_status",
        ),
    ]:
        exceptions += audit_rolling(
            features, cik, source, output, status, "std"
        )

    lineage = build_lineage(
        features, gold_quarterly, analytics
    )

    validation = build_validation_rows(
        features, exceptions, cik
    )

    summary = build_company_summary(
        cik, gold, analytics, features, exceptions
    )

    return validation, lineage, pd.DataFrame(exceptions), summary


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    section("STEP 12.1 - FINANCIAL FEATURE QUALITY AUDIT")

    log(f"Final Gold : {FINAL_GOLD_DIR}")
    log(f"Analytics  : {ANALYTICS_DIR}")
    log(f"Features   : {FEATURES_DIR}")
    log(f"Output     : {OUTPUT_DIR}")
    log(f"Audit version: {AUDIT_VERSION}")
    log(
        f"Supported feature versions: "
        f"{sorted(SUPPORTED_FEATURE_VERSIONS)}"
    )

    companies = discover_companies()
    log(f"Companies discovered: {len(companies)}")

    if not companies:
        raise RuntimeError(
            "No companies discovered in financials_features."
        )

    for d in [
        SUMMARY_DIR,
        COMPANY_SUMMARY_DIR,
        EXCEPTIONS_DIR,
        LINEAGE_DIR,
    ]:
        d.mkdir(parents=True, exist_ok=True)

    validations = []
    lineages = []
    exception_frames = []
    summaries = []
    failed = []

    for cik in companies:
        section(f"PROCESSING CIK: {cik}")

        try:
            validation, lineage, exceptions, summary = process_company(cik)

            validations.append(validation)
            lineages.append(lineage)
            summaries.append(summary)

            if not exceptions.empty:
                exception_frames.append(exceptions)

            log(
                f"Feature rows: {summary['feature_rows']}"
            )
            log(
                f"Unique company-quarter rows: "
                f"{summary['feature_unique_company_quarters']}"
            )
            log(
                f"TTM Revenue VALID: "
                f"{summary['ttm_revenue_valid_rows']}"
            )
            log(
                f"TTM Operating Income VALID: "
                f"{summary['ttm_operating_income_valid_rows']}"
            )
            log(
                f"TTM Net Income VALID: "
                f"{summary['ttm_net_income_valid_rows']}"
            )

            log("Feature quality:")
            summary_keys = {
                "COMPLETE": "complete_feature_rows",
                "PARTIAL": "partial_feature_rows",
                "SOURCE_DATA_INCOMPLETE": "source_incomplete_rows",
                "NON_CONSECUTIVE_DATA": "non_consecutive_rows",
                "INSUFFICIENT_HISTORY": "insufficient_history_rows",
            }

            for status, summary_key in summary_keys.items():
                log(
                    f"  {status}: "
                    f"{summary.get(summary_key, 0)}"
                )

            log(
                f"Fatal exceptions: "
                f"{summary['fatal_exception_rows']}"
            )
            log(
                f"Warnings: "
                f"{summary['warning_exception_rows']}"
            )
            log(
                f"AUDIT STATUS: "
                f"{summary['audit_status']}"
            )

        except Exception as exc:
            failed.append((cik, str(exc), traceback.format_exc()))
            log(f"ERROR processing {cik}: {exc}")
            log(traceback.format_exc())

    validation_df = (
        pd.concat(validations, ignore_index=True)
        if validations else pd.DataFrame()
    )

    lineage_df = (
        pd.concat(lineages, ignore_index=True)
        if lineages else pd.DataFrame()
    )

    exceptions_df = (
        pd.concat(exception_frames, ignore_index=True)
        if exception_frames else pd.DataFrame(
            columns=[
                "cik", "fiscal_year", "quarter", "feature_name",
                "exception_type", "exception_code", "severity",
                "source_layer", "source_metric", "message",
                "expected_value", "observed_value", "audit_version",
            ]
        )
    )

    company_summary_df = pd.DataFrame(summaries)

    audit_ts = pd.Timestamp.utcnow()

    for df in [
        validation_df,
        lineage_df,
        exceptions_df,
        company_summary_df,
    ]:
        if not df.empty:
            df["audit_generated_at_utc"] = audit_ts

    atomic_write(
        validation_df,
        SUMMARY_DIR / "feature_validation_summary.parquet",
    )
    atomic_write(
        company_summary_df,
        COMPANY_SUMMARY_DIR / "company_feature_summary.parquet",
    )
    atomic_write(
        exceptions_df,
        EXCEPTIONS_DIR / "feature_exceptions.parquet",
    )
    atomic_write(
        lineage_df,
        LINEAGE_DIR / "feature_lineage.parquet",
    )

    fatal_count = (
        int(exceptions_df["severity"].eq("FATAL").sum())
        if not exceptions_df.empty else 0
    )
    warning_count = (
        int(exceptions_df["severity"].eq("WARNING").sum())
        if not exceptions_df.empty else 0
    )

    pass_rows = (
        int(validation_df["validation_status"].eq("PASS").sum())
        if not validation_df.empty else 0
    )
    warning_rows = (
        int(validation_df["validation_status"].eq("WARNING").sum())
        if not validation_df.empty else 0
    )
    fail_rows = (
        int(validation_df["validation_status"].eq("FAIL").sum())
        if not validation_df.empty else 0
    )

    section("STEP 12.1 SUMMARY")
    log(f"Total validation rows : {len(validation_df)}")
    log(f"PASS                  : {pass_rows}")
    log(f"WARNING               : {warning_rows}")
    log(f"FAIL                  : {fail_rows}")
    log(f"Fatal exceptions      : {fatal_count}")
    log(f"Warning exceptions    : {warning_count}")

    if not company_summary_df.empty:
        columns = [
            "cik",
            "feature_rows",
            "complete_feature_rows",
            "partial_feature_rows",
            "source_incomplete_rows",
            "non_consecutive_rows",
            "insufficient_history_rows",
            "ttm_revenue_valid_rows",
            "ttm_operating_income_valid_rows",
            "ttm_net_income_valid_rows",
            "ttm_revenue_source_missing_rows",
            "ttm_operating_income_source_missing_rows",
            "ttm_net_income_source_missing_rows",
            "fatal_exception_rows",
            "warning_exception_rows",
            "audit_status",
        ]
        columns = [c for c in columns if c in company_summary_df.columns]

        log("\nCOMPANY AUDIT SUMMARY")
        log(company_summary_df[columns].to_string(index=False))

    log("\nOUTPUTS")
    log(
        str(
            SUMMARY_DIR / "feature_validation_summary.parquet"
        )
    )
    log(
        str(
            COMPANY_SUMMARY_DIR / "company_feature_summary.parquet"
        )
    )
    log(
        str(
            EXCEPTIONS_DIR / "feature_exceptions.parquet"
        )
    )
    log(
        str(
            LINEAGE_DIR / "feature_lineage.parquet"
        )
    )

    if failed:
        section("STEP 12.1 FAILED")
        for cik, message, _trace in failed:
            log(f"{cik}: {message}")
        raise RuntimeError(
            f"Step 12.1 failed for {len(failed)} company/companies."
        )

    if fatal_count > 0:
        section("STEP 12.1 FAILED")
        log(
            "Fatal feature validation exceptions detected. "
            "Review feature_exceptions.parquet."
        )
        raise RuntimeError(
            f"Step 12.1 failed with {fatal_count} fatal exception(s)."
        )

    section("STEP 12.1 COMPLETE")
    log(
        "All companies passed the feature data contract."
    )
    log(
        "Legitimate source-data incompleteness, "
        "non-consecutive history and insufficient history "
        "are reported separately and do not fail the pipeline."
    )
    log(
        "Steps 10, 11 and 12 were not modified."
    )


if __name__ == "__main__":
    main()
