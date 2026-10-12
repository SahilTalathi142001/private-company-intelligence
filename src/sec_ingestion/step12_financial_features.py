"""
======================================================================
STEP 12 - FINANCIAL FEATURE ENGINEERING
======================================================================

Purpose
-------
Build production-grade financial features from:

    Step 10 - Final Quarterly Gold
    Step 11 - Financial Analytics

Production guarantees
---------------------
1. Exactly one feature row per company-quarter.
2. No duplicate company-quarter rows.
3. Quarter continuity is explicitly validated.
4. TTM calculations require four consecutive quarters.
5. Rolling 4-quarter calculations require four consecutive quarters.
6. Missing source data is distinguished from insufficient history.
7. Feature status is explicitly recorded.
8. Feature version uses semantic versioning.
9. Existing Step 10 / Step 11 data is never modified.
10. Output is deterministic and idempotent.

Output
------
data/gold/financials_features/
    cik=<CIK>/
        financial_features.parquet

    feature_summary/
        financial_feature_summary.parquet

Feature version
---------------
1.1.0
"""

from __future__ import annotations

import math
import shutil
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# ======================================================================
# CONFIGURATION
# ======================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

FINAL_GOLD_DIR = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "financials_quarterly_final"
)

ANALYTICS_DIR = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "financials_analytics"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "financials_features"
)

SUMMARY_DIR = OUTPUT_DIR / "feature_summary"

FEATURE_VERSION = "1.2.0"

# Numeric tolerance used only for validation-style comparisons.
EPSILON = 1e-12

# Required consecutive quarters for TTM / rolling features.
ROLLING_QUARTERS = 4


# ======================================================================
# LOGGING
# ======================================================================

def log(message: str = "") -> None:
    print(message)


def section(title: str) -> None:
    log("=" * 60)
    log(title)
    log("=" * 60)


# ======================================================================
# GENERIC HELPERS
# ======================================================================

def safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def ensure_columns(
    df: pd.DataFrame,
    required: List[str],
    dataset_name: str,
) -> None:
    missing = [c for c in required if c not in df.columns]

    if missing:
        raise ValueError(
            f"{dataset_name} is missing required columns: {missing}\n"
            f"Available columns: {list(df.columns)}"
        )


def first_existing_column(
    df: pd.DataFrame,
    candidates: List[str],
) -> Optional[str]:

    for col in candidates:
        if col in df.columns:
            return col

    return None


# ======================================================================
# QUARTER NORMALIZATION
# ======================================================================

QUARTER_ORDER = {
    "Q1": 1,
    "Q2": 2,
    "Q3": 3,
    "Q4": 4,
}


def normalize_quarter(value) -> Optional[str]:

    if pd.isna(value):
        return None

    value = str(value).strip().upper()

    if value in QUARTER_ORDER:
        return value

    if value.startswith("Q") and value[1:].isdigit():
        q = f"Q{int(value[1:])}"

        if q in QUARTER_ORDER:
            return q

    return None


def quarter_index(fiscal_year, quarter) -> Optional[int]:

    if pd.isna(fiscal_year) or pd.isna(quarter):
        return None

    try:
        fy = int(fiscal_year)
    except Exception:
        return None

    q = normalize_quarter(quarter)

    if q is None:
        return None

    return fy * 4 + QUARTER_ORDER[q]


# ======================================================================
# NORMALIZE GOLD
# ======================================================================

def normalize_gold(
    gold_df: pd.DataFrame,
    cik: str,
) -> pd.DataFrame:

    df = gold_df.copy()

    # Step 10 Final Gold is a metric-level dataset.
    # Its authoritative grain is:
    #     cik + metric + fiscal_year + quarter
    # Example: one Q1 record each for revenue, operating_income,
    # and net_income. These are NOT duplicate company-quarter rows.
    ensure_columns(
        df,
        [
            "fiscal_year",
            "quarter",
            "metric",
            "value",
        ],
        f"Final Gold {cik}",
    )

    if "cik" not in df.columns:
        df["cik"] = cik

    df["cik"] = (
        df["cik"]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .str.zfill(10)
    )

    df["metric"] = (
        df["metric"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    df["fiscal_year"] = pd.to_numeric(
        df["fiscal_year"],
        errors="coerce",
    ).astype("Int64")

    df["quarter"] = df["quarter"].map(normalize_quarter)

    df["value"] = safe_numeric(df["value"])

    df["quarter_index"] = [
        quarter_index(fy, q)
        for fy, q in zip(
            df["fiscal_year"],
            df["quarter"],
        )
    ]

    # --------------------------------------------------------------
    # Validate the ACTUAL Final Gold grain.
    # --------------------------------------------------------------

    key = [
        "cik",
        "metric",
        "fiscal_year",
        "quarter",
    ]

    duplicates = df[df.duplicated(key, keep=False)]

    if not duplicates.empty:

        duplicate_counts = (
            duplicates
            .groupby(key, dropna=False)
            .size()
            .reset_index(name="row_count")
            .sort_values(key)
        )

        log(
            "ERROR: Final Gold contains duplicate "
            "metric-quarter rows."
        )

        log(
            duplicate_counts
            .head(100)
            .to_string(index=False)
        )

        raise ValueError(
            f"Final Gold grain violation for {cik}. "
            f"Expected one row per {key}."
        )

    return df


def build_gold_quarterly_metrics(
    gold_df: pd.DataFrame,
    cik: str,
) -> pd.DataFrame:
    """
    Convert metric-level Final Gold into the company-quarter grain
    required by Step 12 feature engineering.

    Input grain:
        cik + metric + fiscal_year + quarter

    Output grain:
        cik + fiscal_year + quarter

    The three financial metrics are pivoted into columns rather than
    being treated as duplicate company-quarter records.
    """

    metric_map = {
        "revenue": "revenue_value_gold",
        "operating_income": "operating_income_value_gold",
        "net_income": "net_income_value_gold",
    }

    df = gold_df.copy()

    unknown_metrics = sorted(
        set(df["metric"].dropna().unique())
        - set(metric_map)
    )

    if unknown_metrics:
        raise ValueError(
            f"Unexpected Final Gold metrics for {cik}: "
            f"{unknown_metrics}. "
            f"Expected metrics: {sorted(metric_map)}"
        )

    quarterly = (
        df[
            [
                "cik",
                "fiscal_year",
                "quarter",
                "metric",
                "value",
            ]
        ]
        .pivot(
            index=[
                "cik",
                "fiscal_year",
                "quarter",
            ],
            columns="metric",
            values="value",
        )
        .reset_index()
    )

    quarterly.columns.name = None

    for metric, output_column in metric_map.items():
        if metric in quarterly.columns:
            quarterly[output_column] = safe_numeric(
                quarterly[metric]
            )
            quarterly = quarterly.drop(
                columns=[metric]
            )
        else:
            quarterly[output_column] = np.nan

    quarterly["quarter_index"] = [
        quarter_index(fy, q)
        for fy, q in zip(
            quarterly["fiscal_year"],
            quarterly["quarter"],
        )
    ]

    quarterly = quarterly.sort_values(
        ["cik", "quarter_index"]
    ).reset_index(drop=True)

    # This is now the company-quarter grain used by Step 12.
    feature_key = [
        "cik",
        "fiscal_year",
        "quarter",
    ]

    duplicates = quarterly[
        quarterly.duplicated(feature_key, keep=False)
    ]

    if not duplicates.empty:
        raise ValueError(
            f"Gold pivot produced duplicate company-quarter "
            f"rows for {cik}."
        )

    return quarterly


# ======================================================================
# NORMALIZE ANALYTICS
# ======================================================================

def normalize_analytics(
    analytics_df: pd.DataFrame,
    cik: str,
) -> pd.DataFrame:

    df = analytics_df.copy()

    ensure_columns(
        df,
        [
            "fiscal_year",
            "quarter",
        ],
        f"Analytics {cik}",
    )

    if "cik" not in df.columns:
        df["cik"] = cik

    df["cik"] = (
        df["cik"]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .str.zfill(10)
    )

    df["fiscal_year"] = pd.to_numeric(
        df["fiscal_year"],
        errors="coerce",
    ).astype("Int64")

    df["quarter"] = df["quarter"].map(normalize_quarter)

    df["quarter_index"] = [
        quarter_index(fy, q)
        for fy, q in zip(
            df["fiscal_year"],
            df["quarter"],
        )
    ]

    numeric_columns = [
        "revenue_value",
        "revenue_qoq_growth_pct",
        "revenue_yoy_growth_pct",
        "operating_margin",
        "net_margin",
        "operating_income_value",
        "net_income_value",
    ]

    for col in numeric_columns:

        if col in df.columns:
            df[col] = safe_numeric(df[col])

    # --------------------------------------------------------------
    # Validate analytics grain.
    # --------------------------------------------------------------

    key = [
        "cik",
        "fiscal_year",
        "quarter",
    ]

    duplicates = df[df.duplicated(key, keep=False)]

    if not duplicates.empty:

        log(
            "ERROR: Analytics contains duplicate company-quarter rows."
        )

        log(
            duplicates[
                key
            ].to_string(index=False)
        )

        raise ValueError(
            f"Analytics grain violation for {cik}."
        )

    return df


# ======================================================================
# CONSECUTIVE QUARTER LOGIC
# ======================================================================

def is_consecutive_window(
    indexes: List,
) -> bool:

    if len(indexes) != ROLLING_QUARTERS:
        return False

    if any(pd.isna(x) for x in indexes):
        return False

    values = [int(x) for x in indexes]

    return (
        values == list(
            range(
                values[0],
                values[0] + ROLLING_QUARTERS,
            )
        )
    )


def window_status(
    indexes: List,
    values: pd.Series,
) -> str:
    """
    Determine why a rolling/TTM feature is unavailable.

    Returns:
        VALID
        INSUFFICIENT_HISTORY
        NON_CONSECUTIVE_QUARTERS
        SOURCE_DATA_MISSING
    """

    if len(values) < ROLLING_QUARTERS:
        return "INSUFFICIENT_HISTORY"

    if not is_consecutive_window(indexes):
        return "NON_CONSECUTIVE_QUARTERS"

    if values.isna().any():
        return "SOURCE_DATA_MISSING"

    return "VALID"


# ======================================================================
# TTM CALCULATION
# ======================================================================

def calculate_ttm(
    df: pd.DataFrame,
    value_column: str,
    prefix: str,
) -> pd.DataFrame:

    values = []
    statuses = []

    for i in range(len(df)):

        start = max(
            0,
            i - ROLLING_QUARTERS + 1,
        )

        window = df.iloc[start : i + 1]

        indexes = window[
            "quarter_index"
        ].tolist()

        metric_values = window[
            value_column
        ]

        status = window_status(
            indexes,
            metric_values,
        )

        statuses.append(status)

        if status == "VALID":
            values.append(
                metric_values.sum()
            )
        else:
            values.append(np.nan)

    df[f"ttm_{prefix}"] = values

    df[f"ttm_{prefix}_status"] = statuses

    return df


# ======================================================================
# ROLLING AVERAGE
# ======================================================================

def calculate_rolling_average(
    df: pd.DataFrame,
    value_column: str,
    output_column: str,
    status_column: str,
) -> pd.DataFrame:

    values = []
    statuses = []

    for i in range(len(df)):

        start = max(
            0,
            i - ROLLING_QUARTERS + 1,
        )

        window = df.iloc[start : i + 1]

        indexes = window[
            "quarter_index"
        ].tolist()

        metric_values = window[
            value_column
        ]

        status = window_status(
            indexes,
            metric_values,
        )

        statuses.append(status)

        if status == "VALID":
            values.append(
                metric_values.mean()
            )
        else:
            values.append(np.nan)

    df[output_column] = values
    df[status_column] = statuses

    return df


# ======================================================================
# ROLLING VOLATILITY
# ======================================================================

def calculate_rolling_std(
    df: pd.DataFrame,
    value_column: str,
    output_column: str,
    status_column: str,
) -> pd.DataFrame:

    values = []
    statuses = []

    for i in range(len(df)):

        start = max(
            0,
            i - ROLLING_QUARTERS + 1,
        )

        window = df.iloc[start : i + 1]

        indexes = window[
            "quarter_index"
        ].tolist()

        metric_values = window[
            value_column
        ]

        status = window_status(
            indexes,
            metric_values,
        )

        statuses.append(status)

        if status == "VALID":
            values.append(
                metric_values.std(
                    ddof=0
                )
            )
        else:
            values.append(np.nan)

    df[output_column] = values
    df[status_column] = statuses

    return df


# ======================================================================
# TREND CLASSIFICATION
# ======================================================================

def classify_trend(
    values: List,
    direction: str,
) -> str:

    clean = [
        x
        for x in values
        if pd.notna(x)
    ]

    if len(clean) < 3:
        return "INSUFFICIENT_HISTORY"

    first = clean[-3]
    last = clean[-1]

    if pd.isna(first) or pd.isna(last):
        return "SOURCE_DATA_MISSING"

    delta = last - first

    if abs(delta) <= EPSILON:
        return "STABLE"

    if direction == "higher_is_better":

        return (
            "EXPANDING"
            if delta > 0
            else "CONTRACTING"
        )

    return (
        "EXPANDING"
        if delta > 0
        else "CONTRACTING"
    )


def calculate_trend(
    df: pd.DataFrame,
    value_column: str,
    output_column: str,
) -> pd.DataFrame:

    trends = []

    for i in range(len(df)):

        start = max(
            0,
            i - 2,
        )

        window = df.iloc[start : i + 1]

        indexes = window[
            "quarter_index"
        ].tolist()

        if len(window) < 3:

            trends.append(
                "INSUFFICIENT_HISTORY"
            )

            continue

        if not is_consecutive_window(
            indexes
            + [indexes[-1] + 1]
        ):
            # We cannot use the artificial value above
            # as a real quarter. Explicitly check
            # continuity of the actual window.
            if len(indexes) >= 2:
                consecutive = all(
                    indexes[j] + 1
                    == indexes[j + 1]
                    for j in range(
                        len(indexes) - 1
                    )
                    if not pd.isna(indexes[j])
                    and not pd.isna(indexes[j + 1])
                )
            else:
                consecutive = False

            if not consecutive:
                trends.append(
                    "NON_CONSECUTIVE_QUARTERS"
                )
                continue

        values = window[
            value_column
        ].tolist()

        if any(
            pd.isna(x)
            for x in values
        ):
            trends.append(
                "SOURCE_DATA_MISSING"
            )
            continue

        delta = values[-1] - values[0]

        if abs(delta) <= EPSILON:
            trends.append("STABLE")

        elif delta > 0:
            trends.append("EXPANDING")

        else:
            trends.append("CONTRACTING")

    df[output_column] = trends

    return df


# ======================================================================
# QUARTER-OVER-QUARTER FEATURES
# ======================================================================

def calculate_qoq_change(
    df: pd.DataFrame,
    source_column: str,
    output_column: str,
) -> pd.DataFrame:

    previous = df[source_column].shift(1)

    df[output_column] = (
        df[source_column]
        - previous
    )

    # Break calculations across quarter gaps.
    consecutive = (
        df["quarter_index"]
        - df["quarter_index"].shift(1)
        == 1
    )

    df.loc[
        ~consecutive,
        output_column,
    ] = np.nan

    return df


# ======================================================================
# GROWTH ACCELERATION
# ======================================================================

def calculate_growth_acceleration(
    df: pd.DataFrame,
) -> pd.DataFrame:

    growth = df[
        "revenue_yoy_growth_pct"
    ]

    previous_growth = growth.shift(1)

    consecutive = (
        df["quarter_index"]
        - df["quarter_index"].shift(1)
        == 1
    )

    acceleration = (
        growth
        - previous_growth
    )

    acceleration.loc[
        ~consecutive
    ] = np.nan

    df[
        "revenue_growth_acceleration"
    ] = acceleration

    return df


# ======================================================================
# FOUR-QUARTER FLAGS
# ======================================================================

def calculate_consistency_features(
    df: pd.DataFrame,
) -> pd.DataFrame:

    positive_growth = []
    growth_consistent = []
    margin_expansion = []
    net_margin_expansion = []

    for i in range(len(df)):

        start = max(
            0,
            i - ROLLING_QUARTERS + 1,
        )

        window = df.iloc[start : i + 1]

        indexes = window[
            "quarter_index"
        ].tolist()

        valid_window = (
            len(window) == ROLLING_QUARTERS
            and is_consecutive_window(indexes)
        )

        if not valid_window:

            positive_growth.append(np.nan)
            growth_consistent.append(np.nan)
            margin_expansion.append(np.nan)
            net_margin_expansion.append(np.nan)

            continue

        growth = window[
            "revenue_yoy_growth_pct"
        ]

        op_margin = window[
            "operating_margin"
        ]

        net_margin = window[
            "net_margin"
        ]

        if growth.isna().any():
            positive_growth.append(np.nan)
            growth_consistent.append(np.nan)
        else:
            positive_growth.append(
                int(
                    (growth > 0).sum()
                )
            )

            growth_consistent.append(
                bool(
                    (growth > 0).all()
                )
            )

        if op_margin.isna().any():
            margin_expansion.append(np.nan)
        else:
            margin_expansion.append(
                int(
                    (
                        op_margin.diff() > 0
                    ).sum()
                )
            )

        if net_margin.isna().any():
            net_margin_expansion.append(
                np.nan
            )
        else:
            net_margin_expansion.append(
                int(
                    (
                        net_margin.diff() > 0
                    ).sum()
                )
            )

    df[
        "revenue_positive_growth_quarters_4q"
    ] = positive_growth

    df[
        "revenue_growth_consistent_4q_flag"
    ] = growth_consistent

    df[
        "operating_margin_expansion_quarters_4q"
    ] = margin_expansion

    df[
        "net_margin_expansion_quarters_4q"
    ] = net_margin_expansion

    return df


# ======================================================================
# FEATURE DATA QUALITY
# ======================================================================

def determine_feature_quality(
    row: pd.Series,
) -> str:

    statuses = [
        row.get(
            "ttm_revenue_status",
            None,
        ),
        row.get(
            "ttm_operating_income_status",
            None,
        ),
        row.get(
            "ttm_net_income_status",
            None,
        ),
    ]

    if any(
        s == "NON_CONSECUTIVE_QUARTERS"
        for s in statuses
    ):
        return "NON_CONSECUTIVE_DATA"

    if any(
        s == "SOURCE_DATA_MISSING"
        for s in statuses
    ):
        return "SOURCE_DATA_INCOMPLETE"

    if all(
        s == "VALID"
        for s in statuses
    ):
        return "COMPLETE"

    if any(
        s == "VALID"
        for s in statuses
    ):
        return "PARTIAL"

    return "INSUFFICIENT_HISTORY"


# ======================================================================
# BUILD FEATURES
# ======================================================================

def build_features(
    gold_df: pd.DataFrame,
    analytics_df: pd.DataFrame,
    cik: str,
) -> pd.DataFrame:

    gold = normalize_gold(
        gold_df,
        cik,
    )

    # Final Gold is metric-level. Pivot it to the company-quarter
    # grain before joining it to Step 11 Analytics.
    gold_quarterly = build_gold_quarterly_metrics(
        gold,
        cik,
    )

    analytics = normalize_analytics(
        analytics_df,
        cik,
    )

    key = [
        "cik",
        "fiscal_year",
        "quarter",
    ]

    # --------------------------------------------------------------
    # Merge only the authoritative Gold financial values into
    # Analytics.
    # --------------------------------------------------------------

    gold_columns = [
        "cik",
        "fiscal_year",
        "quarter",
        "quarter_index",
        "revenue_value_gold",
        "operating_income_value_gold",
        "net_income_value_gold",
    ]

    gold_subset = gold_quarterly[
        gold_columns
    ].copy()

    df = analytics.merge(
        gold_subset,
        on=key,
        how="left",
        suffixes=(
            "",
            "_from_gold",
        ),
        validate="one_to_one",
    )

    if len(df) != len(analytics):
        raise ValueError(
            f"Gold/Analytics merge changed row count for {cik}: "
            f"analytics={len(analytics)}, merged={len(df)}."
        )

    # Analytics revenue is expected to originate from Gold.
    # Use Gold as the authoritative value.
    df["revenue_value"] = df[
        "revenue_value_gold"
    ]

    df = df.sort_values(
        [
            "cik",
            "quarter_index",
        ]
    ).reset_index(
        drop=True
    )

    # --------------------------------------------------------------
    # Ensure financial columns exist.
    # --------------------------------------------------------------

    for col in [
        "operating_income_value",
        "net_income_value",
    ]:

        if col not in df.columns:
            df[col] = np.nan

    # If analytics has missing operating/net income values,
    # retain Gold as the authoritative source where available.
    df["operating_income_value"] = (
        df["operating_income_value"]
        .combine_first(
            df[
                "operating_income_value_gold"
            ]
        )
    )

    df["net_income_value"] = (
        df["net_income_value"]
        .combine_first(
            df[
                "net_income_value_gold"
            ]
        )
    )

    # --------------------------------------------------------------
    # Basic structural feature.
    # --------------------------------------------------------------

    df[
        "feature_version"
    ] = FEATURE_VERSION

    df[
        "quarter_contiguous_with_previous"
    ] = (
        df["quarter_index"]
        - df["quarter_index"].shift(1)
        == 1
    )

    # First observation is not a broken sequence.
    if len(df) > 0:
        df.loc[
            df.index[0],
            "quarter_contiguous_with_previous",
        ] = True

    # --------------------------------------------------------------
    # QoQ margin change.
    # --------------------------------------------------------------

    df = calculate_qoq_change(
        df,
        "operating_margin",
        "operating_margin_qoq_change",
    )

    df = calculate_qoq_change(
        df,
        "net_margin",
        "net_margin_qoq_change",
    )

    # --------------------------------------------------------------
    # YoY changes.
    # --------------------------------------------------------------

    revenue_yoy = df[
        "revenue_yoy_growth_pct"
    ]

    df[
        "revenue_growth_yoy_change"
    ] = (
        revenue_yoy
        - revenue_yoy.shift(4)
    )

    df[
        "operating_margin_yoy_change"
    ] = (
        df["operating_margin"]
        - df["operating_margin"].shift(4)
    )

    df[
        "net_margin_yoy_change"
    ] = (
        df["net_margin"]
        - df["net_margin"].shift(4)
    )

    # --------------------------------------------------------------
    # Prevent YoY calculations across gaps.
    # --------------------------------------------------------------

    for col in [
        "revenue_growth_yoy_change",
        "operating_margin_yoy_change",
        "net_margin_yoy_change",
    ]:

        valid = (
            df["quarter_index"]
            - df["quarter_index"].shift(4)
            == 4
        )

        df.loc[
            ~valid,
            col,
        ] = np.nan

    # --------------------------------------------------------------
    # Revenue acceleration.
    # --------------------------------------------------------------

    df = calculate_growth_acceleration(
        df
    )

    # --------------------------------------------------------------
    # TTM.
    # --------------------------------------------------------------

    df = calculate_ttm(
        df,
        "revenue_value_gold",
        "revenue",
    )

    df = calculate_ttm(
        df,
        "operating_income_value",
        "operating_income",
    )

    df = calculate_ttm(
        df,
        "net_income_value",
        "net_income",
    )

    # --------------------------------------------------------------
    # TTM margins.
    # --------------------------------------------------------------

    df[
        "ttm_operating_margin"
    ] = np.where(
        (
            df[
                "ttm_revenue"
            ].notna()
            &
            df[
                "ttm_operating_income"
            ].notna()
            &
            (
                df[
                    "ttm_revenue"
                ].abs()
                > EPSILON
            )
        ),
        df[
            "ttm_operating_income"
        ]
        /
        df[
            "ttm_revenue"
        ],
        np.nan,
    )

    df[
        "ttm_net_margin"
    ] = np.where(
        (
            df[
                "ttm_revenue"
            ].notna()
            &
            df[
                "ttm_net_income"
            ].notna()
            &
            (
                df[
                    "ttm_revenue"
                ].abs()
                > EPSILON
            )
        ),
        df[
            "ttm_net_income"
        ]
        /
        df[
            "ttm_revenue"
        ],
        np.nan,
    )

    # --------------------------------------------------------------
    # Rolling 4Q averages.
    # --------------------------------------------------------------

    df = calculate_rolling_average(
        df,
        "revenue_yoy_growth_pct",
        "revenue_growth_4q_avg",
        "revenue_growth_4q_status",
    )

    df = calculate_rolling_average(
        df,
        "operating_margin",
        "operating_margin_4q_avg",
        "operating_margin_4q_status",
    )

    df = calculate_rolling_average(
        df,
        "net_margin",
        "net_margin_4q_avg",
        "net_margin_4q_status",
    )

    # --------------------------------------------------------------
    # Rolling volatility.
    # --------------------------------------------------------------

    df = calculate_rolling_std(
        df,
        "revenue_yoy_growth_pct",
        "revenue_growth_volatility_4q",
        "revenue_growth_volatility_4q_status",
    )

    df = calculate_rolling_std(
        df,
        "operating_margin",
        "operating_margin_volatility_4q",
        "operating_margin_volatility_4q_status",
    )

    df = calculate_rolling_std(
        df,
        "net_margin",
        "net_margin_volatility_4q",
        "net_margin_volatility_4q_status",
    )

    # --------------------------------------------------------------
    # Trend.
    # --------------------------------------------------------------

    df = calculate_trend(
        df,
        "revenue_yoy_growth_pct",
        "revenue_growth_trend",
    )

    df = calculate_trend(
        df,
        "operating_margin",
        "operating_margin_trend",
    )

    df = calculate_trend(
        df,
        "net_margin",
        "net_margin_trend",
    )

    # --------------------------------------------------------------
    # Consistency.
    # --------------------------------------------------------------

    df = calculate_consistency_features(
        df
    )

    # --------------------------------------------------------------
    # Feature quality.
    # --------------------------------------------------------------

    df[
        "feature_data_quality"
    ] = df.apply(
        determine_feature_quality,
        axis=1,
    )

    # --------------------------------------------------------------
    # Feature availability.
    # --------------------------------------------------------------

    df[
        "ttm_revenue_available"
    ] = df[
        "ttm_revenue"
    ].notna()

    df[
        "ttm_operating_income_available"
    ] = df[
        "ttm_operating_income"
    ].notna()

    df[
        "ttm_net_income_available"
    ] = df[
        "ttm_net_income"
    ].notna()

    # --------------------------------------------------------------
    # Metadata.
    # --------------------------------------------------------------

    df[
        "feature_generated_at_utc"
    ] = pd.Timestamp.utcnow()

    return df


# ======================================================================
# OUTPUT COLUMN ORDER
# ======================================================================

def order_output_columns(
    df: pd.DataFrame,
) -> pd.DataFrame:

    preferred = [
        "cik",
        "fiscal_year",
        "quarter",
        "quarter_index",

        "feature_version",

        # Base financial values
        "revenue_value",
        "operating_income_value",
        "net_income_value",

        # Growth
        "revenue_qoq_growth_pct",
        "revenue_yoy_growth_pct",
        "revenue_growth_acceleration",
        "revenue_growth_yoy_change",

        # Margins
        "operating_margin",
        "net_margin",

        # Margin changes
        "operating_margin_qoq_change",
        "operating_margin_yoy_change",
        "net_margin_qoq_change",
        "net_margin_yoy_change",

        # TTM
        "ttm_revenue",
        "ttm_operating_income",
        "ttm_net_income",
        "ttm_operating_margin",
        "ttm_net_margin",

        # TTM status
        "ttm_revenue_status",
        "ttm_operating_income_status",
        "ttm_net_income_status",

        # Rolling averages
        "revenue_growth_4q_avg",
        "operating_margin_4q_avg",
        "net_margin_4q_avg",

        # Rolling status
        "revenue_growth_4q_status",
        "operating_margin_4q_status",
        "net_margin_4q_status",

        # Volatility
        "revenue_growth_volatility_4q",
        "operating_margin_volatility_4q",
        "net_margin_volatility_4q",

        # Volatility status
        "revenue_growth_volatility_4q_status",
        "operating_margin_volatility_4q_status",
        "net_margin_volatility_4q_status",

        # Trend
        "revenue_growth_trend",
        "operating_margin_trend",
        "net_margin_trend",

        # Consistency
        "revenue_positive_growth_quarters_4q",
        "revenue_growth_consistent_4q_flag",
        "operating_margin_expansion_quarters_4q",
        "net_margin_expansion_quarters_4q",

        # Continuity
        "quarter_contiguous_with_previous",

        # Availability
        "ttm_revenue_available",
        "ttm_operating_income_available",
        "ttm_net_income_available",

        # Quality
        "feature_data_quality",

        # Metadata
        "feature_generated_at_utc",
    ]

    existing = [
        c
        for c in preferred
        if c in df.columns
    ]

    remaining = [
        c
        for c in df.columns
        if c not in existing
    ]

    return df[
        existing + remaining
    ]


# ======================================================================
# VALIDATE FEATURES
# ======================================================================

def validate_features(
    df: pd.DataFrame,
    cik: str,
) -> None:

    key = [
        "cik",
        "fiscal_year",
        "quarter",
    ]

    # --------------------------------------------------------------
    # Grain.
    # --------------------------------------------------------------

    duplicates = df[
        df.duplicated(
            key,
            keep=False,
        )
    ]

    if not duplicates.empty:

        raise ValueError(
            f"Feature grain violation for {cik}: "
            f"duplicate company-quarter rows."
        )

    # --------------------------------------------------------------
    # Quarter ordering.
    # --------------------------------------------------------------

    expected = (
        df.sort_values(
            "quarter_index"
        )
        .reset_index(drop=True)
    )

    if len(expected) != len(df):

        raise ValueError(
            f"Feature ordering failure for {cik}."
        )

    # --------------------------------------------------------------
    # TTM correctness.
    # --------------------------------------------------------------

    for i in range(
        ROLLING_QUARTERS - 1,
        len(df),
    ):

        window = df.iloc[
            i - ROLLING_QUARTERS + 1 :
            i + 1
        ]

        indexes = window[
            "quarter_index"
        ].tolist()

        if not is_consecutive_window(
            indexes
        ):
            continue

        revenue = window[
            "revenue_value_gold"
        ]

        if revenue.notna().all():

            expected_ttm = revenue.sum()

            actual_ttm = df.iloc[i][
                "ttm_revenue"
            ]

            if not np.isclose(
                expected_ttm,
                actual_ttm,
                rtol=0,
                atol=EPSILON,
            ):

                raise ValueError(
                    f"TTM revenue validation failed "
                    f"for {cik}, "
                    f"{df.iloc[i]['fiscal_year']} "
                    f"{df.iloc[i]['quarter']}."
                )

    # --------------------------------------------------------------
    # Semantic version.
    # --------------------------------------------------------------

    if not (
        isinstance(
            FEATURE_VERSION,
            str,
        )
        and FEATURE_VERSION.count(".") == 2
    ):

        raise ValueError(
            "FEATURE_VERSION must use semantic versioning."
        )


# ======================================================================
# COMPANY PROCESSING
# ======================================================================

def process_company(
    cik: str,
) -> Tuple[pd.DataFrame, Dict]:

    gold_path = (
        FINAL_GOLD_DIR
        / f"cik={cik}"
        / "financials_quarterly.parquet"
    )

    analytics_path = (
        ANALYTICS_DIR
        / f"cik={cik}"
        / "financials_analytics.parquet"
    )

    if not gold_path.exists():
        raise FileNotFoundError(
            f"Final Gold not found: {gold_path}"
        )

    if not analytics_path.exists():
        raise FileNotFoundError(
            f"Analytics not found: {analytics_path}"
        )

    gold_df = pd.read_parquet(
        gold_path
    )

    analytics_df = pd.read_parquet(
        analytics_path
    )

    log(
        f"Final Gold rows: {len(gold_df)}"
    )

    log(
        f"Analytics rows: {len(analytics_df)}"
    )

    features = build_features(
        gold_df,
        analytics_df,
        cik,
    )

    features = order_output_columns(
        features
    )

    validate_features(
        features,
        cik,
    )

    # --------------------------------------------------------------
    # Summary metrics.
    # --------------------------------------------------------------

    summary = {
        "cik": cik,

        "input_gold_rows": len(gold_df),

        "input_analytics_rows": len(
            analytics_df
        ),

        "feature_rows": len(
            features
        ),

        "unique_company_quarters": (
            features[
                [
                    "cik",
                    "fiscal_year",
                    "quarter",
                ]
            ]
            .drop_duplicates()
            .shape[0]
        ),

        "ttm_revenue_rows": int(
            features[
                "ttm_revenue"
            ].notna().sum()
        ),

        "ttm_operating_income_rows": int(
            features[
                "ttm_operating_income"
            ].notna().sum()
        ),

        "ttm_net_income_rows": int(
            features[
                "ttm_net_income"
            ].notna().sum()
        ),

        "revenue_growth_acceleration_rows": int(
            features[
                "revenue_growth_acceleration"
            ].notna().sum()
        ),

        "non_consecutive_rows": int(
            (
                features[
                    "feature_data_quality"
                ]
                == "NON_CONSECUTIVE_DATA"
            ).sum()
        ),

        "source_incomplete_rows": int(
            (
                features[
                    "feature_data_quality"
                ]
                == "SOURCE_DATA_INCOMPLETE"
            ).sum()
        ),

        "partial_feature_rows": int(
            (
                features[
                    "feature_data_quality"
                ]
                == "PARTIAL"
            ).sum()
        ),

        "insufficient_history_rows": int(
            (
                features[
                    "feature_data_quality"
                ]
                == "INSUFFICIENT_HISTORY"
            ).sum()
        ),

        "complete_feature_rows": int(
            (
                features[
                    "feature_data_quality"
                ]
                == "COMPLETE"
            ).sum()
        ),

        "feature_version": FEATURE_VERSION,
    }

    return features, summary


# ======================================================================
# DISCOVER COMPANIES
# ======================================================================

def discover_companies() -> List[str]:

    companies = []

    if not FINAL_GOLD_DIR.exists():
        return companies

    for path in FINAL_GOLD_DIR.glob(
        "cik=*"
    ):

        if not path.is_dir():
            continue

        cik = path.name.replace(
            "cik=",
            "",
        )

        if cik:
            companies.append(cik)

    return sorted(
        companies
    )


# ======================================================================
# SAVE OUTPUT
# ======================================================================

def save_company_output(
    cik: str,
    features: pd.DataFrame,
) -> Path:

    output_dir = (
        OUTPUT_DIR
        / f"cik={cik}"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        output_dir
        / "financial_features.parquet"
    )

    # Atomic-style replacement:
    # write temporary parquet first.
    temp_path = (
        output_dir
        / "financial_features.tmp.parquet"
    )

    if temp_path.exists():
        temp_path.unlink()

    features.to_parquet(
        temp_path,
        index=False,
    )

    temp_path.replace(
        output_path
    )

    return output_path


# ======================================================================
# MAIN
# ======================================================================

def main() -> None:

    section(
        "STEP 12 - FINANCIAL FEATURE ENGINEERING"
    )

    log(
        f"Final Gold : {FINAL_GOLD_DIR}"
    )

    log(
        f"Analytics  : {ANALYTICS_DIR}"
    )

    log(
        f"Output     : {OUTPUT_DIR}"
    )

    log(
        f"Feature version: {FEATURE_VERSION}"
    )

    companies = discover_companies()

    log(
        f"Companies discovered: {len(companies)}"
    )

    if not companies:

        raise RuntimeError(
            "No companies discovered in Final Gold."
        )

    SUMMARY_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    summaries = []

    failed_companies = []

    for cik in companies:

        log(
            "=" * 60
        )

        log(
            f"PROCESSING CIK: {cik}"
        )

        try:

            features, summary = (
                process_company(
                    cik
                )
            )

            output_path = (
                save_company_output(
                    cik,
                    features,
                )
            )

            log(
                f"Feature rows: {len(features)}"
            )

            log(
                "Unique company-quarter rows: "
                f"{summary['unique_company_quarters']}"
            )

            log(
                "TTM Revenue rows: "
                f"{summary['ttm_revenue_rows']}"
            )

            log(
                "TTM Operating Income rows: "
                f"{summary['ttm_operating_income_rows']}"
            )

            log(
                "TTM Net Income rows: "
                f"{summary['ttm_net_income_rows']}"
            )

            log(
                "Feature quality:"
            )

            log(
                f"  COMPLETE: "
                f"{summary['complete_feature_rows']}"
            )

            log(
                f"  PARTIAL: "
                f"{summary['partial_feature_rows']}"
            )

            log(
                f"  SOURCE_DATA_INCOMPLETE: "
                f"{summary['source_incomplete_rows']}"
            )

            log(
                f"  NON_CONSECUTIVE_DATA: "
                f"{summary['non_consecutive_rows']}"
            )

            log(
                f"  INSUFFICIENT_HISTORY: "
                f"{summary['insufficient_history_rows']}"
            )

            log(
                f"Saved: {output_path}"
            )

            # ------------------------------------------------------
            # Recent features.
            # ------------------------------------------------------

            display_columns = [
                "fiscal_year",
                "quarter",
                "revenue_value",
                "revenue_yoy_growth_pct",
                "revenue_growth_acceleration",
                "operating_margin",
                "operating_margin_qoq_change",
                "ttm_revenue",
                "ttm_operating_margin",
                "ttm_revenue_status",
                "revenue_growth_trend",
                "operating_margin_trend",
                "feature_data_quality",
            ]

            display_columns = [
                c
                for c in display_columns
                if c in features.columns
            ]

            log(
                "\nRecent financial features:"
            )

            log(
                features[
                    display_columns
                ]
                .tail(12)
                .to_string(
                    index=False
                )
            )

            summaries.append(
                summary
            )

        except Exception as exc:

            failed_companies.append(
                (
                    cik,
                    str(exc),
                )
            )

            log(
                f"ERROR processing {cik}: {exc}"
            )

            traceback.print_exc()

    # ==================================================================
    # SUMMARY
    # ==================================================================

    if summaries:

        summary_df = pd.DataFrame(
            summaries
        )

        summary_path = (
            SUMMARY_DIR
            / "financial_feature_summary.parquet"
        )

        temp_summary = (
            SUMMARY_DIR
            / "financial_feature_summary.tmp.parquet"
        )

        if temp_summary.exists():
            temp_summary.unlink()

        summary_df.to_parquet(
            temp_summary,
            index=False,
        )

        temp_summary.replace(
            summary_path
        )

        log(
            "=" * 60
        )

        log(
            "STEP 12 SUMMARY"
        )

        log(
            summary_df.to_string(
                index=False
            )
        )

        log(
            f"\nSummary saved: {summary_path}"
        )

    # ==================================================================
    # FAILURE HANDLING
    # ==================================================================

    if failed_companies:

        log(
            "=" * 60
        )

        log(
            "STEP 12 FAILED"
        )

        log(
            "Failed companies:"
        )

        for cik, error in failed_companies:

            log(
                f"  {cik}: {error}"
            )

        raise RuntimeError(
            f"Step 12 failed for "
            f"{len(failed_companies)} "
            f"company/company companies."
        )

    # ==================================================================
    # SUCCESS
    # ==================================================================

    log(
        "=" * 60
    )

    log(
        "STEP 12 COMPLETE - ALL COMPANIES SUCCESSFUL"
    )

    log(
        f"Feature version: {FEATURE_VERSION}"
    )

    log(
        "=" * 60
    )


if __name__ == "__main__":
    main()