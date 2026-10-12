"""
======================================================================
STEP 11 - FINANCIAL ANALYTICS
======================================================================

Purpose
-------
Build production-grade quarterly financial analytics from STEP 10
FINAL QUARTERLY GOLD.

Input
-----
data/gold/financials_quarterly_final/
    cik=*/financials_quarterly.parquet

Output
------
data/gold/financials_analytics/
    cik=*/financials_analytics.parquet

Production Grain
----------------
Exactly ONE row per:

    cik + fiscal_year + quarter

Important
---------
STEP 10 is the source of truth for quarter construction.

STEP 11 does NOT:
    - create Q4
    - derive annual Q4
    - merge separate metadata variants into separate rows
    - silently drop duplicate financial facts

STEP 11 only:
    - consumes canonical STEP 10 quarterly Gold
    - calculates analytics
    - preserves quarter-level provenance
    - enforces analytics grain
======================================================================
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


# ======================================================================
# CONFIG
# ======================================================================

BASE_DIR = Path(__file__).resolve().parents[2]

INPUT_DIR = (
    BASE_DIR
    / "data"
    / "gold"
    / "financials_quarterly_final"
)

OUTPUT_DIR = (
    BASE_DIR
    / "data"
    / "gold"
    / "financials_analytics"
)

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)

METRICS = [
    "revenue",
    "operating_income",
    "net_income",
]

ANALYTICS_KEY = [
    "cik",
    "fiscal_year",
    "quarter",
]

# Floating-point tolerance used for internal consistency checks.
FLOAT_TOL = 1e-12


# ======================================================================
# LOGGING
# ======================================================================

def banner(text: str) -> None:
    print()
    print("=" * 60)
    print(text)
    print("=" * 60)


def log(text: str) -> None:
    print(text)


# ======================================================================
# HELPERS
# ======================================================================

def safe_pct_change(
    current: float,
    previous: float,
) -> float:
    """
    Percentage change:

        (current - previous) / abs(previous)

    Returns NaN when:
        - current is missing
        - previous is missing
        - previous is zero
    """

    if pd.isna(current):
        return np.nan

    if pd.isna(previous):
        return np.nan

    if previous == 0:
        return np.nan

    return (
        (current - previous)
        / abs(previous)
    )


def normalize_quarter(value) -> str | None:
    """
    Normalize quarter labels to Q1/Q2/Q3/Q4.
    """

    if pd.isna(value):
        return None

    value = str(value).strip().upper()

    if value in {"Q1", "Q2", "Q3", "Q4"}:
        return value

    # Handles values such as:
    # quarter_1
    # quarter1
    # 1
    # FY2024Q1

    match = pd.Series([value]).str.extract(
        r"Q?([1-4])",
        expand=False
    ).iloc[0]

    if pd.isna(match):
        return None

    return f"Q{int(match)}"


def quarter_to_num(value: str) -> int:
    """
    Convert Q1/Q2/Q3/Q4 to 1/2/3/4.
    """

    if value is None:
        return np.nan

    try:
        return int(str(value).replace("Q", ""))
    except Exception:
        return np.nan


def quarter_sequence(
    fiscal_year,
    quarter_num,
) -> int | None:
    """
    Convert fiscal year + quarter into a continuous integer sequence.

    Example:

        2024 Q1 -> 8097
        2024 Q2 -> 8098
        2024 Q3 -> 8099
        2024 Q4 -> 8100
        2025 Q1 -> 8101

    This allows us to determine whether two rows are actually
    consecutive quarters.
    """

    if pd.isna(fiscal_year) or pd.isna(quarter_num):
        return None

    return (
        int(fiscal_year) * 4
        + int(quarter_num)
    )


def first_non_null(series: pd.Series):
    """
    Return first non-null value.
    """

    values = series.dropna()

    if values.empty:
        return None

    return values.iloc[0]


def _debug_log(hypothesis_id: str, location: str, message: str, data: dict) -> None:
    # region agent log
    try:
        import json
        import time
        from pathlib import Path
        payload = {
            "sessionId": "63fc53",
            "runId": "post-fix",
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(time.time() * 1000),
        }
        with open(Path(__file__).resolve().parents[2] / "debug-63fc53.log", "a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, default=str) + "\n")
    except Exception:
        pass
    # endregion


def canonical_period_boundary(values: list, column: str):
    """Collapse a one-day quarter-boundary disagreement to one timestamp.

    SEC facts for the same quarter often use either the prior quarter-end
    or the next quarter-start (2009-03-31 versus 2009-04-01). That is one
    boundary, not two periods. A wider disagreement stays a hard conflict.
    """
    if len(values) <= 1:
        return values

    stamps = sorted(pd.Timestamp(value) for value in values)
    if stamps[-1] - stamps[0] > pd.Timedelta(days=1):
        return values

    # Quarter start uses the later date. Quarter end uses the earlier date.
    chosen = stamps[-1] if column == "period_start" else stamps[0]
    return [chosen]


def unique_non_null_values(
    series: pd.Series,
) -> list:
    """
    Return unique non-null values preserving appearance order.
    """

    values = []

    for value in series:

        if pd.isna(value):
            continue

        if value not in values:
            values.append(value)

    return values


# ======================================================================
# VALIDATION - INPUT GRAIN
# ======================================================================

def validate_step10_metric_grain(
    df: pd.DataFrame,
    cik: str,
) -> None:
    """
    STEP 10 should contain at most one row for:

        cik + metric + fiscal_year + quarter

    If duplicates exist, STOP.

    We do NOT silently choose one row because doing so would hide
    a Gold-layer defect.
    """

    metric_key = [
        "cik",
        "metric",
        "fiscal_year",
        "quarter",
    ]

    duplicates = df[
        df.duplicated(
            subset=metric_key,
            keep=False
        )
    ].copy()

    if not duplicates.empty:

        print()
        print(
            "ERROR: STEP 10 contains duplicate "
            "metric-quarter rows."
        )

        print(
            duplicates[
                [
                    col
                    for col in [
                        "cik",
                        "metric",
                        "fiscal_year",
                        "quarter",
                        "value",
                        "period_start",
                        "period_end",
                        "source_period_type",
                        "quarter_assignment_method",
                    ]
                    if col in duplicates.columns
                ]
            ]
            .sort_values(metric_key)
            .to_string(index=False)
        )

        raise ValueError(
            f"STEP 10 grain violation for {cik}. "
            f"Expected one row per "
            f"{metric_key}."
        )


# ======================================================================
# CANONICAL QUARTER GRAIN
# ======================================================================

def build_canonical_quarter_table(
    df: pd.DataFrame,
    cik: str,
) -> pd.DataFrame:
    """
    Build the ONLY quarter-level table used by analytics.

    Critical design decision
    ------------------------
    Metadata is aggregated AFTER establishing the logical quarter grain.

    Therefore:

        source_period_type
        quarter_assignment_method
        period_start
        period_end

    can never create duplicate analytics rows.

    The financial values remain metric-specific and are pivoted later.
    """

    base_key = [
        "cik",
        "fiscal_year",
        "quarter",
    ]

    # --------------------------------------------------------------
    # Ensure required key columns exist
    # --------------------------------------------------------------

    required = [
        "cik",
        "fiscal_year",
        "quarter",
        "quarter_num",
        "period_start",
        "period_end",
        "source_period_type",
        "quarter_assignment_method",
    ]

    for column in required:

        if column not in df.columns:
            df[column] = np.nan

    # --------------------------------------------------------------
    # Quarter-level metadata
    # --------------------------------------------------------------

    grouped = (
        df
        .groupby(
            base_key,
            dropna=False,
            sort=False
        )
    )

    records = []

    for key, group in grouped:

        row = {
            "cik": key[0],
            "fiscal_year": key[1],
            "quarter": key[2],
        }

        # ----------------------------------------------------------
        # Quarter number
        # ----------------------------------------------------------

        quarter_nums = unique_non_null_values(
            group["quarter_num"]
        )

        if len(quarter_nums) > 1:

            raise ValueError(
                f"Conflicting quarter numbers for "
                f"{cik} / {key[1]} / {key[2]}: "
                f"{quarter_nums}"
            )

        if quarter_nums:

            row["quarter_num"] = quarter_nums[0]

        else:

            row["quarter_num"] = quarter_to_num(
                key[2]
            )

        # ----------------------------------------------------------
        # Period start/end
        #
        # A logical quarter should have one period boundary.
        # If Step 10 gives conflicting boundaries, stop.
        # ----------------------------------------------------------

        for column in [
            "period_start",
            "period_end",
        ]:

            raw_values = unique_non_null_values(
                group[column]
            )
            values = canonical_period_boundary(
                raw_values,
                column,
            )

            if len(raw_values) > 1 and len(values) == 1:
                # region agent log
                _debug_log(
                    "C",
                    "financial_analytics.py:build_canonical_quarter_table",
                    "collapsed one-day period boundary",
                    {
                        "cik": str(cik),
                        "fiscal_year": str(key[1]),
                        "quarter": str(key[2]),
                        "column": column,
                        "raw": [str(value) for value in raw_values],
                        "chosen": str(values[0]),
                    },
                )
                # endregion

            if len(values) > 1:

                raise ValueError(
                    f"Conflicting {column} values for "
                    f"{cik} / {key[1]} / {key[2]}: "
                    f"{values}"
                )

            row[column] = (
                values[0]
                if values
                else pd.NaT
            )

        # ----------------------------------------------------------
        # SOURCE PERIOD TYPE
        #
        # STEP 10 owns this information.
        #
        # If multiple metric rows disagree:
        #
        #     annual_derived wins
        #
        # because the quarter contains at least one derived
        # annual-to-quarter value.
        # ----------------------------------------------------------

        source_types = unique_non_null_values(
            group["source_period_type"]
        )

        if "annual_derived" in source_types:

            row["source_period_type"] = (
                "annual_derived"
            )

        elif len(source_types) == 1:

            row["source_period_type"] = (
                source_types[0]
            )

        elif len(source_types) == 0:

            row["source_period_type"] = None

        else:

            raise ValueError(
                f"Conflicting source_period_type values "
                f"for {cik} / {key[1]} / {key[2]}: "
                f"{source_types}"
            )

        # ----------------------------------------------------------
        # QUARTER ASSIGNMENT METHOD
        #
        # Preserve all distinct methods in a stable representation.
        # This is lineage metadata and must never affect grain.
        # ----------------------------------------------------------

        methods = unique_non_null_values(
            group[
                "quarter_assignment_method"
            ]
        )

        if not methods:

            row[
                "quarter_assignment_method"
            ] = None

        elif len(methods) == 1:

            row[
                "quarter_assignment_method"
            ] = methods[0]

        else:

            row[
                "quarter_assignment_method"
            ] = "|".join(
                str(x)
                for x in methods
            )

        records.append(row)

    quarter_df = pd.DataFrame(records)

    if quarter_df.empty:
        raise ValueError(
            f"No canonical quarters created for {cik}"
        )

    return quarter_df


# ======================================================================
# BUILD METRIC VALUE TABLE
# ======================================================================

def build_metric_value_table(
    df: pd.DataFrame,
    cik: str,
) -> pd.DataFrame:
    """
    Convert metric-level Gold rows into:

        one row per company-quarter

    and one column per metric.

    Example:

        revenue
        operating_income
        net_income

    become:

        revenue_value
        operating_income_value
        net_income_value
    """

    key = [
        "cik",
        "fiscal_year",
        "quarter",
    ]

    # --------------------------------------------------------------
    # Explicitly validate metric grain before pivot.
    # --------------------------------------------------------------

    validate_step10_metric_grain(
        df,
        cik
    )

    metric_df = df[
        df["metric"].isin(METRICS)
    ].copy()

    # --------------------------------------------------------------
    # Pivot only values.
    #
    # DO NOT put source metadata in this index.
    #
    # This is the permanent fix for the duplicate analytics rows.
    # --------------------------------------------------------------

    value_pivot = metric_df.pivot(
        index=key,
        columns="metric",
        values="value",
    )

    value_pivot.columns = [
        f"{metric}_value"
        for metric in value_pivot.columns
    ]

    value_pivot = (
        value_pivot
        .reset_index()
    )

    # --------------------------------------------------------------
    # Guarantee all expected metric columns exist.
    # --------------------------------------------------------------

    for metric in METRICS:

        column = f"{metric}_value"

        if column not in value_pivot.columns:

            value_pivot[column] = np.nan

    return value_pivot


# ======================================================================
# QUARTER ORDER
# ======================================================================

def add_quarter_sequence(
    df: pd.DataFrame,
) -> pd.DataFrame:

    df = df.copy()

    df["quarter_sequence"] = [
        quarter_sequence(
            fy,
            qn
        )
        for fy, qn in zip(
            df["fiscal_year"],
            df["quarter_num"]
        )
    ]

    return df


# ======================================================================
# QOQ
# ======================================================================

def calculate_qoq(
    analytics: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate QoQ growth using the immediately preceding quarter.

    We do NOT use simple row.shift() because that can incorrectly
    compare:

        2014 Q4 -> 2016 Q1

    if 2015 data is absent.

    QoQ is therefore only populated when:

        current_sequence - previous_sequence == 1
    """

    analytics = analytics.copy()

    analytics = analytics.sort_values(
        [
            "cik",
            "quarter_sequence",
        ]
    )

    for metric in METRICS:

        value_column = f"{metric}_value"

        qoq_column = (
            f"{metric}_qoq_growth_pct"
        )

        previous_value_column = (
            f"previous_{metric}_quarter_value"
        )

        previous_sequence_column = (
            "previous_quarter_sequence"
        )

        analytics[
            previous_value_column
        ] = (
            analytics
            .groupby("cik")[
                value_column
            ]
            .shift(1)
        )

        analytics[
            previous_sequence_column
        ] = (
            analytics
            .groupby("cik")[
                "quarter_sequence"
            ]
            .shift(1)
        )

        consecutive = (
            analytics["quarter_sequence"]
            - analytics[
                previous_sequence_column
            ]
            == 1
        )

        analytics[qoq_column] = np.where(
            consecutive,
            [
                safe_pct_change(
                    current,
                    previous
                )
                for current, previous in zip(
                    analytics[value_column],
                    analytics[
                        previous_value_column
                    ]
                )
            ],
            np.nan
        )

    analytics.drop(
        columns=[
            f"previous_{metric}_quarter_value"
            for metric in METRICS
        ]
        + [
            "previous_quarter_sequence"
        ],
        inplace=True,
        errors="ignore"
    )

    return analytics


# ======================================================================
# YOY
# ======================================================================

def calculate_yoy(
    analytics: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate YoY growth from the same fiscal quarter in the
    previous fiscal year.
    """

    analytics = analytics.copy()

    lookup = analytics[
        [
            "cik",
            "fiscal_year",
            "quarter",
        ]
        + [
            f"{metric}_value"
            for metric in METRICS
        ]
    ].copy()

    lookup["fiscal_year"] = (
        lookup["fiscal_year"] + 1
    )

    lookup = lookup.rename(
        columns={
            f"{metric}_value":
                f"previous_year_{metric}_value"
            for metric in METRICS
        }
    )

    analytics = analytics.merge(
        lookup,
        on=[
            "cik",
            "fiscal_year",
            "quarter",
        ],
        how="left",
        validate="one_to_one",
    )

    for metric in METRICS:

        value_column = (
            f"{metric}_value"
        )

        previous_column = (
            f"previous_year_{metric}_value"
        )

        yoy_column = (
            f"{metric}_yoy_growth_pct"
        )

        analytics[yoy_column] = [
            safe_pct_change(
                current,
                previous
            )
            for current, previous in zip(
                analytics[value_column],
                analytics[previous_column]
            )
        ]

    analytics.drop(
        columns=[
            f"previous_year_{metric}_value"
            for metric in METRICS
        ],
        inplace=True,
        errors="ignore"
    )

    return analytics


# ======================================================================
# TTM
# ======================================================================

def calculate_ttm(
    analytics: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate TTM only when four consecutive quarters exist.

    This prevents rolling sums from silently spanning missing periods.
    """

    analytics = analytics.copy()

    analytics = analytics.sort_values(
        [
            "cik",
            "quarter_sequence",
        ]
    )

    for metric in METRICS:

        value_column = (
            f"{metric}_value"
        )

        ttm_column = (
            f"{metric}_ttm_value"
        )

        analytics[ttm_column] = np.nan

        for cik, indices in (
            analytics.groupby("cik").groups.items()
        ):

            company = (
                analytics
                .loc[indices]
                .sort_values(
                    "quarter_sequence"
                )
            )

            values = company[
                value_column
            ]

            sequences = company[
                "quarter_sequence"
            ]

            rolling_sum = (
                values
                .rolling(
                    window=4,
                    min_periods=4
                )
                .sum()
            )

            sequence_gap = (
                sequences
                .diff()
                .rolling(
                    window=3,
                    min_periods=3
                )
                .sum()
            )

            valid = (
                sequence_gap == 3
            )

            calculated = rolling_sum.where(
                valid,
                np.nan
            )

            analytics.loc[
                company.index,
                ttm_column
            ] = calculated

    return analytics


# ======================================================================
# PROFITABILITY
# ======================================================================

def calculate_margins(
    analytics: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate quarter-level operating and net margins.
    """

    analytics = analytics.copy()

    revenue = analytics[
        "revenue_value"
    ]

    operating_income = analytics[
        "operating_income_value"
    ]

    net_income = analytics[
        "net_income_value"
    ]

    analytics["operating_margin"] = (
        operating_income
        / revenue.replace(
            0,
            np.nan
        )
    )

    analytics["net_margin"] = (
        net_income
        / revenue.replace(
            0,
            np.nan
        )
    )

    # --------------------------------------------------------------
    # TTM margins
    # --------------------------------------------------------------

    analytics["ttm_operating_margin"] = (
        analytics[
            "operating_income_ttm_value"
        ]
        / analytics[
            "revenue_ttm_value"
        ].replace(
            0,
            np.nan
        )
    )

    analytics["ttm_net_margin"] = (
        analytics[
            "net_income_ttm_value"
        ]
        / analytics[
            "revenue_ttm_value"
        ].replace(
            0,
            np.nan
        )
    )

    return analytics


# ======================================================================
# DATA QUALITY FLAGS
# ======================================================================

def add_quality_flags(
    analytics: pd.DataFrame,
) -> pd.DataFrame:

    analytics = analytics.copy()

    # --------------------------------------------------------------
    # Derived quarter
    # --------------------------------------------------------------

    analytics["is_derived_quarter"] = (
        analytics[
            "source_period_type"
        ]
        .fillna("")
        .eq("annual_derived")
    )

    # --------------------------------------------------------------
    # Fiscal-year completeness
    # --------------------------------------------------------------

    completeness = (
        analytics
        .groupby(
            [
                "cik",
                "fiscal_year",
            ],
            dropna=False
        )["quarter"]
        .nunique()
        .reset_index(
            name="quarters_found"
        )
    )

    completeness[
        "is_complete_fiscal_year"
    ] = (
        completeness[
            "quarters_found"
        ] == 4
    )

    analytics = analytics.merge(
        completeness,
        on=[
            "cik",
            "fiscal_year",
        ],
        how="left",
        validate="many_to_one",
    )

    return analytics


# ======================================================================
# ANALYTICS GRAIN VALIDATION
# ======================================================================

def validate_analytics_grain(
    analytics: pd.DataFrame,
    cik: str,
) -> None:

    # --------------------------------------------------------------
    # Exact expected grain
    # --------------------------------------------------------------

    duplicate_mask = (
        analytics
        .duplicated(
            subset=ANALYTICS_KEY,
            keep=False
        )
    )

    duplicates = analytics[
        duplicate_mask
    ]

    if not duplicates.empty:

        print()
        print(
            "FATAL: Analytics grain violation."
        )

        print(
            duplicates[
                ANALYTICS_KEY
            ]
            .sort_values(
                ANALYTICS_KEY
            )
            .to_string(
                index=False
            )
        )

        raise ValueError(
            f"Analytics grain violation for {cik}. "
            f"Expected exactly one row per "
            f"{ANALYTICS_KEY}."
        )

    # --------------------------------------------------------------
    # Row count must equal unique key count
    # --------------------------------------------------------------

    unique_count = (
        analytics[
            ANALYTICS_KEY
        ]
        .drop_duplicates()
        .shape[0]
    )

    if len(analytics) != unique_count:

        raise ValueError(
            f"Analytics row-count invariant failed "
            f"for {cik}."
        )


# ======================================================================
# REQUIRED COLUMNS
# ======================================================================

def validate_required_columns(
    analytics: pd.DataFrame,
    cik: str,
) -> None:

    required = [
        "cik",
        "fiscal_year",
        "quarter",
        "quarter_num",
        "period_start",
        "period_end",
        "source_period_type",
        "revenue_value",
        "operating_income_value",
        "net_income_value",
        "revenue_qoq_growth_pct",
        "revenue_yoy_growth_pct",
        "operating_margin",
        "net_margin",
        "revenue_ttm_value",
        "operating_income_ttm_value",
        "net_income_ttm_value",
        "ttm_operating_margin",
        "ttm_net_margin",
        "is_derived_quarter",
        "quarters_found",
        "is_complete_fiscal_year",
    ]

    missing = [
        column
        for column in required
        if column not in analytics.columns
    ]

    if missing:

        raise ValueError(
            f"Missing required analytics columns "
            f"for {cik}: {missing}"
        )


# ======================================================================
# PROCESS ONE COMPANY
# ======================================================================

def process_company(
    cik: str,
) -> dict:

    banner(
        f"Processing CIK: {cik}"
    )

    input_path = (
        INPUT_DIR
        / f"cik={cik}"
        / "financials_quarterly.parquet"
    )

    if not input_path.exists():

        raise FileNotFoundError(
            f"Final Gold file not found: "
            f"{input_path}"
        )

    df = pd.read_parquet(
        input_path
    )

    original_input_rows = len(df)

    print(
        f"Input rows: "
        f"{original_input_rows}"
    )

    if df.empty:

        raise ValueError(
            f"No Final Gold data for {cik}"
        )

    # ==============================================================
    # NORMALIZATION
    # ==============================================================

    df = df.copy()

    # --------------------------------------------------------------
    # CIK
    # --------------------------------------------------------------

    if "cik" not in df.columns:

        df["cik"] = cik

    df["cik"] = (
        df["cik"]
        .astype(str)
        .str.strip()
        .str.zfill(10)
    )

    # --------------------------------------------------------------
    # Fiscal year
    # --------------------------------------------------------------

    df["fiscal_year"] = (
        pd.to_numeric(
            df["fiscal_year"],
            errors="coerce"
        )
        .astype("Int64")
    )

    # --------------------------------------------------------------
    # Quarter
    # --------------------------------------------------------------

    df["quarter"] = (
        df["quarter"]
        .apply(normalize_quarter)
    )

    # --------------------------------------------------------------
    # Quarter number
    # --------------------------------------------------------------

    df["quarter_num"] = (
        df["quarter"]
        .apply(quarter_to_num)
    )

    # --------------------------------------------------------------
    # Dates
    # --------------------------------------------------------------

    df["period_start"] = (
        pd.to_datetime(
            df["period_start"],
            errors="coerce"
        )
    )

    df["period_end"] = (
        pd.to_datetime(
            df["period_end"],
            errors="coerce"
        )
    )

    # --------------------------------------------------------------
    # Value
    # --------------------------------------------------------------

    df["value"] = (
        pd.to_numeric(
            df["value"],
            errors="coerce"
        )
    )

    # --------------------------------------------------------------
    # Metric
    # --------------------------------------------------------------

    df["metric"] = (
        df["metric"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    # ==============================================================
    # KEEP ANALYTICS METRICS
    # ==============================================================

    df = df[
        df["metric"].isin(
            METRICS
        )
    ].copy()

    if df.empty:

        raise ValueError(
            f"No supported financial metrics "
            f"found for {cik}"
        )

    # ==============================================================
    # STEP 10 GRAIN VALIDATION
    # ==============================================================

    validate_step10_metric_grain(
        df,
        cik
    )

    # ==============================================================
    # CANONICAL QUARTER TABLE
    # ==============================================================

    quarter_df = (
        build_canonical_quarter_table(
            df,
            cik
        )
    )

    # ==============================================================
    # METRIC VALUE TABLE
    # ==============================================================

    value_df = (
        build_metric_value_table(
            df,
            cik
        )
    )

    # ==============================================================
    # MERGE ONLY AT CANONICAL QUARTER GRAIN
    # ==============================================================

    analytics = quarter_df.merge(
        value_df,
        on=[
            "cik",
            "fiscal_year",
            "quarter",
        ],
        how="inner",
        validate="one_to_one",
    )

    # ==============================================================
    # QUARTER SEQUENCE
    # ==============================================================

    analytics = add_quarter_sequence(
        analytics
    )

    # ==============================================================
    # QOQ
    # ==============================================================

    analytics = calculate_qoq(
        analytics
    )

    # ==============================================================
    # YOY
    # ==============================================================

    analytics = calculate_yoy(
        analytics
    )

    # ==============================================================
    # TTM
    # ==============================================================

    analytics = calculate_ttm(
        analytics
    )

    # ==============================================================
    # PROFITABILITY
    # ==============================================================

    analytics = calculate_margins(
        analytics
    )

    # ==============================================================
    # DATA QUALITY
    # ==============================================================

    analytics = add_quality_flags(
        analytics
    )

    # ==============================================================
    # FINAL GRAIN VALIDATION
    # ==============================================================

    validate_analytics_grain(
        analytics,
        cik
    )

    # ==============================================================
    # REQUIRED COLUMN VALIDATION
    # ==============================================================

    validate_required_columns(
        analytics,
        cik
    )

    # ==============================================================
    # SORT
    # ==============================================================

    analytics = (
        analytics
        .sort_values(
            [
                "cik",
                "fiscal_year",
                "quarter_num",
            ]
        )
        .reset_index(
            drop=True
        )
    )

    # ==============================================================
    # REMOVE INTERNAL COLUMN
    # ==============================================================

    analytics.drop(
        columns=[
            "quarter_sequence"
        ],
        inplace=True,
        errors="ignore"
    )

    # ==============================================================
    # FINAL TYPE CLEANUP
    # ==============================================================

    analytics["fiscal_year"] = (
        analytics["fiscal_year"]
        .astype("Int64")
    )

    analytics["quarter_num"] = (
        pd.to_numeric(
            analytics["quarter_num"],
            errors="coerce"
        )
        .astype("Int64")
    )

    # ==============================================================
    # FINAL HARD ASSERTION
    # ==============================================================

    validate_analytics_grain(
        analytics,
        cik
    )

    # ==============================================================
    # OUTPUT
    # ==============================================================

    output_dir = (
        OUTPUT_DIR
        / f"cik={cik}"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    output_path = (
        output_dir
        / "financials_analytics.parquet"
    )

    analytics.to_parquet(
        output_path,
        index=False,
    )

    # ==============================================================
    # REPORT
    # ==============================================================

    print(
        f"Analytics rows: "
        f"{len(analytics)}"
    )

    print(
        "Analytics grain: "
        f"{len(analytics)} unique company-quarter rows"
    )

    print(
        f"Saved: {output_path}"
    )

    # --------------------------------------------------------------
    # Recent analytics
    # --------------------------------------------------------------

    print()
    print(
        "Recent analytics:"
    )

    display_columns = [
        "fiscal_year",
        "quarter",
        "revenue_value",
        "revenue_qoq_growth_pct",
        "revenue_yoy_growth_pct",
        "operating_margin",
        "net_margin",
    ]

    display_columns = [
        column
        for column in display_columns
        if column in analytics.columns
    ]

    print(
        analytics[
            display_columns
        ]
        .tail(12)
        .to_string(
            index=False
        )
    )

    return {
        "cik": cik,
        "input_rows": original_input_rows,
        "analytics_rows": len(analytics),
        "duplicate_rows_removed": 0,
        "output_path": str(output_path),
    }


# ======================================================================
# DISCOVER COMPANIES
# ======================================================================

def discover_companies() -> list[str]:

    companies = []

    if not INPUT_DIR.exists():
        return companies

    for path in sorted(
        INPUT_DIR.glob(
            "cik=*/financials_quarterly.parquet"
        )
    ):

        cik = path.parent.name.replace(
            "cik=",
            ""
        )

        if cik:
            companies.append(
                cik.zfill(10)
            )

    return sorted(
        set(companies)
    )


# ======================================================================
# MAIN
# ======================================================================

def main() -> None:

    banner(
        "STEP 11 - FINANCIAL ANALYTICS"
    )

    companies = (
        discover_companies()
    )

    print(
        f"Companies discovered: "
        f"{len(companies)}"
    )

    if not companies:

        raise RuntimeError(
            "No STEP 10 Final Gold companies found."
        )

    results = []

    failures = []

    for cik in companies:

        try:

            result = (
                process_company(
                    cik
                )
            )

            results.append(
                result
            )

        except Exception as exc:

            print()
            print(
                f"ERROR processing {cik}: "
                f"{exc}"
            )

            failures.append(
                {
                    "cik": cik,
                    "error": str(exc),
                }
            )

    # ==============================================================
    # SUMMARY
    # ==============================================================

    banner(
        "STEP 11 SUMMARY"
    )

    if results:

        summary = pd.DataFrame(
            results
        )

        print(
            summary[
                [
                    "cik",
                    "input_rows",
                    "analytics_rows",
                    "duplicate_rows_removed",
                ]
            ]
            .to_string(
                index=False
            )
        )

    # ==============================================================
    # FAILURE HANDLING
    # ==============================================================

    if failures:

        print()
        print(
            "=" * 60
        )

        print(
            "STEP 11 FAILED"
        )

        print(
            "=" * 60
        )

        failure_df = pd.DataFrame(
            failures
        )

        print(
            failure_df.to_string(
                index=False
            )
        )

        raise RuntimeError(
            f"STEP 11 failed for "
            f"{len(failures)} company(s). "
            f"No downstream step should run."
        )

    # ==============================================================
    # SUCCESS
    # ==============================================================

    print()
    print(
        "=" * 60
    )

    print(
        "STEP 11 COMPLETE - "
        "ALL COMPANIES SUCCESSFUL"
    )

    print(
        "=" * 60
    )


# ======================================================================
# ENTRY POINT
# ======================================================================

if __name__ == "__main__":
    main()