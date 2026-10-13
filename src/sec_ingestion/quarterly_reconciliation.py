"""
STEP 9 - FINANCIAL DATA QUALITY & RECONCILIATION

Production rules
----------------
1. Quarterly Gold is the source for quarter-level validation.
2. Annual Gold is the source for annual reconciliation.
3. Only complete fiscal-year metric groups are eligible for reconciliation.
4. Incomplete years are reported separately and are NOT reconciliation failures.
5. Reconciliation failures are never silently corrected.
6. Step 9 does not modify Step 6/7 financial values.
7. All outputs are Parquet for downstream pipeline consumption.

Outputs
-------
data/gold/financial_dq/
    quarterly_completeness/
    annual_reconciliation/
    reconciliation_failures/
    incomplete_fiscal_years/
    dq_summary/
"""

from pathlib import Path
import pandas as pd
import numpy as np


# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[2]

QUARTERLY_DIR = (
    BASE_DIR
    / "data"
    / "gold"
    / "quarterly_financials"
)

ANNUAL_DIR = (
    BASE_DIR
    / "data"
    / "gold"
    / "financials"
)

OUTPUT_DIR = (
    BASE_DIR
    / "data"
    / "gold"
    / "financial_dq"
)

COMPANIES = [
    "0000320193",
    "0000789019",
    "0001018724",
    "0001652044",
    "0001045810",
]

FLOW_METRICS = {
    "revenue",
    "operating_income",
    "net_income",
}

EXPECTED_QUARTERS = {"Q1", "Q2", "Q3", "Q4"}

# Rounding tolerance.
#
# We compare absolute difference AND relative difference.
#
# This catches:
#   1,000 dollar differences on billions -> PASS
#
# but:
#   3.7 billion difference -> FAIL
#
ABS_TOLERANCE = 1_000_000
REL_TOLERANCE = 0.001


# ============================================================
# HELPERS
# ============================================================

def ensure_output_dirs():
    paths = [
        OUTPUT_DIR / "quarterly_completeness",
        OUTPUT_DIR / "annual_reconciliation",
        OUTPUT_DIR / "reconciliation_failures",
        OUTPUT_DIR / "incomplete_fiscal_years",
        OUTPUT_DIR / "dq_summary",
    ]

    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def normalize_columns(df):
    df = df.copy()

    df.columns = [
        str(c).strip()
        for c in df.columns
    ]

    return df


def normalize_quarter(df):
    df = df.copy()

    if "quarter" in df.columns:
        df["quarter"] = (
            df["quarter"]
            .astype(str)
            .str.upper()
            .str.strip()
        )

    return df


def numeric_value(series):
    return pd.to_numeric(
        series,
        errors="coerce"
    )


def is_reconciled(quarter_sum, annual_value):
    if pd.isna(quarter_sum) or pd.isna(annual_value):
        return False

    difference = quarter_sum - annual_value

    absolute_difference = abs(difference)

    scale = max(
        abs(float(annual_value)),
        1.0
    )

    relative_difference = (
        absolute_difference / scale
    )

    return (
        absolute_difference <= ABS_TOLERANCE
        or relative_difference <= REL_TOLERANCE
    )


def safe_fiscal_year(df):
    if "fiscal_year" not in df.columns:
        return pd.Series(
            dtype="Int64"
        )

    return pd.to_numeric(
        df["fiscal_year"],
        errors="coerce"
    ).astype("Int64")


# ============================================================
# LOAD
# ============================================================

def load_quarterly(cik):
    path = (
        QUARTERLY_DIR
        / f"cik={cik}"
        / "quarterly_financials.parquet"
    )

    if not path.exists():
        raise FileNotFoundError(
            f"Quarterly Gold not found:\n{path}"
        )

    df = pd.read_parquet(path)

    df = normalize_columns(df)
    df = normalize_quarter(df)

    return df


def load_annual(cik):
    path = (
        ANNUAL_DIR
        / f"cik={cik}"
        / "financials.parquet"
    )

    if not path.exists():
        raise FileNotFoundError(
            f"Annual Gold not found:\n{path}"
        )

    df = pd.read_parquet(path)

    df = normalize_columns(df)

    return df


# ============================================================
# QUARTER COMPLETENESS
# ============================================================

def build_quarter_completeness(
    quarterly,
    cik,
):
    df = quarterly.copy()

    df = df[
        df["metric"].isin(FLOW_METRICS)
    ].copy()

    df["fiscal_year"] = safe_fiscal_year(df)

    df = df[
        df["fiscal_year"].notna()
    ].copy()

    df = df[
        df["quarter"].isin(
            EXPECTED_QUARTERS
        )
    ].copy()

    # Remove accidental duplicate quarter rows
    # from the completeness calculation.
    #
    # Duplicates themselves are not silently discarded
    # from the source. They are separately detected.
    unique_groups = (
        df[
            [
                "metric",
                "fiscal_year",
                "quarter",
            ]
        ]
        .drop_duplicates()
    )

    grouped = (
        unique_groups
        .groupby(
            [
                "metric",
                "fiscal_year",
            ]
        )
        ["quarter"]
        .agg(
            lambda x: sorted(
                set(x)
            )
        )
        .reset_index()
    )

    grouped["quarters_found"] = (
        grouped["quarter"]
        .apply(
            lambda x: ",".join(x)
        )
    )

    grouped["missing_quarters"] = (
        grouped["quarter"]
        .apply(
            lambda x: ",".join(
                sorted(
                    EXPECTED_QUARTERS
                    - set(x)
                )
            )
        )
    )

    grouped["quarter_count"] = (
        grouped["quarter"]
        .apply(len)
    )

    grouped["is_complete"] = (
        grouped["quarter_count"] == 4
    )

    grouped["cik"] = cik

    grouped = grouped[
        [
            "cik",
            "metric",
            "fiscal_year",
            "quarters_found",
            "missing_quarters",
            "quarter_count",
            "is_complete",
        ]
    ]

    return grouped


# ============================================================
# DUPLICATE QUARTER CHECK
# ============================================================

def find_quarter_duplicates(
    quarterly,
    cik,
):
    keys = [
        "metric",
        "fiscal_year",
        "quarter",
    ]

    df = quarterly.copy()

    available = [
        c for c in keys
        if c in df.columns
    ]

    if len(available) != len(keys):
        return pd.DataFrame(
            columns=[
                "cik",
                *keys,
                "duplicate_count",
            ]
        )

    duplicates = (
        df.groupby(keys)
        .size()
        .reset_index(
            name="duplicate_count"
        )
    )

    duplicates = duplicates[
        duplicates["duplicate_count"] > 1
    ].copy()

    duplicates["cik"] = cik

    return duplicates[
        [
            "cik",
            *keys,
            "duplicate_count",
        ]
    ]


# ============================================================
# ANNUAL RECONCILIATION
# ============================================================

def build_annual_reconciliation(
    quarterly,
    annual,
    completeness,
    cik,
):
    """
    Reconcile:

        Q1 + Q2 + Q3 + Q4

    against:

        Annual Gold value

    ONLY when all four quarters exist.

    Incomplete fiscal years are excluded from this
    reconciliation table and handled separately.
    """

    q = quarterly.copy()
    a = annual.copy()

    q["fiscal_year"] = safe_fiscal_year(q)
    a["fiscal_year"] = safe_fiscal_year(a)

    q = q[
        q["metric"].isin(
            FLOW_METRICS
        )
    ].copy()

    a = a[
        a["metric"].isin(
            FLOW_METRICS
        )
    ].copy()

    q["value"] = numeric_value(
        q["value"]
    )

    a["value"] = numeric_value(
        a["value"]
    )

    # Only complete fiscal years.
    complete = completeness[
        completeness["is_complete"]
    ][
        [
            "metric",
            "fiscal_year",
        ]
    ].copy()

    q = q.merge(
        complete,
        on=[
            "metric",
            "fiscal_year",
        ],
        how="inner",
    )

    # Sum quarters.
    q_sum = (
        q.groupby(
            [
                "metric",
                "fiscal_year",
            ],
            as_index=False,
        )
        ["value"]
        .sum()
        .rename(
            columns={
                "value": "quarter_sum"
            }
        )
    )

    # Annual value.
    #
    # Annual Gold should already have been
    # canonicalized in Step 5.
    #
    # If multiple annual rows remain,
    # retain the latest row rather than
    # summing annual values.
    a = (
        a.sort_values(
            [
                "metric",
                "fiscal_year",
            ]
        )
        .drop_duplicates(
            [
                "metric",
                "fiscal_year",
            ],
            keep="last",
        )
    )

    a = a[
        [
            "metric",
            "fiscal_year",
            "value",
        ]
    ].rename(
        columns={
            "value": "annual_value"
        }
    )

    result = q_sum.merge(
        a,
        on=[
            "metric",
            "fiscal_year",
        ],
        how="left",
    )

    result["difference"] = (
        result["quarter_sum"]
        - result["annual_value"]
    )

    result["difference_pct"] = np.where(
        result["annual_value"].abs() > 0,
        result["difference"].abs()
        / result["annual_value"].abs(),
        np.nan,
    )

    result["is_reconciled"] = result.apply(
        lambda row:
            is_reconciled(
                row["quarter_sum"],
                row["annual_value"],
            ),
        axis=1,
    )

    result["cik"] = cik

    result["reconciliation_status"] = (
        np.where(
            result["annual_value"].isna(),
            "MISSING_ANNUAL",
            np.where(
                result["is_reconciled"],
                "RECONCILED",
                "RECONCILIATION_EXCEPTION",
            ),
        )
    )

    result = result[
        [
            "cik",
            "metric",
            "fiscal_year",
            "quarter_sum",
            "annual_value",
            "difference",
            "difference_pct",
            "is_reconciled",
            "reconciliation_status",
        ]
    ]

    return result


# ============================================================
# INCOMPLETE YEARS
# ============================================================

def build_incomplete_years(
    completeness,
):
    return (
        completeness[
            ~completeness["is_complete"]
        ]
        .copy()
        .sort_values(
            [
                "cik",
                "fiscal_year",
                "metric",
            ]
        )
    )


# ============================================================
# SUMMARY
# ============================================================

def build_summary(
    cik,
    quarterly,
    annual,
    completeness,
    reconciliation,
    duplicates,
):
    return {
        "cik": cik,
        "quarterly_rows": len(
            quarterly
        ),
        "annual_rows": len(
            annual
        ),
        "quarter_duplicate_groups": len(
            duplicates
        ),
        "quarter_completeness_checks": len(
            completeness
        ),
        "incomplete_groups": int(
            (
                ~completeness[
                    "is_complete"
                ]
            ).sum()
        ),
        "complete_groups": int(
            completeness[
                "is_complete"
            ].sum()
        ),
        "annual_reconciliation_checks": len(
            reconciliation
        ),
        "reconciliation_failures": int(
            (
                reconciliation[
                    "reconciliation_status"
                ]
                == "RECONCILIATION_EXCEPTION"
            ).sum()
        ),
        "missing_annual": int(
            (
                reconciliation[
                    "reconciliation_status"
                ]
                == "MISSING_ANNUAL"
            ).sum()
        ),
        "reconciled": int(
            (
                reconciliation[
                    "reconciliation_status"
                ]
                == "RECONCILED"
            ).sum()
        ),
    }