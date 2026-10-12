# ============================================================
# STEP 11.1 - ANALYTICS VALIDATION & LINEAGE
# PRODUCTION-CORRECT VERSION
# ============================================================

from pathlib import Path
import pandas as pd
import numpy as np
import sys
import traceback


# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(
    r"C:\Users\stalathi\Desktop\private-company-intelligence"
)

FINAL_GOLD_DIR = (
    BASE_DIR
    / "data"
    / "gold"
    / "financials_quarterly_final"
)

ANALYTICS_DIR = (
    BASE_DIR
    / "data"
    / "gold"
    / "financials_analytics"
)

OUTPUT_DIR = (
    BASE_DIR
    / "data"
    / "gold"
    / "financials_analytics_validation"
)


SUMMARY_DIR = OUTPUT_DIR / "analytics_validation_summary"
EXCEPTION_DIR = OUTPUT_DIR / "analytics_exceptions"
LINEAGE_DIR = OUTPUT_DIR / "analytics_lineage"


VALIDATION_SUMMARY_FILE = (
    SUMMARY_DIR
    / "analytics_validation_summary.parquet"
)

COMPANY_SUMMARY_FILE = (
    SUMMARY_DIR
    / "analytics_company_summary.parquet"
)

EXCEPTIONS_FILE = (
    EXCEPTION_DIR
    / "analytics_exceptions.parquet"
)

LINEAGE_FILE = (
    LINEAGE_DIR
    / "analytics_lineage.parquet"
)


# ============================================================
# VALIDATION CONFIG
# ============================================================

# Floating-point tolerance.
#
# Differences such as:
#
# 1.1102230246251565e-16
#
# are numerical floating-point noise and should PASS.
#
ANALYTICS_TOLERANCE = 1e-9


# Financial metrics expected in Final Gold
REQUIRED_METRICS = [
    "revenue",
    "operating_income",
    "net_income",
]


KEY_COLUMNS = [
    "cik",
    "fiscal_year",
    "quarter",
]


# ============================================================
# LOGGING
# ============================================================

def log(message=""):
    print(message)


def separator(char="=", length=60):
    print(char * length)


# ============================================================
# DIRECTORY SETUP
# ============================================================

def create_directories():

    SUMMARY_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    EXCEPTION_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    LINEAGE_DIR.mkdir(
        parents=True,
        exist_ok=True
    )


# ============================================================
# GENERIC HELPERS
# ============================================================

def find_column(df, candidates, required=True):

    normalized = {
        str(col).strip().lower(): col
        for col in df.columns
    }

    for candidate in candidates:

        key = candidate.strip().lower()

        if key in normalized:
            return normalized[key]

    if required:

        raise KeyError(
            f"Could not find any of these columns: "
            f"{candidates}\n"
            f"Available columns: {list(df.columns)}"
        )

    return None


def normalize_cik(value):

    if pd.isna(value):
        return None

    value = str(value).strip()

    # Prevent values such as 320193.0
    if value.endswith(".0"):
        value = value[:-2]

    return value.zfill(10)


def normalize_quarter(value):

    if pd.isna(value):
        return None

    value = str(value).strip().upper()

    # Accept variants such as:
    # 1 -> Q1
    # 2 -> Q2
    # Q1 -> Q1
    if value in {"1", "01"}:
        return "Q1"

    if value in {"2", "02"}:
        return "Q2"

    if value in {"3", "03"}:
        return "Q3"

    if value in {"4", "04"}:
        return "Q4"

    return value


def numeric(series):

    return pd.to_numeric(
        series,
        errors="coerce"
    )


def values_match(
    expected,
    actual,
    tolerance=ANALYTICS_TOLERANCE
):

    if pd.isna(expected) and pd.isna(actual):
        return True

    if pd.isna(expected) or pd.isna(actual):
        return False

    try:

        return (
            abs(
                float(expected)
                - float(actual)
            )
            <= tolerance
        )

    except (TypeError, ValueError):

        return False


def safe_pct_change(
    current,
    previous
):

    if pd.isna(current) or pd.isna(previous):
        return np.nan

    if previous == 0:
        return np.nan

    return (
        float(current)
        / float(previous)
    ) - 1.0


# ============================================================
# LOAD PARQUET
# ============================================================

def load_single_parquet(directory):

    files = sorted(
        directory.glob("*.parquet")
    )

    if not files:

        raise FileNotFoundError(
            f"No parquet files found in: {directory}"
        )

    if len(files) > 1:

        log(
            f"WARNING: Multiple parquet files found "
            f"in {directory}. Using first: {files[0]}"
        )

    return pd.read_parquet(files[0])


# ============================================================
# FINAL GOLD NORMALIZATION
# ============================================================

def normalize_final_gold(df, cik):

    df = df.copy()

    # --------------------------------------------------------
    # Normalize CIK
    # --------------------------------------------------------

    cik_col = find_column(
        df,
        ["cik"],
        required=False
    )

    if cik_col:

        df["cik"] = (
            df[cik_col]
            .apply(normalize_cik)
        )

    else:

        df["cik"] = normalize_cik(cik)

    # --------------------------------------------------------
    # Fiscal year
    # --------------------------------------------------------

    fy_col = find_column(
        df,
        [
            "fiscal_year",
            "fy",
            "fiscalyear"
        ]
    )

    df["fiscal_year"] = numeric(
        df[fy_col]
    ).astype("Int64")

    # --------------------------------------------------------
    # Quarter
    # --------------------------------------------------------

    quarter_col = find_column(
        df,
        [
            "quarter",
            "fiscal_quarter"
        ]
    )

    df["quarter"] = (
        df[quarter_col]
        .apply(normalize_quarter)
    )

    # --------------------------------------------------------
    # Metric
    # --------------------------------------------------------

    metric_col = find_column(
        df,
        [
            "metric",
            "financial_metric",
            "metric_name"
        ]
    )

    df["metric"] = (
        df[metric_col]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    # --------------------------------------------------------
    # Value
    # --------------------------------------------------------

    value_col = find_column(
        df,
        [
            "value",
            "metric_value"
        ]
    )

    df["value"] = numeric(
        df[value_col]
    )

    return df


# ============================================================
# BUILD COMPANY-QUARTER GOLD DATASET
# ============================================================

def build_gold_quarterly_metrics(
    gold_df
):

    # Only metrics required by analytics validation.
    gold_df = gold_df[
        gold_df["metric"].isin(
            REQUIRED_METRICS
        )
    ].copy()

    # --------------------------------------------------------
    # Structural duplicate check at metric grain
    #
    # A Gold table contains multiple metrics per quarter.
    # Therefore the correct grain is:
    #
    # CIK + FY + Quarter + Metric
    #
    # --------------------------------------------------------

    metric_key = [
        "cik",
        "fiscal_year",
        "quarter",
        "metric"
    ]

    duplicate_rows = gold_df[
        gold_df.duplicated(
            metric_key,
            keep=False
        )
    ].copy()

    if not duplicate_rows.empty:

        log(
            "WARNING: Duplicate financial metric "
            "records detected."
        )

        log(
            duplicate_rows[
                metric_key
            ]
            .sort_values(metric_key)
            .to_string(index=False)
        )

        # Do NOT silently choose one.
        #
        # Production behavior:
        # duplicate metrics are invalid.
        raise ValueError(
            "Final Gold contains duplicate metric-grain rows: "
            f"{metric_key}"
        )

    # --------------------------------------------------------
    # Pivot into company-quarter grain
    # --------------------------------------------------------

    quarterly = (
        gold_df
        .pivot(
            index=[
                "cik",
                "fiscal_year",
                "quarter"
            ],
            columns="metric",
            values="value"
        )
        .reset_index()
    )

    quarterly.columns.name = None

    # Ensure expected columns exist
    for metric in REQUIRED_METRICS:

        if metric not in quarterly.columns:

            quarterly[metric] = np.nan

    quarterly = quarterly.rename(
        columns={
            "revenue": "revenue_value_gold",
            "operating_income":
                "operating_income_value_gold",
            "net_income":
                "net_income_value_gold",
        }
    )

    return quarterly


# ============================================================
# ANALYTICS NORMALIZATION
# ============================================================

def normalize_analytics(
    df,
    cik
):

    df = df.copy()

    # --------------------------------------------------------
    # CIK
    # --------------------------------------------------------

    cik_col = find_column(
        df,
        ["cik"],
        required=False
    )

    if cik_col:

        df["cik"] = (
            df[cik_col]
            .apply(normalize_cik)
        )

    else:

        df["cik"] = normalize_cik(cik)

    # --------------------------------------------------------
    # Fiscal year
    # --------------------------------------------------------

    fy_col = find_column(
        df,
        [
            "fiscal_year",
            "fy",
            "fiscalyear"
        ]
    )

    df["fiscal_year"] = numeric(
        df[fy_col]
    ).astype("Int64")

    # --------------------------------------------------------
    # Quarter
    # --------------------------------------------------------

    quarter_col = find_column(
        df,
        [
            "quarter",
            "fiscal_quarter"
        ]
    )

    df["quarter"] = (
        df[quarter_col]
        .apply(normalize_quarter)
    )

    # --------------------------------------------------------
    # Required analytics columns
    # --------------------------------------------------------

    column_map = {

        "revenue_value":
            [
                "revenue_value",
                "revenue"
            ],

        "revenue_qoq_growth_pct":
            [
                "revenue_qoq_growth_pct",
                "qoq_growth_pct"
            ],

        "revenue_yoy_growth_pct":
            [
                "revenue_yoy_growth_pct",
                "yoy_growth_pct"
            ],

        "operating_margin":
            [
                "operating_margin"
            ],

        "net_margin":
            [
                "net_margin"
            ],
    }

    for target, candidates in column_map.items():

        source = find_column(
            df,
            candidates,
            required=False
        )

        if source:

            df[target] = numeric(
                df[source]
            )

        else:

            df[target] = np.nan

    # --------------------------------------------------------
    # Ensure one analytics row per company-quarter
    # --------------------------------------------------------

    duplicate_mask = df.duplicated(
        KEY_COLUMNS,
        keep=False
    )

    duplicate_rows = df[
        duplicate_mask
    ].copy()

    if not duplicate_rows.empty:

        log(
            "\nERROR: Analytics contains duplicate "
            "company-quarter rows."
        )

        log(
            duplicate_rows[
                KEY_COLUMNS
            ]
            .sort_values(KEY_COLUMNS)
            .to_string(index=False)
        )

        raise ValueError(
            "Analytics grain violation. "
            "Expected one row per "
            f"{KEY_COLUMNS}"
        )

    return df


# ============================================================
# BUILD ANALYTICS EXPECTED VALUES
# ============================================================

def calculate_expected_analytics(
    analytics_df,
    gold_quarterly
):

    # Expected calculations that depend on source-of-truth Gold metrics
    # must be calculated against the Gold company-quarter dataset.
    #
    # Previous implementation attempted to access:
    #   operating_income_value_gold
    #   net_income_value_gold
    # before those columns had been joined to analytics_df.
    #
    # That was a sequencing bug, not a missing-column/data problem.
    df = analytics_df.copy()

    gold_columns = [
        "cik",
        "fiscal_year",
        "quarter",
        "revenue_value_gold",
        "operating_income_value_gold",
        "net_income_value_gold",
    ]

    missing_gold_columns = [
        col for col in gold_columns
        if col not in gold_quarterly.columns
    ]

    if missing_gold_columns:
        raise KeyError(
            "Final Gold quarterly dataset is missing required columns: "
            f"{missing_gold_columns}"
        )

    # Join Gold source metrics before calculating expected margins.
    # Analytics is one row per company-quarter and Gold quarterly is
    # also one row per company-quarter, so this must be one-to-one.
    df = df.merge(
        gold_quarterly[gold_columns],
        on=[
            "cik",
            "fiscal_year",
            "quarter",
        ],
        how="left",
        validate="one_to_one",
        indicator="_gold_join",
    )

    df["gold_record_exists"] = (
        df["_gold_join"].eq("both")
    )

    df.drop(
        columns=["_gold_join"],
        inplace=True
    )

    df = df.sort_values(
        [
            "cik",
            "fiscal_year",
            "quarter"
        ]
    ).reset_index(drop=True)

    # --------------------------------------------------------
    # Previous quarter
    #
    # We deliberately use the actual chronological sequence.
    # --------------------------------------------------------

    quarter_order = {
        "Q1": 1,
        "Q2": 2,
        "Q3": 3,
        "Q4": 4
    }

    df["_quarter_number"] = (
        df["quarter"]
        .map(quarter_order)
    )

    # --------------------------------------------------------
    # QoQ
    # --------------------------------------------------------

    previous = df[
        [
            "cik",
            "fiscal_year",
            "quarter",
            "revenue_value"
        ]
    ].copy()

    previous = previous.rename(
        columns={
            "fiscal_year":
                "previous_fiscal_year",

            "quarter":
                "previous_quarter",

            "revenue_value":
                "previous_quarter_revenue"
        }
    )

    previous["_quarter_number"] = (
        previous["previous_quarter"]
        .map(quarter_order)
    )

    # Build expected previous quarter key
    df["previous_fiscal_year"] = np.where(
        df["_quarter_number"] == 1,
        df["fiscal_year"] - 1,
        df["fiscal_year"]
    )

    df["previous_quarter"] = np.select(
        [
            df["_quarter_number"] == 1,
            df["_quarter_number"] == 2,
            df["_quarter_number"] == 3,
            df["_quarter_number"] == 4,
        ],
        [
            "Q4",
            "Q1",
            "Q2",
            "Q3",
        ],
        default=None
    )

    previous_lookup = previous[
        [
            "cik",
            "previous_fiscal_year",
            "previous_quarter",
            "previous_quarter_revenue"
        ]
    ].copy()

    df = df.merge(
        previous_lookup,
        on=[
            "cik",
            "previous_fiscal_year",
            "previous_quarter"
        ],
        how="left",
        validate="many_to_one"
    )

    # --------------------------------------------------------
    # Expected QoQ
    # --------------------------------------------------------

    df["expected_revenue_qoq_growth_pct"] = df.apply(
        lambda r:
            safe_pct_change(
                r["revenue_value"],
                r["previous_quarter_revenue"]
            ),
        axis=1
    )

    # --------------------------------------------------------
    # YoY
    #
    # Match exact fiscal year + quarter.
    # Never use shift(4), because historical gaps exist.
    # --------------------------------------------------------

    prior_year = df[
        [
            "cik",
            "fiscal_year",
            "quarter",
            "revenue_value"
        ]
    ].copy()

    prior_year["fiscal_year"] = (
        prior_year["fiscal_year"] + 1
    )

    prior_year = prior_year.rename(
        columns={
            "revenue_value":
                "prior_year_revenue"
        }
    )

    df = df.merge(
        prior_year[
            [
                "cik",
                "fiscal_year",
                "quarter",
                "prior_year_revenue"
            ]
        ],
        on=[
            "cik",
            "fiscal_year",
            "quarter"
        ],
        how="left",
        validate="one_to_one"
    )

    df["expected_revenue_yoy_growth_pct"] = df.apply(
        lambda r:
            safe_pct_change(
                r["revenue_value"],
                r["prior_year_revenue"]
            ),
        axis=1
    )

    # --------------------------------------------------------
    # Operating margin
    # --------------------------------------------------------

    df["expected_operating_margin"] = np.where(
        (
            df["revenue_value"].notna()
            &
            df["revenue_value"].ne(0)
            &
            df["operating_income_value_gold"].notna()
        ),
        (
            df["operating_income_value_gold"]
            /
            df["revenue_value"]
        ),
        np.nan
    )

    # --------------------------------------------------------
    # Net margin
    # --------------------------------------------------------

    df["expected_net_margin"] = np.where(
        (
            df["revenue_value"].notna()
            &
            df["revenue_value"].ne(0)
            &
            df["net_income_value_gold"].notna()
        ),
        (
            df["net_income_value_gold"]
            /
            df["revenue_value"]
        ),
        np.nan
    )

    return df


# ============================================================
# VALIDATION FUNCTIONS
# ============================================================

def validate_revenue(
    analytics_value,
    gold_value
):

    # Both unavailable:
    # Revenue cannot be validated because neither source
    # contains a value for this company-quarter.
    if pd.isna(analytics_value) and pd.isna(gold_value):
        return "NOT_TESTABLE"

    # Gold has a value but Analytics is missing it.
    if pd.isna(analytics_value) and not pd.isna(gold_value):
        return "FAIL"

    # Analytics has a value but Gold does not.
    if not pd.isna(analytics_value) and pd.isna(gold_value):
        return "FAIL"

    # Both values exist — compare them.
    return (
        "PASS"
        if values_match(
            analytics_value,
            gold_value
        )
        else "FAIL"
    )


def validate_metric(
    expected,
    actual
):

    # Both unavailable
    if pd.isna(expected) and pd.isna(actual):

        return "NOT_TESTABLE"

    # Expected unavailable but stored exists.
    #
    # Example:
    # missing prior-year revenue means YoY cannot be calculated.
    #
    if pd.isna(expected):

        return "NOT_TESTABLE"

    # Expected exists but actual does not.
    if pd.isna(actual):

        return "FAIL"

    return (
        "PASS"
        if values_match(
            expected,
            actual
        )
        else "FAIL"
    )


# ============================================================
# VALIDATE ANALYTICS
# ============================================================

def validate_analytics(
    gold_quarterly,
    analytics_df
):

    # --------------------------------------------------------
    # Join analytics to Final Gold
    #
    # IMPORTANT:
    #
    # Analytics should have one row per company-quarter.
    # Gold has already been transformed to the same grain.
    #
    # --------------------------------------------------------

    gold_value_columns = [
        "revenue_value_gold",
        "operating_income_value_gold",
        "net_income_value_gold",
    ]

    analytics_for_validation = analytics_df.drop(
        columns=[
            *gold_value_columns,
            "gold_record_exists",
        ],
        errors="ignore",
    )

    validation = analytics_for_validation.merge(
        gold_quarterly,
        on=KEY_COLUMNS,
        how="left",
        validate="one_to_one",
        indicator=True
    )

    # --------------------------------------------------------
    # Missing Gold records
    # --------------------------------------------------------

    validation["gold_record_exists"] = (
        validation["_merge"]
        .eq("both")
    )

    validation.drop(
        columns=["_merge"],
        inplace=True
    )

    # --------------------------------------------------------
    # Revenue validation
    # --------------------------------------------------------

    validation["revenue_validation"] = validation.apply(
        lambda r:
            validate_revenue(
                r["revenue_value"],
                r["revenue_value_gold"]
            )
        if r["gold_record_exists"]
        else "FAIL",
        axis=1
    )

    # --------------------------------------------------------
    # QoQ validation
    # --------------------------------------------------------

    validation["qoq_validation"] = validation.apply(
        lambda r:
            validate_metric(
                r["expected_revenue_qoq_growth_pct"],
                r["revenue_qoq_growth_pct"]
            ),
        axis=1
    )

    # --------------------------------------------------------
    # YoY validation
    # --------------------------------------------------------

    validation["yoy_validation"] = validation.apply(
        lambda r:
            validate_metric(
                r["expected_revenue_yoy_growth_pct"],
                r["revenue_yoy_growth_pct"]
            ),
        axis=1
    )

    # --------------------------------------------------------
    # Operating margin validation
    # --------------------------------------------------------

    validation["operating_margin_validation"] = validation.apply(
        lambda r:
            validate_metric(
                r["expected_operating_margin"],
                r["operating_margin"]
            ),
        axis=1
    )

    # --------------------------------------------------------
    # Net margin validation
    # --------------------------------------------------------

    validation["net_margin_validation"] = validation.apply(
        lambda r:
            validate_metric(
                r["expected_net_margin"],
                r["net_margin"]
            ),
        axis=1
    )

    # --------------------------------------------------------
    # Missing Net Income
    # --------------------------------------------------------

    validation["missing_net_income"] = (
        validation["net_income_value_gold"]
        .isna()
    )

    # --------------------------------------------------------
    # Overall validation
    #
    # NOT_TESTABLE does not mean failure.
    # --------------------------------------------------------

    validation["validation_status"] = np.where(
        (
            validation["revenue_validation"].isin(
            ["PASS", "NOT_TESTABLE"]
            )
            &
            validation["qoq_validation"].isin(
                ["PASS", "NOT_TESTABLE"]
            )
            &
            validation["yoy_validation"].isin(
                ["PASS", "NOT_TESTABLE"]
            )
            &
            validation[
                "operating_margin_validation"
            ].isin(
                ["PASS", "NOT_TESTABLE"]
            )
            &
            validation[
                "net_margin_validation"
            ].isin(
                ["PASS", "NOT_TESTABLE"]
            )
        ),
        "PASS",
        "FAIL"
    )
    revenue_mismatches = validation[
    validation["revenue_validation"].eq("FAIL")
    ].copy()

    if not revenue_mismatches.empty:

        revenue_mismatches["revenue_difference"] = (
        revenue_mismatches["revenue_value"]
        - revenue_mismatches["revenue_value_gold"]
    )

        revenue_mismatches["revenue_difference_pct"] = np.where(
        revenue_mismatches["revenue_value_gold"].ne(0),
        (
            revenue_mismatches["revenue_difference"]
            / revenue_mismatches["revenue_value_gold"]
        ),
        np.nan
    )

        log("\nREVENUE MISMATCH DETAILS")
        separator("-")

        log(
            revenue_mismatches[
                [
                    "cik",
                    "fiscal_year",
                    "quarter",
                    "revenue_value",
                    "revenue_value_gold",
                    "revenue_difference",
                    "revenue_difference_pct",
                ]
            ]
            .sort_values(KEY_COLUMNS)
            .to_string(index=False)
        )

        separator("-")

        

    # --------------------------------------------------------
    # Exception reasons
    # --------------------------------------------------------

    def build_exception_reason(row):

        reasons = []

        if not row["gold_record_exists"]:

            reasons.append(
                "MISSING_FINAL_GOLD_RECORD"
            )

        if (
            row["revenue_validation"]
            == "FAIL"
        ):

            reasons.append(
                "REVENUE_VALUE_MISMATCH"
            )

        if (
            row["qoq_validation"]
            == "FAIL"
        ):

            reasons.append(
                "QOQ_CALCULATION_MISMATCH"
            )

        if (
            row["yoy_validation"]
            == "FAIL"
        ):

            reasons.append(
                "YOY_CALCULATION_MISMATCH"
            )

        if (
            row["operating_margin_validation"]
            == "FAIL"
        ):

            reasons.append(
                "OPERATING_MARGIN_MISMATCH"
            )

        if (
            row["net_margin_validation"]
            == "FAIL"
        ):

            reasons.append(
                "NET_MARGIN_MISMATCH"
            )

        return (
            ";".join(reasons)
            if reasons
            else None
        )

    validation["exception_reason"] = (
        validation.apply(
            build_exception_reason,
            axis=1
        )
    )

    return validation


# ============================================================
# LINEAGE DATASET
# ============================================================

def build_lineage(
    validation
):

    lineage_columns = [

        "cik",
        "fiscal_year",
        "quarter",

        "revenue_value",
        "revenue_value_gold",

        "operating_income_value_gold",
        "net_income_value_gold",

        "revenue_qoq_growth_pct",
        "expected_revenue_qoq_growth_pct",

        "revenue_yoy_growth_pct",
        "expected_revenue_yoy_growth_pct",

        "operating_margin",
        "expected_operating_margin",

        "net_margin",
        "expected_net_margin",

        "revenue_validation",
        "qoq_validation",
        "yoy_validation",

        "operating_margin_validation",
        "net_margin_validation",

        "validation_status",
        "exception_reason",

        "gold_record_exists",
        "missing_net_income",
    ]

    existing_columns = [
        col
        for col in lineage_columns
        if col in validation.columns
    ]

    lineage = validation[
        existing_columns
    ].copy()

    # --------------------------------------------------------
    # Source references
    # --------------------------------------------------------

    lineage["source_final_gold"] = (
        "financials_quarterly_final"
    )

    lineage["source_analytics"] = (
        "financials_analytics"
    )

    lineage["validation_rule_version"] = (
        "STEP_11.1_V2"
    )

    lineage["tolerance"] = (
        ANALYTICS_TOLERANCE
    )

    return lineage


# ============================================================
# COMPANY SUMMARY
# ============================================================

def build_company_summary(
    validation
):

    records = []

    for cik, group in validation.groupby(
        "cik",
        dropna=False
    ):

        validation_rows = len(group)

        pass_rows = (
            group[
                "validation_status"
            ]
            .eq("PASS")
            .sum()
        )

        fail_rows = (
            group[
                "validation_status"
            ]
            .eq("FAIL")
            .sum()
        )

        missing_net_income_rows = (
            group[
                "missing_net_income"
            ]
            .sum()
        )

        qoq_failures = (
            group[
                "qoq_validation"
            ]
            .eq("FAIL")
            .sum()
        )

        yoy_failures = (
            group[
                "yoy_validation"
            ]
            .eq("FAIL")
            .sum()
        )

        operating_margin_failures = (
            group[
                "operating_margin_validation"
            ]
            .eq("FAIL")
            .sum()
        )

        net_margin_failures = (
            group[
                "net_margin_validation"
            ]
            .eq("FAIL")
            .sum()
        )

        revenue_failures = (
            group[
                "revenue_validation"
            ]
            .eq("FAIL")
            .sum()
        )

        not_testable_qoq = (
            group[
                "qoq_validation"
            ]
            .eq("NOT_TESTABLE")
            .sum()
        )

        not_testable_yoy = (
            group[
                "yoy_validation"
            ]
            .eq("NOT_TESTABLE")
            .sum()
        )

        records.append(
            {
                "cik": cik,

                "validation_rows":
                    validation_rows,

                "pass_rows":
                    int(pass_rows),

                "failure_rows":
                    int(fail_rows),

                "revenue_failures":
                    int(revenue_failures),

                "qoq_failures":
                    int(qoq_failures),

                "yoy_failures":
                    int(yoy_failures),

                "operating_margin_failures":
                    int(operating_margin_failures),

                "net_margin_failures":
                    int(net_margin_failures),

                "missing_net_income_rows":
                    int(
                        missing_net_income_rows
                    ),

                "qoq_not_testable_rows":
                    int(
                        not_testable_qoq
                    ),

                "yoy_not_testable_rows":
                    int(
                        not_testable_yoy
                    ),

                "validation_pass_pct":
                    (
                        pass_rows
                        / validation_rows
                        if validation_rows
                        else np.nan
                    ),
            }
        )

    return pd.DataFrame(records)


# ============================================================
# MAIN COMPANY PROCESSING
# ============================================================

def process_company(
    cik
):

    separator("=")

    log(
        f"PROCESSING CIK: {cik}"
    )

    # --------------------------------------------------------
    # Locate company directories
    # --------------------------------------------------------

    gold_company_dir = (
        FINAL_GOLD_DIR
        / f"cik={cik}"
    )

    analytics_company_dir = (
        ANALYTICS_DIR
        / f"cik={cik}"
    )

    if not gold_company_dir.exists():

        raise FileNotFoundError(
            f"Final Gold directory missing: "
            f"{gold_company_dir}"
        )

    if not analytics_company_dir.exists():

        raise FileNotFoundError(
            f"Analytics directory missing: "
            f"{analytics_company_dir}"
        )

    # --------------------------------------------------------
    # Load Final Gold
    # --------------------------------------------------------

    gold_df = load_single_parquet(
        gold_company_dir
    )

    log(
        f"Final Gold rows: {len(gold_df)}"
    )

    # --------------------------------------------------------
    # Load Analytics
    # --------------------------------------------------------

    analytics_df = load_single_parquet(
        analytics_company_dir
    )

    log(
        f"Analytics rows: {len(analytics_df)}"
    )

    # --------------------------------------------------------
    # Normalize
    # --------------------------------------------------------

    gold_df = normalize_final_gold(
        gold_df,
        cik
    )

    analytics_df = normalize_analytics(
        analytics_df,
        cik
    )

    # --------------------------------------------------------
    # Build company-quarter Final Gold
    # --------------------------------------------------------

    gold_quarterly = (
        build_gold_quarterly_metrics(
            gold_df
        )
    )

    # --------------------------------------------------------
    # Calculate expected analytics
    # --------------------------------------------------------

    analytics_df = (
        calculate_expected_analytics(
            analytics_df,
            gold_quarterly
        )
    )

    # --------------------------------------------------------
    # Validate
    # --------------------------------------------------------

    validation = validate_analytics(
        gold_quarterly,
        analytics_df
    )

    # --------------------------------------------------------
    # Output statistics
    # --------------------------------------------------------

    validation_rows = len(validation)

    pass_rows = (
        validation[
            "validation_status"
        ]
        .eq("PASS")
        .sum()
    )

    fail_rows = (
        validation[
            "validation_status"
        ]
        .eq("FAIL")
        .sum()
    )

    missing_gold_rows = (
        ~validation[
            "gold_record_exists"
        ]
    ).sum()

    missing_net_income = (
        validation[
            "missing_net_income"
        ].sum()
    )

    qoq_failures = (
        validation[
            "qoq_validation"
        ]
        .eq("FAIL")
        .sum()
    )

    yoy_failures = (
        validation[
            "yoy_validation"
        ]
        .eq("FAIL")
        .sum()
    )

    operating_margin_failures = (
        validation[
            "operating_margin_validation"
        ]
        .eq("FAIL")
        .sum()
    )

    net_margin_failures = (
        validation[
            "net_margin_validation"
        ]
        .eq("FAIL")
        .sum()
    )

    # --------------------------------------------------------
    # Print
    # --------------------------------------------------------

    log(
        f"\nValidation rows : {validation_rows}"
    )

    log(
        f"PASS            : {pass_rows}"
    )

    log(
        f"FAIL            : {fail_rows}"
    )

    log(
        f"Missing Gold    : {missing_gold_rows}"
    )

    log(
        f"Missing Net Income: {missing_net_income}"
    )

    log(
        f"QoQ failures    : {qoq_failures}"
    )

    log(
        f"YoY failures    : {yoy_failures}"
    )

    log(
        "Operating margin failures: "
        f"{operating_margin_failures}"
    )

    log(
        f"Net margin failures: {net_margin_failures}"
    )

    # --------------------------------------------------------
    # Company summary record
    # --------------------------------------------------------

    company_summary = {
        "cik": cik,

        "final_gold_rows":
            len(gold_df),

        "gold_company_quarter_rows":
            len(gold_quarterly),

        "analytics_rows":
            len(analytics_df),

        "validation_rows":
            validation_rows,

        "pass_rows":
            int(pass_rows),

        "failure_rows":
            int(fail_rows),

        "missing_gold_rows":
            int(missing_gold_rows),

        "qoq_failures":
            int(qoq_failures),

        "yoy_failures":
            int(yoy_failures),

        "operating_margin_failures":
            int(
                operating_margin_failures
            ),

        "net_margin_failures":
            int(
                net_margin_failures
            ),

        "missing_net_income_rows":
            int(
                missing_net_income
            ),
    }

    return (
        validation,
        company_summary
    )


# ============================================================
# MAIN
# ============================================================

def main():

    separator("=")

    log(
        "STEP 11.1 - ANALYTICS VALIDATION & LINEAGE"
    )

    separator("=")

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
        f"Tolerance  : {ANALYTICS_TOLERANCE}"
    )

    create_directories()

    # --------------------------------------------------------
    # Discover companies
    # --------------------------------------------------------

    gold_company_dirs = sorted(
        [
            p
            for p in FINAL_GOLD_DIR.glob(
                "cik=*"
            )
            if p.is_dir()
        ]
    )

    companies = []

    for path in gold_company_dirs:

        cik = path.name.replace(
            "cik=",
            ""
        )

        companies.append(cik)

    log(
        f"\nCompanies discovered: "
        f"{len(companies)}"
    )

    if not companies:

        raise RuntimeError(
            "No companies discovered."
        )

    all_validation = []

    company_summaries = []

    # --------------------------------------------------------
    # Process companies
    # --------------------------------------------------------

    for cik in companies:

        try:

            (
                validation,
                company_summary
            ) = process_company(
                cik
            )

            all_validation.append(
                validation
            )

            company_summaries.append(
                company_summary
            )

        except Exception as exc:

            log(
                f"\nERROR processing {cik}: "
                f"{exc}"
            )

            traceback.print_exc()

            raise

    # --------------------------------------------------------
    # Combine
    # --------------------------------------------------------

    validation_summary = pd.concat(
        all_validation,
        ignore_index=True
    )

    company_summary = pd.DataFrame(
        company_summaries
    )

    # --------------------------------------------------------
    # Exceptions
    # --------------------------------------------------------

    exceptions = validation_summary[
        validation_summary[
            "validation_status"
        ].eq("FAIL")
    ].copy()

    # --------------------------------------------------------
    # Lineage
    # --------------------------------------------------------

    lineage = build_lineage(
        validation_summary
    )

    # --------------------------------------------------------
    # Sort outputs
    # --------------------------------------------------------

    validation_summary = (
        validation_summary
        .sort_values(KEY_COLUMNS)
        .reset_index(drop=True)
    )

    exceptions = (
        exceptions
        .sort_values(KEY_COLUMNS)
        .reset_index(drop=True)
    )

    lineage = (
        lineage
        .sort_values(KEY_COLUMNS)
        .reset_index(drop=True)
    )

    company_summary = (
        company_summary
        .sort_values("cik")
        .reset_index(drop=True)
    )

    # --------------------------------------------------------
    # Save validation summary
    # --------------------------------------------------------

    validation_summary.to_parquet(
        VALIDATION_SUMMARY_FILE,
        index=False
    )

    # --------------------------------------------------------
    # Save company summary
    # --------------------------------------------------------

    company_summary.to_parquet(
        COMPANY_SUMMARY_FILE,
        index=False
    )

    # --------------------------------------------------------
    # Save exceptions
    # --------------------------------------------------------

    exceptions.to_parquet(
        EXCEPTIONS_FILE,
        index=False
    )

    # --------------------------------------------------------
    # Save lineage
    # --------------------------------------------------------

    lineage.to_parquet(
        LINEAGE_FILE,
        index=False
    )

    # ========================================================
    # FINAL SUMMARY
    # ========================================================

    separator("=")

    log(
        "STEP 11.1 SUMMARY"
    )

    separator("=")

    display_columns = [
        "cik",
        "final_gold_rows",
        "analytics_rows",
        "validation_rows",
        "pass_rows",
        "failure_rows",
        "missing_gold_rows",
        "qoq_failures",
        "yoy_failures",
        "operating_margin_failures",
        "net_margin_failures",
        "missing_net_income_rows",
    ]

    log(
        company_summary[
            display_columns
        ].to_string(index=False)
    )

    # --------------------------------------------------------
    # Overall totals
    # --------------------------------------------------------

    total_validation = len(
        validation_summary
    )

    total_pass = (
        validation_summary[
            "validation_status"
        ]
        .eq("PASS")
        .sum()
    )

    total_fail = (
        validation_summary[
            "validation_status"
        ]
        .eq("FAIL")
        .sum()
    )

    total_missing_net_income = (
        validation_summary[
            "missing_net_income"
        ].sum()
    )

    separator("-")

    log(
        f"Total validation rows : "
        f"{total_validation}"
    )

    log(
        f"Total PASS            : "
        f"{total_pass}"
    )

    log(
        f"Total FAIL            : "
        f"{total_fail}"
    )

    log(
        f"Missing Net Income    : "
        f"{total_missing_net_income}"
    )

    separator("-")

    # --------------------------------------------------------
    # Exception breakdown
    # --------------------------------------------------------

    if not exceptions.empty:

        log(
            "EXCEPTION BREAKDOWN"
        )

        reason_counts = (
            exceptions[
                "exception_reason"
            ]
            .value_counts()
        )

        log(
            reason_counts.to_string()
        )

    else:

        log(
            "EXCEPTION BREAKDOWN"
        )

        log(
            "No validation exceptions."
        )

    # --------------------------------------------------------
    # Outputs
    # --------------------------------------------------------

    separator("=")

    log(
        "OUTPUTS"
    )

    separator("=")

    log(
        "Validation summary:"
    )

    log(
        str(VALIDATION_SUMMARY_FILE)
    )

    log(
        "\nCompany summary:"
    )

    log(
        str(COMPANY_SUMMARY_FILE)
    )

    log(
        "\nExceptions:"
    )

    log(
        str(EXCEPTIONS_FILE)
    )

    log(
        "\nLineage:"
    )

    log(
        str(LINEAGE_FILE)
    )

    separator("=")

    log(
        "STEP 11.1 COMPLETE"
    )

    separator("=")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        main()

    except Exception as exc:

        separator("=")

        log(
            "STEP 11.1 FAILED"
        )

        log(
            f"ERROR: {exc}"
        )

        separator("=")

        sys.exit(1)