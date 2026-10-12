import re
import sys
from pathlib import Path

import pandas as pd


# ============================================================
# IMPORT PROJECT CONFIG
# ============================================================

if __package__ in (None, ""):
    project_root = Path(__file__).resolve().parents[2]

    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    from src.sec_ingestion.config import SILVER_DIR

else:
    from .config import SILVER_DIR


# ============================================================
# CONFIG
# ============================================================

DURATION_METRICS = {
    "revenue",
    "net_income",
    "operating_income",
}

PERIOD_RANGES = {
    "quarter": (70, 110),
    "six_month": (150, 200),
    "nine_month": (230, 290),
    "annual": (330, 400),
}

SEC_FRAME_QUARTER = re.compile(r"^(?:CY|FY)(\d{4})Q([1-4])$")


# ============================================================
# DATE PREPARATION
# ============================================================

def prepare_dates(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    for column in [
        "period_start",
        "period_end",
        "filing_date",
    ]:
        df[column] = pd.to_datetime(
            df[column],
            errors="coerce",
        )

    df["period_days"] = (
        df["period_end"] - df["period_start"]
    ).dt.days + 1

    return df


# ============================================================
# PERIOD CLASSIFICATION
# ============================================================

def classify_period(days):

    if pd.isna(days):
        return None

    days = int(days)

    for period_type, (minimum, maximum) in PERIOD_RANGES.items():

        if minimum <= days <= maximum:
            return period_type

    return None


def classify_periods(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    df["derived_period_type"] = (
        df["period_days"]
        .apply(classify_period)
    )

    return df


# ============================================================
# SOURCE FISCAL PERIOD
# ============================================================

def add_source_fiscal_period(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    if "fiscal_period" in df.columns:

        df["source_fiscal_period"] = (
            df["fiscal_period"]
            .astype("string")
            .str.upper()
            .str.strip()
        )

    elif "fp" in df.columns:

        df["source_fiscal_period"] = (
            df["fp"]
            .astype("string")
            .str.upper()
            .str.strip()
        )

    else:

        df["source_fiscal_period"] = pd.NA

    return df


# ============================================================
# BASIC DEDUPLICATION
#
# Same economic period can appear in multiple filings.
# Keep the earliest filing for now, consistent with Step 5.
# ============================================================

def deduplicate_observations(
    df: pd.DataFrame,
) -> pd.DataFrame:

    if df.empty:
        return df

    df = df.copy()

    # --------------------------------------------------------
    # Filing version priority
    # --------------------------------------------------------
    #
    # Same economic period can appear in multiple filings.
    #
    # We want the latest available reported version.
    #
    # Priority:
    #   10-K/A > 10-K > 10-Q/A > 10-Q
    #
    # Filing date remains the primary ordering criterion.
    # --------------------------------------------------------

    form_priority = {
        "10-K/A": 4,
        "10-K": 3,
        "10-Q/A": 2,
        "10-Q": 1,
    }

    df["_form_priority"] = (
        df["form"]
        .astype(str)
        .str.upper()
        .str.strip()
        .map(form_priority)
        .fillna(0)
    )

    df["_filing_date_sort"] = pd.to_datetime(
        df["filing_date"],
        errors="coerce",
    )

    # --------------------------------------------------------
    # Latest authoritative observation wins
    # --------------------------------------------------------

    df = (
        df
        .sort_values(
            [
                "cik",
                "metric",
                "unit",
                "period_start",
                "period_end",
                "_filing_date_sort",
                "_form_priority",
                "accession_number",
            ],
            ascending=[
                True,
                True,
                True,
                True,
                True,
                False,   # latest filing
                False,   # preferred form
                False,
            ],
            kind="stable",
        )
        .drop_duplicates(
            subset=[
                "cik",
                "metric",
                "unit",
                "period_start",
                "period_end",
            ],
            keep="first",
        )
        .copy()
    )

    df = df.drop(
        columns=[
            "_form_priority",
            "_filing_date_sort",
        ],
        errors="ignore",
    )

    return df


# ============================================================
# BUILD ONE OUTPUT RECORD
# ============================================================

def build_record(
    row,
    fiscal_year,
    quarter,
    value,
    source_period_type,
    period_start=None,
    period_end=None,
    annual_period_start=None,
    annual_period_end=None,
    quarter_assignment_method=None,
):

    return {
        "cik": row["cik"],
        "metric": row["metric"],
        "unit": row["unit"],

        "fiscal_year": fiscal_year,
        "quarter": quarter,
        "value": value,

        "period_start": (
            period_start
            if period_start is not None
            else row["period_start"]
        ),

        "period_end": (
            period_end
            if period_end is not None
            else row["period_end"]
        ),

        "annual_period_start": annual_period_start,
        "annual_period_end": annual_period_end,

        "source_accession": row["accession_number"],
        "source_filing_date": row["filing_date"],

        "source_period_type": source_period_type,

        "quarter_assignment_method": (
            quarter_assignment_method
            if quarter_assignment_method is not None
            else "period_boundary"
        ),
    }


# ============================================================
# FIND FISCAL-YEAR ANNUAL ANCHORS
#
# IMPORTANT:
#
# Only 10-K / 10-K/A annual periods are allowed to define
# the fiscal year.
#
# A 10-Q observation can have 12 months of duration and still
# be classified as "annual" by duration. It must NOT become
# a fiscal-year anchor.
# ============================================================

def build_fiscal_year_anchors(
    group: pd.DataFrame,
) -> pd.DataFrame:

    annuals = group[
        (group["derived_period_type"] == "annual")
        & (
            group["form"].isin(
                [
                    "10-K",
                    "10-K/A",
                ]
            )
        )
    ].copy()

    if annuals.empty:
        return pd.DataFrame()

    annuals["_form_priority"] = (
        annuals["form"]
        .astype(str)
        .str.upper()
        .str.strip()
        .map({
            "10-K/A": 3,
            "10-K": 2,
        })
        .fillna(0)
    )

    annuals["_filing_date_sort"] = pd.to_datetime(
        annuals["filing_date"],
        errors="coerce",
    )

    annuals = (
        annuals
        .sort_values(
            [
                "period_end",
                "period_start",
                "_filing_date_sort",
                "_form_priority",
                "accession_number",
            ],
            ascending=[
                True,
                True,
                False,   # latest filing
                False,   # 10-K/A first
                False,
            ],
            kind="stable",
        )
        .drop_duplicates(
            subset=[
                "period_start",
                "period_end",
            ],
            keep="first",
        )
        .copy()
    )

    annuals["fiscal_year"] = (
        annuals["period_end"]
        .dt.year
        .astype("Int64")
    )

    annuals["reported_fiscal_year"] = (
        annuals["fiscal_year"]
    )

    anchors = annuals[
        [
            "fiscal_year",
            "reported_fiscal_year",
            "period_start",
            "period_end",
        ]
    ].copy()

    anchors.rename(
        columns={
            "period_start": "fy_start",
            "period_end": "fy_end",
        },
        inplace=True,
    )

    return anchors


# ============================================================
# ASSIGN OBSERVATION TO FISCAL YEAR
#
# An observation belongs to the FY whose annual period
# contains its period_end.
# ============================================================

def assign_fiscal_year(
    observations: pd.DataFrame,
    anchors: pd.DataFrame,
) -> pd.DataFrame:

    if observations.empty:
        return observations

    result = observations.copy()

    result["assigned_fiscal_year"] = pd.NA

    for _, anchor in anchors.iterrows():

        mask = (
            (result["period_end"] > anchor["fy_start"])
            & (result["period_end"] <= anchor["fy_end"])
        )

        result.loc[
            mask,
            "assigned_fiscal_year",
        ] = anchor["fiscal_year"]

    return result


# ============================================================
# SELECT ONE ECONOMIC PERIOD
# ============================================================

def select_period(
    df: pd.DataFrame,
    period_type: str,
    start=None,
    end=None,
):
    result = df[
        df["derived_period_type"] == period_type
    ].copy()

    if start is not None:
        result = result[
            result["period_start"] == start
        ]

    if end is not None:
        result = result[
            result["period_end"] == end
        ]

    if result.empty:
        return None

    result["_form_priority"] = (
        result["form"]
        .astype(str)
        .str.upper()
        .str.strip()
        .map({
            "10-K/A": 4,
            "10-K": 3,
            "10-Q/A": 2,
            "10-Q": 1,
        })
        .fillna(0)
    )

    result["_filing_date_sort"] = pd.to_datetime(
        result["filing_date"],
        errors="coerce",
    )

    result = result.sort_values(
        [
            "_filing_date_sort",
            "_form_priority",
            "accession_number",
        ],
        ascending=[
            False,
            False,
            False,
        ],
        kind="stable",
    )

    return result.iloc[0]


# ============================================================
# DERIVE QUARTERS FOR ONE FISCAL YEAR
# ============================================================

def derive_quarters_for_fiscal_year(
    group: pd.DataFrame,
    fiscal_year,
    reported_fiscal_year,
    fy_start,
    fy_end,
):

    records = []

    # --------------------------------------------------------
    # Keep only observations inside this fiscal year.
    # --------------------------------------------------------

    group = group[
        (group["period_end"] > fy_start)
        & (group["period_end"] <= fy_end)
    ].copy()

    if group.empty:
        return records

    # --------------------------------------------------------
    # Actual standalone quarters
    # --------------------------------------------------------

    standalone = group[
        group["derived_period_type"] == "quarter"
    ].copy()

    # --------------------------------------------------------
    # Q1
    #
    # Q1 must start exactly at the fiscal-year start.
    # We do NOT say "first quarter we find = Q1".
    # --------------------------------------------------------

    q1 = select_period(
        standalone,
        "quarter",
        start=fy_start,
    )

    if q1 is not None:

        records.append(
            build_record(
                row=q1,
                fiscal_year=fiscal_year,
                quarter="Q1",
                value=q1["value"],
                source_period_type="quarter",
                period_start=q1["period_start"],
                period_end=q1["period_end"],
                annual_period_start=fy_start,
                annual_period_end=fy_end,
                quarter_assignment_method="period_boundary",
            )
        )

    # --------------------------------------------------------
    # Six-month cumulative
    #
    # Must start at FY start.
    # --------------------------------------------------------

    six_month = select_period(
        group,
        "six_month",
        start=fy_start,
    )

    # --------------------------------------------------------
    # Q2
    #
    # First prefer actual standalone Q2.
    # We identify it using the six-month period end.
    # --------------------------------------------------------

    q2 = None

    if six_month is not None:

        q2 = select_period(
            standalone,
            "quarter",
            end=six_month["period_end"],
        )

    if q2 is not None:

        records.append(
            build_record(
                row=q2,
                fiscal_year=fiscal_year,
                quarter="Q2",
                value=q2["value"],
                source_period_type="quarter",
                period_start=q2["period_start"],
                period_end=q2["period_end"],
                annual_period_start=fy_start,
                annual_period_end=fy_end,
                quarter_assignment_method="period_boundary",
            )
        )

    # --------------------------------------------------------
    # Otherwise derive Q2 = 6M - Q1
    # --------------------------------------------------------

    elif (
        six_month is not None
        and q1 is not None
    ):

        q2_start = (
            q1["period_end"]
            + pd.Timedelta(days=1)
        )

        q2_end = six_month["period_end"]

        if (
            q2_start <= q2_end
            and q2_end <= fy_end
        ):

            q2_value = (
                six_month["value"]
                - q1["value"]
            )

            records.append(
                build_record(
                    row=six_month,
                    fiscal_year=fiscal_year,
                    quarter="Q2",
                    value=q2_value,
                    source_period_type="six_month_derived",
                    period_start=q2_start,
                    period_end=q2_end,
                    annual_period_start=fy_start,
                    annual_period_end=fy_end,
                    quarter_assignment_method="cumulative_derivation",
                )
            )

    # --------------------------------------------------------
    # Nine-month cumulative
    #
    # Must start at FY start.
    # --------------------------------------------------------

    nine_month = select_period(
        group,
        "nine_month",
        start=fy_start,
    )

    # --------------------------------------------------------
    # Q3
    #
    # Prefer actual standalone Q3.
    # Otherwise 9M - 6M.
    # --------------------------------------------------------

    q3 = None

    if nine_month is not None:

        q3 = select_period(
            standalone,
            "quarter",
            end=nine_month["period_end"],
        )

    if q3 is not None:

        records.append(
            build_record(
                row=q3,
                fiscal_year=fiscal_year,
                quarter="Q3",
                value=q3["value"],
                source_period_type="quarter",
                period_start=q3["period_start"],
                period_end=q3["period_end"],
                annual_period_start=fy_start,
                annual_period_end=fy_end,
                quarter_assignment_method="period_boundary",
            )
        )

    elif (
        nine_month is not None
        and six_month is not None
    ):

        q3_start = (
            six_month["period_end"]
            + pd.Timedelta(days=1)
        )

        q3_end = nine_month["period_end"]

        if (
            q3_start <= q3_end
            and q3_end <= fy_end
        ):

            q3_value = (
                nine_month["value"]
                - six_month["value"]
            )

            records.append(
                build_record(
                    row=nine_month,
                    fiscal_year=fiscal_year,
                    quarter="Q3",
                    value=q3_value,
                    source_period_type="nine_month_derived",
                    period_start=q3_start,
                    period_end=q3_end,
                    annual_period_start=fy_start,
                    annual_period_end=fy_end,
                    quarter_assignment_method="cumulative_derivation",
                )
            )

    # --------------------------------------------------------
    # Annual 10-K
    # --------------------------------------------------------

    annual = select_period(
        group,
        "annual",
        start=fy_start,
        end=fy_end,
    )

    # --------------------------------------------------------
    # Q4
    #
    # Prefer actual standalone Q4.
    # Otherwise Annual - 9M.
    # --------------------------------------------------------

    q4 = select_period(
        standalone,
        "quarter",
        end=fy_end,
    )

    if q4 is not None:

        records.append(
            build_record(
                row=q4,
                fiscal_year=fiscal_year,
                quarter="Q4",
                value=q4["value"],
                source_period_type="quarter",
                period_start=q4["period_start"],
                period_end=q4["period_end"],
                annual_period_start=fy_start,
                annual_period_end=fy_end,
                quarter_assignment_method="period_boundary",
            )
        )

    elif (
        annual is not None
        and nine_month is not None
    ):

        q4_start = (
            nine_month["period_end"]
            + pd.Timedelta(days=1)
        )

        q4_end = fy_end

        if (
            q4_start <= q4_end
        ):

            q4_value = (
                annual["value"]
                - nine_month["value"]
            )

            records.append(
                build_record(
                    row=annual,
                    fiscal_year=fiscal_year,
                    quarter="Q4",
                    value=q4_value,
                    source_period_type="annual_derived",
                    period_start=q4_start,
                    period_end=q4_end,
                    annual_period_start=fy_start,
                    annual_period_end=fy_end,
                    quarter_assignment_method="cumulative_derivation",
                )
            )

    return records


# ============================================================
# MAIN DERIVATION
# ============================================================

def _debug_log(hypothesis_id: str, location: str, message: str, data: dict) -> None:
    # region agent log
    try:
        import json
        import time
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


def quarters_from_sec_frame(metric_group: pd.DataFrame) -> list:
    """Pass through standalone quarters identified by an SEC frame.

    A 10-K is required to derive cumulative quarters. It is not required to
    keep a quarter the filing already labels, for example CY2025Q2. Companies
    that have only 10-Q history would otherwise disappear from the universe.
    """
    if "frame" not in metric_group.columns:
        return []

    standalone = metric_group[
        metric_group["derived_period_type"] == "quarter"
    ].copy()

    if standalone.empty:
        return []

    standalone["_frame"] = (
        standalone["frame"].astype("string").str.strip().str.upper()
    )
    parsed = standalone["_frame"].map(
        lambda value: SEC_FRAME_QUARTER.match(str(value)) if pd.notna(value) else None
    )
    standalone = standalone[parsed.notna()].copy()
    if standalone.empty:
        return []

    standalone["_frame_year"] = parsed.dropna().map(lambda match: int(match.group(1)))
    standalone["_frame_quarter"] = parsed.dropna().map(
        lambda match: f"Q{match.group(2)}"
    )
    standalone["_filing_date_sort"] = pd.to_datetime(
        standalone["filing_date"],
        errors="coerce",
    )
    standalone = standalone.sort_values(
        ["_filing_date_sort", "accession_number"],
        ascending=[False, False],
        kind="stable",
    )
    standalone = standalone.drop_duplicates(
        subset=["_frame_year", "_frame_quarter"],
        keep="first",
    )

    records = []
    for _, row in standalone.iterrows():
        records.append(
            build_record(
                row=row,
                fiscal_year=int(row["_frame_year"]),
                quarter=row["_frame_quarter"],
                value=row["value"],
                source_period_type="quarter",
                quarter_assignment_method="sec_frame",
            )
        )
    return records


def derive_quarters(
    df: pd.DataFrame,
) -> pd.DataFrame:

    if df.empty:
        return pd.DataFrame()

    df = df.copy()

    # --------------------------------------------------------
    # Prepare
    # --------------------------------------------------------

    df = prepare_dates(df)

    df = df[
        df["metric"].isin(DURATION_METRICS)
    ].copy()

    if df.empty:
        return pd.DataFrame()

    df = classify_periods(df)

    df = df[
        df["derived_period_type"].notna()
    ].copy()

    df = add_source_fiscal_period(df)

    # --------------------------------------------------------
    # Deduplicate economic observations
    # --------------------------------------------------------

    df = deduplicate_observations(df)

    if df.empty:
        return pd.DataFrame()

    all_records = []

    # --------------------------------------------------------
    # Process each metric separately
    # --------------------------------------------------------

    for _, metric_group in df.groupby(
        ["cik", "metric", "unit"],
        dropna=False,
    ):

        # ----------------------------------------------------
        # Annual 10-K periods define FY boundaries.
        # ----------------------------------------------------

        anchors = build_fiscal_year_anchors(
            metric_group
        )

        if anchors.empty:
            framed = quarters_from_sec_frame(metric_group)
            # region agent log
            if framed:
                _debug_log(
                    "B",
                    "derive_quarters.py:derive_quarters",
                    "sec frame quarters without 10-K anchor",
                    {
                        "cik": str(metric_group["cik"].iloc[0]),
                        "metric": str(metric_group["metric"].iloc[0]),
                        "quarters": [
                            f"{record['fiscal_year']} {record['quarter']}"
                            for record in framed
                        ],
                    },
                )
            # endregion
            all_records.extend(framed)
            continue

        # ----------------------------------------------------
        # Assign every observation to a fiscal year.
        # ----------------------------------------------------

        assigned = assign_fiscal_year(
            metric_group,
            anchors,
        )

        assigned = assigned[
            assigned["assigned_fiscal_year"].notna()
        ].copy()

        # ----------------------------------------------------
        # Process every fiscal year.
        # ----------------------------------------------------

        for _, anchor in anchors.iterrows():

            fiscal_year = anchor["fiscal_year"]

            fiscal_group = assigned[
                assigned["assigned_fiscal_year"]
                == fiscal_year
            ].copy()

            records = derive_quarters_for_fiscal_year(
                group=fiscal_group,
                fiscal_year=fiscal_year,
                reported_fiscal_year=(
                    anchor["reported_fiscal_year"]
                ),
                fy_start=anchor["fy_start"],
                fy_end=anchor["fy_end"],
            )

            all_records.extend(records)

    # --------------------------------------------------------
    # No records
    # --------------------------------------------------------

    if not all_records:

        return pd.DataFrame(
            columns=[
                "cik",
                "metric",
                "unit",
                "fiscal_year",
                "quarter",
                "value",
                "period_start",
                "period_end",
                "annual_period_start",
                "annual_period_end",
                "source_accession",
                "source_filing_date",
                "source_period_type",
                "quarter_assignment_method",
            ]
        )

    # --------------------------------------------------------
    # Build DataFrame
    # --------------------------------------------------------

    result = pd.DataFrame(all_records)

    # --------------------------------------------------------
    # Explicitly guarantee expected columns.
    #
    # This prevents KeyError('quarter') from a malformed
    # intermediate result.
    # --------------------------------------------------------

    expected_columns = [
        "cik",
        "metric",
        "unit",
        "fiscal_year",
        "quarter",
        "value",
        "period_start",
        "period_end",
        "annual_period_start",
        "annual_period_end",
        "source_accession",
        "source_filing_date",
        "source_period_type",
        "quarter_assignment_method",
    ]

    for column in expected_columns:

        if column not in result.columns:
            result[column] = pd.NA

    result = result[expected_columns]

    # --------------------------------------------------------
    # Final deduplication
    # --------------------------------------------------------

    result = (
        result
        .sort_values(
            [
                "cik",
                "metric",
                "unit",
                "fiscal_year",
                "quarter",
                "period_end",
            ]
        )
        .drop_duplicates(
            subset=[
                "cik",
                "metric",
                "unit",
                "fiscal_year",
                "quarter",
            ],
            keep="first",
        )
        .reset_index(drop=True)
    )

    return result


# ============================================================
# MAIN
# ============================================================

def main():

    companies = [
        "0000320193",  # Apple
        "0000789019",  # Microsoft
        "0001018724",  # Amazon
        "0001652044",  # Alphabet
        "0001045810",  # NVIDIA
    ]

    print()
    print("=" * 60)
    print("STEP 6 - QUARTERLY FINANCIAL DERIVATION")
    print("=" * 60)

    for cik in companies:

        print()
        print("=" * 60)
        print(f"Processing CIK: {cik}")
        print("=" * 60)

        silver_path = (
            SILVER_DIR
            / "financial_facts"
            / f"cik={cik}"
            / "financial_facts.parquet"
        )

        if not silver_path.exists():

            print(
                f"Silver file not found: {silver_path}"
            )

            continue

        try:

            df = pd.read_parquet(
                silver_path
            )

            print(
                f"Silver rows: {len(df)}"
            )

            quarterly_df = derive_quarters(df)

            print(
                f"Quarterly rows: {len(quarterly_df)}"
            )

            if quarterly_df.empty:

                print(
                    "No quarterly records generated."
                )

                continue

            # ------------------------------------------------
            # Save output
            # ------------------------------------------------

            output_dir = (
                SILVER_DIR.parent
                / "gold"
                / "financials_quarterly"
                / f"cik={cik}"
            )

            output_dir.mkdir(
                parents=True,
                exist_ok=True,
            )

            output_path = (
                output_dir
                / "financials_quarterly.parquet"
            )

            quarterly_df.to_parquet(
                output_path,
                index=False,
            )

            print(
                f"Saved: {output_path}"
            )

            # ------------------------------------------------
            # Show recent revenue
            # ------------------------------------------------

            revenue = quarterly_df[
                quarterly_df["metric"]
                == "revenue"
            ].copy()

            if not revenue.empty:

                print()
                print("Recent revenue quarters:")
                print()

                print(
                    revenue[
                        [
                            "fiscal_year",
                            "quarter",
                            "value",
                            "period_start",
                            "period_end",
                            "source_period_type",
                            "quarter_assignment_method",
                        ]
                    ]
                    .sort_values(
                        [
                            "fiscal_year",
                            "quarter",
                        ]
                    )
                    .tail(12)
                    .to_string(index=False)
                )

        except Exception as exc:

            print()
            print(
                f"ERROR processing {cik}:"
            )

            print(
                repr(exc)
            )

    print()
    print("=" * 60)
    print("STEP 6 COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()