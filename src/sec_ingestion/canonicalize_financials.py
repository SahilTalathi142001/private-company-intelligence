from pathlib import Path

import pandas as pd

from company_platform.manifests.lineage import retain_lineaged_rows


INSTANT_METRICS = {
    "assets",
    "liabilities",
    "cash",
    "equity",
}


def load_silver_financials(
    silver_root: Path,
    cik: str,
) -> pd.DataFrame:

    cik = str(cik).zfill(10)

    path = (
        silver_root
        / "financial_facts"
        / f"cik={cik}"
        / "financial_facts.parquet"
    )

    if not path.exists():
        raise FileNotFoundError(
            f"Silver financial file not found: {path}"
        )

    return pd.read_parquet(path)


def prepare_dates(df: pd.DataFrame) -> pd.DataFrame:

    date_columns = [
        "period_start",
        "period_end",
        "filing_date",
    ]

    for column in date_columns:
        if column in df.columns:
            df[column] = pd.to_datetime(
                df[column],
                errors="coerce",
            )

    return df


def select_annual_facts(
    df: pd.DataFrame,
) -> pd.DataFrame:

    duration_df = df[
        (~df["metric"].isin(INSTANT_METRICS))
        & (df["period_type"] == "annual")
        & (df["form"].isin(["10-K", "10-K/A"]))
    ].copy()

    if duration_df.empty:
        return duration_df

    # --------------------------------------------------
    # Preserve SEC metadata
    # --------------------------------------------------

    duration_df["reported_fiscal_year"] = (
        duration_df["fiscal_year"]
    )

    # --------------------------------------------------
    # Economic period identity
    # --------------------------------------------------

    # --------------------------------------------------
# Select latest authoritative filing version
# --------------------------------------------------
#
# Same economic period may appear in:
#   - original 10-K
#   - 10-K/A
#   - later 10-K comparative disclosure
#
# For the canonical analytical dataset, prefer:
#
#   1. latest filing date
#   2. amended 10-K/A over 10-K
#   3. latest accession as deterministic tie-breaker
#
# Economic identity remains:
#   CIK + metric + unit + period_start + period_end
#

    duration_df["form_priority"] = (
        duration_df["form"]
    .astype(str)
    .str.upper()
    .str.strip()
    .map({
        "10-K/A": 3,
        "10-K": 2,
    })
    .fillna(0)
)

    duration_df["filing_date_sort"] = pd.to_datetime(
    duration_df["filing_date"],
    errors="coerce",
)

    duration_df = (
        duration_df
        .sort_values(
        [
            "cik",
            "metric",
            "unit",
            "period_start",
            "period_end",
            "filing_date_sort",
            "form_priority",
            "accession_number",
            ],
            ascending=[
            True,
            True,
            True,
            True,
            True,
            False,   # latest filing
            False,   # 10-K/A before 10-K
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
    )

    # --------------------------------------------------
    # Canonical fiscal year
    # --------------------------------------------------

    duration_df["fiscal_year"] = (
        duration_df["period_end"].dt.year
    )

    return duration_df


def select_instant_facts(
    df: pd.DataFrame,
) -> pd.DataFrame:

    instant_df = df[
        df["metric"].isin(INSTANT_METRICS)
        & (df["period_type"] == "instant")
    ].copy()

    # For balance-sheet metrics, the period_end is the
    # actual measurement date.
    #
    # If the same period is reported again in a later
    # filing, retain the latest filing for now.

    instant_df["form_priority"] = (
    instant_df["form"]
    .astype(str)
    .str.upper()
    .str.strip()
    .map({
        "10-K/A": 3,
        "10-K": 2,
        "10-Q/A": 1,
        "10-Q": 0,
    })
    .fillna(0)
)

    instant_df["filing_date_sort"] = pd.to_datetime(
    instant_df["filing_date"],
    errors="coerce",
)

    instant_df = (
    instant_df
    .sort_values(
        [
            "cik",
            "metric",
            "unit",
            "period_end",
            "filing_date_sort",
            "form_priority",
            "accession_number",
        ],
        ascending=[
            True,
            True,
            True,
            True,
            False,
            False,
            False,
        ],
        kind="stable",
    )
    .drop_duplicates(
        subset=[
            "cik",
            "metric",
            "unit",
            "period_end",
        ],
        keep="first",
    )
    .copy()
)

    # SEC fy on an instant fact is the fiscal year of the filing that
    # disclosed the amount, including comparative balance sheets. The
    # measurement date is period_end. Keep the filing year as lineage
    # and use the measurement year as the canonical fiscal year.
    if "fiscal_year" in instant_df.columns:
        instant_df["reported_fiscal_year"] = instant_df["fiscal_year"]
    measurement_end = pd.to_datetime(
        instant_df["period_end"],
        errors="coerce",
    )
    instant_df["fiscal_year"] = measurement_end.dt.year.astype("Int64")

    instant_df = instant_df.drop(
    columns=[
        "form_priority",
        "filing_date_sort",
    ],
    errors="ignore",
)
    return instant_df


def canonicalize_company(
    silver_root: Path,
    gold_root: Path,
    cik: str,
) -> pd.DataFrame:

    df = load_silver_financials(
        silver_root,
        cik,
    )

    if df.empty:
        return df

    df = prepare_dates(df)

    annual_df = select_annual_facts(df)

    instant_df = select_instant_facts(df)

    canonical_df = pd.concat(
        [
            annual_df,
            instant_df,
        ],
        ignore_index=True,
    )
    canonical_df = retain_lineaged_rows(canonical_df)
    if canonical_df.empty:
        output_path = (
            gold_root
            / "financials"
            / f"cik={str(cik).zfill(10)}"
            / "financials.parquet"
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        canonical_df.to_parquet(output_path, index=False)
        return canonical_df

    canonical_df = canonical_df.sort_values(
        [
            "metric",
            "period_end",
            "filing_date",
        ]
    )

    output_path = (
        gold_root
        / "financials"
        / f"cik={str(cik).zfill(10)}"
        / "financials.parquet"
    )

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    canonical_df.to_parquet(
        output_path,
        index=False,
    )

    return canonical_df