import pandas as pd


FINANCIAL_COLUMNS = [
    "cik",
    "metric",
    "unit",
    "fiscal_year",
    "quarter",
    "period_start",
    "period_end",
    "value",
    "source_accession",
    "source_filing_date",
    "source_period_type",
]


def build_quarterly_gold(
    quarterly_df: pd.DataFrame,
) -> pd.DataFrame:

    if quarterly_df.empty:
        return pd.DataFrame(
            columns=FINANCIAL_COLUMNS
        )

    df = quarterly_df.copy()

    # Ensure consistent types
    df["cik"] = (
        df["cik"]
        .astype(str)
        .str.zfill(10)
    )

    df["fiscal_year"] = (
        pd.to_numeric(
            df["fiscal_year"],
            errors="coerce",
        )
        .astype("Int64")
    )

    df["value"] = pd.to_numeric(
        df["value"],
        errors="coerce",
    )

    df["period_start"] = pd.to_datetime(
        df["period_start"],
        errors="coerce",
    )

    df["period_end"] = pd.to_datetime(
        df["period_end"],
        errors="coerce",
    )

    df["source_filing_date"] = pd.to_datetime(
        df["source_filing_date"],
        errors="coerce",
    )

    # Keep only the canonical Gold columns
    df = df[
        [
            column
            for column in FINANCIAL_COLUMNS
            if column in df.columns
        ]
    ].copy()

    # Gold-level uniqueness
    df = df.drop_duplicates(
        subset=[
            "cik",
            "metric",
            "unit",
            "fiscal_year",
            "quarter",
        ],
        keep="first",
    )

    df = df.sort_values(
        [
            "cik",
            "metric",
            "fiscal_year",
            "quarter",
        ]
    ).reset_index(drop=True)

    return df