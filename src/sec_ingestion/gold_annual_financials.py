import pandas as pd


ANNUAL_COLUMNS = [
    "cik",
    "metric",
    "unit",
    "fiscal_year",
    "period_start",
    "period_end",
    "value",
    "source_accession",
    "source_filing_date",
    "source_period_type",
]


def build_annual_gold(
    annual_df: pd.DataFrame,
) -> pd.DataFrame:

    if annual_df.empty:
        return pd.DataFrame(
            columns=ANNUAL_COLUMNS
        )

    df = annual_df.copy()

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
        df["filing_date"],
        errors="coerce",
    )

    df["source_accession"] = df[
        "accession_number"
    ]

    df["source_period_type"] = "annual"

    df = df[
        [
            column
            for column in ANNUAL_COLUMNS
            if column in df.columns
        ]
    ].copy()

    df = df.drop_duplicates(
        subset=[
            "cik",
            "metric",
            "unit",
            "period_end",
        ],
        keep="first",
    )

    df = df.sort_values(
        [
            "cik",
            "metric",
            "fiscal_year",
        ]
    ).reset_index(drop=True)

    return df