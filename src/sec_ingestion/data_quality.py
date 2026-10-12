import pandas as pd


REQUIRED_COLUMNS = [
    "cik",
    "metric",
    "value",
    "unit",
    "form",
    "filing_date",
]


def validate_financial_facts(
    df: pd.DataFrame,
) -> dict:

    results = {}

    results["row_count"] = len(df)

    results["null_cik"] = int(
        df["cik"].isna().sum()
    )

    results["null_metric"] = int(
        df["metric"].isna().sum()
    )

    results["null_value"] = int(
        df["value"].isna().sum()
    )

    results["null_filing_date"] = int(
        df["filing_date"].isna().sum()
    )

    results["duplicate_rows"] = int(
        df.duplicated().sum()
    )

    results["negative_values"] = int(
        (df["value"] < 0).sum()
    )

    return results

def create_fact_key(df):

    columns = [
        "cik",
        "metric",
        "unit",
        "period_start",
        "period_end",
        "fiscal_period"
    ]

    df["fact_key"] = (
        df[columns]
        .fillna("")
        .astype(str)
        .agg("|".join, axis=1)
    )

    return df

