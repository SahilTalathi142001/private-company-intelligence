import pandas as pd


def validate_quarterly_data(
    quarterly_df: pd.DataFrame,
) -> pd.DataFrame:

    if quarterly_df.empty:
        return pd.DataFrame()

    results = []

    group_columns = [
        "cik",
        "metric",
        "unit",
        "fiscal_year",
    ]

    for keys, group in quarterly_df.groupby(
        group_columns,
        dropna=False,
    ):

        quarters = set(
            group["quarter"]
        )

        missing = {
            "Q1",
            "Q2",
            "Q3",
            "Q4",
        } - quarters

        results.append({
            "cik": keys[0],
            "metric": keys[1],
            "unit": keys[2],
            "fiscal_year": keys[3],
            "quarters_found": ",".join(
                sorted(quarters)
            ),
            "missing_quarters": ",".join(
                sorted(missing)
            ),
            "is_complete": len(missing) == 0,
        })

    return pd.DataFrame(results)