from pathlib import Path
import pandas as pd

from .financial_normalizer import (
    extract_financial_facts,
)
from .data_quality import (
    validate_financial_facts,
)


BASE_DIR = Path(__file__).resolve().parents[2]

BRONZE_DIR = (
    BASE_DIR
    / "data"
    / "bronze"
    / "sec"
    / "companyfacts"
)

SILVER_DIR = (
    BASE_DIR
    / "data"
    / "silver"
    / "financial_facts"
)


def process_company(
    cik: str,
):

    cik = str(cik).zfill(10)

    bronze_file = (
        BRONZE_DIR
        / f"{cik}.json"
    )

    if not bronze_file.exists():

        raise FileNotFoundError(
            f"CompanyFacts file not found: "
            f"{bronze_file}"
        )

    # -----------------------------------------
    # Read Bronze
    # -----------------------------------------

    with open(
        bronze_file,
        "r",
        encoding="utf-8",
    ) as file:

        import json

        facts = json.load(file)

    # -----------------------------------------
    # Normalize
    # -----------------------------------------

    df = extract_financial_facts(
        facts,
        cik,
    )

    if df.empty:

        print(
            f"No financial facts found "
            f"for {cik}"
        )

        return

    # -----------------------------------------
    # Data Quality
    # -----------------------------------------

    quality = validate_financial_facts(
        df
    )

    print(
        f"\nData Quality - {cik}"
    )

    for key, value in quality.items():

        print(
            f"{key}: {value}"
        )

    # -----------------------------------------
    # Deduplication
    # -----------------------------------------





    print(
    df[
        df.duplicated(
            subset=[
                "cik",
                "metric",
                "unit",
                "period_start",
                "period_end",
                "fiscal_period",
            ],
            keep=False
        )
    ][
        [
            "metric",
            "value",
            "period_start",
            "period_end",
            "fiscal_year",
            "fiscal_period",
            "form",
            "filing_date",
            "accession_number",
            "xbrl_tag",
        ]
    ]
    .sort_values(
        [
            "metric",
            "period_end",
            "filing_date"
        ]
    )
    .head(50)
    .to_string(index=False)
)





    fact_key_columns = [
        "cik",
        "metric",
        "unit",
        "period_start",
        "period_end",
        "fiscal_period",
    ]

    df["fact_key"] = (
        df[
            fact_key_columns
        ]
        .fillna("")
        .astype(str)
        .agg("|".join, axis=1)
    )

    

    # -----------------------------------------
    # Save Silver
    # -----------------------------------------

    output_dir = (
        SILVER_DIR
        / f"cik={cik}"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_file = (
        output_dir
        / "financial_facts.parquet"
    )

    df.to_parquet(
        output_file,
        index=False,
    )

    print(
        f"Silver written: {output_file}"
    )

    print(
        f"Rows: {len(df):,}"
    )


def process_all(
    companies: list[str],
):

    for cik in companies:

        try:

            print(
                "\n" + "=" * 60
            )

            print(
                f"Processing {cik}"
            )

            process_company(cik)

        except Exception as e:

            print(
                f"ERROR processing "
                f"{cik}: {e}"
            )