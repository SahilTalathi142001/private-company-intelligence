import json
from pathlib import Path

import pandas as pd

from .client import SECAPIClient
from .config import BRONZE_DIR, SILVER_DIR


COMPANY_TICKERS_URL = (
    "https://www.sec.gov/files/company_tickers.json"
)


def fetch_company_tickers(
    client: SECAPIClient,
) -> dict:

    response = client.session.get(
        COMPANY_TICKERS_URL,
        timeout=30,
    )

    response.raise_for_status()

    return response.json()


def normalize_company_tickers(
    raw_data: dict,
) -> pd.DataFrame:

    records = []

    for item in raw_data.values():

        records.append({
            "cik": str(item["cik_str"]).zfill(10),
            "ticker": item["ticker"],
            "company_name": item["title"],
        })

    return pd.DataFrame(records)


def ingest_company_master(
    client: SECAPIClient,
) -> pd.DataFrame:

    raw_data = fetch_company_tickers(client)

    # Bronze
    bronze_path = (
        BRONZE_DIR
        / "company_tickers.json"
    )

    bronze_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with open(
        bronze_path,
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            raw_data,
            f,
            indent=2,
        )

    # Silver
    df = normalize_company_tickers(raw_data)

    silver_path = (
        SILVER_DIR
        / "company"
        / "company.parquet"
    )

    silver_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.to_parquet(
        silver_path,
        index=False,
    )

    return df