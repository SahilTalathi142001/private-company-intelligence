import json

import pandas as pd

from .client import SECAPIClient
from .config import BRONZE_DIR, SILVER_DIR


def fetch_company_facts(
    client: SECAPIClient,
    cik: str,
) -> dict:

    cik = str(cik).zfill(10)

    endpoint = (
        f"/api/xbrl/companyfacts/"
        f"CIK{cik}.json"
    )

    return client.get(endpoint)


def extract_metric(
    facts: dict,
    metric_name: str,
    xbrl_tags: list[str],
) -> pd.DataFrame:

    records = []

    us_gaap = facts[
        "facts"
    ].get("us-gaap", {})

    for tag in xbrl_tags:

        if tag not in us_gaap:
            continue

        metric = us_gaap[tag]

        units = metric.get(
            "units",
            {},
        )

        for unit, values in units.items():

            for item in values:

                records.append({
                    "metric": metric_name,
                    "xbrl_tag": tag,
                    "unit": unit,
                    "value": item.get("val"),
                    "period_start":
                        item.get("start"),
                    "period_end":
                        item.get("end"),
                    "fiscal_year":
                        item.get("fy"),
                    "fiscal_period":
                        item.get("fp"),
                    "form":
                        item.get("form"),
                    "filing_date":
                        item.get("filed"),
                    "accession_number":
                        item.get("accn"),
                    "frame":
                        item.get("frame"),
                })

    return pd.DataFrame(records)


METRIC_TAGS = {

    "revenue": [
        "Revenues",
        "SalesRevenueNet",
    ],

    "net_income": [
        "NetIncomeLoss",
    ],

    "assets": [
        "Assets",
    ],

    "liabilities": [
        "Liabilities",
    ],

    "cash": [
        "CashAndCashEquivalentsAtCarryingValue",
    ],

    "equity": [
        "StockholdersEquity",
    ],

    "operating_income": [
        "OperatingIncomeLoss",
    ],
}


def normalize_company_facts(
    facts: dict,
) -> pd.DataFrame:

    all_metrics = []

    for metric_name, tags in METRIC_TAGS.items():

        df = extract_metric(
            facts,
            metric_name,
            tags,
        )

        if not df.empty:
            all_metrics.append(df)

    if not all_metrics:
        return pd.DataFrame()

    result = pd.concat(
        all_metrics,
        ignore_index=True,
    )

    result["cik"] = str(
        facts["entityName"]
    )

    return result


def ingest_company_facts(
    client: SECAPIClient,
    cik: str,
) -> pd.DataFrame:

    facts = fetch_company_facts(
        client,
        cik,
    )

    cik = str(cik).zfill(10)

    # -------------------------
    # Bronze
    # -------------------------

    bronze_path = (
        BRONZE_DIR
        / "companyfacts"
        / f"{cik}.json"
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
            facts,
            f,
            indent=2,
        )

    # -------------------------
    # Silver
    # -------------------------

    df = normalize_company_facts(
        facts
    )

    df["cik"] = cik

    silver_path = (
        SILVER_DIR
        / "financial_facts"
        / f"cik={cik}"
        / "facts.parquet"
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