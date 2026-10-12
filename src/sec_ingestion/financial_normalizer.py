import json
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]

METRICS_CONFIG = (
    BASE_DIR
    / "config"
    / "metrics.json"
)

def classify_period(row):

    start = row["period_start"]
    end = row["period_end"]

    if pd.isna(start) or pd.isna(end):

        return "instant"

    start = pd.to_datetime(start)
    end = pd.to_datetime(end)

    days = (end - start).days

    if days <= 100:
        return "quarter"

    if days <= 200:
        return "six_month"

    if days <= 300:
        return "nine_month"

    return "annual"





def load_metric_mapping():

    with open(
        METRICS_CONFIG,
        "r",
        encoding="utf-8",
    ) as file:

        return json.load(file)


def extract_financial_facts(
    facts: dict,
    cik: str,
) -> pd.DataFrame:

    metric_mapping = load_metric_mapping()

    records = []

    us_gaap = facts.get(
        "facts",
        {}
    ).get(
        "us-gaap",
        {}
    )

    for canonical_metric, xbrl_tags in metric_mapping.items():

        for xbrl_tag in xbrl_tags:

            if xbrl_tag not in us_gaap:
                continue

            tag_data = us_gaap[xbrl_tag]

            units = tag_data.get(
                "units",
                {}
            )

            for unit, observations in units.items():

                for observation in observations:

                    records.append({
                        "cik": cik,
                        "company_name":
                            facts.get("entityName"),

                        "metric":
                            canonical_metric,

                        "xbrl_tag":
                            xbrl_tag,

                        "unit":
                            unit,

                        "value":
                            observation.get("val"),

                        "period_start":
                            observation.get("start"),

                        "period_end":
                            observation.get("end"),

                        "fiscal_year":
                            observation.get("fy"),

                        "fiscal_period":
                            observation.get("fp"),

                        "form":
                            observation.get("form"),

                        "filing_date":
                            observation.get("filed"),

                        "accession_number":
                            observation.get("accn"),

                        "frame":
                            observation.get("frame")
                    })

    df = pd.DataFrame(records)
    if df.empty:
        return df

    df["period_type"] = df.apply(
        classify_period,
        axis=1
    )
    print(df.head(10))
    return df 


