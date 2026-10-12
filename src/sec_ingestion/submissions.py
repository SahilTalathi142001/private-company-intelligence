import json
from pathlib import Path

import pandas as pd

from .client import SECAPIClient
from .config import BRONZE_DIR, SILVER_DIR


def fetch_submissions(
    client: SECAPIClient,
    cik: str,
) -> dict:

    cik = str(cik).zfill(10)

    endpoint = (
        f"/submissions/CIK{cik}.json"
    )

    return client.get(endpoint)


_FILING_COLUMNS = [
    "accessionNumber",
    "filingDate",
    "reportDate",
    "form",
    "primaryDocument",
    "primaryDocDescription",
]


def _filing_block(payload: dict) -> dict:
    """SEC older files are either a filing block or a full submissions document."""
    if "accessionNumber" in payload:
        return payload
    filings = payload.get("filings") or {}
    if "accessionNumber" in filings:
        return filings
    recent = filings.get("recent") or {}
    if "accessionNumber" in recent:
        return recent
    return {}


def _frame_from_block(
    block: dict,
    cik: str,
    company_name: str,
) -> pd.DataFrame:
    if not block or "accessionNumber" not in block:
        return pd.DataFrame()
    frame = pd.DataFrame(block)
    available = [column for column in _FILING_COLUMNS if column in frame.columns]
    frame = frame[available].copy()
    frame.rename(
        columns={
            "accessionNumber": "accession_number",
            "filingDate": "filing_date",
            "reportDate": "report_date",
            "primaryDocument": "primary_document",
            "primaryDocDescription": "primary_doc_description",
        },
        inplace=True,
    )
    frame["cik"] = cik
    frame["company_name"] = company_name
    return frame


def normalize_submissions(
    raw_data: dict,
    historical_payloads: list[dict] | None = None,
) -> pd.DataFrame:
    cik = str(raw_data["cik"]).zfill(10)
    company_name = raw_data["name"]
    frames = [
        _frame_from_block(
            (raw_data.get("filings") or {}).get("recent") or {},
            cik,
            company_name,
        )
    ]
    for payload in historical_payloads or []:
        frames.append(_frame_from_block(_filing_block(payload), cik, company_name))
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(
        subset=["accession_number"],
        keep="first",
    )
    return combined.reset_index(drop=True)


def ingest_submissions(
    client: SECAPIClient,
    cik: str,
) -> pd.DataFrame:

    raw_data = fetch_submissions(
        client,
        cik,
    )

    cik = str(cik).zfill(10)

    # -------------------------
    # Bronze
    # -------------------------

    bronze_path = (
        BRONZE_DIR
        / "submissions"
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
            raw_data,
            f,
            indent=2,
        )

    # -------------------------
    # Silver
    # -------------------------

    historical_payloads = []
    history_dir = bronze_path.parent / "files"
    history_dir.mkdir(parents=True, exist_ok=True)
    for file_meta in (raw_data.get("filings") or {}).get("files") or []:
        name = str((file_meta or {}).get("name") or "").strip()
        if not name or "/" in name or "\\" in name or ".." in name:
            continue
        payload = client.get(f"/submissions/{name}")
        history_path = history_dir / name
        with open(history_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        historical_payloads.append(payload)

    df = normalize_submissions(
        raw_data,
        historical_payloads,
    )

    silver_path = (
        SILVER_DIR
        / "filings"
        / f"cik={cik}"
        / "filings.parquet"
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