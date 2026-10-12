
from __future__ import annotations

import hashlib
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urljoin

import pandas as pd
import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .config import SEC_USER_AGENT


REPO_ROOT = Path(__file__).resolve().parents[2]
FILINGS_ROOT = REPO_ROOT / "data" / "silver" / "filings"
DOCUMENT_ROOT = REPO_ROOT / "data" / "bronze" / "sec" / "documents"

ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data/"
REQUIRED_COLUMNS = {
    "accession_number", "filing_date", "form",
    "primary_document", "cik", "company_name",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_part(value: str, field: str) -> str:
    value = str(value).strip()
    if not value or value in {".", ".."}:
        raise ValueError(f"Invalid {field}")
    if not re.fullmatch(r"[A-Za-z0-9._-]+", value):
        raise ValueError(f"Unsafe {field}: {value!r}")
    return value


def _safe_document_path(value: str) -> str:
    # SEC primary_document may contain subfolders, e.g. xslF345X06/form4.xml
    value = str(value).replace("\\", "/").strip()
    parts = value.split("/")
    if not value or any(p in {"", ".", ".."} for p in parts):
        raise ValueError(f"Unsafe primary_document: {value!r}")
    for part in parts:
        if not re.fullmatch(r"[A-Za-z0-9._-]+", part):
            raise ValueError(f"Unsafe primary_document part: {part!r}")
    return "/".join(parts)


def _document_url(cik: str, accession: str, primary_document: str) -> str:
    cik_num = str(int(cik))
    accession_dir = accession.replace("-", "")
    if not re.fullmatch(r"\d{10}", str(cik)):
        raise ValueError(f"Invalid CIK: {cik!r}")
    if not re.fullmatch(r"\d{10}-\d{2}-\d{6}", accession):
        raise ValueError(f"Invalid accession number: {accession!r}")

    doc = _safe_document_path(primary_document)
    encoded_doc = "/".join(quote(p, safe="._-") for p in doc.split("/"))
    return urljoin(
        ARCHIVES_BASE,
        f"{cik_num}/{accession_dir}/{encoded_doc}",
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True,
)
def _download_document_bytes(session: requests.Session, url: str, timeout: int) -> bytes:
    """Retry transient SEC connection and HTTP failures before recording a miss."""
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    return response.content


def ingest_sec_filing_documents(
    cik: str,
    limit: int | None = 10,
    timeout: int = 45,
    min_interval: float = 0.12,
    filing_records: list[dict] | None = None,
    accession_number: str | None = None,
    primary_document: str | None = None,
) -> dict:
    """Download primary SEC filing documents for one CIK; idempotent by document_id."""
    cik = str(cik).zfill(10)
    if not re.fullmatch(r"\d{10}", cik):
        raise ValueError(f"Invalid CIK: {cik!r}")

    company_dir = DOCUMENT_ROOT / f"cik={cik}"
    company_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = company_dir / "document_manifest.parquet"

    if filing_records is None:
        filings_path = FILINGS_ROOT / f"cik={cik}" / "filings.parquet"
        if not filings_path.exists():
            raise FileNotFoundError(f"Filings metadata not found: {filings_path}")
        filings = pd.read_parquet(filings_path)
    else:
        filings = pd.DataFrame(filing_records)

    missing = REQUIRED_COLUMNS - set(filings.columns)
    if missing:
        raise ValueError(f"Filings parquet missing columns: {sorted(missing)}")

    filings["cik"] = filings["cik"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(10)
    filings = filings[filings["cik"] == cik].copy()
    filings = filings.dropna(subset=["accession_number", "primary_document"])
    if accession_number:
        requested = filings[filings["accession_number"].astype(str).eq(str(accession_number).strip())]
        if not requested.empty:
            filings = requested
            if primary_document:
                exact = filings[filings["primary_document"].astype(str).eq(str(primary_document).strip())]
                if not exact.empty:
                    filings = exact
    filings = filings.drop_duplicates(
        subset=["cik", "accession_number", "primary_document"]
    )
    filings = filings.sort_values("filing_date", ascending=False)
    if limit is not None:
        filings = filings.head(max(0, int(limit)))

    if manifest_path.exists():
        old = pd.read_parquet(manifest_path)
    else:
        old = pd.DataFrame()

    old_by_id = {}
    if not old.empty and "document_id" in old.columns:
        old_by_id = {
            str(row["document_id"]): row.to_dict()
            for _, row in old.iterrows()
        }

    session = requests.Session()
    session.headers.update({
        "User-Agent": SEC_USER_AGENT,
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
    })

    results = []
    last_request = 0.0

    for _, filing in filings.iterrows():
        accession = str(filing["accession_number"]).strip()
        primary = str(filing["primary_document"]).strip()
        document_id = hashlib.sha256(
            f"{cik}|{accession}|{primary}".encode("utf-8")
        ).hexdigest()

        previous = old_by_id.get(document_id, {})
        if previous.get("status") == "DOWNLOADED":
            existing_path = Path(str(previous.get("local_path", "")))
            if existing_path.is_file() and existing_path.stat().st_size > 0:
                results.append(previous)
                continue

        try:
            url = _document_url(cik, accession, primary)
            safe_accession = _safe_part(accession.replace("-", ""), "accession")
            relative_doc = _safe_document_path(primary)
            target = company_dir / safe_accession / Path(*relative_doc.split("/"))
            target.parent.mkdir(parents=True, exist_ok=True)

            wait = min_interval - (time.monotonic() - last_request)
            if wait > 0:
                time.sleep(wait)

            content = _download_document_bytes(session, url, timeout)
            last_request = time.monotonic()
            if not content:
                raise ValueError("SEC returned an empty document")

            temp_path = target.with_suffix(target.suffix + ".part")
            temp_path.write_bytes(content)
            temp_path.replace(target)

            record = {
                "document_id": document_id,
                "company_id": f"SEC_{cik}",
                "cik": cik,
                "company_name": str(filing["company_name"]),
                "accession_number": accession,
                "filing_date": str(filing["filing_date"]),
                "report_date": str(filing.get("report_date", "")),
                "form": str(filing["form"]),
                "primary_document": primary,
                "document_url": url,
                "local_path": str(target.resolve()),
                "sha256": hashlib.sha256(content).hexdigest(),
                "byte_count": len(content),
                "status": "DOWNLOADED",
                "discovered_at_utc": _utc_now(),
                "downloaded_at_utc": _utc_now(),
                "error": "",
            }
        except Exception as exc:
            record = {
                "document_id": document_id,
                "company_id": f"SEC_{cik}",
                "cik": cik,
                "company_name": str(filing.get("company_name", "")),
                "accession_number": accession,
                "filing_date": str(filing.get("filing_date", "")),
                "report_date": str(filing.get("report_date", "")),
                "form": str(filing.get("form", "")),
                "primary_document": primary,
                "document_url": "",
                "local_path": "",
                "sha256": "",
                "byte_count": 0,
                "status": "FAILED",
                "discovered_at_utc": _utc_now(),
                "downloaded_at_utc": "",
                "error": f"{type(exc).__name__}: {exc}",
            }

        results.append(record)

    combined = pd.DataFrame(results)
    if not old.empty:
        untouched = old[~old["document_id"].astype(str).isin(
            combined["document_id"].astype(str)
        )]
        combined = pd.concat([untouched, combined], ignore_index=True)

    if not combined.empty:
        combined = combined.drop_duplicates("document_id", keep="last")
        combined.to_parquet(manifest_path, index=False)

    counts = combined["status"].value_counts().to_dict() if not combined.empty else {}
    return {
        "cik": cik,
        "company_id": f"SEC_{cik}",
        "discovered_in_batch": len(filings),
        "downloaded_or_skipped": sum(
            1 for r in results if r.get("status") == "DOWNLOADED"
        ),
        "failed_in_batch": sum(
            1 for r in results if r.get("status") == "FAILED"
        ),
        "manifest_path": str(manifest_path.resolve()),
        "status_counts": counts,
        "document_ids": [str(r.get("document_id")) for r in results if r.get("document_id")],
    }
