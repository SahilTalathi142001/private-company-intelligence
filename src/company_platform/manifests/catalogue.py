"""Filing catalogue shaped like a bulk filings lake.

One row per SEC accession. The accession is the stable filing id. Release
year is the only partition. A later run replaces that row in the year
snapshot and lists the change in the daily incremental file. This module
reads stored submissions and document manifests. It does not call SEC.
"""

from __future__ import annotations

import argparse
import json
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from company_platform.manifests.lineage import retain_lineaged_rows
from company_platform.registry.company_registry import normalize_cik
from sec_ingestion.document_ingestion import _document_url


ROOT = Path(__file__).resolve().parents[3]
DATA = ROOT / "data"

FILING_COLUMNS = [
    "filing_id",
    "cik",
    "company_id",
    "company_name",
    "form",
    "filing_date",
    "report_date",
    "primary_document",
    "archive_url",
    "local_document_path",
    "document_status",
    "source_id",
    "updated_date",
]
COMPANY_COLUMNS = [
    "company_id",
    "cik",
    "company_name",
    "ticker",
    "status",
    "onboarding_status",
    "ingestion_enabled",
    "source",
    "updated_at_utc",
]
FAILURE_COLUMNS = [
    "filing_id",
    "cik",
    "form",
    "filing_date",
    "document_status",
    "error",
]
SCHEMA_VERSION = 1
SOURCE_ID = "SEC"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _run_date(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).date().isoformat()


def _text(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "nat"}:
        return ""
    return text


def _cik(value) -> str:
    text = _text(value)
    if not text:
        return ""
    try:
        return normalize_cik(text)
    except ValueError:
        return ""


def manifests_root(data_root: Path, env: str = "dev") -> Path:
    if env not in {"dev", "prod"}:
        raise ValueError(f"env must be dev or prod, got {env!r}")
    return Path(data_root) / "manifests" / env


def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _registry(data_root: Path) -> pd.DataFrame:
    frame = _read_parquet(data_root / "silver" / "platform" / "company_registry.parquet")
    if frame.empty:
        return frame
    frame = frame.copy()
    frame["cik"] = frame["cik"].map(_cik)
    return frame[frame["cik"].ne("")].copy()


def _active_ciks(registry: pd.DataFrame) -> list[str]:
    if registry.empty:
        return []
    work = registry.copy()
    if "status" in work.columns:
        work = work[work["status"].map(_text).str.upper().eq("ACTIVE")]
    if "ingestion_enabled" in work.columns:
        enabled = work["ingestion_enabled"].map(
            lambda value: value is True or _text(value).lower() in {"true", "1", "yes", "y"}
        )
        work = work[enabled]
    return sorted(work["cik"].unique().tolist())


def _company_snapshot(registry: pd.DataFrame) -> pd.DataFrame:
    if registry.empty:
        return pd.DataFrame(columns=COMPANY_COLUMNS)
    frame = registry.copy()
    for column in COMPANY_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    frame["company_id"] = frame.apply(
        lambda row: _text(row["company_id"]) or f"SEC_{row['cik']}",
        axis=1,
    )
    return frame[COMPANY_COLUMNS].drop_duplicates("cik", keep="last").reset_index(drop=True)


def _load_filings(data_root: Path, ciks: set[str]) -> pd.DataFrame:
    frames = []
    root = data_root / "silver" / "filings"
    if not root.exists():
        return pd.DataFrame()
    for path in sorted(root.glob("cik=*/filings.parquet")):
        cik = _cik(path.parent.name.split("=", 1)[-1])
        if ciks and cik not in ciks:
            continue
        frame = _read_parquet(path)
        if frame.empty:
            continue
        frame = frame.copy()
        frame["cik"] = frame["cik"].map(_cik) if "cik" in frame.columns else cik
        frame.loc[frame["cik"].eq(""), "cik"] = cik
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _load_manifests(data_root: Path, ciks: set[str]) -> pd.DataFrame:
    frames = []
    root = data_root / "bronze" / "sec" / "documents"
    if not root.exists():
        return pd.DataFrame()
    for path in sorted(root.glob("cik=*/document_manifest.parquet")):
        cik = _cik(path.parent.name.split("=", 1)[-1])
        if ciks and cik not in ciks:
            continue
        frame = _read_parquet(path)
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    combined["cik"] = combined["cik"].map(_cik)
    combined["accession_number"] = combined["accession_number"].map(_text)
    combined["primary_document"] = combined["primary_document"].map(_text)
    status_rank = combined["status"].map(_text).eq("DOWNLOADED").astype(int)
    combined = combined.assign(_rank=status_rank)
    combined = combined.sort_values("_rank").drop_duplicates(
        ["cik", "accession_number", "primary_document"],
        keep="last",
    )
    return combined.drop(columns="_rank")


def _archive_url(cik: str, accession: str, primary: str, stated_url: str) -> str:
    if stated_url:
        return stated_url
    if not primary:
        return ""
    try:
        return _document_url(cik, accession, primary)
    except ValueError:
        return ""


def _relative_document(data_root: Path, local_path: str, status: str) -> tuple[str, str]:
    if status != "DOWNLOADED" or not local_path:
        return "", status or "NOT_DOWNLOADED"
    path = Path(local_path)
    if not path.is_file() or path.stat().st_size <= 0:
        return "", "FILE_MISSING"
    try:
        return path.resolve().relative_to(data_root.resolve()).as_posix(), "DOWNLOADED"
    except ValueError:
        return str(path), "DOWNLOADED"


def _build_rows(
    filings: pd.DataFrame,
    manifests: pd.DataFrame,
    companies: pd.DataFrame,
    data_root: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if filings.empty:
        return pd.DataFrame(columns=FILING_COLUMNS), pd.DataFrame(columns=FAILURE_COLUMNS)

    names = {}
    ids = {}
    if not companies.empty:
        names = dict(zip(companies["cik"], companies["company_name"].map(_text)))
        ids = dict(zip(companies["cik"], companies["company_id"].map(_text)))

    manifest_index = {}
    if not manifests.empty:
        for record in manifests.to_dict(orient="records"):
            key = (
                _cik(record.get("cik")),
                _text(record.get("accession_number")),
                _text(record.get("primary_document")),
            )
            manifest_index[key] = record

    catalogue_by_id = {}
    failure_by_id = {}
    ordered = filings.copy()
    ordered["_filing_date"] = ordered["filing_date"].map(_text) if "filing_date" in ordered.columns else ""
    ordered = ordered.sort_values("_filing_date")
    for record in ordered.to_dict(orient="records"):
        accession = _text(record.get("accession_number"))
        cik = _cik(record.get("cik"))
        form = _text(record.get("form"))
        filing_date = _text(record.get("filing_date"))
        primary = _text(record.get("primary_document"))
        if not accession or not cik:
            continue
        year = filing_date[:4]
        if not (len(filing_date) >= 10 and year.isdigit() and len(year) == 4):
            failure_by_id[accession] = {
                "filing_id": accession,
                "cik": cik,
                "form": form,
                "filing_date": filing_date,
                "document_status": "INVALID_FILING_DATE",
                "error": "filing_date is required to place the row in a release-year partition",
            }
            catalogue_by_id.pop(accession, None)
            continue
        located = manifest_index.get((cik, accession, primary), {})
        status = _text(located.get("status")) or "NOT_DOWNLOADED"
        local_path, status = _relative_document(data_root, _text(located.get("local_path")), status)
        error = _text(located.get("error"))
        if status == "FAILED" or status == "FILE_MISSING" or not primary:
            if not primary and status == "NOT_DOWNLOADED":
                status = "MISSING_PRIMARY"
            failure_by_id[accession] = {
                "filing_id": accession,
                "cik": cik,
                "form": form,
                "filing_date": filing_date,
                "document_status": status,
                "error": error,
            }
        else:
            failure_by_id.pop(accession, None)
        catalogue_by_id[accession] = {
            "filing_id": accession,
            "cik": cik,
            "company_id": ids.get(cik) or f"SEC_{cik}",
            "company_name": names.get(cik) or _text(record.get("company_name")),
            "form": form,
            "filing_date": filing_date[:10],
            "report_date": _text(record.get("report_date")) or None,
            "primary_document": primary or None,
            "archive_url": _archive_url(cik, accession, primary, _text(located.get("document_url"))) or None,
            "local_document_path": local_path or None,
            "document_status": status,
            "source_id": SOURCE_ID,
            "updated_date": "",
        }
    catalogue = pd.DataFrame(list(catalogue_by_id.values()), columns=FILING_COLUMNS)
    failures = pd.DataFrame(list(failure_by_id.values()), columns=FAILURE_COLUMNS)
    return catalogue, failures


def _signature(row: dict) -> tuple:
    return tuple(_text(row.get(column)) for column in FILING_COLUMNS if column != "updated_date")


def _apply_updated_dates(incoming: pd.DataFrame, existing: pd.DataFrame, run_date: str) -> pd.DataFrame:
    if incoming.empty:
        return incoming
    previous = {}
    if not existing.empty and "filing_id" in existing.columns:
        for record in existing.to_dict(orient="records"):
            previous[_text(record.get("filing_id"))] = record
    dates = []
    for record in incoming.to_dict(orient="records"):
        filing_id = _text(record.get("filing_id"))
        prior = previous.get(filing_id)
        if prior is None or _signature(record) != _signature(prior):
            dates.append(run_date)
        else:
            dates.append(_text(prior.get("updated_date")) or run_date)
    result = incoming.copy()
    result["updated_date"] = dates
    return result


def _year_path(root: Path, year: str) -> Path:
    return root / "filings" / f"year={year}" / "part-00000.parquet"


def _write_frame(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _read_year(root: Path, year: str) -> pd.DataFrame:
    return _read_parquet(_year_path(root, year))


def _merge_year(existing: pd.DataFrame, incoming: pd.DataFrame, replace_year: bool) -> pd.DataFrame:
    if replace_year or existing.empty:
        base = incoming
    else:
        incoming_ids = set(incoming["filing_id"].map(_text))
        kept = existing[~existing["filing_id"].map(_text).isin(incoming_ids)]
        base = pd.concat([kept, incoming], ignore_index=True)
    if base.empty:
        return pd.DataFrame(columns=FILING_COLUMNS)
    base = base.drop_duplicates("filing_id", keep="last")
    return base[FILING_COLUMNS].sort_values(["filing_date", "filing_id"]).reset_index(drop=True)


def _write_schema(root: Path) -> None:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "grain": "one row per SEC accession",
        "filing_id": "SEC accession number. Stable. A correction replaces the row.",
        "partition": "filings/year=YYYY where YYYY is the calendar year of filing_date",
        "incremental": "incremental/YYYY-MM-DD.parquet contains rows whose updated_date is that UTC day",
        "columns": FILING_COLUMNS,
        "company_columns": COMPANY_COLUMNS,
        "excluded": [
            "version",
            "ownership_pct",
            "valuation",
            "investment_amount_usd",
        ],
    }
    path = root / "schema.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_failures(
    root: Path,
    run_date: str,
    failures: pd.DataFrame,
    ciks: set[str],
    replay_year: int | None,
    full_replace: bool,
) -> Path:
    path = root / "incremental" / f"{run_date}.failures.parquet"
    existing = _read_parquet(path)
    current = failures.copy() if not failures.empty else pd.DataFrame(columns=FAILURE_COLUMNS)
    if not full_replace and not existing.empty:
        if replay_year is not None:
            kept = existing[~existing["filing_date"].map(_text).str[:4].eq(str(replay_year))]
        else:
            kept = existing[~existing["cik"].map(_cik).isin(ciks)]
        current = pd.concat([kept, current], ignore_index=True)
    if current.empty:
        current = pd.DataFrame(columns=FAILURE_COLUMNS)
    else:
        current = current[FAILURE_COLUMNS].drop_duplicates("filing_id", keep="last")
    _write_frame(path, current)
    return path


def _incremental_from_catalogue(root: Path, run_date: str) -> pd.DataFrame:
    frames = []
    filings_root = root / "filings"
    if filings_root.exists():
        for path in sorted(filings_root.glob("year=*/part-00000.parquet")):
            frame = _read_parquet(path)
            if frame.empty or "updated_date" not in frame.columns:
                continue
            changed = frame[frame["updated_date"].map(_text).eq(run_date)]
            if not changed.empty:
                frames.append(changed)
    if not frames:
        return pd.DataFrame(columns=FILING_COLUMNS)
    combined = pd.concat(frames, ignore_index=True)
    return combined[FILING_COLUMNS].drop_duplicates("filing_id", keep="last")


def read_incremental_since(data_root: Path, after_date: str, env: str = "dev") -> pd.DataFrame:
    """Rows from daily incremental files dated after ``after_date``.

    Callers advance a watermark with these files. They do not diff year partitions.
    """
    root = manifests_root(data_root, env) / "incremental"
    frames = []
    if root.exists():
        for path in sorted(root.glob("*.parquet")):
            if path.name.endswith(".failures.parquet"):
                continue
            day = path.stem
            if len(day) == 10 and day > after_date:
                frame = _read_parquet(path)
                if not frame.empty:
                    frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=FILING_COLUMNS)
    combined = pd.concat(frames, ignore_index=True)
    return combined[FILING_COLUMNS].drop_duplicates("filing_id", keep="last")


def publish_catalogue(
    data_root: Path | None = None,
    ciks: list[str] | None = None,
    run_at: datetime | None = None,
    replay_year: int | None = None,
    env: str = "dev",
) -> dict:
    """Rewrite the company snapshot and merge filing rows into year partitions."""
    data_root = Path(data_root) if data_root is not None else DATA
    moment = run_at or _utc_now()
    run_date = _run_date(moment)
    root = manifests_root(data_root, env)
    registry = _registry(data_root)
    selected = [_cik(cik) for cik in ciks] if ciks is not None else _active_ciks(registry)
    selected = [cik for cik in selected if cik]
    selected_set = set(selected)
    filings = _load_filings(data_root, selected_set)
    if replay_year is not None and not filings.empty:
        filings = filings[filings["filing_date"].map(_text).str[:4].eq(str(replay_year))].copy()
    manifests = _load_manifests(data_root, selected_set)
    incoming, failures = _build_rows(filings, manifests, _company_snapshot(registry), data_root)

    years = sorted(incoming["filing_date"].str[:4].unique().tolist()) if not incoming.empty else []
    if replay_year is not None:
        years = [str(replay_year)]
    written_years = []
    for year in years:
        year_incoming = incoming[incoming["filing_date"].str[:4].eq(year)].copy() if not incoming.empty else pd.DataFrame(columns=FILING_COLUMNS)
        existing = _read_year(root, year)
        dated = _apply_updated_dates(year_incoming, existing, run_date)
        merged = _merge_year(existing, dated, replace_year=ciks is None)
        if merged.empty:
            continue
        _write_frame(_year_path(root, year), merged)
        written_years.append(year)
    if ciks is None and replay_year is None:
        filings_root = root / "filings"
        if filings_root.exists():
            for directory in filings_root.glob("year=*"):
                year = directory.name.split("=", 1)[-1]
                if year not in written_years:
                    part = directory / "part-00000.parquet"
                    if part.exists():
                        part.unlink()

    _write_schema(root)
    company_frame = _company_snapshot(registry)
    _write_frame(root / "companies.parquet", company_frame)
    incremental = _incremental_from_catalogue(root, run_date)
    incremental_path = root / "incremental" / f"{run_date}.parquet"
    _write_frame(incremental_path, incremental)
    failure_path = _write_failures(
        root,
        run_date,
        failures,
        selected_set,
        replay_year,
        full_replace=ciks is None and replay_year is None,
    )
    promoted = {}
    if replay_year is not None:
        promoted = promote_local(data_root, env=env, year=replay_year)
    return {
        "run_date": run_date,
        "companies": int(len(company_frame)),
        "catalogue_rows_written": int(sum(len(_read_year(root, year)) for year in written_years)),
        "years": written_years,
        "incremental_rows": int(len(incremental)),
        "failure_rows": int(len(_read_parquet(failure_path))),
        "incremental_path": str(incremental_path),
        "companies_path": str(root / "companies.parquet"),
        "financial_rows": promoted.get("financial_rows", 0),
        "holder_rows": promoted.get("holder_rows", 0),
        "valuation_status": "NO_DATA",
    }


def ensure_company_master(
    data_root: Path,
    cik: str,
    company_name: str | None = None,
    ticker: str | None = None,
) -> dict:
    """Add a CIK to the company master when it is not already there."""
    master_path = data_root / "silver" / "company" / "company.parquet"
    normalized = normalize_cik(cik)
    master = _read_parquet(master_path)
    if master.empty:
        master = pd.DataFrame(columns=["cik", "ticker", "company_name"])
    else:
        master = master.copy()
        master["cik"] = master["cik"].map(_cik)
    existing = master[master["cik"].eq(normalized)]
    if not existing.empty:
        row = existing.iloc[0]
        return {
            "cik": normalized,
            "company_name": _text(row.get("company_name")),
            "ticker": _text(row.get("ticker")),
            "added": False,
        }
    name = _text(company_name)
    if not name:
        raise ValueError(
            f"CIK {normalized} is not in the company master. "
            "Pass company_name to add it."
        )
    added = pd.DataFrame([{
        "cik": normalized,
        "ticker": _text(ticker) or None,
        "company_name": name,
    }])
    master = pd.concat([master, added], ignore_index=True)
    master_path.parent.mkdir(parents=True, exist_ok=True)
    master.to_parquet(master_path, index=False)
    return {
        "cik": normalized,
        "company_name": name,
        "ticker": _text(ticker),
        "added": True,
    }


@contextmanager
def _registry_paths(data_root: Path):
    from company_platform.registry import company_registry as registry

    previous = (registry.COMPANY_MASTER_PATH, registry.REGISTRY_DIR, registry.REGISTRY_PATH)
    registry.COMPANY_MASTER_PATH = data_root / "silver" / "company" / "company.parquet"
    registry.REGISTRY_DIR = data_root / "silver" / "platform"
    registry.REGISTRY_PATH = registry.REGISTRY_DIR / "company_registry.parquet"
    try:
        yield registry
    finally:
        registry.COMPANY_MASTER_PATH, registry.REGISTRY_DIR, registry.REGISTRY_PATH = previous


def enroll_and_backfill(
    cik: str,
    company_name: str | None = None,
    ticker: str | None = None,
    data_root: Path | None = None,
    run_at: datetime | None = None,
    env: str = "dev",
) -> dict:
    """Add the CIK to the master, register it, and publish only that company's rows."""
    data_root = Path(data_root) if data_root is not None else DATA
    identity = ensure_company_master(data_root, cik, company_name=company_name, ticker=ticker)
    with _registry_paths(data_root) as registry:
        registered = registry.register_company(
            identity["cik"],
            source="USER",
            preferred_ticker=identity["ticker"] or None,
        )
    published = publish_catalogue(data_root, ciks=[identity["cik"]], run_at=run_at, env=env)
    published["enrolled_cik"] = identity["cik"]
    published["company_id"] = registered["company_id"]
    published["master_added"] = identity["added"]
    return published


def iter_manifest_keys(data_root: Path, env: str = "dev") -> list[tuple[str, Path]]:
    root = manifests_root(data_root, env)
    keys = []
    if not root.exists():
        return keys
    for path in sorted(root.rglob("*")):
        if path.is_file():
            keys.append((path.relative_to(root).as_posix(), path))
    return keys


def sync_manifests_to_s3(data_root: Path, bucket: str, prefix: str, client, env: str = "dev") -> dict:
    """Copy the manifests tree onto an existing bucket. Does not create the bucket."""
    bucket = bucket.strip()
    prefix = prefix.strip().strip("/")
    if not bucket:
        raise ValueError("S3 bucket is required")
    client.head_bucket(Bucket=bucket)
    uploaded = []
    for key, path in iter_manifest_keys(data_root, env):
        object_key = f"{prefix}/{key}" if prefix else key
        client.upload_file(str(path), bucket, object_key)
        uploaded.append(object_key)
    return {"bucket": bucket, "uploaded": uploaded, "objects": len(uploaded)}


def parse_s3_uri(uri: str) -> tuple[str, str]:
    text = uri.strip()
    if not text.startswith("s3://"):
        raise ValueError(f"Expected an s3:// URI, got {uri!r}")
    remainder = text[5:]
    bucket, _, prefix = remainder.partition("/")
    if not bucket:
        raise ValueError(f"S3 URI is missing a bucket: {uri!r}")
    return bucket, prefix.strip("/")


def _s3_client():
    import boto3

    return boto3.client("s3")


def _known_filing_ids(data_root: Path, env: str) -> set[str]:
    ids = set()
    filings_root = manifests_root(data_root, env) / "filings"
    if not filings_root.exists():
        return ids
    for path in filings_root.glob("year=*/part-00000.parquet"):
        frame = _read_parquet(path)
        if not frame.empty and "filing_id" in frame.columns:
            ids.update(frame["filing_id"].map(_text))
    return {item for item in ids if item}


def _append_silver_filing(data_root: Path, cik: str, filing: dict) -> None:
    path = data_root / "silver" / "filings" / f"cik={cik}" / "filings.parquet"
    row = {
        "accession_number": _text(filing.get("accession_number")),
        "filing_date": _text(filing.get("filing_date")),
        "report_date": _text(filing.get("report_date")),
        "form": _text(filing.get("form")),
        "primary_document": _text(filing.get("primary_document")),
        "primary_doc_description": _text(filing.get("primary_doc_description")),
        "cik": cik,
        "company_name": _text(filing.get("company_name")),
    }
    frame = _read_parquet(path)
    if not frame.empty and row["accession_number"] in set(frame["accession_number"].map(_text)):
        return
    frame = pd.concat([frame, pd.DataFrame([row])], ignore_index=True)
    _write_frame(path, frame)


def _normalize_polled(cik: str, filing: dict) -> dict:
    return {
        "accession_number": _text(filing.get("accession_number") or filing.get("accessionNumber")),
        "filing_date": _text(filing.get("filing_date") or filing.get("filingDate")),
        "report_date": _text(filing.get("report_date") or filing.get("reportDate")),
        "form": _text(filing.get("form")),
        "primary_document": _text(filing.get("primary_document") or filing.get("primaryDocument")),
        "primary_doc_description": _text(filing.get("primary_doc_description") or filing.get("primaryDocDescription")),
        "cik": cik,
        "company_name": _text(filing.get("company_name") or filing.get("companyName")),
    }


def _default_poll_recent(cik: str) -> list[dict]:
    from company_platform.sec_detection.detector import SECFilingDetector

    return SECFilingDetector()._recent(cik)


def _default_download(cik: str, filing: dict) -> None:
    from sec_ingestion.document_ingestion import ingest_sec_filing_documents

    ingest_sec_filing_documents(
        cik,
        filing_records=[filing],
        accession_number=filing["accession_number"],
        limit=1,
    )


def _year_text(record: dict, columns: tuple[str, ...]) -> str:
    for column in columns:
        text = _text(record.get(column))
        if len(text) >= 4 and text[:4].isdigit():
            return text[:4]
    return ""


def _load_local_financials(data_root: Path) -> pd.DataFrame:
    for relative, name in (
        ("gold/financials", "financials.parquet"),
        ("silver/financial_facts", "financial_facts.parquet"),
    ):
        base = data_root.joinpath(*relative.split("/"))
        if not base.exists():
            continue
        frames = [_read_parquet(path) for path in sorted(base.glob(f"cik=*/{name}"))]
        frames = [frame for frame in frames if not frame.empty]
        if frames:
            return pd.concat(frames, ignore_index=True)
    return pd.DataFrame()


def _load_local_holders(data_root: Path) -> pd.DataFrame:
    path = data_root / "silver" / "funding" / "investors.parquet"
    frame = _read_parquet(path)
    if frame.empty:
        return frame
    if "accession_number" not in frame.columns and "source_accession_number" in frame.columns:
        frame = frame.copy()
        frame["accession_number"] = frame["source_accession_number"]
    return frame


def _write_promoted(root: Path, name: str, frame: pd.DataFrame, year: int | None) -> None:
    target = root / "promoted" / name
    if frame.empty:
        if year is not None:
            _write_frame(target / f"year={year}" / "part-00000.parquet", frame)
        return
    records = frame.to_dict(orient="records")
    date_columns = ("filing_date", "period_end") if name == "financials" else ("as_of_date", "filing_date")
    grouped: dict[str, list] = {}
    for record in records:
        row_year = _year_text(record, date_columns)
        if year is not None and row_year != str(year):
            continue
        if not row_year:
            continue
        grouped.setdefault(row_year, []).append(record)
    years = [str(year)] if year is not None else sorted(grouped)
    for item in years:
        rows = grouped.get(item, [])
        _write_frame(target / f"year={item}" / "part-00000.parquet", pd.DataFrame(rows))


def promote_local(data_root: Path, env: str = "dev", year: int | None = None) -> dict:
    """Promote local financial and holder rows that state an accession or an as-of date.

    Valuation is left NO_DATA. This reads files already on disk and does not call SEC.
    """
    financials = retain_lineaged_rows(_load_local_financials(data_root))
    holders = retain_lineaged_rows(_load_local_holders(data_root))
    root = manifests_root(data_root, env)
    _write_promoted(root, "financials", financials, year)
    _write_promoted(root, "holders", holders, year)
    if year is not None:
        financials = financials[financials.apply(lambda row: _year_text(row, ("filing_date", "period_end")) == str(year), axis=1)] if not financials.empty else financials
        holders = holders[holders.apply(lambda row: _year_text(row, ("as_of_date", "filing_date")) == str(year), axis=1)] if not holders.empty else holders
    return {
        "financial_rows": int(len(financials)),
        "holder_rows": int(len(holders)),
        "valuation_status": "NO_DATA",
        "valuation_file_written": False,
    }


def _failure_row(cik: str, filing: dict | None, status: str, error: str) -> dict:
    filing = filing or {}
    accession = _text(filing.get("accession_number"))
    return {
        "filing_id": accession or f"{cik}:{status}",
        "cik": cik,
        "form": _text(filing.get("form")),
        "filing_date": _text(filing.get("filing_date")),
        "document_status": status,
        "error": error,
    }


def _add_failure_rows(data_root: Path, env: str, run_date: str, rows: list[dict]) -> None:
    if not rows:
        return
    root = manifests_root(data_root, env)
    path = root / "incremental" / f"{run_date}.failures.parquet"
    existing = _read_parquet(path)
    current = pd.DataFrame(rows, columns=FAILURE_COLUMNS)
    if not existing.empty:
        current = pd.concat([existing, current], ignore_index=True)
    current = current[FAILURE_COLUMNS].drop_duplicates("filing_id", keep="last")
    _write_frame(path, current)


TASKS = ("extract", "catalogue", "refresh", "promote")


def _run_file(root: Path, run_id: str) -> Path:
    return root / "runs" / f"{run_id}.json"


def _load_run(path: Path, run_id: str, env: str) -> dict:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {
        "run_id": run_id,
        "env": env,
        "status": "RUNNING",
        "tasks": {name: {"status": "PENDING"} for name in TASKS},
    }


def _save_run(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")


def _default_refresh(ciks: list[str]) -> dict:
    from company_platform.sec_detection.apply_refresh import apply_refresh

    return apply_refresh(ciks)


def run_daily(
    data_root: Path | None = None,
    env: str = "dev",
    run_at: datetime | None = None,
    run_id: str | None = None,
    poll_recent=None,
    download_accession=None,
    promote_fn=None,
    refresh_fn=None,
) -> dict:
    """Poll new accessions, refresh only those companies, and promote lineaged gold.

    One company or accession failure is recorded. The other companies still run.
    A rerun with the same run id retries only tasks that are not SUCCESS.
    """
    data_root = Path(data_root) if data_root is not None else DATA
    moment = run_at or _utc_now()
    run_date = _run_date(moment)
    root = manifests_root(data_root, env)
    run_id = run_id or moment.strftime("%Y-%m-%dT%H%M%SZ")
    path = _run_file(root, run_id)
    state = _load_run(path, run_id, env)
    poll_recent = poll_recent or _default_poll_recent
    download_accession = download_accession or _default_download
    refresh_fn = refresh_fn or _default_refresh
    registry = _registry(data_root)
    ciks = _active_ciks(registry)

    def extract() -> dict:
        known = _known_filing_ids(data_root, env)
        failures = []
        downloaded = []
        downloaded_ciks = []
        for cik in ciks:
            try:
                recent = poll_recent(cik) or []
            except Exception as exc:
                failures.append(_failure_row(cik, None, "POLL_FAILED", f"{type(exc).__name__}: {exc}"))
                continue
            for raw in recent:
                filing = _normalize_polled(cik, raw)
                accession = filing["accession_number"]
                if not accession or accession in known:
                    continue
                try:
                    _append_silver_filing(data_root, cik, filing)
                    if filing["primary_document"]:
                        download_accession(cik, filing)
                    else:
                        raise ValueError("primary document was not stated")
                    downloaded.append(accession)
                    if cik not in downloaded_ciks:
                        downloaded_ciks.append(cik)
                    known.add(accession)
                except Exception as exc:
                    failures.append(_failure_row(cik, filing, "EXTRACT_FAILED", f"{type(exc).__name__}: {exc}"))
        _add_failure_rows(data_root, env, run_date, failures)
        return {
            "company_failures": failures,
            "downloaded": downloaded,
            "downloaded_ciks": downloaded_ciks,
        }

    def catalogue() -> dict:
        failures = []
        for cik in ciks:
            try:
                publish_catalogue(data_root, ciks=[cik], run_at=moment, env=env)
            except Exception as exc:
                failures.append(_failure_row(cik, None, "CATALOGUE_FAILED", f"{type(exc).__name__}: {exc}"))
        extract_failures = state["tasks"].get("extract", {}).get("company_failures", [])
        _add_failure_rows(data_root, env, run_date, extract_failures)
        _add_failure_rows(data_root, env, run_date, failures)
        return {"company_failures": failures}

    def refresh() -> dict:
        selected = list(state["tasks"].get("extract", {}).get("downloaded_ciks") or [])
        if not selected:
            return {"ciks": [], "company_failures": []}
        failures = []
        refreshed = []
        for cik in selected:
            try:
                result = refresh_fn([cik]) or {}
                failed_count = int(result.get("company_failures") or 0) if isinstance(result, dict) else 0
                if failed_count:
                    failures.append(_failure_row(
                        cik,
                        None,
                        "REFRESH_FAILED",
                        f"{failed_count} company refresh failure(s)",
                    ))
                else:
                    refreshed.append(cik)
            except Exception as exc:
                failures.append(_failure_row(cik, None, "REFRESH_FAILED", f"{type(exc).__name__}: {exc}"))
        _add_failure_rows(data_root, env, run_date, failures)
        return {"ciks": refreshed, "company_failures": failures}

    def promote() -> dict:
        if promote_fn is not None:
            return promote_fn(data_root, env)
        return promote_local(data_root, env=env)

    runners = {"extract": extract, "catalogue": catalogue, "refresh": refresh, "promote": promote}
    for name in TASKS:
        if state["tasks"].get(name, {}).get("status") == "SUCCESS":
            continue
        try:
            detail = runners[name]()
            state["tasks"][name] = {"status": "SUCCESS", **detail}
            state["status"] = "RUNNING"
        except Exception as exc:
            state["tasks"][name] = {"status": "FAILED", "error": f"{type(exc).__name__}: {exc}"}
            state["status"] = "FAILED"
            _save_run(path, state)
            return state
        _save_run(path, state)
    state["status"] = "SUCCESS"
    _save_run(path, state)
    return state


def main(argv: list[str] | None = None) -> dict:
    parser = argparse.ArgumentParser(description="Publish the local filing catalogue")
    parser.add_argument("--data-root", default=str(DATA))
    parser.add_argument("--env", default="dev", choices=("dev", "prod"))
    parser.add_argument("--run-id", default="")
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--replay-year", type=int, default=None)
    parser.add_argument("--enroll-cik", default="")
    parser.add_argument("--company-name", default="")
    parser.add_argument("--ticker", default="")
    parser.add_argument("--sync-s3", default="")
    args = parser.parse_args(argv)
    data_root = Path(args.data_root)
    if args.sync_s3:
        bucket, prefix = parse_s3_uri(args.sync_s3)
        result = sync_manifests_to_s3(data_root, bucket, prefix, _s3_client(), env=args.env)
        print(json.dumps({"manifest_s3": "COPIED", **{k: result[k] for k in ("bucket", "objects")}}))
        return result
    if args.enroll_cik:
        result = enroll_and_backfill(
            args.enroll_cik,
            company_name=args.company_name or None,
            ticker=args.ticker or None,
            data_root=data_root,
            env=args.env,
        )
    elif args.replay_year is not None or args.publish:
        result = publish_catalogue(data_root, replay_year=args.replay_year, env=args.env)
    else:
        result = run_daily(data_root, env=args.env, run_id=args.run_id or None)
    print(json.dumps(result))
    return result


if __name__ == "__main__":
    main()
