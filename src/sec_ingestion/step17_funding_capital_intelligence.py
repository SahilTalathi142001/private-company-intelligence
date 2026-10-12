"""
STEP 17 - FUNDING & CAPITAL INTELLIGENCE
Version: 3.0.1

Purpose
-------
Build a production-grade funding/capital observation layer from SEC Form D/D-A
filings and optional investor enrichment, then derive company-level capital
intelligence without mislabeling SEC exempt-offering observations as complete
company fundraising history.

This step intentionally does NOT invent venture stages or investor names.
SEC Form D provides offering/sales amounts, first-sale information, security
types, exemptions and investor counts. Investor identities and conventional
"Seed / Series A / Series B" labels require an additional source.

Outputs
-------
data/gold/funding_capital_intelligence/
├── funding_events/
│   └── funding_events.parquet
├── funding_event_history/
│   └── funding_event_history.parquet
├── company_funding_snapshot/
│   └── company_funding_snapshot.parquet
├── funding_signals/
│   └── funding_signals.parquet
├── funding_summary/
│   └── funding_summary.parquet
└── funding_lineage/
    └── funding_lineage.parquet

Optional investor enrichment input
-----------------------------------
data/silver/funding/investors.parquet

Expected optional columns:
    cik
    funding_event_id OR offering_id
    investor_name
    investor_type
    investor_source
    investor_confidence

Important source limitation
---------------------------
Form D is a notice of an exempt securities offering. It is NOT a complete
database of all private-company fundraising. This step therefore labels
Form-D-derived amounts as SEC_EXEMPT_OFFERING_CAPITAL, not "total company
funding", unless an external funding source is later integrated.

Run
---
python step17_funding_capital_intelligence.py

Environment
-----------
SEC_USER_AGENT must contain a real contact email. lxml is required for recovery of malformed legacy Form D documents. For example:
SEC_USER_AGENT=Your Name your.email@company.com
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
import io
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote
import xml.etree.ElementTree as ET

try:
    from lxml import etree as LET
except ImportError:
    LET = None

import pandas as pd
from company_platform.registry.active_company_scope import load_active_company_ciks
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from company_platform.registry.active_company_scope import (
    assert_company_scope,
)


# =============================================================================
# CONFIG
# =============================================================================

load_dotenv()

SCRIPT_DIR = Path(__file__).resolve().parent

# Repository layout:
#   <repo>/src/sec_ingestion/<this_script>
#   <repo>/data/...
# Always resolve DATA_ROOT from the repository root. Do not select
# src/sec_ingestion/data merely because an accidental directory exists there.
REPO_ROOT = SCRIPT_DIR.parent.parent
DATA_ROOT = REPO_ROOT / "data"

BRONZE_ROOT = DATA_ROOT / "bronze" / "sec" / "form_d"
SILVER_ROOT = DATA_ROOT / "silver" / "funding"
GOLD_ROOT = DATA_ROOT / "gold" / "funding_capital_intelligence"

INVESTOR_INPUT = SILVER_ROOT / "investors.parquet"
COMPANY_MASTER_PATH = DATA_ROOT / "silver" / "company" / "company.parquet"

def configured_companies() -> list[str]:
    """Resolve the authoritative active production company universe."""
    ciks = [str(c).zfill(10) for c in load_active_company_ciks()]
    if not ciks:
        raise ValueError("Active company execution scope is empty.")
    return sorted(set(ciks))

COMPANIES = configured_companies()
SEC_DATA_BASE = "https://data.sec.gov"
SEC_ARCHIVE_BASE = "https://www.sec.gov/Archives/edgar/data"
SEC_FORM_D_DATASET_PAGE = "https://www.sec.gov/data-research/sec-markets-data/form-d-data-sets"
SEC_FORM_D_DATASET_BASE = "https://www.sec.gov/files/datastandardsinnovation/data/form-d-data-sets"

# Full historical SEC Form D dataset coverage. The SEC publishes quarterly
# datasets and the official page is used to discover the available quarters.
FORM_D_START_QUARTER = os.getenv("FORM_D_START_QUARTER", "2008Q1")
FORM_D_END_QUARTER = os.getenv("FORM_D_END_QUARTER", "")
FORM_D_DATASET_CACHE = BRONZE_ROOT / "datasets"
FORM_D_DATASET_REQUEST_TIMEOUT = 120
FORM_D_DATASET_REQUIRED_TABLES = {"FORMDSUBMISSION", "ISSUERS", "OFFERING"}
FORM_D_DATASET_OPTIONAL_TABLES = {"RECIPIENTS", "RELATEDPERSONS", "SIGNATURES"}

SEC_USER_AGENT = os.getenv(
    "SEC_USER_AGENT",
    "PrivateCompanyIntelligence/1.0 contact@example.com",
)

REQUEST_TIMEOUT = 60
MIN_REQUEST_INTERVAL = 0.25
RETRY_TOTAL = 5
RETRY_BACKOFF_FACTOR = 1.5
RETRY_STATUS_CODES = [429, 500, 502, 503, 504]

STEP17_VERSION = "3.0.1"
SOURCE_SYSTEM = "SEC_FORM_D"

VALID_FILING_TYPES = {"D", "D/A"}

# These are source classifications, not investment recommendations.
CAPITAL_SIGNAL_TYPES = {
    "RECENT_CAPITAL_ACTIVITY",
    "CAPITAL_ACCELERATION",
    "CAPITAL_DECELERATION",
    "CAPITAL_STABLE",
    "NO_RECENT_CAPITAL_ACTIVITY",
    "CAPITAL_DATA_LIMITED",
}

FRESHNESS_RECENT_DAYS = 180
FRESHNESS_STALE_DAYS = 730


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("step17")


# =============================================================================
# HELPERS
# =============================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_cik(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if not digits:
        return ""
    return digits.zfill(10)
COMPANIES = configured_companies()

def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.lower() in {"indefinite", "n/a", "na", "none", "null"}:
        return None
    text = text.replace(",", "").replace("$", "").strip()
    try:
        return float(text)
    except ValueError:
        return None


def parse_date(value: Any) -> Optional[pd.Timestamp]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return pd.to_datetime(text, errors="coerce")
    except Exception:
        return None


def safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return int(float(value))
    except Exception:
        return None


def json_hash(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def file_mtime_utc(path: Path) -> Optional[pd.Timestamp]:
    if not path.exists():
        return None
    return pd.Timestamp.fromtimestamp(
        path.stat().st_mtime,
        tz="UTC",
    )


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    ensure_parent(path)
    df.to_parquet(path, index=False)


def normalize_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"true", "yes", "y", "1"}:
        return True
    if text in {"false", "no", "n", "0"}:
        return False
    return None


# =============================================================================
# SEC CLIENT
# =============================================================================

class SECClient:
    def __init__(
        self,
        user_agent: str = SEC_USER_AGENT,
        timeout: int = REQUEST_TIMEOUT,
        min_request_interval: float = MIN_REQUEST_INTERVAL,
    ):
        self.timeout = timeout
        self.min_request_interval = min_request_interval
        self.last_request_time = 0.0

        self.session = requests.Session()

        retry = Retry(
            total=RETRY_TOTAL,
            connect=RETRY_TOTAL,
            read=RETRY_TOTAL,
            backoff_factor=RETRY_BACKOFF_FACTOR,
            status_forcelist=RETRY_STATUS_CODES,
            allowed_methods=["GET"],
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)

        self.session.headers.update({
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
        })

    def _rate_limit(self) -> None:
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)

    def get_json(self, url: str) -> Any:
        self._rate_limit()

        response = self.session.get(
            url,
            timeout=self.timeout,
            headers={"Accept": "application/json"},
        )
        self.last_request_time = time.time()

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            sleep_for = float(retry_after) if retry_after else 5.0
            time.sleep(min(max(sleep_for, 1.0), 60.0))
            response = self.session.get(
                url,
                timeout=self.timeout,
                headers={"Accept": "application/json"},
            )
            self.last_request_time = time.time()

        response.raise_for_status()
        return response.json()

    def get_text(self, url: str) -> str:
        self._rate_limit()

        response = self.session.get(
            url,
            timeout=self.timeout,
            headers={"Accept": "application/xml,text/xml,*/*"},
        )
        self.last_request_time = time.time()

        # urllib3 Retry handles transient 429/5xx responses. If a final
        # response still fails, raise it so the ingestion layer can persist
        # the discovery/download exception instead of silently dropping it.
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            sleep_for = float(retry_after) if retry_after else 5.0
            time.sleep(min(max(sleep_for, 1.0), 60.0))
            response = self.session.get(
                url,
                timeout=self.timeout,
                headers={"Accept": "application/xml,text/xml,*/*"},
            )
            self.last_request_time = time.time()

        response.raise_for_status()
        return response.text


# =============================================================================
# COMPANY MASTER
# =============================================================================

def load_company_master(
    required_ciks: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """
    Load the Silver company dataset and resolve it to the Step 17 company
    master grain: exactly one authoritative company name per required CIK.

    The Silver company dataset may legitimately contain multiple rows per CIK
    because it is an ingestion-level dataset. Step 17 must not treat that
    source grain as the Gold/master grain.

    Resolution policy:
      1. Validate required columns and normalized CIK/name values.
      2. Restrict to the companies Step 17 is actually processing.
      3. Collapse repeated rows when all nonblank names for a CIK agree.
      4. Fail if a CIK has conflicting company names.
      5. Fail if any required CIK is absent.
    """
    path = COMPANY_MASTER_PATH

    if not path.exists():
        raise FileNotFoundError(
            f"Authoritative company source not found: {path}"
        )

    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    elif path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported company master format: {path}")

    required = {"cik", "company_name"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Company master missing required columns: {sorted(missing)}"
        )

    df = df[["cik", "company_name"]].copy()
    df["cik"] = df["cik"].map(normalize_cik)
    df["company_name"] = df["company_name"].map(clean_text)

    if df["cik"].eq("").any():
        raise ValueError("Company master contains blank/invalid CIK values.")

    # Blank names are not usable identity records.
    df = df[df["company_name"].notna() & df["company_name"].ne("")].copy()

    if required_ciks is not None:
        required = [normalize_cik(cik) for cik in required_ciks]
        df = df[df["cik"].isin(required)].copy()

        found = set(df["cik"])
        missing_ciks = sorted(set(required) - found)
        if missing_ciks:
            raise ValueError(
                "Company master is missing required configured CIKs: "
                f"{missing_ciks}"
            )

    resolved_rows: list[dict[str, str]] = []

    for cik, group in df.groupby("cik", sort=True):
        names = sorted(
            {
                str(name).strip()
                for name in group["company_name"].dropna()
                if str(name).strip()
            }
        )

        if not names:
            raise ValueError(
                f"Company master has no usable company_name for CIK {cik}."
            )

        if len(names) > 1:
            raise ValueError(
                f"Company master has conflicting company names for CIK "
                f"{cik}: {names}"
            )

        resolved_rows.append(
            {
                "cik": cik,
                "company_name": names[0],
            }
        )

    out = pd.DataFrame(
        resolved_rows,
        columns=["cik", "company_name"],
    )

    if out.empty:
        raise ValueError(
            "Company master resolved to zero companies after filtering."
        )

    if out["cik"].duplicated().any():
        raise ValueError(
            "Internal company-master resolution produced duplicate CIKs."
        )

    return out.reset_index(drop=True)


# =============================================================================
# SEC SUBMISSIONS
# =============================================================================

def submissions_url(cik: str) -> str:
    return f"{SEC_DATA_BASE}/submissions/CIK{normalize_cik(cik)}.json"


def historical_submission_url(file_name: str) -> str:
    return f"{SEC_DATA_BASE}/submissions/{quote(file_name)}"


def get_submission_rows(
    client: SECClient,
    cik: str,
) -> List[Dict[str, Any]]:
    """
    Return current + historical SEC submission rows for Form D/D-A.

    The SEC submissions JSON can reference historical submission files through
    filings.files. Those files are loaded so the pipeline is not limited to
    the most recent filing block.
    """
    payload = client.get_json(submissions_url(cik))
    rows: List[Dict[str, Any]] = []

    recent = payload.get("filings", {})
    rows.extend(_submission_block_to_rows(recent, cik))

    for file_meta in recent.get("files", []) or []:
        name = file_meta.get("name")
        if not name:
            continue

        try:
            historical = client.get_json(historical_submission_url(name))
            rows.extend(_submission_block_to_rows(historical, cik))
        except Exception as exc:
            logger.warning(
                "Could not load historical submissions %s for %s: %s",
                name,
                cik,
                exc,
            )

    # Only Form D family.
    rows = [
        row for row in rows
        if clean_text(row.get("form")) in VALID_FILING_TYPES
    ]

    # Deduplicate by accession.
    dedup: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        accession = clean_text(row.get("accession_number"))
        if accession:
            dedup[accession] = row

    return list(dedup.values())


def _submission_block_to_rows(
    block: Dict[str, Any],
    cik: str,
) -> List[Dict[str, Any]]:
    if not block:
        return []

    keys = [
        "accessionNumber",
        "filingDate",
        "reportDate",
        "form",
        "fileNumber",
        "primaryDocument",
        "primaryDocDescription",
        "items",
        "size",
        "isXBRL",
        "isInlineXBRL",
    ]

    arrays = {
        key: block.get(key, []) or []
        for key in keys
    }

    length = len(arrays["accessionNumber"])
    rows: List[Dict[str, Any]] = []

    for i in range(length):
        row = {
            "cik": normalize_cik(cik),
            "accession_number": arrays["accessionNumber"][i],
            "filing_date": arrays["filingDate"][i],
            "report_date": arrays["reportDate"][i],
            "form": arrays["form"][i],
            "file_number": arrays["fileNumber"][i],
            "primary_document": arrays["primaryDocument"][i],
            "primary_doc_description": arrays["primaryDocDescription"][i],
            "items": arrays["items"][i],
            "size": arrays["size"][i],
            "is_xbrl": arrays["isXBRL"][i],
            "is_inline_xbrl": arrays["isInlineXBRL"][i],
        }
        rows.append(row)

    return rows


def build_filing_url(cik: str, accession: str, primary_document: str) -> str:
    cik_int = str(int(normalize_cik(cik)))
    accession_no_dash = accession.replace("-", "")
    return (
        f"{SEC_ARCHIVE_BASE}/{cik_int}/"
        f"{accession_no_dash}/{primary_document}"
    )



# =============================================================================
# SEC FORM D QUARTERLY DATASET INGESTION
# =============================================================================

def quarter_key_to_tuple(value: str) -> Tuple[int, int]:
    match = re.fullmatch(r"(\d{4})Q([1-4])", str(value).strip().upper())
    if not match:
        raise ValueError(f"Invalid quarter key: {value}. Expected YYYYQn.")
    return int(match.group(1)), int(match.group(2))


def quarter_in_range(key: str, start: str, end: Optional[str] = None) -> bool:
    y, q = quarter_key_to_tuple(key)
    sy, sq = quarter_key_to_tuple(start)
    if (y, q) < (sy, sq):
        return False
    if end:
        ey, eq = quarter_key_to_tuple(end)
        if (y, q) > (ey, eq):
            return False
    return True


def discover_form_d_dataset_quarters(client: SECClient) -> List[Dict[str, str]]:
    """Discover available quarterly Form D ZIPs from the official SEC page."""
    client._rate_limit()
    response = client.session.get(
        SEC_FORM_D_DATASET_PAGE,
        timeout=FORM_D_DATASET_REQUEST_TIMEOUT,
        headers={"Accept": "text/html,*/*"},
    )
    client.last_request_time = time.time()
    response.raise_for_status()

    pattern = re.compile(
        r'href=["\']([^"\']*/(\d{4}q[1-4])_d\.zip)["\']',
        flags=re.IGNORECASE,
    )

    found: Dict[str, str] = {}
    for href, quarter in pattern.findall(response.text):
        quarter = quarter.upper()
        if not quarter_in_range(
            quarter,
            FORM_D_START_QUARTER,
            FORM_D_END_QUARTER,
        ):
            continue
        if href.startswith("http://") or href.startswith("https://"):
            url = href
        else:
            url = "https://www.sec.gov" + href if href.startswith("/") else href
        found[quarter] = url

    if not found:
        raise RuntimeError(
            "No SEC Form D dataset quarters discovered for "
            f"{FORM_D_START_QUARTER} to {FORM_D_END_QUARTER or 'latest'}."
        )

    return [
        {"quarter": q, "dataset_url": found[q]}
        for q in sorted(found, key=quarter_key_to_tuple)
    ]


def dataset_cache_path(quarter: str) -> Path:
    return FORM_D_DATASET_CACHE / f"{quarter.lower()}_d.zip"


def download_dataset_zip(
    client: SECClient,
    quarter: str,
    url: str,
) -> Tuple[Path, str]:
    path = dataset_cache_path(quarter)
    ensure_parent(path)

    if path.exists() and path.stat().st_size > 0:
        return path, "CACHED_BRONZE"

    client._rate_limit()
    response = client.session.get(
        url,
        timeout=FORM_D_DATASET_REQUEST_TIMEOUT,
        headers={"Accept": "application/zip,application/octet-stream,*/*"},
    )
    client.last_request_time = time.time()
    response.raise_for_status()

    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_bytes(response.content)
    temp.replace(path)
    return path, "DOWNLOADED"


def identify_dataset_members(zf: zipfile.ZipFile) -> Dict[str, str]:
    members: Dict[str, str] = {}
    valid = FORM_D_DATASET_REQUIRED_TABLES | FORM_D_DATASET_OPTIONAL_TABLES

    for name in zf.namelist():
        if name.endswith("/"):
            continue
        stem = Path(name).stem.upper()
        if stem in valid:
            members[stem] = name

    missing = FORM_D_DATASET_REQUIRED_TABLES - set(members)
    if missing:
        raise ValueError(
            f"SEC Form D dataset missing required tables: {sorted(missing)}"
        )
    return members


def read_dataset_table(
    zf: zipfile.ZipFile,
    member: str,
) -> pd.DataFrame:
    with zf.open(member) as handle:
        data = handle.read()

    return pd.read_csv(
        io.BytesIO(data),
        sep="\t",
        dtype=str,
        encoding="utf-8-sig",
        keep_default_na=False,
        na_values=[""],
        low_memory=False,
    )


def normalize_dataset_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [
        re.sub(r"[^A-Z0-9_]+", "_", str(col).strip().upper()).strip("_")
        for col in out.columns
    ]
    return out


def first_existing_column(
    df: pd.DataFrame,
    candidates: Sequence[str],
) -> Optional[str]:
    cols = {str(c).upper(): c for c in df.columns}
    for candidate in candidates:
        if candidate.upper() in cols:
            return cols[candidate.upper()]
    return None


def resolve_offering_root(
    accession: str,
    previous_map: Dict[str, str],
) -> Tuple[str, str]:
    """Resolve an amendment chain to the original Form D accession."""
    current = clean_text(accession)
    if not current:
        return "", "MISSING_ACCESSION"

    visited = set()
    while current in previous_map and clean_text(previous_map.get(current)):
        if current in visited:
            return current, "CYCLE_DETECTED"
        visited.add(current)
        previous = clean_text(previous_map.get(current))
        if not previous:
            break
        if previous in visited:
            return previous, "CYCLE_DETECTED"
        current = previous
        if len(visited) > 100:
            return current, "CHAIN_TOO_LONG"

    return current, "RESOLVED"


def normalize_yes_no_flag(value: Any) -> Optional[bool]:
    return normalize_bool(value)


def map_dataset_to_events(
    submission: pd.DataFrame,
    issuers: pd.DataFrame,
    offering: pd.DataFrame,
    quarter: str,
    dataset_url: str,
) -> pd.DataFrame:
    """
    Join FORMDSUBMISSION + ISSUERS + OFFERING at one row per accession,
    restricted to the configured CIKs.
    """
    submission = normalize_dataset_columns(submission)
    issuers = normalize_dataset_columns(issuers)
    offering = normalize_dataset_columns(offering)

    required_sets = [
        ({"ACCESSIONNUMBER", "SUBMISSIONTYPE", "FILING_DATE"}, submission, "FORMDSUBMISSION"),
        ({"ACCESSIONNUMBER", "CIK", "ENTITYNAME"}, issuers, "ISSUERS"),
        ({"ACCESSIONNUMBER"}, offering, "OFFERING"),
    ]
    for required, frame, label in required_sets:
        missing = required - set(frame.columns)
        if missing:
            raise ValueError(f"{label} missing required columns: {sorted(missing)}")

    for frame in (submission, issuers, offering):
        frame["ACCESSIONNUMBER"] = frame["ACCESSIONNUMBER"].map(clean_text)

    issuers["CIK_NORMALIZED"] = issuers["CIK"].map(normalize_cik)
    issuers["ENTITYNAME_CLEAN"] = issuers["ENTITYNAME"].map(clean_text)

    primary_col = first_existing_column(issuers, ["IS_PRIMARYISSUER_FLAG"])
    issuers["_PRIMARY"] = (
        issuers[primary_col].map(normalize_yes_no_flag).fillna(False)
        if primary_col
        else False
    )
    issuers["_SEQ"] = pd.to_numeric(
        issuers.get("ISSUER_SEQ_KEY", pd.Series(index=issuers.index)),
        errors="coerce",
    )

    issuers = issuers.sort_values(
        ["ACCESSIONNUMBER", "_PRIMARY", "_SEQ"],
        ascending=[True, False, True],
        na_position="last",
    )
    issuer_one = (
        issuers.drop_duplicates("ACCESSIONNUMBER", keep="first")
        [["ACCESSIONNUMBER", "CIK_NORMALIZED", "ENTITYNAME_CLEAN"]]
        .rename(columns={
            "CIK_NORMALIZED": "ISSUER_CIK",
            "ENTITYNAME_CLEAN": "ISSUER_NAME",
        })
    )

    merged = submission.merge(
        issuer_one,
        on="ACCESSIONNUMBER",
        how="inner",
        validate="one_to_one",
    )

    configured = {normalize_cik(cik) for cik in COMPANIES}
    merged = merged[merged["ISSUER_CIK"].isin(configured)].copy()
    if merged.empty:
        return pd.DataFrame()

    merged = merged.merge(
        offering,
        on="ACCESSIONNUMBER",
        how="left",
        suffixes=("", "_OFFERING"),
        validate="one_to_one",
    )

    previous_col = first_existing_column(
        merged,
        ["PREVIOUSACCESSIONNUMBER"],
    )
    previous_map = (
        dict(zip(
            merged["ACCESSIONNUMBER"].map(clean_text),
            merged[previous_col].map(clean_text),
        ))
        if previous_col
        else {}
    )

    roots = []
    statuses = []
    for accession in merged["ACCESSIONNUMBER"]:
        root, status = resolve_offering_root(
            clean_text(accession) or "",
            previous_map,
        )
        roots.append(root)
        statuses.append(status)

    merged["offering_id"] = roots
    merged["offering_id_resolution_status"] = statuses

    def col_or_none(name: str) -> pd.Series:
        col = first_existing_column(merged, [name])
        if col is None:
            return pd.Series([None] * len(merged), index=merged.index, dtype="object")
        return merged[col]

    def numeric_series(name: str) -> pd.Series:
        return pd.to_numeric(col_or_none(name), errors="coerce")

    def date_series(name: str) -> pd.Series:
        return pd.to_datetime(col_or_none(name), errors="coerce")

    out = pd.DataFrame(index=merged.index)
    out["funding_event_id"] = [
        build_event_id(cik, offering_id)
        for cik, offering_id in zip(merged["ISSUER_CIK"], merged["offering_id"])
    ]
    out["offering_id"] = merged["offering_id"]
    out["cik"] = merged["ISSUER_CIK"]
    out["issuer_name_source"] = merged["ISSUER_NAME"]
    out["issuer_cik_source"] = merged["ISSUER_CIK"]
    out["filing_type"] = merged["SUBMISSIONTYPE"].map(clean_text)
    out["filing_date"] = date_series("FILING_DATE")
    out["first_sale_date"] = date_series("SALE_DATE")
    out["accession_number"] = merged["ACCESSIONNUMBER"]
    out["file_number"] = col_or_none("FILE_NUM").map(clean_text)

    out["source_url"] = [
        build_filing_url(cik, accession, "primary_doc.xml")
        for cik, accession in zip(merged["ISSUER_CIK"], merged["ACCESSIONNUMBER"])
    ]

    out["total_offering_amount_usd"] = numeric_series("TOTALOFFERINGAMOUNT")
    out["total_amount_sold_usd"] = numeric_series("TOTALAMOUNTSOLD")
    out["total_remaining_usd"] = numeric_series("TOTALREMAINING")
    out["investor_count"] = numeric_series("TOTALNUMBERALREADYINVESTED")
    out["non_accredited_investor_count"] = numeric_series("NUMBERNONACCREDITEDINVESTORS")
    out["minimum_investment_usd"] = numeric_series("MINIMUMINVESTMENTACCEPTED")
    out["sales_commissions_usd"] = numeric_series("SALESCOMM_DOLLARAMOUNT")
    out["finders_fees_usd"] = numeric_series("FINDERSFEE_DOLLARAMOUNT")
    out["gross_proceeds_used_usd"] = numeric_series("GROSSPROCEEDSUSED_DOLLARAMOUNT")

    for target, source in [
        ("duration_more_than_one_year", "MORETHANONEYEAR"),
        ("first_sale_yet_to_occur", "YETTOOCCUR"),
        ("has_non_accredited_investors", "HASNONACCREDITEDINVESTORS"),
        ("is_amendment", "ISAMENDMENT"),
    ]:
        out[target] = col_or_none(source).map(normalize_yes_no_flag)

    text_fields = [
        ("industry_group_type", "INDUSTRYGROUPTYPE"),
        ("investment_fund_type", "INVESTMENTFUNDTYPE"),
        ("revenue_range", "REVENUERANGE"),
        ("aggregate_net_asset_value_range", "AGGREGATENETASSETVALUERANGE"),
        ("federal_exemptions", "FEDERALEXEMPTIONS_ITEMS_LIST"),
        ("business_combination_clarification", "BUSCOMBCLARIFICATIONOFRESP"),
        ("sales_amount_clarification", "SALESAMTCLARIFICATIONOFRESP"),
        ("finder_fee_clarification", "FINDERFEECLARIFICATIONOFRESP"),
        ("gross_proceeds_clarification", "GROSSPROCEEDSUSED_CLAROFRESP"),
        ("security_other_description", "DESCRIPTIONOFOTHERTYPE"),
    ]
    for target, source in text_fields:
        out[target] = col_or_none(source).map(clean_text)

    out["business_combination_transaction"] = col_or_none(
        "ISBUSINESSCOMBINATIONTRANS"
    ).map(normalize_yes_no_flag)

    for target, source in [
        ("security_equity", "ISEQUITYTYPE"),
        ("security_debt", "ISDEBTTYPE"),
        ("security_option_or_warrant", "ISOPTIONTOACQUIRETYPE"),
        ("security_to_be_acquired", "ISSECURITYTOBEACQUIREDTYPE"),
        ("security_pooled_fund", "ISPOOLEDINVESTMENTFUNDTYPE"),
        ("security_tenant_in_common", "ISTENANTINCOMMONTYPE"),
        ("security_mineral_property", "ISMINERALPROPERTYTYPE"),
        ("security_other", "ISOTHERTYPE"),
        ("sales_commission_is_estimate", "SALESCOMM_ISESTIMATE"),
        ("finders_fee_is_estimate", "FINDERSFEE_ISESTIMATE"),
        ("gross_proceeds_used_is_estimate", "GROSSPROCEEDSUSED_ISESTIMATE"),
    ]:
        out[target] = col_or_none(source).map(normalize_yes_no_flag)

    out["previous_accession_number"] = col_or_none(
        "PREVIOUSACCESSIONNUMBER"
    ).map(clean_text)
    out["capital_amount_usd"] = out["total_amount_sold_usd"]
    out["capital_measure"] = "TOTAL_AMOUNT_SOLD"
    out["capital_source_scope"] = "SEC_EXEMPT_OFFERING"
    out["funding_stage"] = "UNKNOWN_SEC_ONLY"
    out["funding_stage_source"] = "NOT_AVAILABLE_IN_FORM_D"
    out["round_label"] = None
    out["round_label_source"] = "NOT_AVAILABLE_IN_FORM_D"
    out["capital_data_quality"] = out.apply(
        classify_event_data_quality,
        axis=1,
    )
    out["event_status"] = "AS_FILED"
    out["dataset_quarter"] = quarter
    out["dataset_url"] = dataset_url
    out["source_system"] = SOURCE_SYSTEM
    out["source_form"] = out["filing_type"]
    out["source_retrieved_at_utc"] = utc_now_iso()
    out["parser_recovery_used"] = False
    out["extraction_status"] = "STRUCTURED_DATASET"
    out["extraction_method_summary"] = "SEC_FORM_D_DATASET"
    out["offering_id_resolution_status"] = merged[
        "offering_id_resolution_status"
    ].values

    return out.reset_index(drop=True)


def ingest_form_d_dataset(
    client: SECClient,
    companies: Sequence[str],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Download and filter the SEC's quarterly Form D dataset universe."""
    quarters = discover_form_d_dataset_quarters(client)
    event_frames: List[pd.DataFrame] = []
    related_frames: List[pd.DataFrame] = []
    manifest_rows: List[Dict[str, Any]] = []

    for item in quarters:
        quarter = item["quarter"]
        url = item["dataset_url"]
        manifest = {
            "dataset_quarter": quarter,
            "dataset_url": url,
            "download_status": "PENDING",
            "parse_status": "NOT_ATTEMPTED",
            "discovery_status": "DISCOVERED",
            "downloaded_at_utc": None,
            "bronze_path": None,
            "submission_rows": 0,
            "target_event_rows": 0,
            "error_type": None,
            "error_message": None,
        }

        try:
            zip_path, download_status = download_dataset_zip(client, quarter, url)
            manifest["download_status"] = download_status
            manifest["downloaded_at_utc"] = utc_now_iso()
            manifest["bronze_path"] = str(zip_path)

            with zipfile.ZipFile(zip_path, "r") as zf:
                members = identify_dataset_members(zf)
                submission = read_dataset_table(zf, members["FORMDSUBMISSION"])
                issuers = read_dataset_table(zf, members["ISSUERS"])
                offering = read_dataset_table(zf, members["OFFERING"])

                manifest["submission_rows"] = len(submission)
                events = map_dataset_to_events(
                    submission, issuers, offering, quarter, url
                )
                manifest["target_event_rows"] = len(events)

                if not events.empty:
                    event_frames.append(events)

                if "RELATEDPERSONS" in members and not events.empty:
                    related = normalize_dataset_columns(
                        read_dataset_table(zf, members["RELATEDPERSONS"])
                    )
                    if "ACCESSIONNUMBER" in related.columns:
                        target_accessions = set(
                            events["accession_number"].dropna().astype(str)
                        )
                        related = related[
                            related["ACCESSIONNUMBER"].map(clean_text).isin(
                                target_accessions
                            )
                        ].copy()
                        if not related.empty:
                            related["dataset_quarter"] = quarter
                            related["dataset_url"] = url
                            related_frames.append(related)

                manifest["parse_status"] = "PARSED"

        except Exception as exc:
            manifest["download_status"] = (
                manifest["download_status"]
                if manifest["download_status"] != "PENDING"
                else "FAILED"
            )
            manifest["parse_status"] = (
                "NOT_ATTEMPTED"
                if manifest["download_status"] == "FAILED"
                else "FAILED"
            )
            manifest["error_type"] = (
                "DOWNLOAD"
                if manifest["download_status"] == "FAILED"
                else "PARSE"
            )
            manifest["error_message"] = str(exc)[:4000]
            logger.warning(
                "SEC Form D dataset failure | quarter=%s | %s",
                quarter,
                exc,
            )

        manifest_rows.append(manifest)

    events = (
        pd.concat(event_frames, ignore_index=True)
        if event_frames
        else empty_event_frame()
    )
    related = (
        pd.concat(related_frames, ignore_index=True)
        if related_frames
        else pd.DataFrame()
    )
    discovery = pd.DataFrame(manifest_rows)

    if events.empty:
        history = empty_event_frame()
    else:
        events["filing_date"] = pd.to_datetime(events["filing_date"], errors="coerce")
        events = events.sort_values(
            ["accession_number", "dataset_quarter"]
        )
        history = events.copy()
        events = (
            events.drop_duplicates("accession_number", keep="last")
            .reset_index(drop=True)
        )

    return events, history, discovery, related


# =============================================================================
# FORM D XML PARSING
# =============================================================================

def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    if ":" in tag:
        return tag.rsplit(":", 1)[1]
    return tag


def iter_elements(root: ET.Element) -> Iterable[ET.Element]:
    yield root
    yield from root.iter()


def find_first_text(
    root: ET.Element,
    names: Sequence[str],
) -> Optional[str]:
    wanted = {name.lower() for name in names}

    for element in iter_elements(root):
        if local_name(element.tag).lower() not in wanted:
            continue

        text = clean_text(" ".join(element.itertext()))
        if text:
            return text

    return None


def find_all_text(
    root: ET.Element,
    names: Sequence[str],
) -> List[str]:
    wanted = {name.lower() for name in names}
    values: List[str] = []

    for element in iter_elements(root):
        if local_name(element.tag).lower() not in wanted:
            continue

        text = clean_text(" ".join(element.itertext()))
        if text:
            values.append(text)

    return values


def find_all_leaf_text(
    root: ET.Element,
    names: Sequence[str],
) -> List[str]:
    wanted = {name.lower() for name in names}
    values: List[str] = []

    for element in iter_elements(root):
        if local_name(element.tag).lower() not in wanted:
            continue

        if list(element):
            continue

        text = clean_text(element.text)
        if text:
            values.append(text)

    return values


def first_nonempty(values: Sequence[Optional[str]]) -> Optional[str]:
    for value in values:
        cleaned = clean_text(value)
        if cleaned:
            return cleaned
    return None


def normalize_list(values: Sequence[str]) -> List[str]:
    result = []
    seen = set()

    for value in values:
        value = clean_text(value)
        if not value:
            continue

        key = value.lower()
        if key in seen:
            continue

        seen.add(key)
        result.append(value)

    return result



def extract_field(
    root: ET.Element,
    names: Sequence[str],
    parser=None,
) -> Tuple[Any, Optional[str], str]:
    """
    Extract the first non-empty candidate and return:
        value, matched_tag, extraction_status

    extraction_status:
        FOUND      - candidate tag/value found and parsed
        MISSING    - no candidate value found
        INVALID    - candidate existed but could not be parsed
    """
    wanted = {name.lower() for name in names}

    for element in iter_elements(root):
        if local_name(element.tag).lower() not in wanted:
            continue

        raw = clean_text(" ".join(element.itertext()))
        if not raw:
            continue

        if parser is None:
            return raw, local_name(element.tag), "FOUND"

        value = parser(raw)
        if value is not None:
            return value, local_name(element.tag), "FOUND"

        return None, local_name(element.tag), "INVALID"

    return None, None, "MISSING"


def extraction_audit_status(statuses: Sequence[str]) -> str:
    statuses = [str(s) for s in statuses if s]
    if any(s == "INVALID" for s in statuses):
        return "INVALID_FIELD"
    if any(s == "FOUND" for s in statuses):
        return "PARTIAL_OR_COMPLETE"
    return "NO_TARGET_FIELDS_FOUND"


def parse_xml_field_value(
    root: ET.Element,
    names: Sequence[str],
    parser=None,
) -> Tuple[Any, Optional[str], str]:
    return extract_field(root, names, parser=parser)


def normalized_label(value: Any) -> str:
    """Normalize a human-readable XML/HTML label for matching."""
    value = clean_text(value) or ""
    value = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return re.sub(r"\s+", " ", value).strip()



def extract_html_table_value(
    raw_html: str,
    labels: Sequence[str],
    parser=None,
) -> Tuple[Any, Optional[str], str]:
    """
    Extract a value from legacy SEC Form D HTML table structure.

    SEC legacy Form D documents may be HTML saved with an .xml extension.
    For those documents the reliable relationship is the label cell and its
    value cell within the same table row, or the immediately following row.
    """
    if not raw_html or LET is None:
        return None, None, "MISSING"

    try:
        document = LET.HTML(raw_html)
    except Exception:
        return None, None, "MISSING"

    targets = {
        normalized_label(label): label
        for label in labels
        if normalized_label(label)
    }

    for node in document.xpath("//th|//td"):
        node_text = clean_text(" ".join(node.itertext()))
        if not node_text:
            continue

        matched_label = targets.get(normalized_label(node_text))
        if matched_label is None:
            continue

        row = node.getparent()
        if row is None:
            continue

        cells = row.xpath("./th|./td")
        try:
            index = cells.index(node)
        except ValueError:
            index = -1

        candidates = cells[index + 1:] if index >= 0 else []

        # CIK-style rows may place the value immediately before the label.
        # Keep those cells as a secondary candidate set, after forward cells.
        if index > 0:
            candidates = candidates + list(reversed(cells[:index]))

        form_data = [
            cell for cell in candidates
            if "FormData" in (cell.get("class") or "").split()
        ]
        candidates = form_data or candidates

        # Some SEC fields use a label-only row followed by a value row.
        if not candidates:
            next_row = row.getnext()
            if next_row is not None and str(next_row.tag).lower() == "tr":
                next_cells = next_row.xpath("./th|./td")
                candidates = [
                    cell for cell in next_cells
                    if "FormData" in (cell.get("class") or "").split()
                ] or next_cells

        for candidate in candidates:
            raw_value = clean_text(" ".join(candidate.itertext()))
            if not raw_value:
                continue

            if parser is None:
                value = raw_value
            else:
                try:
                    value = parser(raw_value)
                except (TypeError, ValueError, OverflowError):
                    value = None

            if value is not None:
                if isinstance(value, str) and not value.strip():
                    continue
                return value, matched_label, "FOUND"

    return None, None, "MISSING"


def extract_labeled_value_from_text(
    text: str,
    labels: Sequence[str],
    parser,
) -> Tuple[Any, Optional[str], str]:
    """
    Conservative fallback for recovered/legacy Form D documents.

    Looks for an exact normalized label followed by a colon or whitespace,
    then parses the nearby value. This is deliberately bounded to a short
    window and never interprets arbitrary numbers elsewhere in the document.
    """
    if not text:
        return None, None, "MISSING"

    compact = re.sub(r"[ \t]+", " ", text)
    for label in labels:
        label_norm = normalized_label(label)
        if not label_norm:
            continue

        pattern = re.compile(
            rf"(?i)(?:^|[\n\r])\s*{re.escape(label_norm).replace(r'\ ', r'\s+')}"
            rf"\s*(?::|-)?\s*([^\n\r]{{1,160}})"
        )

        # Also allow the label to occur after HTML/XML text has been flattened.
        pattern_inline = re.compile(
            rf"(?i)\b{re.escape(label_norm).replace(r'\ ', r'\s+')}"
            rf"\s*(?::|-)?\s*([A-Za-z0-9$€£,./()\- ]{{1,160}})"
        )

        for candidate_pattern in (pattern, pattern_inline):
            match = candidate_pattern.search(compact)
            if not match:
                continue

            raw = clean_text(match.group(1))
            if not raw:
                continue

            # Avoid consuming the next known label when labels are adjacent.
            raw = re.split(
                r"(?i)\b(?:date of first sale|total offering amount|"
                r"total amount sold|total remaining|total number already invested|"
                r"minimum investment accepted)\b",
                raw,
                maxsplit=1,
            )[0].strip(" :;-")

            if parser is None:
                value = raw
            else:
                try:
                    value = parser(raw)
                except (TypeError, ValueError, OverflowError):
                    value = None

            if value is not None:
                return value, label, "FOUND"

    return None, None, "MISSING"


def extract_field_with_text_fallback(
    root: ET.Element,
    full_text: str,
    names: Sequence[str],
    parser=None,
    text_labels: Optional[Sequence[str]] = None,
    raw_document: Optional[str] = None,
) -> Tuple[Any, Optional[str], str, str]:
    """
    Return:
      value, source_label, extraction_status, extraction_method

    XML is authoritative. Text fallback is only used when XML extraction
    cannot find/parse the field.
    """
    value, tag, status = extract_field(root, names, parser=parser)
    if status == "FOUND":
        return value, tag, status, "XML_TAG"

    labels = text_labels or names

    if raw_document:
        value, label, html_status = extract_html_table_value(
            raw_document,
            labels,
            parser,
        )
        if html_status == "FOUND":
            return value, label, html_status, "HTML_TABLE"

    value, label, text_status = extract_labeled_value_from_text(
        full_text,
        labels,
        parser,
    )
    if text_status == "FOUND":
        return value, label, text_status, "LABEL_CONTEXT"

    return None, tag or label, status if status != "MISSING" else text_status, "NONE"


def flatten_document_text(root: ET.Element, raw_xml: str) -> str:
    """
    Build a conservative text representation from the recovered tree plus
    the raw document. The recovered tree is preferred because it removes
    markup noise; raw text is retained as a fallback for malformed filings.
    """
    try:
        tree_text = clean_text(" ".join(root.itertext()))
    except Exception:
        tree_text = ""

    raw_text = clean_text(
        re.sub(r"<[^>]+>", " ", raw_xml)
    )

    if tree_text and raw_text:
        return f"{tree_text}\n{raw_text}"
    return tree_text or raw_text

def parse_form_d_xml(
    xml_text: str,
    submission: Dict[str, Any],
    source_url: str,
) -> Dict[str, Any]:
    parser_recovery_used = False

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as strict_exc:
        if LET is None:
            raise ValueError(
                "Strict XML parse failed and lxml is not installed: "
                f"{strict_exc}"
            ) from strict_exc

        parser = LET.XMLParser(
            recover=True,
            huge_tree=True,
            remove_comments=True,
        )
        try:
            root = LET.fromstring(
                xml_text.encode("utf-8", errors="replace"),
                parser=parser,
            )
            parser_recovery_used = True
            logger.warning(
                "Recovered malformed Form D XML using lxml: %s | %s",
                clean_text(submission.get("cik")),
                clean_text(submission.get("accession_number")),
            )
        except Exception as recovery_exc:
            raise ValueError(
                "Form D XML parse failed in strict and recovery modes: "
                f"strict={strict_exc}; recovery={recovery_exc}"
            ) from recovery_exc

    full_text = flatten_document_text(root, xml_text)

    cik = normalize_cik(submission["cik"])
    accession = clean_text(submission.get("accession_number"))
    file_number = clean_text(submission.get("file_number"))
    filing_date = parse_date(submission.get("filing_date"))
    filing_type = clean_text(submission.get("form")) or "D"

    issuer_name, issuer_tag, issuer_status, issuer_method = (
        extract_field_with_text_fallback(
            root,
            full_text,
            ["issuerName", "companyName", "filerName"],
            parser=None,
            text_labels=["Issuer Name", "Name of Issuer", "Company Name", "Filer Name"],
            raw_document=xml_text,
        )
    )

    issuer_cik_from_xml, issuer_cik_tag, issuer_cik_status, issuer_cik_method = (
        extract_field_with_text_fallback(
            root,
            full_text,
            ["issuerCik", "cik"],
            parser=normalize_cik,
            text_labels=["Issuer CIK", "CIK", "CIK (Filer ID Number)"],
            raw_document=xml_text,
        )
    )

    first_sale_date, first_sale_tag, first_sale_status, first_sale_method = (
        extract_field_with_text_fallback(
            root,
            full_text,
            ["dateOfFirstSale", "firstSaleDate"],
            parser=parse_date,
            text_labels=[
                "Date of First Sale",
                "First Sale Date",
            ],
            raw_document=xml_text,
        )
    )

    total_offering_amount, total_offering_tag, total_offering_status, total_offering_method = (
        extract_field_with_text_fallback(
            root,
            full_text,
            ["totalOfferingAmount", "totalOffering"],
            parser=parse_float,
            text_labels=[
                "Total Offering Amount",
                "Total Offering",
            ],
            raw_document=xml_text,
        )
    )

    total_amount_sold, amount_sold_tag, amount_sold_status, amount_sold_method = (
        extract_field_with_text_fallback(
            root,
            full_text,
            ["totalAmountSold", "amountSold"],
            parser=parse_float,
            text_labels=[
                "Total Amount Sold",
                "Amount Sold",
            ],
            raw_document=xml_text,
        )
    )

    total_remaining, remaining_tag, remaining_status, remaining_method = (
        extract_field_with_text_fallback(
            root,
            full_text,
            ["totalRemaining", "totalRemainingToBeSold"],
            parser=parse_float,
            text_labels=[
                "Total Remaining",
                "Total Remaining to be Sold",
            ],
            raw_document=xml_text,
        )
    )

    investor_count, investor_count_tag, investor_count_status, investor_count_method = (
        extract_field_with_text_fallback(
            root,
            full_text,
            [
                "totalNumberAlreadyInvested",
                "totalNumberOfInvestors",
                "numberOfInvestors",
            ],
            parser=lambda x: safe_int(parse_float(x)),
            text_labels=[
                "Total Number Already Invested",
                "Total Number of Investors",
                "Number of Investors",
                "Regardless of whether securities in the offering have been or may be sold to persons who do not qualify as accredited investors, enter the total number of investors who already have invested in the offering",
                "number of investors who already have invested in the offering",
            ],
            raw_document=xml_text,
        )
    )

    non_accredited_investors, non_accredited_tag, non_accredited_status, non_accredited_method = (
        extract_field_with_text_fallback(
            root,
            full_text,
            [
                "numberNonAccreditedInvestors",
                "numberOfNonAccreditedInvestors",
            ],
            parser=lambda x: safe_int(parse_float(x)),
            text_labels=[
                "Number of Non-Accredited Investors",
                "Number Non-Accredited Investors",
                "Select if securities in the offering have been or may be sold to persons who do not qualify as accredited investors, and enter the number of such non-accredited investors who already have invested in the offering",
            ],
            raw_document=xml_text,
        )
    )

    minimum_investment, minimum_investment_tag, minimum_investment_status, minimum_investment_method = (
        extract_field_with_text_fallback(
            root,
            full_text,
            ["minimumInvestmentAccepted", "minimumInvestment"],
            parser=parse_float,
            text_labels=[
                "Minimum Investment Accepted",
                "Minimum Investment",
            ],
            raw_document=xml_text,
        )
    )

    duration_more_than_one_year, duration_tag, duration_status, duration_method = (
        extract_field_with_text_fallback(
            root,
            full_text,
            ["moreThanOneYear", "moreThanOneYearOffering"],
            parser=normalize_bool,
            text_labels=[
                "More Than One Year",
                "More Than One Year Offering",
            ],
            raw_document=xml_text,
        )
    )

    security_values = normalize_list(
        find_all_leaf_text(
            root,
            ["type", "securityType", "securityTypeOffered"],
        )
    )
    security_values = [
        value
        for value in security_values
        if any(
            token in value.lower()
            for token in [
                "equity", "debt", "option", "warrant", "pooled",
                "mineral", "tenant", "other", "security",
            ]
        )
    ]

    exemption_values = normalize_list(
        find_all_leaf_text(
            root,
            ["value", "federalExemption", "exemption"],
        )
    )
    exemption_values = [
        value
        for value in exemption_values
        if any(
            token in value.lower()
            for token in [
                "rule", "section", "4(a)", "506", "504",
            ]
        )
    ]

    clarification = first_nonempty([
        find_first_text(root, ["clarification"]),
        find_first_text(root, ["clarificationOfResponse"]),
    ])

    business_combination = first_nonempty([
        find_first_text(root, ["businessCombinationTransaction"]),
        find_first_text(root, ["businessCombination"]),
    ])

    offering_duration = first_nonempty([
        find_first_text(root, ["moreThanOneYear"]),
        find_first_text(root, ["offeringDuration"]),
    ])

    offering_id = file_number or accession
    event_id = build_event_id(cik=cik, offering_id=offering_id)

    extraction_statuses = [
        first_sale_status,
        total_offering_status,
        amount_sold_status,
        remaining_status,
        investor_count_status,
    ]

    capital_amount_usd = total_amount_sold

    extraction_methods = {
        "first_sale_date": first_sale_method,
        "total_offering_amount_usd": total_offering_method,
        "total_amount_sold_usd": amount_sold_method,
        "total_remaining_usd": remaining_method,
        "investor_count": investor_count_method,
    }

    return {
        "funding_event_id": event_id,
        "offering_id": offering_id,
        "cik": cik,
        "issuer_name_source": issuer_name,
        "issuer_cik_source": issuer_cik_from_xml,
        "filing_type": filing_type,
        "filing_date": filing_date,
        "first_sale_date": first_sale_date,
        "accession_number": accession,
        "file_number": file_number,
        "primary_document": submission.get("primary_document"),
        "source_url": source_url,

        "total_offering_amount_usd": total_offering_amount,
        "total_amount_sold_usd": total_amount_sold,
        "total_remaining_usd": total_remaining,
        "investor_count": investor_count,
        "non_accredited_investor_count": non_accredited_investors,
        "minimum_investment_usd": minimum_investment,
        "duration_more_than_one_year": duration_more_than_one_year,

        "security_types": "|".join(security_values) if security_values else None,
        "exemptions": "|".join(exemption_values) if exemption_values else None,
        "business_combination_transaction": business_combination,
        "offering_duration_source": offering_duration,
        "clarification_text": clarification,

        "capital_amount_usd": capital_amount_usd,

        "extraction_status": extraction_audit_status(extraction_statuses),
        "issuer_name_extraction_status": issuer_status,
        "issuer_name_matched_tag": issuer_tag,
        "issuer_name_extraction_method": issuer_method,
        "issuer_cik_extraction_status": issuer_cik_status,
        "issuer_cik_matched_tag": issuer_cik_tag,
        "issuer_cik_extraction_method": issuer_cik_method,

        "first_sale_date_extraction_status": first_sale_status,
        "first_sale_date_matched_tag": first_sale_tag,
        "first_sale_date_extraction_method": first_sale_method,

        "total_offering_amount_extraction_status": total_offering_status,
        "total_offering_amount_matched_tag": total_offering_tag,
        "total_offering_amount_extraction_method": total_offering_method,

        "total_amount_sold_extraction_status": amount_sold_status,
        "total_amount_sold_matched_tag": amount_sold_tag,
        "total_amount_sold_extraction_method": amount_sold_method,

        "total_remaining_extraction_status": remaining_status,
        "total_remaining_matched_tag": remaining_tag,
        "total_remaining_extraction_method": remaining_method,

        "investor_count_extraction_status": investor_count_status,
        "investor_count_matched_tag": investor_count_tag,
        "investor_count_extraction_method": investor_count_method,

        "non_accredited_investor_extraction_status": non_accredited_status,
        "non_accredited_investor_matched_tag": non_accredited_tag,
        "non_accredited_investor_extraction_method": non_accredited_method,

        "minimum_investment_extraction_status": minimum_investment_status,
        "minimum_investment_matched_tag": minimum_investment_tag,
        "minimum_investment_extraction_method": minimum_investment_method,

        "duration_extraction_status": duration_status,
        "duration_matched_tag": duration_tag,
        "duration_extraction_method": duration_method,

        "source_system": SOURCE_SYSTEM,
        "source_form": filing_type,
        "source_submission_hash": json_hash(submission),
        "source_retrieved_at_utc": utc_now_iso(),
        "parser_recovery_used": parser_recovery_used,
        "extraction_method_summary": json.dumps(
            extraction_methods,
            sort_keys=True,
        ),
    }


def build_event_id(cik: str, offering_id: str) -> str:
    raw = f"{normalize_cik(cik)}|{clean_text(offering_id) or ''}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


# =============================================================================
# BRONZE
# =============================================================================

def save_bronze_filing(
    cik: str,
    accession: str,
    xml_text: str,
) -> Path:
    path = (
        BRONZE_ROOT
        / f"cik={normalize_cik(cik)}"
        / f"{accession}.xml"
    )
    ensure_parent(path)

    if not path.exists():
        path.write_text(xml_text, encoding="utf-8")

    return path


# =============================================================================
# FORM D INGESTION
# =============================================================================

def ingest_form_d(
    client: SECClient,
    companies: Sequence[str],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Discover, download and parse Form D/D-A filings.

    Production reliability contract:
      * Every discovered filing is retained in ``discovery_rows``.
      * A transient SEC download failure never becomes ``zero filings``.
      * Existing valid Bronze XML is reused rather than redownloaded.
      * Successful parsing creates the canonical event candidate.
      * Failed downloads/parses remain observable for retry/DQ.
    """
    event_rows: List[Dict[str, Any]] = []
    filing_rows: List[Dict[str, Any]] = []
    discovery_rows: List[Dict[str, Any]] = []

    for cik in companies:
        cik = normalize_cik(cik)
        logger.info("Discovering Form D filings for CIK %s", cik)

        try:
            submissions = get_submission_rows(client, cik)
        except Exception as exc:
            logger.error(
                "Submission discovery failed for %s: %s",
                cik,
                exc,
            )
            discovery_rows.append({
                "cik": cik,
                "accession_number": None,
                "file_number": None,
                "filing_type": None,
                "filing_date": None,
                "primary_document": None,
                "source_url": None,
                "discovery_status": "FAILED",
                "download_status": "NOT_ATTEMPTED",
                "parse_status": "NOT_ATTEMPTED",
                "error_type": "SUBMISSION_DISCOVERY",
                "error_message": str(exc)[:2000],
                "retrieved_at_utc": utc_now_iso(),
            })
            continue

        logger.info(
            "Found %d Form D/D-A submissions for %s",
            len(submissions),
            cik,
        )

        if not submissions:
            continue

        for submission in submissions:
            accession = clean_text(submission.get("accession_number"))
            primary_document = clean_text(submission.get("primary_document"))
            file_number = clean_text(submission.get("file_number"))
            filing_type = clean_text(submission.get("form"))
            filing_date = parse_date(submission.get("filing_date"))

            if not accession or not primary_document:
                discovery_rows.append({
                    "cik": cik,
                    "accession_number": accession,
                    "file_number": file_number,
                    "filing_type": filing_type,
                    "filing_date": filing_date,
                    "primary_document": primary_document,
                    "source_url": None,
                    "discovery_status": "INCOMPLETE",
                    "download_status": "NOT_ATTEMPTED",
                    "parse_status": "NOT_ATTEMPTED",
                    "error_type": "MISSING_ARCHIVE_METADATA",
                    "error_message": "Accession number or primary document missing.",
                    "retrieved_at_utc": utc_now_iso(),
                })
                logger.warning(
                    "Skipping incomplete submission for %s: accession/document missing",
                    cik,
                )
                continue

            url = build_filing_url(cik, accession, primary_document)
            bronze_path = (
                BRONZE_ROOT
                / f"cik={normalize_cik(cik)}"
                / f"{accession}.xml"
            )

            discovery = {
                "cik": cik,
                "accession_number": accession,
                "file_number": file_number,
                "filing_type": filing_type,
                "filing_date": filing_date,
                "primary_document": primary_document,
                "source_url": url,
                "discovery_status": "DISCOVERED",
                "download_status": "PENDING",
                "parse_status": "NOT_ATTEMPTED",
                "error_type": None,
                "error_message": None,
                "bronze_path": str(bronze_path),
                "retrieved_at_utc": None,
            }

            try:
                if bronze_path.exists() and bronze_path.stat().st_size > 0:
                    xml_text = bronze_path.read_text(encoding="utf-8", errors="replace")
                    download_status = "CACHED_BRONZE"
                else:
                    xml_text = client.get_text(url)
                    save_bronze_filing(cik, accession, xml_text)
                    download_status = "DOWNLOADED"

                discovery["download_status"] = download_status
                discovery["retrieved_at_utc"] = utc_now_iso()

                event = parse_form_d_xml(
                    xml_text,
                    submission,
                    url,
                )
                event_rows.append(event)

                discovery["parse_status"] = "PARSED"
                filing_rows.append({
                    "cik": cik,
                    "accession_number": accession,
                    "file_number": file_number,
                    "filing_type": filing_type,
                    "filing_date": filing_date,
                    "primary_document": primary_document,
                    "source_url": url,
                    "retrieved_at_utc": discovery["retrieved_at_utc"],
                    "download_status": download_status,
                    "parse_status": "PARSED",
                    "bronze_path": str(bronze_path),
                })

            except Exception as exc:
                discovery["download_status"] = (
                    discovery["download_status"]
                    if discovery["download_status"] not in {"PENDING"}
                    else "FAILED"
                )
                discovery["parse_status"] = (
                    "NOT_ATTEMPTED"
                    if discovery["download_status"] == "FAILED"
                    else "FAILED"
                )
                discovery["error_type"] = (
                    "DOWNLOAD"
                    if discovery["download_status"] == "FAILED"
                    else "PARSE"
                )
                discovery["error_message"] = str(exc)[:2000]
                discovery["retrieved_at_utc"] = utc_now_iso()
                logger.warning(
                    "Failed Form D ingestion: %s | %s | %s | status=%s",
                    cik,
                    accession,
                    exc,
                    discovery["download_status"],
                )

            discovery_rows.append(discovery)

    events = pd.DataFrame(event_rows)
    filings = pd.DataFrame(filing_rows)
    discovery = pd.DataFrame(discovery_rows)

    return events, filings, discovery


# =============================================================================
# EVENT CANONICALIZATION
# =============================================================================

def canonicalize_form_d_events(
    events: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Canonicalize SEC Form D/D-A filings at the offering grain.

    Identity contract
    -----------------
    * An original Form D (D) is its own offering: offering_id = accession_number.
    * A Form D/A belongs to the offering identified by its previous-accession
      chain. The chain is resolved across the COMPLETE input history, including
      older quarters in the configured dataset window.
    * funding_event_id is derived ONLY from (CIK, canonical offering_id).
    * Separate original offerings for the same company MUST remain separate
      funding events, even when they have similar dates, amounts, or file numbers.
    * Every filing remains in historical_events. Only the latest filing for each
      canonical offering becomes the current event state.

    This prevents the prior v3.0.0 failure where two independent Microsoft
    offerings were grouped into one funding_event_id because offering identity
    was not enforced strongly enough.
    """
    if events.empty:
        return empty_event_frame(), empty_event_frame()

    df = events.copy()

    for col in ["filing_date", "first_sale_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    df["cik"] = df["cik"].map(normalize_cik)
    df["accession_number"] = df["accession_number"].map(clean_text)
    df["previous_accession_number"] = df["previous_accession_number"].map(clean_text)
    df["filing_type"] = df["filing_type"].map(clean_text)

    # -------------------------------------------------------------------------
    # Build one global amendment graph from ALL rows in the configured history.
    # This is deliberately done here rather than inside each quarterly dataset,
    # because a D/A can reference an original filing from an older quarter.
    # -------------------------------------------------------------------------
    previous_map: Dict[str, str] = {}
    duplicate_accessions = []

    for _, row in df.sort_values(
        ["accession_number", "filing_date"],
        na_position="last",
    ).iterrows():
        accession = clean_text(row.get("accession_number"))
        previous = clean_text(row.get("previous_accession_number"))
        if not accession:
            continue
        if accession in previous_map and previous_map[accession] != previous:
            duplicate_accessions.append(accession)
        elif accession not in previous_map:
            previous_map[accession] = previous or ""

    # -------------------------------------------------------------------------
    # Resolve offering identity independently for every filing.
    # -------------------------------------------------------------------------
    offering_ids: List[Optional[str]] = []
    resolution_statuses: List[str] = []

    for _, row in df.iterrows():
        accession = clean_text(row.get("accession_number"))
        filing_type = (clean_text(row.get("filing_type")) or "").upper()
        is_amendment = normalize_bool(row.get("is_amendment"))
        previous = clean_text(row.get("previous_accession_number"))

        amendment = filing_type == "D/A" or is_amendment is True

        if not accession:
            offering_ids.append(None)
            resolution_statuses.append("MISSING_ACCESSION")
            continue

        if not amendment:
            # Every original D is an independent offering unless it is explicitly
            # identified as an amendment by the source.
            offering_ids.append(accession)
            resolution_statuses.append("RESOLVED_ORIGINAL")
            continue

        if not previous:
            # Never invent an amendment parent. Retain the accession so the row is
            # still addressable, but make the unresolved state explicit and allow
            # validation to block canonical promotion if required.
            offering_ids.append(accession)
            resolution_statuses.append("UNRESOLVED_AMENDMENT_PARENT")
            continue

        root, status = resolve_offering_root(accession, previous_map)
        if not root:
            offering_ids.append(accession)
            resolution_statuses.append("UNRESOLVED_AMENDMENT_PARENT")
        elif status == "CYCLE_DETECTED":
            offering_ids.append(root)
            resolution_statuses.append("CYCLE_DETECTED")
        elif status == "CHAIN_TOO_LONG":
            offering_ids.append(root)
            resolution_statuses.append("CHAIN_TOO_LONG")
        else:
            # If the root accession is not present in the configured window, the
            # parent is still a verified SEC source reference. Keep it as the
            # offering identity and expose that the root filing is external to
            # this dataset window.
            root_present = root in set(df["accession_number"].dropna())
            offering_ids.append(root)
            resolution_statuses.append(
                "RESOLVED_ROOT" if root_present else "RESOLVED_ROOT_EXTERNAL"
            )

    df["offering_id"] = offering_ids
    df["offering_id_resolution_status"] = resolution_statuses

    # Canonical event identity MUST be derived from the canonical offering root,
    # never from CIK alone and never from a nullable fallback.
    df["funding_event_id"] = [
        build_event_id(cik, offering_id)
        if clean_text(cik) and clean_text(offering_id)
        else None
        for cik, offering_id in zip(df["cik"], df["offering_id"])
    ]

    # Keep every filing in history. Current state is latest filing within each
    # canonical offering, not latest filing within the company.
    df["filing_priority"] = df["filing_date"].fillna(
        pd.Timestamp("1900-01-01")
    )

    df = df.sort_values(
        ["cik", "offering_id", "filing_priority", "accession_number"],
        ascending=[True, True, False, False],
        na_position="last",
    )

    history = df.copy()

    # If the same accession appears twice because overlapping dataset sources
    # supplied it, retain only the latest source observation in history. This is
    # not an amendment operation; accession remains the immutable filing key.
    history = history.drop_duplicates(
        subset=["accession_number"],
        keep="first",
    ).reset_index(drop=True)

    current = (
        history.groupby("funding_event_id", as_index=False, sort=False)
        .head(1)
        .copy()
    )

    amendment_counts = (
        history.groupby("funding_event_id")
        .agg(
            amendment_count=(
                "filing_type",
                lambda s: int((s.astype("string").str.upper() == "D/A").sum()),
            ),
            filing_count=("accession_number", "nunique"),
            first_filing_date=("filing_date", "min"),
            latest_filing_date=("filing_date", "max"),
        )
        .reset_index()
    )

    current = current.drop(
        columns=["filing_priority"],
        errors="ignore",
    )

    current = current.merge(
        amendment_counts,
        on="funding_event_id",
        how="left",
        validate="one_to_one",
    )

    # Capital stage is deliberately unknown for SEC-only data.
    current["funding_stage"] = "UNKNOWN_SEC_ONLY"
    current["funding_stage_source"] = "NOT_AVAILABLE_IN_FORM_D"

    # Funding round label is also not invented.
    current["round_label"] = None
    current["round_label_source"] = "NOT_AVAILABLE_IN_FORM_D"

    # SEC Form D capital classification.
    current["capital_measure"] = "TOTAL_AMOUNT_SOLD"
    current["capital_amount_usd"] = current["total_amount_sold_usd"]

    current["capital_data_quality"] = current.apply(
        classify_event_data_quality,
        axis=1,
    )

    current["capital_source_scope"] = "SEC_EXEMPT_OFFERING"
    current["event_status"] = "CURRENT_LATEST_FILING"

    current = current.sort_values(
        ["cik", "first_sale_date", "filing_date", "funding_event_id"],
        na_position="last",
    ).reset_index(drop=True)

    history = history.drop(
        columns=["filing_priority"],
        errors="ignore",
    )

    return current, history

def classify_event_data_quality(row: pd.Series) -> str:
    amount = row.get("total_amount_sold_usd")
    first_sale = row.get("first_sale_date")

    if pd.notna(amount) and amount >= 0 and pd.notna(first_sale):
        return "COMPLETE"

    if pd.notna(amount):
        return "PARTIAL_MISSING_FIRST_SALE_DATE"

    if pd.notna(first_sale):
        return "PARTIAL_MISSING_AMOUNT"

    return "LIMITED"


def empty_event_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "funding_event_id",
            "offering_id",
            "cik",
            "issuer_name_source",
            "filing_type",
            "filing_date",
            "first_sale_date",
            "accession_number",
            "file_number",
            "source_url",
            "total_offering_amount_usd",
            "total_amount_sold_usd",
            "total_remaining_usd",
            "investor_count",
            "security_types",
            "exemptions",
            "capital_amount_usd",
            "capital_source_scope",
            "extraction_status",
            "parser_recovery_used",
            "offering_id_resolution_status",
            "extraction_method_summary",
            "first_sale_date_extraction_method",
            "total_offering_amount_extraction_method",
            "total_amount_sold_extraction_method",
            "total_remaining_extraction_method",
            "investor_count_extraction_method",
        ]
    )


# =============================================================================
# INVESTOR ENRICHMENT
# =============================================================================

def load_optional_investors() -> pd.DataFrame:
    if not INVESTOR_INPUT.exists():
        return pd.DataFrame()

    df = pd.read_parquet(INVESTOR_INPUT)

    required = {"cik", "investor_name"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Investor enrichment is missing required columns: {sorted(missing)}"
        )

    df = df.copy()
    df["cik"] = df["cik"].map(normalize_cik)
    df["investor_name"] = df["investor_name"].map(clean_text)

    if "funding_event_id" not in df.columns:
        df["funding_event_id"] = None

    if "offering_id" not in df.columns:
        df["offering_id"] = None

    if "investor_type" not in df.columns:
        df["investor_type"] = None

    if "investor_source" not in df.columns:
        df["investor_source"] = None

    if "investor_confidence" not in df.columns:
        df["investor_confidence"] = None

    df = df[df["cik"].ne("")]
    df = df[df["investor_name"].notna()]

    return df


def build_investor_metrics(
    events: pd.DataFrame,
    investors: pd.DataFrame,
) -> pd.DataFrame:
    """
    Resolve optional investor enrichment to the current funding-event grain.

    Resolution order:
      1. explicit funding_event_id
      2. cik + offering_id

    Blank IDs are never joined, avoiding null-key many-to-many matches.
    """
    if events.empty:
        return pd.DataFrame()

    base = events[
        [
            "funding_event_id",
            "cik",
            "offering_id",
        ]
    ].copy()

    if investors.empty:
        result = base.copy()
        result["investor_identity_count"] = 0
        result["investor_names"] = None
        result["investor_types"] = None
        result["investor_data_source"] = None
        return result

    inv = investors.copy()

    inv["funding_event_id"] = inv["funding_event_id"].map(clean_text)
    inv["offering_id"] = inv["offering_id"].map(clean_text)
    inv["cik"] = inv["cik"].map(normalize_cik)
    inv["investor_name"] = inv["investor_name"].map(clean_text)

    event_by_id = {
        row["funding_event_id"]: row["funding_event_id"]
        for _, row in base.iterrows()
        if clean_text(row["funding_event_id"])
    }

    event_by_offering = {
        (normalize_cik(row["cik"]), clean_text(row["offering_id"])): row["funding_event_id"]
        for _, row in base.iterrows()
        if normalize_cik(row["cik"]) and clean_text(row["offering_id"])
    }

    resolved_ids: List[Optional[str]] = []

    for _, row in inv.iterrows():
        event_id = None

        explicit_id = clean_text(row.get("funding_event_id"))
        if explicit_id and explicit_id in event_by_id:
            event_id = event_by_id[explicit_id]

        if event_id is None:
            key = (
                normalize_cik(row.get("cik")),
                clean_text(row.get("offering_id")),
            )
            if key[0] and key[1]:
                event_id = event_by_offering.get(key)

        resolved_ids.append(event_id)

    inv["resolved_funding_event_id"] = resolved_ids
    inv = inv[inv["resolved_funding_event_id"].notna()].copy()

    if inv.empty:
        result = base.copy()
        result["investor_identity_count"] = 0
        result["investor_names"] = None
        result["investor_types"] = None
        result["investor_data_source"] = None
        return result

    def join_unique(series: pd.Series) -> Optional[str]:
        values = sorted({
            str(value).strip()
            for value in series.dropna()
            if str(value).strip()
        })
        return "|".join(values) if values else None

    agg = (
        inv.groupby("resolved_funding_event_id", as_index=False)
        .agg(
            investor_identity_count=(
                "investor_name",
                lambda s: int(
                    s.dropna().astype(str).str.strip().replace("", pd.NA).dropna().nunique()
                ),
            ),
            investor_names=("investor_name", join_unique),
            investor_types=("investor_type", join_unique),
            investor_data_source=("investor_source", join_unique),
        )
        .rename(
            columns={
                "resolved_funding_event_id": "funding_event_id",
            }
        )
    )

    result = base.merge(
        agg,
        on="funding_event_id",
        how="left",
        validate="one_to_one",
    )

    result["investor_identity_count"] = (
        pd.to_numeric(
            result["investor_identity_count"],
            errors="coerce",
        )
        .fillna(0)
        .astype(int)
    )

    return result


# =============================================================================
# COMPANY FUNDING FEATURES
# =============================================================================

def build_company_funding_snapshot(
    events: pd.DataFrame,
    company_master: pd.DataFrame,
) -> pd.DataFrame:
    """Build one snapshot row for every configured/master CIK.

    No observed Form D event is not equivalent to no funding. Companies with
    zero observed events receive NO_DATA / UNKNOWN and remain visible.
    """
    master = company_master[["cik", "company_name"]].copy()
    master["cik"] = master["cik"].map(normalize_cik)
    master["company_name"] = master["company_name"].map(clean_text)

    if events.empty:
        snapshot = master.copy()
        for col in [
            "latest_funding_event_id", "latest_funding_date", "latest_filing_date",
            "latest_capital_amount_usd", "latest_offering_amount_usd",
            "latest_investor_count", "total_sec_capital_raised_usd",
            "sec_capital_raised_180d_usd", "sec_capital_raised_365d_usd",
            "first_funding_date", "last_funding_date",
            "funding_velocity_events_per_year", "median_days_between_funding_events",
            "funding_stage", "funding_stage_source", "funding_profile_as_of_date",
        ]:
            snapshot[col] = None
        snapshot["funding_event_count"] = 0
        snapshot["capital_trajectory"] = "INSUFFICIENT_HISTORY"
        snapshot["capital_data_scope"] = "SEC_EXEMPT_OFFERING_ONLY"
        snapshot["funding_data_quality"] = "NO_DATA"
        snapshot["funding_freshness_status"] = "UNKNOWN"
        snapshot["funding_age_days"] = None
        snapshot["snapshot_version"] = STEP17_VERSION
        snapshot["snapshot_generated_at_utc"] = utc_now_iso()
        return snapshot.sort_values("cik").reset_index(drop=True)

    df = events.copy()
    df["first_sale_date"] = pd.to_datetime(df["first_sale_date"], errors="coerce")
    df["capital_amount_usd"] = pd.to_numeric(df["capital_amount_usd"], errors="coerce")

    grouped = []
    for cik, group in df.groupby("cik"):
        group = group.sort_values(["first_sale_date", "filing_date"], na_position="last")
        amounts = group["capital_amount_usd"].dropna()
        first_date = group["first_sale_date"].min()
        last_date = group["first_sale_date"].max()
        total_capital = amounts.sum() if not amounts.empty else None

        now = pd.Timestamp.utcnow().tz_localize(None)
        recent_180 = group["first_sale_date"].notna() & (group["first_sale_date"] >= now - pd.Timedelta(days=180))
        recent_365 = group["first_sale_date"].notna() & (group["first_sale_date"] >= now - pd.Timedelta(days=365))
        capital_180 = group.loc[recent_180, "capital_amount_usd"].sum(min_count=1)
        capital_365 = group.loc[recent_365, "capital_amount_usd"].sum(min_count=1)

        event_count = int(len(group))
        velocity = None
        if pd.notna(first_date) and pd.notna(last_date) and event_count >= 2:
            days = (last_date - first_date).days
            if days > 0:
                velocity = event_count / (days / 365.25)

        gap_days = None
        if event_count >= 2:
            dates = group["first_sale_date"].dropna().sort_values()
            if len(dates) >= 2:
                gap_days = float(dates.diff().dt.days.dropna().median())

        latest = group.iloc[-1]
        grouped.append({
            "cik": cik,
            "latest_funding_event_id": latest.get("funding_event_id"),
            "latest_funding_date": latest.get("first_sale_date"),
            "latest_filing_date": latest.get("filing_date"),
            "latest_capital_amount_usd": latest.get("capital_amount_usd"),
            "latest_offering_amount_usd": latest.get("total_offering_amount_usd"),
            "latest_investor_count": latest.get("investor_count"),
            "funding_event_count": event_count,
            "total_sec_capital_raised_usd": total_capital,
            "sec_capital_raised_180d_usd": capital_180,
            "sec_capital_raised_365d_usd": capital_365,
            "first_funding_date": first_date,
            "last_funding_date": last_date,
            "funding_velocity_events_per_year": velocity,
            "median_days_between_funding_events": gap_days,
            "capital_trajectory": derive_capital_trajectory(group),
            "funding_stage": latest.get("funding_stage"),
            "funding_stage_source": latest.get("funding_stage_source"),
            "capital_data_scope": "SEC_EXEMPT_OFFERING_ONLY",
            "funding_data_quality": derive_company_funding_quality(group),
            "funding_profile_as_of_date": last_date,
        })

    observed = pd.DataFrame(grouped)
    snapshot = master.merge(observed, on="cik", how="left", validate="one_to_one")
    snapshot["funding_event_count"] = snapshot["funding_event_count"].fillna(0).astype(int)
    snapshot["capital_trajectory"] = snapshot["capital_trajectory"].fillna("INSUFFICIENT_HISTORY")
    snapshot["capital_data_scope"] = snapshot["capital_data_scope"].fillna("SEC_EXEMPT_OFFERING_ONLY")
    snapshot["funding_data_quality"] = snapshot["funding_data_quality"].fillna("NO_DATA")
    snapshot["funding_stage"] = snapshot["funding_stage"].fillna("UNKNOWN_SEC_ONLY")
    snapshot["funding_stage_source"] = snapshot["funding_stage_source"].fillna("NOT_AVAILABLE_IN_FORM_D")
    snapshot["funding_freshness_status"] = snapshot["funding_profile_as_of_date"].map(derive_freshness_status)
    snapshot["funding_age_days"] = snapshot["funding_profile_as_of_date"].map(days_since_date)
    snapshot["snapshot_version"] = STEP17_VERSION
    snapshot["snapshot_generated_at_utc"] = utc_now_iso()

    return snapshot.sort_values("cik").reset_index(drop=True)


def derive_company_funding_quality(group: pd.DataFrame) -> str:
    if group.empty:
        return "NO_DATA"

    amount_available = group["capital_amount_usd"].notna().any()
    date_available = group["first_sale_date"].notna().any()

    if amount_available and date_available:
        return "COMPLETE"

    if amount_available or date_available:
        return "PARTIAL"

    return "LIMITED"


def derive_freshness_status(value: Any) -> str:
    if value is None or pd.isna(value):
        return "UNKNOWN"

    days = (pd.Timestamp.utcnow().tz_localize(None) - pd.Timestamp(value).tz_localize(None)).days

    if days <= FRESHNESS_RECENT_DAYS:
        return "FRESH"

    if days <= FRESHNESS_STALE_DAYS:
        return "STALE"

    return "VERY_STALE"


def days_since_date(value: Any) -> Optional[int]:
    if value is None or pd.isna(value):
        return None

    return int(
        (
            pd.Timestamp.utcnow().tz_localize(None)
            - pd.Timestamp(value).tz_localize(None)
        ).days
    )


def derive_capital_trajectory(group: pd.DataFrame) -> str:
    x = group.sort_values(
        ["first_sale_date", "filing_date"],
        na_position="last",
    ).copy()

    x["capital_amount_usd"] = pd.to_numeric(
        x["capital_amount_usd"],
        errors="coerce",
    )

    x = x.dropna(
        subset=["first_sale_date", "capital_amount_usd"]
    )

    if len(x) < 2:
        return "INSUFFICIENT_HISTORY"

    recent = x.tail(min(3, len(x)))["capital_amount_usd"]
    older = x.head(min(3, len(x)))["capital_amount_usd"]

    if recent.empty or older.empty:
        return "INSUFFICIENT_HISTORY"

    recent_mean = recent.mean()
    older_mean = older.mean()

    if older_mean == 0:
        return "CAPITAL_ACTIVITY_PRESENT"

    change = (recent_mean - older_mean) / abs(older_mean)

    if change >= 0.25:
        return "ACCELERATING"

    if change <= -0.25:
        return "DECELERATING"

    return "STABLE"


# =============================================================================
# FUNDING SIGNALS
# =============================================================================

def build_funding_signals(
    events: pd.DataFrame,
    snapshot: pd.DataFrame,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    if snapshot.empty:
        return pd.DataFrame()

    for _, company in snapshot.iterrows():
        cik = company["cik"]

        event_count = safe_int(company.get("funding_event_count")) or 0
        total_capital = company.get("total_sec_capital_raised_usd")
        capital_180 = company.get("sec_capital_raised_180d_usd")
        capital_365 = company.get("sec_capital_raised_365d_usd")

        rows.append({
            "cik": cik,
            "signal_type": "RECENT_CAPITAL_ACTIVITY"
            if (capital_180 is not None and pd.notna(capital_180) and capital_180 > 0)
            else "NO_RECENT_CAPITAL_ACTIVITY",
            "signal_value": capital_180,
            "signal_unit": "USD",
            "signal_direction": "POSITIVE"
            if (capital_180 is not None and pd.notna(capital_180) and capital_180 > 0)
            else "NEUTRAL",
            "signal_strength": capital_signal_strength(capital_180),
            "evidence": "SEC Form D amount sold in trailing 180 days",
            "source_scope": "SEC_EXEMPT_OFFERING_ONLY",
        })

        rows.append({
            "cik": cik,
            "signal_type": "CAPITAL_ACCELERATION"
            if company.get("capital_trajectory") == "ACCELERATING"
            else (
                "CAPITAL_DECELERATION"
                if company.get("capital_trajectory") == "DECELERATING"
                else "CAPITAL_STABLE"
            ),
            "signal_value": None,
            "signal_unit": "CLASSIFICATION",
            "signal_direction": (
                "POSITIVE"
                if company.get("capital_trajectory") == "ACCELERATING"
                else (
                    "NEGATIVE"
                    if company.get("capital_trajectory") == "DECELERATING"
                    else "NEUTRAL"
                )
            ),
            "signal_strength": "MEDIUM",
            "evidence": "Comparison of historical SEC Form D capital activity",
            "source_scope": "SEC_EXEMPT_OFFERING_ONLY",
        })

        rows.append({
            "cik": cik,
            "signal_type": "CAPITAL_DATA_LIMITED"
            if company.get("funding_data_quality") != "COMPLETE"
            else "SEC_CAPITAL_COVERAGE",
            "signal_value": total_capital,
            "signal_unit": "USD",
            "signal_direction": "NEUTRAL",
            "signal_strength": (
                "LOW"
                if company.get("funding_data_quality") != "COMPLETE"
                else "MEDIUM"
            ),
            "evidence": (
                "SEC Form D data is not a complete private funding database"
                if company.get("funding_data_quality") != "COMPLETE"
                else "SEC Form D offering history available"
            ),
            "source_scope": "SEC_EXEMPT_OFFERING_ONLY",
        })

        rows.append({
            "cik": cik,
            "signal_type": "FUNDING_VELOCITY",
            "signal_value": company.get("funding_velocity_events_per_year"),
            "signal_unit": "EVENTS_PER_YEAR",
            "signal_direction": "NEUTRAL",
            "signal_strength": (
                "MEDIUM"
                if event_count >= 2
                else "LOW"
            ),
            "evidence": "Observed Form D offering events per elapsed year",
            "source_scope": "SEC_EXEMPT_OFFERING_ONLY",
        })

        rows.append({
            "cik": cik,
            "signal_type": "FUNDING_STAGE",
            "signal_value": company.get("funding_stage"),
            "signal_unit": "CLASSIFICATION",
            "signal_direction": "UNKNOWN",
            "signal_strength": "LOW",
            "evidence": "Funding stage is not reliably represented in Form D",
            "source_scope": "SEC_EXEMPT_OFFERING_ONLY",
        })

        rows.append({
            "cik": cik,
            "signal_type": "INVESTOR_COVERAGE",
            "signal_value": company.get("latest_investor_count"),
            "signal_unit": "INVESTORS",
            "signal_direction": "NEUTRAL",
            "signal_strength": (
                "MEDIUM"
                if pd.notna(company.get("latest_investor_count"))
                else "LOW"
            ),
            "evidence": "Form D investor count; investor identities require enrichment",
            "source_scope": "SEC_EXEMPT_OFFERING_ONLY",
        })

    result = pd.DataFrame(rows)
    result["signal_value_numeric"] = pd.to_numeric(
        result["signal_value"],
        errors="coerce",
    )
    result["signal_value"] = result["signal_value"].map(
        lambda value: None
        if value is None or pd.isna(value)
        else str(value)
    )
    result["signal_version"] = STEP17_VERSION
    result["signal_generated_at_utc"] = utc_now_iso()

    return result


def capital_signal_strength(value: Any) -> str:
    if value is None or pd.isna(value):
        return "LOW"

    if value >= 50_000_000:
        return "HIGH"

    if value >= 5_000_000:
        return "MEDIUM"

    return "LOW"


# =============================================================================
# SUMMARY / LINEAGE
# =============================================================================

def build_summary(
    events: pd.DataFrame,
    history: pd.DataFrame,
    snapshot: pd.DataFrame,
    signals: pd.DataFrame,
    discovery: pd.DataFrame,
) -> pd.DataFrame:
    companies = int(snapshot["cik"].nunique()) if not snapshot.empty else 0
    current_events = len(events)
    historical_filings = len(history)

    total_capital = None
    if not events.empty:
        total_capital = events["capital_amount_usd"].sum(min_count=1)

    summary = pd.DataFrame([{
        "step": "STEP_17",
        "step_name": "Funding & Capital Intelligence",
        "step_version": STEP17_VERSION,
        "source_system": SOURCE_SYSTEM,
        "companies": companies,
        "current_funding_events": current_events,
        "historical_form_d_filings": historical_filings,
        "discovered_form_d_datasets": int(len(discovery)),
        "datasets_with_target_events": int((pd.to_numeric(discovery.get("target_event_rows", pd.Series(dtype=float)), errors="coerce").fillna(0) > 0).sum()) if not discovery.empty else 0,
        "download_failed_datasets": int((discovery.get("download_status", pd.Series(dtype=str)) == "FAILED").sum()) if not discovery.empty else 0,
        "parse_failed_datasets": int((discovery.get("parse_status", pd.Series(dtype=str)) == "FAILED").sum()) if not discovery.empty else 0,
        "funding_signals": len(signals),
        "sec_capital_amount_sold_total_usd": total_capital,
        "companies_with_funding_data": (
            int((snapshot["funding_event_count"] > 0).sum())
            if not snapshot.empty else 0
        ),
        "companies_with_recent_180d_capital": (
            int(
                (
                    pd.to_numeric(
                        snapshot["sec_capital_raised_180d_usd"],
                        errors="coerce",
                    ).fillna(0) > 0
                ).sum()
            )
            if not snapshot.empty else 0
        ),
        "companies_funding_data_limited": (
            int(
                (
                    snapshot["funding_data_quality"]
                    .isin(["LIMITED", "PARTIAL"])
                ).sum()
            )
            if not snapshot.empty else 0
        ),
        "generated_at_utc": utc_now_iso(),
    }])

    return summary


def build_lineage(
    events: pd.DataFrame,
    history: pd.DataFrame,
    snapshot: pd.DataFrame,
    signals: pd.DataFrame,
    discovery: pd.DataFrame,
) -> pd.DataFrame:
    return pd.DataFrame([{
        "step": "STEP_17",
        "step_version": STEP17_VERSION,
        "source_system": SOURCE_SYSTEM,
        "source_endpoint": "SEC Form D quarterly dataset + SEC EDGAR Archives for filing drill-through",
        "source_scope": "SEC_EXEMPT_OFFERING_ONLY",
        "input_form_types": "D|D/A",
        "dataset_start_quarter": FORM_D_START_QUARTER,
        "dataset_end_quarter": FORM_D_END_QUARTER or "LATEST_AVAILABLE",
        "current_event_rows": len(events),
        "historical_filing_rows": len(history),
        "discovered_dataset_rows": len(discovery),
        "download_failed_rows": int((discovery.get("download_status", pd.Series(dtype=str)) == "FAILED").sum()) if not discovery.empty else 0,
        "parse_failed_rows": int((discovery.get("parse_status", pd.Series(dtype=str)) == "FAILED").sum()) if not discovery.empty else 0,
        "snapshot_rows": len(snapshot),
        "signal_rows": len(signals),
        "investor_enrichment_available": INVESTOR_INPUT.exists(),
        "investor_enrichment_path": str(INVESTOR_INPUT),
        "company_master_source": str(COMPANY_MASTER_PATH),
        "generated_at_utc": utc_now_iso(),
    }])


# =============================================================================
# VALIDATION
# =============================================================================

def validate_step17(
    events: pd.DataFrame,
    history: pd.DataFrame,
    snapshot: pd.DataFrame,
    signals: pd.DataFrame,
    discovery: pd.DataFrame,
) -> Dict[str, Any]:
    failures: List[str] = []
    warnings: List[str] = []

    # Discovery/download observability. A download failure is a controlled
    # degradation, not evidence that the filing does not exist.
    if not discovery.empty:
        if "dataset_quarter" in discovery.columns and discovery["dataset_quarter"].duplicated().any():
            failures.append("Dataset discovery contains duplicate quarters.")

        download_failed = discovery[discovery["download_status"].eq("FAILED")]
        parse_failed = discovery[discovery["parse_status"].eq("FAILED")]

        if not download_failed.empty:
            warnings.append(
                f"{len(download_failed)} discovered Form D dataset(s) could not be downloaded. "
                "Gold promotion must be blocked from replacing prior valid data."
            )
        if not parse_failed.empty:
            warnings.append(
                f"{len(parse_failed)} downloaded Form D dataset(s) could not be parsed."
            )

    # Field-level extraction audit.
    if not events.empty:
        allowed_extraction_status = {
            "FOUND",
            "MISSING",
            "INVALID",
        }

        for col in [
            "first_sale_date_extraction_status",
            "total_offering_amount_extraction_status",
            "total_amount_sold_extraction_status",
            "total_remaining_extraction_status",
            "investor_count_extraction_status",
        ]:
            if col in events.columns:
                invalid = (
                    set(events[col].dropna().astype(str))
                    - allowed_extraction_status
                )
                if invalid:
                    failures.append(
                        f"Invalid {col} values: {sorted(invalid)}"
                    )

        if "total_amount_sold_extraction_status" in events.columns:
            missing_amount = events[
                events["total_amount_sold_extraction_status"].eq("MISSING")
            ]
            if not missing_amount.empty:
                warnings.append(
                    f"{len(missing_amount)} current Form D event(s) have no "
                    "extractable total amount sold."
                )

        if "first_sale_date_extraction_status" in events.columns:
            missing_date = events[
                events["first_sale_date_extraction_status"].eq("MISSING")
            ]
            if not missing_date.empty:
                warnings.append(
                    f"{len(missing_date)} current Form D event(s) have no "
                    "extractable first-sale date."
                )

        if "parser_recovery_used" in events.columns:
            recovered_count = int(
                events["parser_recovery_used"].fillna(False).astype(bool).sum()
            )
            if recovered_count:
                warnings.append(
                    f"{recovered_count} Form D event(s) required recovered XML parsing."
                )

    # Extraction-method audit.
    if not events.empty:
        allowed_methods = {"XML_TAG", "HTML_TABLE", "LABEL_CONTEXT", "NONE"}

        for col in [
            "first_sale_date_extraction_method",
            "total_offering_amount_extraction_method",
            "total_amount_sold_extraction_method",
            "total_remaining_extraction_method",
            "investor_count_extraction_method",
        ]:
            if col in events.columns:
                invalid = (
                    set(events[col].dropna().astype(str))
                    - allowed_methods
                )
                if invalid:
                    failures.append(
                        f"Invalid {col} values: {sorted(invalid)}"
                    )

        if "parser_recovery_used" in events.columns:
            recovered = events[
                events["parser_recovery_used"].fillna(False).astype(bool)
            ]
            for _, row in recovered.iterrows():
                # Recovery itself is not a failure; it is explicitly retained
                # as lineage and should only produce a warning.
                pass

    # Event grain.
    if not events.empty:
        if events["funding_event_id"].duplicated().any():
            failures.append(
                "Current funding events contain duplicate funding_event_id."
            )

        if events["cik"].isna().any() or events["cik"].eq("").any():
            failures.append(
                "Current funding events contain blank CIK."
            )

        bad_amounts = events[
            pd.to_numeric(
                events["capital_amount_usd"],
                errors="coerce",
            ).notna()
            & (
                pd.to_numeric(
                    events["capital_amount_usd"],
                    errors="coerce",
                ) < 0
            )
        ]
        if not bad_amounts.empty:
            failures.append(
                "Negative capital_amount_usd found."
            )

        # capital_amount_usd is defined as total_amount_sold_usd. Missing
        # values are valid for a LIMITED/PARTIAL Form D event and must compare
        # equal here. Plain pandas NaN != NaN evaluates True, which previously
        # produced a false validation failure whenever both fields were absent.
        capital_amount = pd.to_numeric(
            events["capital_amount_usd"],
            errors="coerce",
        )
        amount_sold = pd.to_numeric(
            events["total_amount_sold_usd"],
            errors="coerce",
        )

        mismatch = ~(
            capital_amount.eq(amount_sold)
            | (capital_amount.isna() & amount_sold.isna())
        )

        if mismatch.any():
            failures.append(
                "capital_amount_usd is not aligned with total_amount_sold_usd."
            )

    # Offering/amendment identity contract.
    if not events.empty:
        if events["offering_id"].isna().any() or events["offering_id"].astype(str).str.strip().eq("").any():
            failures.append(
                "Current funding events contain blank offering_id."
            )

        if events["funding_event_id"].isna().any() or events["funding_event_id"].astype(str).str.strip().eq("").any():
            failures.append(
                "Current funding events contain blank funding_event_id."
            )

        # One canonical offering must map to exactly one funding event.
        offering_key = events[["cik", "offering_id", "funding_event_id"]].dropna().copy()
        if not offering_key.empty:
            conflicts = (
                offering_key.groupby(["cik", "offering_id"])["funding_event_id"]
                .nunique()
            )
            if (conflicts > 1).any():
                failures.append(
                    "A CIK/offering_id maps to multiple funding_event_id values."
                )

        if "offering_id_resolution_status" in events.columns:
            invalid_resolution = set(
                events["offering_id_resolution_status"].dropna().astype(str)
            ) - {
                "RESOLVED_ORIGINAL",
                "RESOLVED_ROOT",
                "RESOLVED_ROOT_EXTERNAL",
                "UNRESOLVED_AMENDMENT_PARENT",
                "CYCLE_DETECTED",
                "CHAIN_TOO_LONG",
                "MISSING_ACCESSION",
            }
            if invalid_resolution:
                failures.append(
                    "Invalid offering_id_resolution_status values: "
                    f"{sorted(invalid_resolution)}"
                )

            unresolved = events[
                events["offering_id_resolution_status"].isin(
                    [
                        "UNRESOLVED_AMENDMENT_PARENT",
                        "CYCLE_DETECTED",
                        "CHAIN_TOO_LONG",
                    ]
                )
            ]
            if not unresolved.empty:
                failures.append(
                    f"{len(unresolved)} current event(s) have unresolved/invalid amendment lineage."
                )

    if not history.empty:
        # An original D must be its own offering identity.
        originals = history[
            history["filing_type"].astype("string").str.upper().eq("D")
            & history["accession_number"].notna()
        ]
        if not originals.empty:
            bad_originals = originals[
                originals["offering_id"].astype(str)
                != originals["accession_number"].astype(str)
            ]
            if not bad_originals.empty:
                failures.append(
                    "Original Form D filings do not use their accession_number as offering_id."
                )

        # A D/A must have a previous accession and must resolve to an offering.
        amendments = history[
            history["filing_type"].astype("string").str.upper().eq("D/A")
        ]
        if not amendments.empty:
            missing_parent = amendments[
                amendments["previous_accession_number"].isna()
                | amendments["previous_accession_number"].astype(str).str.strip().eq("")
            ]
            if not missing_parent.empty:
                failures.append(
                    f"{len(missing_parent)} Form D/A filing(s) are missing previous_accession_number."
                )

    # History must retain amendment filings.
    if not history.empty:
        if history["accession_number"].duplicated().any():
            failures.append(
                "Historical Form D filings contain duplicate accession numbers."
            )

    # Snapshot grain.
    if not snapshot.empty:
        if snapshot["cik"].duplicated().any():
            failures.append(
                "Company funding snapshot is not one row per CIK."
            )

        allowed_quality = {
            "COMPLETE",
            "PARTIAL",
            "LIMITED",
            "NO_DATA",
        }

        invalid_quality = set(snapshot["funding_data_quality"].dropna()) - allowed_quality
        if invalid_quality:
            failures.append(
                f"Invalid funding_data_quality values: {sorted(invalid_quality)}"
            )

        allowed_freshness = {
            "FRESH",
            "STALE",
            "VERY_STALE",
            "UNKNOWN",
        }

        invalid_freshness = (
            set(snapshot["funding_freshness_status"].dropna())
            - allowed_freshness
        )
        if invalid_freshness:
            failures.append(
                f"Invalid funding_freshness_status values: {sorted(invalid_freshness)}"
            )

    # Signal integrity.
    if not signals.empty:
        if signals[["cik", "signal_type"]].duplicated().any():
            failures.append(
                "Duplicate company/signal_type pairs found."
            )

        invalid_signals = (
            set(signals["signal_type"].dropna())
            - (
                CAPITAL_SIGNAL_TYPES
                | {
                    "FUNDING_VELOCITY",
                    "FUNDING_STAGE",
                    "INVESTOR_COVERAGE",
                    "SEC_CAPITAL_COVERAGE",
                }
            )
        )

        if invalid_signals:
            failures.append(
                f"Invalid funding signal types: {sorted(invalid_signals)}"
            )

    # Explicit limitation warning.
    if not events.empty:
        warnings.append(
            "SEC Form D does not represent complete private-company funding history."
        )

    if events.empty:
        if not discovery.empty and discovery["accession_number"].notna().any():
            warnings.append(
                "Form D/D-A filings were discovered, but no filing produced a parsed event; "
                "this is an ingestion/extraction degradation, not evidence of zero funding."
            )
        else:
            warnings.append(
                "No Form D/D-A filings were discovered for the configured companies."
            )

    return {
        "status": "FAIL" if failures else ("WARNING" if warnings else "PASS"),
        "fatal_failures": failures,
        "warnings": warnings,
    }


def resolve_funding_profile_date(
    first_sale_date,
    filing_date,
):
    first_sale = pd.to_datetime(
        first_sale_date,
        errors="coerce",
    )

    if pd.notna(first_sale):
        return first_sale, "FIRST_SALE_DATE"

    filing = pd.to_datetime(
        filing_date,
        errors="coerce",
    )

    if pd.notna(filing):
        return filing, "FILING_DATE_FALLBACK"

    return pd.NaT, "NO_DATE"

# =============================================================================
# OUTPUT
# =============================================================================

def write_outputs(
    events: pd.DataFrame,
    history: pd.DataFrame,
    snapshot: pd.DataFrame,
    signals: pd.DataFrame,
    summary: pd.DataFrame,
    lineage: pd.DataFrame,
    discovery: pd.DataFrame,
) -> None:
    write_parquet(
        discovery,
        GOLD_ROOT
        / "funding_discovery"
        / "funding_discovery.parquet",
    )

    write_parquet(
        events,
        GOLD_ROOT
        / "funding_events"
        / "funding_events.parquet",
    )

    write_parquet(
        history,
        GOLD_ROOT
        / "funding_event_history"
        / "funding_event_history.parquet",
    )

    write_parquet(
        snapshot,
        GOLD_ROOT
        / "company_funding_snapshot"
        / "company_funding_snapshot.parquet",
    )

    write_parquet(
        signals,
        GOLD_ROOT
        / "funding_signals"
        / "funding_signals.parquet",
    )

    write_parquet(
        summary,
        GOLD_ROOT
        / "funding_summary"
        / "funding_summary.parquet",
    )

    write_parquet(
        lineage,
        GOLD_ROOT
        / "funding_lineage"
        / "funding_lineage.parquet",
    )


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    logger.info("=" * 72)
    logger.info("STEP 17 - FUNDING & CAPITAL INTELLIGENCE")
    logger.info("Version: %s", STEP17_VERSION)
    logger.info("=" * 72)

    logger.info("Configured companies: %d", len(COMPANIES))
    logger.info("Source: SEC quarterly Form D datasets")
    logger.info("Output: %s", GOLD_ROOT)

    client = SECClient()

    logger.info(
        "Company master source: %s",
        COMPANY_MASTER_PATH,
    )
    company_master = load_company_master(COMPANIES)
    logger.info(
        "Company master resolved: %d unique configured CIKs",
        len(company_master),
    )

    # -------------------------------------------------------------------------
    # 17.1 SEC quarterly Form D dataset ingestion
    # -------------------------------------------------------------------------
    raw_events, dataset_history, discovery, related_persons = ingest_form_d_dataset(
        client,
        COMPANIES,
    )

    logger.info(
        "Raw Form D dataset event rows: %d",
        len(raw_events),
    )
    logger.info(
        "Quarterly dataset history rows: %d",
        len(dataset_history),
    )

    # -------------------------------------------------------------------------
    # 17.2 Canonicalize offerings and amendments into current offering state
    # -------------------------------------------------------------------------
    current_events, historical_events = canonicalize_form_d_events(
        raw_events,
    )

    logger.info(
        "Current funding events: %d",
        len(current_events),
    )
    logger.info(
        "Historical Form D filings retained: %d",
        len(historical_events),
    )

    # -------------------------------------------------------------------------
    # 17.3 Optional investor enrichment
    # -------------------------------------------------------------------------
    investors = load_optional_investors()

    investor_metrics = build_investor_metrics(
        current_events,
        investors,
    )

    if not investor_metrics.empty:
        current_events = current_events.drop(
            columns=[
                "investor_identity_count",
                "investor_names",
                "investor_types",
                "investor_data_source",
            ],
            errors="ignore",
        )

        current_events = current_events.merge(
            investor_metrics[
                [
                    "funding_event_id",
                    "investor_identity_count",
                    "investor_names",
                    "investor_types",
                    "investor_data_source",
                ]
            ],
            on="funding_event_id",
            how="left",
            validate="one_to_one",
        )

    # -------------------------------------------------------------------------
    # 17.4 Company capital snapshot
    # -------------------------------------------------------------------------
    snapshot = build_company_funding_snapshot(
        current_events,
        company_master,
    )

    # -------------------------------------------------------------------------
    # 17.5 Funding intelligence signals
    # -------------------------------------------------------------------------
    signals = build_funding_signals(
        current_events,
        snapshot,
    )

    # -------------------------------------------------------------------------
    # 17.6 Summary + lineage
    # -------------------------------------------------------------------------
    summary = build_summary(
        current_events,
        historical_events,
        snapshot,
        signals,
        discovery,
    )

    lineage = build_lineage(
        current_events,
        historical_events,
        snapshot,
        signals,
        discovery,
    )

    # -------------------------------------------------------------------------
    # 17.7 Validation
    # -------------------------------------------------------------------------
    validation = validate_step17(
        current_events,
        historical_events,
        snapshot,
        signals,
        discovery,
    )

    logger.info("")
    logger.info("Validation status: %s", validation["status"])

    for failure in validation["fatal_failures"]:
        logger.error("FAIL: %s", failure)

    for warning in validation["warnings"]:
        logger.warning("WARNING: %s", warning)

    if validation["status"] == "FAIL":
        raise RuntimeError(
            "STEP 17 validation failed. Outputs were not promoted."
        )

    # -------------------------------------------------------------------------
    # 17.8 Safe promotion
    # -------------------------------------------------------------------------
    # Never replace a previously valid Gold dataset with an empty/partial
    # dataset caused by transient SEC archive failures. Discovery diagnostics
    # are always written separately.
    download_failures = int(
        (discovery.get("download_status", pd.Series(dtype=str)) == "FAILED").sum()
    ) if not discovery.empty else 0
    parse_failures = int(
        (discovery.get("parse_status", pd.Series(dtype=str)) == "FAILED").sum()
    ) if not discovery.empty else 0
    discovery_failures = int(
        (discovery.get("discovery_status", pd.Series(dtype=str)) == "FAILED").sum()
    ) if not discovery.empty else 0

    # Any unresolved source-ingestion failure blocks replacement of Gold. This
    # prevents a transient SEC outage or parser regression from deleting a
    # previously valid funding dataset.
    promotion_blocked = (
        download_failures > 0
        or parse_failures > 0
        or discovery_failures > 0
    )
    if promotion_blocked:
        logger.error(
            "GOLD PROMOTION BLOCKED: source-ingestion failures detected "
            "(discovery=%d, download=%d, parse=%d). Existing Gold data will not be overwritten.",
            discovery_failures,
            download_failures,
            parse_failures,
        )
        write_parquet(
            discovery,
            GOLD_ROOT / "funding_discovery" / "funding_discovery.parquet",
        )
        write_parquet(
            summary,
            GOLD_ROOT / "funding_summary" / "funding_summary_run_diagnostic.parquet",
        )
        write_parquet(
            lineage,
            GOLD_ROOT / "funding_lineage" / "funding_lineage_run_diagnostic.parquet",
        )
    else:
        write_outputs(
            current_events,
            historical_events,
            snapshot,
            signals,
            summary,
            lineage,
            discovery,
        )
        write_parquet(
            dataset_history,
            GOLD_ROOT / "funding_dataset_history" / "funding_dataset_history.parquet",
        )
        write_parquet(
            related_persons,
            GOLD_ROOT / "funding_related_persons" / "funding_related_persons.parquet",
        )
        logger.info("Gold outputs promoted successfully.")

    logger.info("")
    logger.info("STEP 17 COMPLETE")
    logger.info("Companies: %d", len(snapshot))
    logger.info("Current funding events: %d", len(current_events))
    if not current_events.empty:
        logger.info(
            "Events with capital amount: %d",
            int(pd.to_numeric(
                current_events["capital_amount_usd"], errors="coerce"
            ).notna().sum()),
        )
        logger.info(
            "Events with first-sale date: %d",
            int(pd.to_datetime(
                current_events["first_sale_date"], errors="coerce"
            ).notna().sum()),
        )
        for field, status_col in [
            ("total_amount_sold_usd", "total_amount_sold_extraction_status"),
            ("first_sale_date", "first_sale_date_extraction_status"),
        ]:
            if status_col in current_events.columns:
                counts = (
                    current_events[status_col]
                    .fillna("MISSING")
                    .astype(str)
                    .value_counts()
                    .to_dict()
                )
                logger.info(
                    "%s extraction status: %s",
                    field,
                    counts,
                )
    logger.info("Historical Form D filings: %d", len(historical_events))
    logger.info("Funding signals: %d", len(signals))
    logger.info("Discovered Form D datasets: %d", len(discovery))
    logger.info("Discovery failures: %d", discovery_failures)
    logger.info("Download failures: %d", download_failures)
    logger.info("Parse failures: %d", parse_failures)
    logger.info("Gold promotion: %s", "BLOCKED" if promotion_blocked else "PROMOTED")
    logger.info("Validation: %s", validation["status"])

    if not snapshot.empty:
        display_cols = [
            "cik",
            "company_name",
            "funding_event_count",
            "total_sec_capital_raised_usd",
            "latest_funding_date",
            "latest_capital_amount_usd",
            "funding_stage",
            "capital_trajectory",
            "funding_data_quality",
            "funding_freshness_status",
        ]

        logger.info("")
        logger.info("Company funding snapshot:")
        logger.info(
            "\n%s",
            snapshot[display_cols].to_string(index=False),
        )


if __name__ == "__main__":
    main()
