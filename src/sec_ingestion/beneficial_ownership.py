"""Read holder names and ownership percentages that a filing states.

The inputs are the latest Schedule 13D or 13G and the latest DEF 14A for
each active company. Form 4 share counts are not converted into a percentage.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from sec_ingestion.document_ingestion import ingest_sec_filing_documents


ROOT = Path(__file__).resolve().parents[2]
FILINGS_ROOT = ROOT / "data" / "silver" / "filings"
INVESTOR_PATH = ROOT / "data" / "silver" / "funding" / "investors.parquet"

OWNERSHIP_FORMS = {
    "SC 13D",
    "SC 13D/A",
    "SC 13G",
    "SC 13G/A",
    "SCHEDULE 13D",
    "SCHEDULE 13D/A",
    "SCHEDULE 13G",
    "SCHEDULE 13G/A",
}
PROXY_FORMS = {"DEF 14A"}

_NAME_STOP = re.compile(
    r"^(name of|title of|percent of|cusip|item \d|see instructions|irs identification|i\.r\.s)",
    re.I,
)
_PERCENT = re.compile(r"(?<!\d)(\d{1,2}(?:\.\d+)?)\s*%")
_SHARE_AND_PERCENT = re.compile(
    r"([A-Za-z][A-Za-z0-9&.,'’()\- ]{2,90}?)\s+"
    r"(\d{1,3}(?:,\d{3})+|\d{4,})\s+"
    r"(\d{1,2}(?:\.\d+)?)\s*%"
)


def _normalize_cik(value) -> str:
    return str(value).strip().removesuffix(".0").zfill(10)


def _clean_name(value: str) -> str:
    name = re.sub(r"\s+", " ", str(value or "")).strip(" :-")
    name = re.sub(r"\s*\(\d+\)$", "", name)
    return name


def _percent(value: str) -> float | None:
    match = _PERCENT.search(value or "")
    if not match:
        return None
    number = float(match.group(1))
    if number <= 0 or number > 100:
        return None
    return number


def _source_for_form(form: str) -> str:
    upper = form.upper()
    if "13D" in upper:
        return "SEC_SCHEDULE_13D"
    if "13G" in upper:
        return "SEC_SCHEDULE_13G"
    return "SEC_DEF_14A"


def _document_text(path: Path) -> str:
    raw = path.read_text(encoding="utf-8-sig", errors="replace")
    if "<html" not in raw.lower() and "<table" not in raw.lower():
        return raw
    try:
        beautifulsoup = __import__("bs4")
    except ModuleNotFoundError:
        return raw
    soup = beautifulsoup.BeautifulSoup(raw, "html.parser")
    for element in soup(["script", "style"]):
        element.decompose()
    return soup.get_text("\n")


def _reporting_persons(text: str) -> list[dict]:
    """Cover-page reporting person and percent of class, as printed."""
    holders = []
    markers = list(
        re.finditer(r"NAMES? OF REPORTING PERSONS?", text, flags=re.I)
    )
    for index, marker in enumerate(markers):
        end = markers[index + 1].start() if index + 1 < len(markers) else marker.end() + 1800
        window = text[marker.end():end]
        name = ""
        for line in window.splitlines():
            candidate = _clean_name(line)
            if len(candidate) < 3 or candidate.isdigit() or _NAME_STOP.search(candidate):
                continue
            if "IDENTIFICATION" in candidate.upper() or "CHECK THE APPROPRIATE" in candidate.upper():
                continue
            name = candidate
            break
        if not name:
            continue
        percent_match = re.search(
            r"PERCENT OF CLASS.{0,240}?(\d{1,2}(?:\.\d+)?)\s*%",
            window,
            flags=re.I | re.S,
        )
        percent = float(percent_match.group(1)) if percent_match else None
        if percent is not None and (percent <= 0 or percent > 100):
            percent = None
        holders.append({"investor_name": name, "ownership_pct": percent})
    return holders


_PROXY_NAME_REJECT = re.compile(
    r"\b(?:street|blvd|boulevard|avenue|suite|floor|p\.?o\.?\s*box|road|drive|yards)\b"
    r"|\b[A-Z]{2}\s+\d{5}\b"
    r"|^(?:outstanding|common stock|percent|owned|of class|total|n/a|table of contents|shares|rsus|class)$",
    re.I,
)


def _acceptable_holder_name(name: str, issuer: str) -> bool:
    cleaned = _clean_name(name)
    if len(cleaned) < 3 or len(cleaned) > 80 or len(cleaned.split()) > 12:
        return False
    if not cleaned[:1].isupper() or not re.search(r"[A-Za-z]", cleaned) or _NAME_STOP.search(cleaned):
        return False
    if re.search(r"\d{5}", cleaned):
        return False
    if _PROXY_NAME_REJECT.search(cleaned):
        return False
    upper = cleaned.upper()
    if "AS A GROUP" in upper or upper == issuer:
        return False
    if upper in {"DIRECTORS", "NAMED EXECUTIVE OFFICERS", "5% STOCKHOLDERS"}:
        return False
    return True


def _is_share_count(value: str) -> bool:
    compact = value.replace(",", "").replace(".", "", 1)
    return compact.isdigit() and ("," in value or len(compact) >= 4)


def _proxy_section(text: str) -> str:
    matches = list(
        re.finditer(
            r"Security Ownership of Certain Beneficial Owners",
            text,
            flags=re.I,
        )
    )
    if not matches:
        return ""
    chosen = matches[0]
    for match in matches:
        following = text[match.start():match.start() + 2500]
        if re.search(r"more than 5%|5%\s+stockholders|name of beneficial owner", following, flags=re.I):
            chosen = match
            break
    section = text[chosen.start():chosen.start() + 25000]
    stop = re.search(
        r"\n\s*(?:Certain Relationships|Executive Compensation|Proposal\s+No|Director Compensation|Audit Committee Report)\b",
        section,
        flags=re.I,
    )
    if stop and stop.start() > 500:
        section = section[:stop.start()]
    return section


def _proxy_holders(text: str, issuer_name: str) -> list[dict]:
    section = _proxy_section(text)
    if not section:
        return []
    issuer = _clean_name(issuer_name).upper()
    lines = [_clean_name(line) for line in section.splitlines()]
    lines = [line for line in lines if line]
    holders = []
    seen = set()

    def add(name: str, percent: float | None) -> None:
        cleaned = _clean_name(name)
        if not _acceptable_holder_name(cleaned, issuer):
            return
        upper = cleaned.upper()
        if upper in seen:
            return
        seen.add(upper)
        holders.append({"investor_name": cleaned, "ownership_pct": percent})

    for match in _SHARE_AND_PERCENT.finditer(section):
        percent = float(match.group(3))
        if 0 < percent <= 100:
            add(match.group(1), percent)

    for index, line in enumerate(lines):
        if not re.search(r"[A-Za-z]", line) or _is_share_count(line) or _percent(line) is not None:
            continue
        if line.endswith("%") or line in {"*", "**"}:
            continue
        if index + 1 >= len(lines) or not _is_share_count(lines[index + 1]):
            continue
        percent = None
        if index + 2 < len(lines):
            if lines[index + 2] not in {"*", "**"}:
                percent = _percent(lines[index + 2])
        add(line, percent)
    return holders


def select_ownership_filings() -> pd.DataFrame:
    """Latest 13D/13G filing and latest DEF 14A for each company catalog."""
    rows = []
    for path in sorted(FILINGS_ROOT.glob("cik=*/filings.parquet")):
        frame = pd.read_parquet(path)
        if frame.empty or "form" not in frame.columns:
            continue
        frame = frame.copy()
        frame["form_norm"] = frame["form"].fillna("").astype(str).str.strip().str.upper()
        frame["filing_date"] = pd.to_datetime(frame["filing_date"], errors="coerce")
        groups = {
            "OWNERSHIP": frame[frame["form_norm"].isin(OWNERSHIP_FORMS)],
            "PROXY": frame[frame["form_norm"].isin(PROXY_FORMS)],
        }
        for kind, part in groups.items():
            if part.empty:
                continue
            chosen = part.sort_values("filing_date").iloc[-1]
            rows.append(chosen.to_dict())
            rows[-1]["ownership_kind"] = kind
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def download_selected_filings(selected: pd.DataFrame) -> pd.DataFrame:
    """Download the selected primary documents. Already-downloaded files are kept."""
    downloaded = []
    for record in selected.to_dict(orient="records"):
        cik = _normalize_cik(record["cik"])
        result = ingest_sec_filing_documents(
            cik=cik,
            limit=None,
            filing_records=[record],
            accession_number=str(record["accession_number"]),
            primary_document=str(record["primary_document"]),
        )
        manifest = pd.read_parquet(result["manifest_path"])
        match = manifest[
            manifest["accession_number"].astype(str).eq(str(record["accession_number"]))
            & manifest["primary_document"].astype(str).eq(str(record["primary_document"]))
            & manifest["status"].astype(str).str.upper().eq("DOWNLOADED")
        ]
        if match.empty:
            downloaded.append({**record, "local_path": "", "download_status": "FAILED"})
            continue
        downloaded.append({
            **record,
            "local_path": str(match.iloc[-1]["local_path"]),
            "download_status": "DOWNLOADED",
        })
    return pd.DataFrame(downloaded)


def parse_ownership_documents(downloaded: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for record in downloaded.to_dict(orient="records"):
        path = Path(str(record.get("local_path") or ""))
        if not path.is_file():
            continue
        text = _document_text(path)
        form = str(record.get("form_norm") or record.get("form") or "")
        source = _source_for_form(form)
        issuer = str(record.get("company_name") or "")
        if source == "SEC_DEF_14A":
            holders = _proxy_holders(text, issuer)
        else:
            holders = _reporting_persons(text)
        for holder in holders:
            if not holder["investor_name"]:
                continue
            if holder["investor_name"].upper() == _clean_name(issuer).upper():
                continue
            rows.append({
                "cik": _normalize_cik(record["cik"]),
                "investor_name": holder["investor_name"],
                "investor_type": "UNKNOWN",
                "ownership_pct": holder["ownership_pct"],
                "investor_source": source,
                "source_accession_number": str(record["accession_number"]),
                "as_of_date": str(record.get("filing_date") or "")[:10],
            })
    if not rows:
        return pd.DataFrame(columns=[
            "cik",
            "investor_name",
            "investor_type",
            "ownership_pct",
            "investor_source",
            "source_accession_number",
            "as_of_date",
        ])
    frame = pd.DataFrame(rows)
    frame["as_of_date"] = pd.to_datetime(frame["as_of_date"], errors="coerce")
    frame = frame.sort_values("as_of_date")
    frame = frame.drop_duplicates(
        subset=["cik", "investor_name"],
        keep="last",
    )
    return frame.reset_index(drop=True)


def write_investors(frame: pd.DataFrame) -> Path:
    INVESTOR_PATH.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(INVESTOR_PATH, index=False)
    return INVESTOR_PATH
