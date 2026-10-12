from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]

SILVER_ROOT = REPO_ROOT / "data" / "silver" / "documents"

SUPPORTED_DOCUMENT_CLASSES = {
    "SEC_10_K",
    "SEC_10_Q",
    "SEC_8_K",
    "SEC_FORM_D",
    "SEC_OTHER",
    "TEXT_DOCUMENT",
    "HTML_DOCUMENT",
    "PDF_DOCUMENT",
    "OTHER",
}

UNDERSTANDING_STATUSES = {
    "SUCCESS",
    "NO_SUPPORTED_FACTS",
    "FAILED",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalise(value) -> str:
    if value is None:
        return ""

    if pd.isna(value):
        return ""

    return str(value).strip()


def _safe_float(value):
    value = _normalise(value)

    if not value:
        return None

    value = value.replace(",", "")
    value = value.replace("$", "")

    try:
        return float(value)
    except ValueError:
        return None


def _load_parquet(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(path)

    frame = pd.read_parquet(path)

    if frame.empty:
        raise ValueError(f"Empty parquet artifact: {path}")

    return frame


def _load_classification(
    document_dir: Path,
) -> pd.DataFrame:
    return _load_parquet(
        document_dir / "document_classification.parquet"
    )


def _load_metadata(
    document_dir: Path,
) -> pd.DataFrame:
    return _load_parquet(
        document_dir / "document_metadata.parquet"
    )


def _load_text(
    document_dir: Path,
) -> pd.DataFrame:
    return _load_parquet(
        document_dir / "document_text.parquet"
    )


def _first_value(frame: pd.DataFrame, column: str) -> str:
    if column not in frame.columns or frame.empty:
        return ""

    return _normalise(frame.iloc[0][column])


def _document_text(frame: pd.DataFrame) -> str:
    if frame.empty:
        return ""

    text_columns = [
        "text",
        "page_text",
        "content",
        "document_text",
    ]

    for column in text_columns:
        if column in frame.columns:
            values = [
                _normalise(value)
                for value in frame[column].tolist()
            ]

            values = [value for value in values if value]

            if values:
                return "\n".join(values)

    return ""


def _search(
    text: str,
    pattern: str,
    flags=re.IGNORECASE,
):
    match = re.search(pattern, text, flags)

    if not match:
        return None

    return match


def _extract_filing_date(text: str) -> str | None:
    patterns = [
        r"\b(?:filing|filed)\s+date\s*[:\-]?\s*(\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{4})\b",
        r"\bfiled\s+on\s*[:\-]?\s*(\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{4})\b",
        r"\bfiled\s*[:\-]?\s*(\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{4})\b",
    ]

    for pattern in patterns:
        match = _search(text, pattern)

        if match:
            return match.group(1)

    return None


def _extract_form_d_facts(text: str) -> dict:
    facts = {}

    filing_date = _extract_filing_date(text)

    if filing_date:
        facts["filing_date"] = filing_date

    issuer_patterns = [
        r"ISSUER(?:\s+NAME)?\s*[:\-]\s*([^\n\r]+)",
        r"NAME OF ISSUER\s*[:\-]\s*([^\n\r]+)",
    ]

    for pattern in issuer_patterns:
        match = _search(text, pattern)

        if match:
            facts["issuer_name"] = match.group(1).strip()
            break

    offering_patterns = [
        r"TOTAL OFFERING AMOUNT\s*[:\-]?\s*\$?\s*([\d,]+(?:\.\d+)?)",
        r"TOTAL OFFERING\s*[:\-]?\s*\$?\s*([\d,]+(?:\.\d+)?)",
    ]

    for pattern in offering_patterns:
        match = _search(text, pattern)

        if match:
            facts["total_offering_amount"] = _safe_float(
                match.group(1)
            )
            break

    sold_patterns = [
        r"TOTAL AMOUNT SOLD\s*[:\-]?\s*\$?\s*([\d,]+(?:\.\d+)?)",
        r"AMOUNT SOLD\s*[:\-]?\s*\$?\s*([\d,]+(?:\.\d+)?)",
    ]

    for pattern in sold_patterns:
        match = _search(text, pattern)

        if match:
            facts["total_amount_sold"] = _safe_float(
                match.group(1)
            )
            break

    return facts


def _stated_number(value):
    number = _safe_float(value)
    if number is None:
        return None
    if number.is_integer():
        return int(number)
    return number


def _form4_marked_relationships(cell) -> tuple[list[str], str]:
    """Read only the relationship boxes and title the Form 4 actually marks."""
    relationships = []
    officer_title = ""
    table = cell.find("table")
    if table is None:
        return relationships, officer_title
    for row in table.find_all("tr"):
        cells = row.find_all("td", recursive=False)
        index = 0
        while index + 1 < len(cells):
            mark = cells[index].get_text(" ", strip=True).upper()
            label = cells[index + 1].get_text(" ", strip=True)
            if mark == "X":
                if "10% Owner" in label:
                    relationships.append("10% Owner")
                elif "Director" in label:
                    relationships.append("Director")
                elif "Officer" in label:
                    relationships.append("Officer")
                elif "Other" in label:
                    relationships.append("Other")
            elif (
                mark == ""
                and label
                and not any(
                    role in label
                    for role in ("Director", "10% Owner", "Officer", "Other")
                )
            ):
                if not officer_title:
                    officer_title = label
            index += 2
    if "Officer" not in relationships:
        officer_title = ""
    return relationships, officer_title


def _form4_data_text(cell) -> str:
    data = cell.find(class_=re.compile(r"(?:Small)?FormData$"))
    if data is None:
        return ""
    return _normalise(data.get_text(" ", strip=True))


def _extract_form4_html(raw: str) -> dict:
    try:
        beautifulsoup = __import__("bs4")
    except ModuleNotFoundError:
        return {}
    soup = beautifulsoup.BeautifulSoup(raw, "html.parser")
    for element in soup(["script", "style"]):
        element.decompose()

    owners = []
    for cell in soup.find_all("td"):
        if "Name and Address of Reporting Person" not in cell.get_text(" ", strip=True):
            continue
        link = cell.find("a")
        name = _normalise(link.get_text(" ", strip=True) if link else "")
        if not name:
            continue
        row = cell.find_parent("tr")
        relationships: list[str] = []
        title = ""
        if row is not None:
            for sibling in row.find_all("td"):
                if "Relationship of Reporting Person" in sibling.get_text(" ", strip=True):
                    relationships, title = _form4_marked_relationships(sibling)
                    break
        owner = {"name": name}
        if relationships:
            owner["relationship"] = "; ".join(relationships)
        if title:
            owner["title"] = title
        owners.append(owner)

    transactions = []
    for table in soup.find_all("table"):
        thead = table.find("thead", recursive=False)
        if thead is None:
            continue
        header = thead.get_text(" ", strip=True)
        if "Table I" not in header or "Non-Derivative" not in header:
            continue
        body = table.find("tbody", recursive=False) or table
        for data_row in body.find_all("tr", recursive=False):
            cells = data_row.find_all("td", recursive=False)
            if len(cells) < 10:
                continue
            security_title = _form4_data_text(cells[0])
            transaction_code = _form4_data_text(cells[3])
            shares = _stated_number(_form4_data_text(cells[5]))
            if not security_title or not transaction_code or shares is None:
                continue
            transaction = {
                "security_title": security_title,
                "transaction_code": transaction_code,
                "shares": shares,
            }
            transaction_date = _form4_data_text(cells[1])
            if transaction_date:
                transaction["transaction_date"] = transaction_date
            acquired = _form4_data_text(cells[6]).upper()
            if acquired in {"A", "D"}:
                transaction["acquired_or_disposed"] = acquired
            price = _stated_number(_form4_data_text(cells[7]))
            if price is not None:
                transaction["price"] = price
            owned = _stated_number(_form4_data_text(cells[8]))
            if owned is not None:
                transaction["shares_owned_following"] = owned
            ownership_form = _form4_data_text(cells[9]).upper()
            if ownership_form in {"D", "I"}:
                transaction["ownership_form"] = ownership_form
            transactions.append(transaction)
        break

    facts = {}
    if owners:
        facts["reporting_owner_name"] = owners[0]["name"]
        if owners[0].get("relationship"):
            facts["reporting_owner_relationship"] = owners[0]["relationship"]
        if owners[0].get("title"):
            facts["reporting_owner_title"] = owners[0]["title"]
        if len(owners) > 1:
            facts["reporting_owners"] = owners
    if transactions:
        facts["non_derivative_transactions"] = transactions
    return facts


def _extract_form4_facts(source_path: str, text: str) -> dict:
    raw = ""
    path = Path(source_path) if source_path else None
    if path is not None and path.is_file():
        raw = path.read_text(encoding="utf-8-sig", errors="replace")
    if "<html" not in raw.lower() and "<html" in text.lower():
        raw = text
    if "<html" not in raw.lower():
        return {}
    return _extract_form4_html(raw)


def _extract_sec_facts(
    document_class: str,
    text: str,
    *,
    source_form: str = "",
    source_path: str = "",
) -> tuple[dict, list[dict]]:

    facts = {}
    events = []

    filing_date = _extract_filing_date(text)

    if filing_date:
        facts["filing_date"] = filing_date

    if document_class == "SEC_FORM_D":
        form_d_facts = _extract_form_d_facts(text)
        facts.update(form_d_facts)

        events.append(
            {
                "event_type": "SEC_FORM_D_FILING",
                "event_status": "CONFIRMED",
                "event_confidence": 1.0,
                "event_method": "DETERMINISTIC_RULE",
            }
        )

    elif document_class in {
        "SEC_10_K",
        "SEC_10_Q",
        "SEC_8_K",
        "SEC_OTHER",
    }:
        events.append(
            {
                "event_type": "SEC_FILING",
                "event_status": "CONFIRMED",
                "event_confidence": 1.0,
                "event_method": "DOCUMENT_CLASS",
            }
        )
        if source_form.strip().upper() in {"4", "4/A"}:
            facts.update(_extract_form4_facts(source_path, text))

    return facts, events


def _build_event_rows(
    *,
    document_id: str,
    company_id: str,
    cik: str,
    document_class: str,
    source_filename: str,
    source_path: str,
    sha256: str,
    facts: dict,
    events: list[dict],
) -> list[dict]:

    rows = []
    serialised_facts = facts.copy()

    for amount_field in (
        "total_offering_amount",
        "total_amount_sold",
    ):
        amount = serialised_facts.get(amount_field)

        if isinstance(amount, (int, float)):
            serialised_facts[amount_field] = format(amount, ",")

    for event in events:
        rows.append(
            {
                "document_id": document_id,
                "company_id": company_id,
                "cik": cik,
                "document_class": document_class,
                "event_type": event["event_type"],
                "event_status": event["event_status"],
                "event_confidence": event["event_confidence"],
                "event_method": event["event_method"],
                "source_filename": source_filename,
                "source_path": source_path,
                "sha256": sha256,
                "facts_json": json.dumps(
                    serialised_facts,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "created_at_utc": _utc_now(),
            }
        )

    return rows


def _already_understood(
    path: Path,
    sha256: str,
) -> bool:

    if not path.is_file():
        return False

    try:
        frame = pd.read_parquet(path)
    except Exception:
        return False

    if frame.empty:
        return False

    if "sha256" not in frame.columns:
        return False

    if "understanding_status" not in frame.columns:
        return False

    if "document_id" not in frame.columns:
        return False

    document_ids = frame["document_id"].dropna().astype(str).str.strip()
    if document_ids.empty or not document_ids.eq(path.parent.name).all():
        return False

    matching_rows = (
        (
            frame["sha256"].astype(str) == sha256
        )
        & (
            frame["understanding_status"].astype(str)
            == "SUCCESS"
        )
    )
    return bool(matching_rows.any())


def _write_atomic(
    frame: pd.DataFrame,
    path: Path,
) -> None:

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    tmp = path.with_suffix(".tmp.parquet")

    frame.to_parquet(
        tmp,
        index=False,
    )

    tmp.replace(path)


def understand_documents_for_company(
    cik: str,
    *,
    force: bool = False,
    document_id: str | None = None,
) -> dict:

    cik = str(cik).zfill(10)

    company_root = (
        SILVER_ROOT / f"cik={cik}"
    )

    if not company_root.is_dir():
        return {
            "discovered": 0,
            "understood": 0,
            "skipped": 0,
            "failed": 0,
            "results": [],
        }

    results = []

    document_dirs = [
        path
        for path in company_root.iterdir()
        if path.is_dir()
    ]

    discovered = len(document_dirs)
    understood = 0
    skipped = 0
    failed = 0

    for document_dir in document_dirs:

        current_document_id = document_dir.name
        if document_id and current_document_id != str(document_id).strip():
            continue

        try:
            metadata = _load_metadata(
                document_dir
            )

            classification = _load_classification(
                document_dir
            )

            text_frame = _load_text(
                document_dir
            )

            company_id = _first_value(
                metadata,
                "company_id",
            )

            source_path = _first_value(
                metadata,
                "source_path",
            )

            source_filename = _first_value(
                metadata,
                "source_filename",
            )

            sha256 = _first_value(
                metadata,
                "sha256",
            )

            document_class = _first_value(
                classification,
                "document_class",
            )

            if document_class not in SUPPORTED_DOCUMENT_CLASSES:
                raise ValueError(
                    f"Unsupported document class: "
                    f"{document_class}"
                )

            text = _document_text(text_frame)

            if not text:
                raise ValueError(
                    f"No extracted text for "
                    f"document {current_document_id}"
                )

            output_path = (
                document_dir
                / "document_understanding.parquet"
            )

            if not force and _already_understood(
                output_path,
                sha256,
            ):
                skipped += 1

                existing = pd.read_parquet(output_path)
                events = []
                facts = {}

                for _, row in existing.iterrows():
                    row_facts = json.loads(
                        _normalise(row.get("facts_json")) or "{}"
                    )
                    if not facts:
                        facts = row_facts

                    event_type = _normalise(row.get("event_type"))
                    if event_type:
                        events.append(
                            {
                                "event_type": event_type,
                                "event_status": _normalise(
                                    row.get("event_status")
                                ),
                                "event_confidence": float(
                                    row.get("event_confidence") or 0.0
                                ),
                                "event_method": _normalise(
                                    row.get("event_method")
                                ),
                            }
                        )

                results.append(
                    {
                        "document_id": current_document_id,
                        "document_class": document_class,
                        "source_path": source_path,
                        "sha256": sha256,
                        "status": "SKIPPED",
                        "facts": facts,
                        "events": events,
                    }
                )

                continue

            facts = {}
            events = []

            if document_class.startswith("SEC_"):
                facts, events = _extract_sec_facts(
                    document_class,
                    text,
                    source_form=_first_value(metadata, "form"),
                    source_path=source_path,
                )

            understanding_status = (
                "SUCCESS"
                if facts or events
                else "NO_SUPPORTED_FACTS"
            )

            event_rows = _build_event_rows(
                document_id=current_document_id,
                company_id=company_id,
                cik=cik,
                document_class=document_class,
                source_filename=source_filename,
                source_path=source_path,
                sha256=sha256,
                facts=facts,
                events=events,
            )

            output_rows = []

            if event_rows:
                for event_row in event_rows:
                    output_rows.append(
                        {
                            **event_row,
                            "understanding_status":
                                understanding_status,
                        }
                    )
            else:
                output_rows.append(
                    {
                        "document_id": current_document_id,
                        "company_id": company_id,
                        "cik": cik,
                        "document_class": document_class,
                        "event_type": None,
                        "event_status": None,
                        "event_confidence": None,
                        "event_method": None,
                        "source_filename": source_filename,
                        "source_path": source_path,
                        "sha256": sha256,
                        "facts_json": json.dumps(facts, sort_keys=True),
                        "understanding_status":
                            understanding_status,
                        "created_at_utc": _utc_now(),
                    }
                )

            _write_atomic(
                pd.DataFrame(output_rows),
                output_path,
            )

            understood += 1

            results.append(
                {
                    "document_id": current_document_id,
                    "document_class": document_class,
                    "source_path": source_path,
                    "sha256": sha256,
                    "status": understanding_status,
                    "facts": facts,
                    "events": events,
                }
            )

        except Exception as exc:

            failed += 1

            results.append(
                {
                    "document_id": current_document_id,
                    "status": "FAILED",
                    "error": str(exc),
                }
            )

    return {
        "discovered": discovered,
        "understood": understood,
        "skipped": skipped,
        "failed": failed,
        "results": results,
    }


def refresh_downloaded_form4_facts() -> dict:
    """Re-read downloaded Form 4 files and store their stated transaction facts.

    This does not download filings and does not create funding amounts.
    """
    from company_platform.events.structured_event_store import (
        StructuredEventStore,
    )

    manifest_root = REPO_ROOT / "data" / "bronze" / "sec" / "documents"
    store = StructuredEventStore()
    summary = {
        "documents": 0,
        "understood": 0,
        "events_updated": 0,
        "events_created": 0,
        "failed": 0,
        "without_transactions": 0,
    }
    if not manifest_root.is_dir():
        return summary

    for manifest_path in sorted(manifest_root.glob("cik=*/document_manifest.parquet")):
        frame = pd.read_parquet(manifest_path)
        if frame.empty or not {"form", "status", "document_id", "cik"}.issubset(frame.columns):
            continue
        selected = frame[
            frame["form"].astype(str).str.strip().str.upper().eq("4")
            & frame["status"].astype(str).str.upper().eq("DOWNLOADED")
        ]
        for record in selected.to_dict(orient="records"):
            summary["documents"] += 1
            cik = str(record["cik"]).zfill(10)
            document_id = str(record["document_id"])
            result = understand_documents_for_company(
                cik,
                force=True,
                document_id=document_id,
            )
            matched = [
                item
                for item in result["results"]
                if item.get("document_id") == document_id
                and item.get("status") != "FAILED"
            ]
            if result["failed"] or not matched:
                summary["failed"] += 1
                continue
            summary["understood"] += 1
            facts = matched[0].get("facts") or {}
            if not facts.get("non_derivative_transactions"):
                summary["without_transactions"] += 1
            event_id = store.build_event_id(
                company_id=f"SEC_{cik}",
                document_id=document_id,
                event_type="SEC_FILING",
                source_document_sha256=str(matched[0].get("sha256") or ""),
            )
            if store.get(event_id) is None:
                store.persist(
                    event_type="SEC_FILING",
                    company_id=f"SEC_{cik}",
                    cik=cik,
                    document_id=document_id,
                    document_class=str(matched[0].get("document_class") or ""),
                    source_document_path=matched[0].get("source_path"),
                    source_document_sha256=str(matched[0].get("sha256") or ""),
                    source_event_id=None,
                    source_run_id=None,
                    event_confidence=1.0,
                    facts=facts,
                )
                summary["events_created"] += 1
            elif store.update_facts(event_id, facts):
                summary["events_updated"] += 1
    return summary