from __future__ import annotations

import hashlib
import html
import html.parser as html_parser
import importlib
import re
import xml.etree.ElementTree as ElementTree
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

pdfium = None
try:
    pdfium = importlib.import_module("pypdfium2")
except ModuleNotFoundError:
    pdfium = None


REPO_ROOT = Path(__file__).resolve().parents[3]

BRONZE_ROOTS = (
    REPO_ROOT / "data" / "bronze" / "sec" / "documents",
    REPO_ROOT / "data" / "bronze" / "documents",
)

SILVER_ROOT = REPO_ROOT / "data" / "silver" / "documents"

SUPPORTED_EXTENSIONS = {".pdf", ".txt", ".html", ".htm", ".xml"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalise_text(value: str) -> str:
    value = html.unescape(value)
    value = value.replace("\x00", "")
    value = value.replace("\r\n", "\n").replace("\r", "\n")

    # Remove excessive horizontal whitespace but preserve line structure.
    value = re.sub(r"[ \t]+", " ", value)

    # Avoid huge blank regions.
    value = re.sub(r"\n[ \t]*\n[ \t]*\n+", "\n\n", value)

    return value.strip()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def _extract_txt(path: Path) -> list[dict]:
    text = path.read_text(encoding="utf-8-sig", errors="replace")

    return [
        {
            "page_number": 1,
            "text": _normalise_text(text),
        }
    ]


class _VisibleTextParser(html_parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._skip = 0
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag in {"script", "style", "noscript", "template"}:
            self._skip += 1

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style", "noscript", "template"} and self._skip:
            self._skip -= 1

    def handle_data(self, data: str) -> None:
        if not self._skip and data.strip():
            self._parts.append(data)

    def text(self) -> str:
        return "\n".join(self._parts)


def _visible_markup_text(raw: str) -> str:
    try:
        beautifulsoup = importlib.import_module("bs4")
    except ModuleNotFoundError:
        parser = _VisibleTextParser()
        parser.feed(raw)
        return parser.text()
    soup = beautifulsoup.BeautifulSoup(raw, "html.parser")
    for element in soup(["script", "style", "noscript", "template"]):
        element.decompose()
    return soup.get_text("\n")


def _extract_html(path: Path) -> list[dict]:
    raw = path.read_text(encoding="utf-8-sig", errors="replace")
    text = _normalise_text(_visible_markup_text(raw))

    return [
        {
            "page_number": 1,
            "text": text,
        }
    ]

def _extract_xml(path: Path) -> list[dict]:
    raw = path.read_text(encoding="utf-8-sig", errors="replace")
    try:
        beautifulsoup = importlib.import_module("bs4")
    except ModuleNotFoundError:
        try:
            root = ElementTree.fromstring(raw)
        except ElementTree.ParseError:
            text = _visible_markup_text(raw)
        else:
            text = "\n".join(part for part in root.itertext() if part and part.strip())
    else:
        soup = beautifulsoup.BeautifulSoup(raw, "xml")
        text = soup.get_text("\n")
    text = _normalise_text(text)

    return [
        {
            "page_number": 1,
            "text": text,
        }
    ]


def _extract_pdf(path: Path) -> list[dict]:
    document = pdfium.PdfDocument(str(path))
    rows: list[dict] = []

    try:
        for page_number in range(len(document)):
            page = document[page_number]

            try:
                text_page = page.get_textpage()

                try:
                    text = text_page.get_text_range()
                finally:
                    text_page.close()
            finally:
                page.close()

            rows.append(
                {
                    "page_number": page_number + 1,
                    "text": _normalise_text(text or ""),
                }
            )
    finally:
        document.close()

    return rows


def _extract(path: Path) -> tuple[list[dict], str]:
    suffix = path.suffix.lower()

    if suffix == ".pdf":
        return _extract_pdf(path), "PYPDFIUM2"

    if suffix == ".txt":
        return _extract_txt(path), "PYTHON_TEXT"

    if suffix in {".html", ".htm"}:
        return _extract_html(path), "BEAUTIFULSOUP"

    if suffix == ".xml":
        return _extract_xml(path), "BEAUTIFULSOUP_XML"

    raise ValueError(
        f"Unsupported document type: {path.suffix or '<no extension>'}"
    )


def _discover_manifests(
    cik: str,
    source_id: str | None = None,
    document_id: str | None = None,
) -> list[Path]:
    cik = str(cik).zfill(10)
    manifests: list[Path] = []

    normalized_source = str(source_id or "").strip().upper()

    # Default extraction is intentionally SEC-scoped. Uploaded documents must
    # be requested explicitly with source_id=DOCUMENT; otherwise a company
    # level SEC extraction could accidentally consume unrelated uploaded
    # artifacts sharing the same CIK.
    roots = {
        "SEC": (BRONZE_ROOTS[0],),
        "SEC_FORM_D": (BRONZE_ROOTS[0],),
        "DOCUMENT": (BRONZE_ROOTS[1],),
    }.get(normalized_source, (BRONZE_ROOTS[0],))

    for root in roots:
        company_root = root / f"cik={cik}"

        if not company_root.is_dir():
            continue

        if document_id and root == BRONZE_ROOTS[1]:
            manifest = company_root / document_id / "document_manifest.parquet"
            if manifest.is_file():
                manifests.append(manifest)
            continue

        manifests.extend(company_root.rglob("document_manifest.parquet"))

    return sorted(set(manifests))


def _load_document_records(
    cik: str,
    source_id: str | None = None,
    document_id: str | None = None,
) -> list[dict]:
    records: dict[str, dict] = {}

    for manifest_path in _discover_manifests(cik, source_id, document_id):
        frame = pd.read_parquet(manifest_path)

        if frame.empty:
            continue

        required = {"document_id", "cik", "local_path", "status"}
        missing = required - set(frame.columns)

        if missing:
            raise ValueError(
                f"Document manifest missing columns "
                f"{sorted(missing)}: {manifest_path}"
            )

        for _, row in frame.iterrows():
            if document_id and str(row.get("document_id", "")).strip() != str(document_id).strip():
                continue
            row_cik = (
                str(row["cik"])
                .replace(".0", "")
                .strip()
                .zfill(10)
            )

            if row_cik != cik:
                continue

            if str(row["status"]).upper() != "DOWNLOADED":
                continue

            record_document_id = str(row["document_id"]).strip()
            local_path = str(row["local_path"]).strip()

            if not record_document_id or not local_path:
                continue

            records[record_document_id] = row.to_dict()

    return list(records.values())


def _write_document(
    *,
    record: dict,
    cik: str,
    extracted_rows: list[dict],
    extraction_method: str,
    sha256: str,
    source_path: Path,
) -> dict:
    document_id = str(record["document_id"]).strip()
    company_id = str(
        record.get("company_id") or f"SEC_{cik}"
    ).strip()

    document_root = (
        SILVER_ROOT
        / f"cik={cik}"
        / document_id
    )

    document_root.mkdir(parents=True, exist_ok=True)

    text_path = document_root / "document_text.parquet"
    metadata_path = document_root / "document_metadata.parquet"

    extracted_at = _utc_now()

    text_rows = []

    for row in extracted_rows:
        text_rows.append(
            {
                "document_id": document_id,
                "company_id": company_id,
                "cik": cik,
                "page_number": int(row["page_number"]),
                "text": str(row["text"]),
                "source_path": str(source_path.resolve()),
                "sha256": sha256,
                "extracted_at_utc": extracted_at,
                "extraction_method": extraction_method,
                "extraction_status": "SUCCESS",
            }
        )

    text_frame = pd.DataFrame(
        text_rows,
        columns=[
            "document_id",
            "company_id",
            "cik",
            "page_number",
            "text",
            "source_path",
            "sha256",
            "extracted_at_utc",
            "extraction_method",
            "extraction_status",
        ],
    )

    metadata = {
        "document_id": document_id,
        "company_id": company_id,
        "cik": cik,
        "source_path": str(source_path.resolve()),
        "source_filename": source_path.name,
        "source_extension": source_path.suffix.lower(),
        "sha256": sha256,
        "page_count": len(extracted_rows),
        "character_count": int(
            sum(len(str(row["text"])) for row in extracted_rows)
        ),
        "extraction_method": extraction_method,
        "extraction_status": "SUCCESS",
        "extracted_at_utc": extracted_at,
        "accession_number": str(record.get("accession_number", "")),
        "filing_date": str(record.get("filing_date", "")),
        "form": str(record.get("form", "")),
    }

    metadata_frame = pd.DataFrame([metadata])

    # Atomic writes.
    text_tmp = text_path.with_suffix(".parquet.tmp")
    metadata_tmp = metadata_path.with_suffix(".parquet.tmp")

    text_frame.to_parquet(text_tmp, index=False)
    metadata_frame.to_parquet(metadata_tmp, index=False)

    text_tmp.replace(text_path)
    metadata_tmp.replace(metadata_path)

    return {
        "document_id": document_id,
        "text_path": str(text_path.resolve()),
        "metadata_path": str(metadata_path.resolve()),
        "page_count": len(extracted_rows),
        "character_count": metadata["character_count"],
        "status": "EXTRACTED",
    }

def _already_extracted(
    *,
    cik: str,
    document_id: str,
    sha256: str,
) -> bool:
    root = SILVER_ROOT / f"cik={cik}" / document_id

    metadata_path = root / "document_metadata.parquet"
    text_path = root / "document_text.parquet"

    if not metadata_path.is_file() or not text_path.is_file():
        return False

    try:
        metadata = pd.read_parquet(metadata_path)

        if metadata.empty:
            return False

        row = metadata.iloc[0]

        return (
            str(row.get("sha256", "")) == sha256
            and str(row.get("extraction_status", "")).upper() == "SUCCESS"
            and int(row.get("page_count", 0)) > 0
        )
    except Exception:
        return False

def extract_documents_for_company(
    cik: str,
    *,
    force: bool = False,
    source_id: str | None = None,
    document_id: str | None = None,
) -> dict:
    cik = str(cik).replace(".0", "").strip().zfill(10)

    if not re.fullmatch(r"\d{10}", cik):
        raise ValueError(f"Invalid CIK: {cik!r}")

    records = _load_document_records(cik, source_id, document_id)

    if not records:
        raise FileNotFoundError(
            f"No DOWNLOADED document records found for CIK {cik}"
        )

    extracted = 0
    skipped = 0
    failed = 0
    results = []

    for record in records:
        document_id = str(record["document_id"]).strip()
        source_path = Path(str(record["local_path"])).expanduser()

        try:
            if not source_path.is_file():
                raise FileNotFoundError(
                    f"Bronze document not found: {source_path}"
                )

            if source_path.suffix.lower() not in SUPPORTED_EXTENSIONS:
                raise ValueError(
                    f"Unsupported document type: {source_path.suffix}"
                )

            sha256 = _sha256(source_path)

            if not force and _already_extracted(
                cik=cik,
                document_id=document_id,
                sha256=sha256,
            ):
                skipped += 1
                results.append(
                    {
                        "document_id": document_id,
                        "status": "SKIPPED",
                    }
                )
                continue

            rows, extraction_method = _extract(source_path)

            if not rows:
                raise ValueError("Extraction produced no pages")

            if not any(str(row["text"]).strip() for row in rows):
                raise ValueError("Extraction produced no text")

            result = _write_document(
                record=record,
                cik=cik,
                extracted_rows=rows,
                extraction_method=extraction_method,
                sha256=sha256,
                source_path=source_path,
            )

            extracted += 1
            results.append(result)

        except Exception as exc:
            failed += 1

            results.append(
                {
                    "document_id": document_id,
                    "status": "FAILED",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )

    if failed:
        raise RuntimeError(
            f"Document extraction failed for {failed} document(s): "
            f"{results}"
        )

    return {
        "cik": cik,
        "company_id": f"SEC_{cik}",
        "discovered": len(records),
        "extracted": extracted,
        "skipped": skipped,
        "failed": failed,
        "results": results,
    }