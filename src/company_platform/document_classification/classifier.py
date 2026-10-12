from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[3]

SILVER_ROOT = REPO_ROOT / "data" / "silver" / "documents"


CLASSIFICATIONS = {
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


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalise(value) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def _classify_sec_form(form: str) -> str | None:
    form = _normalise(form)

    if not form:
        return None

    # Handle amended SEC forms as the same document family.
    base_form = form.split("/")[0].strip()

    if base_form == "10-K":
        return "SEC_10_K"

    if base_form == "10-Q":
        return "SEC_10_Q"

    if base_form == "8-K":
        return "SEC_8_K"

    if base_form == "D":
        return "SEC_FORM_D"

    return "SEC_OTHER"


def _classify_filename(filename: str) -> str | None:
    value = _normalise(filename)

    if not value:
        return None

    if re.search(r"(^|[^A-Z])10[-_ ]?K([^A-Z]|$)", value):
        return "SEC_10_K"

    if re.search(r"(^|[^A-Z])10[-_ ]?Q([^A-Z]|$)", value):
        return "SEC_10_Q"

    if re.search(r"(^|[^A-Z])8[-_ ]?K([^A-Z]|$)", value):
        return "SEC_8_K"

    if re.search(r"FORM[-_ ]?D|FORMD", value):
        return "SEC_FORM_D"

    return None


def _classify_text(text: str) -> str | None:
    value = _normalise(text)

    if not value:
        return None

    # Only classify when strong document-level indicators exist.
    if re.search(r"\bFORM\s+D\b", value):
        return "SEC_FORM_D"

    if re.search(
        r"\bUNITED STATES SECURITIES AND EXCHANGE COMMISSION\b"
        r".{0,500}\b10-K\b",
        value,
        flags=re.DOTALL,
    ):
        return "SEC_10_K"

    if re.search(
        r"\bUNITED STATES SECURITIES AND EXCHANGE COMMISSION\b"
        r".{0,500}\b10-Q\b",
        value,
        flags=re.DOTALL,
    ):
        return "SEC_10_Q"

    if re.search(
        r"\bUNITED STATES SECURITIES AND EXCHANGE COMMISSION\b"
        r".{0,500}\b8-K\b",
        value,
        flags=re.DOTALL,
    ):
        return "SEC_8_K"

    return None


def _extension_classification(source_extension: str) -> str:
    extension = str(source_extension or "").strip().lower()

    if extension == ".pdf":
        return "PDF_DOCUMENT"

    if extension == ".txt":
        return "TEXT_DOCUMENT"

    if extension in {".html", ".htm"}:
        return "HTML_DOCUMENT"

    if extension == ".xml":
        return "OTHER"

    return "OTHER"


def _classify_metadata(row: pd.Series) -> tuple[str, str, float]:
    """
    Return:
        classification,
        method,
        confidence
    """

    form_class = _classify_sec_form(row.get("form", ""))

    if form_class:
        return form_class, "SEC_FORM_METADATA", 1.0

    filename_class = _classify_filename(
        row.get("source_filename", "")
    )

    if filename_class:
        return filename_class, "FILENAME_RULE", 0.90

    return (
        _extension_classification(
            row.get("source_extension", "")
        ),
        "FILE_EXTENSION",
        0.70,
    )


def _load_metadata(cik: str) -> list[tuple[Path, pd.DataFrame]]:
    root = SILVER_ROOT / f"cik={cik}"

    if not root.is_dir():
        raise FileNotFoundError(
            f"Document Silver root not found: {root}"
        )

    results = []

    for metadata_path in sorted(
        root.glob("*/document_metadata.parquet")
    ):
        frame = pd.read_parquet(metadata_path)

        if frame.empty:
            continue

        required = {
            "document_id",
            "company_id",
            "cik",
            "source_path",
            "source_filename",
            "source_extension",
            "extraction_status",
        }

        missing = required - set(frame.columns)

        if missing:
            raise ValueError(
                f"Document metadata missing columns "
                f"{sorted(missing)}: {metadata_path}"
            )

        results.append((metadata_path, frame))

    return results


def _load_document_text(
    cik: str,
    document_id: str,
) -> str:
    path = (
        SILVER_ROOT
        / f"cik={cik}"
        / document_id
        / "document_text.parquet"
    )

    if not path.is_file():
        return ""

    frame = pd.read_parquet(path)

    if frame.empty or "text" not in frame.columns:
        return ""

    return "\n".join(
        frame["text"].fillna("").astype(str)
    )


def _classification_path(
    cik: str,
    document_id: str,
) -> Path:
    return (
        SILVER_ROOT
        / f"cik={cik}"
        / document_id
        / "document_classification.parquet"
    )


def _already_classified(
    cik: str,
    document_id: str,
    sha256: str,
) -> bool:
    path = _classification_path(cik, document_id)

    if not path.is_file():
        return False

    try:
        frame = pd.read_parquet(path)

        if frame.empty:
            return False

        row = frame.iloc[0]

        return (
            str(row.get("sha256", "")) == sha256
            and str(row.get("classification_status", "")).upper()
            == "SUCCESS"
            and str(row.get("document_class", "")).strip()
            in CLASSIFICATIONS
        )
    except Exception:
        return False


def _write_classification(
    *,
    row: pd.Series,
    classification: str,
    method: str,
    confidence: float,
    text_used: bool,
) -> Path:
    cik = (
        str(row["cik"])
        .replace(".0", "")
        .strip()
        .zfill(10)
    )

    document_id = str(row["document_id"]).strip()

    path = _classification_path(cik, document_id)

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    output = pd.DataFrame(
        [
            {
                "document_id": document_id,
                "company_id": str(row["company_id"]),
                "cik": cik,
                "document_class": classification,
                "classification_method": method,
                "classification_confidence": float(confidence),
                "classification_status": "SUCCESS",
                "sha256": str(row["sha256"]),
                "source_path": str(row["source_path"]),
                "source_filename": str(row["source_filename"]),
                "source_extension": str(row["source_extension"]),
                "source_form": str(row.get("form", "")),
                "text_used": bool(text_used),
                "classified_at_utc": _utc_now(),
            }
        ]
    )

    temp = path.with_suffix(".parquet.tmp")

    output.to_parquet(
        temp,
        index=False,
    )

    temp.replace(path)

    return path


def classify_documents_for_company(
    cik: str,
    *,
    force: bool = False,
    document_id: str | None = None,
) -> dict:
    cik = (
        str(cik)
        .replace(".0", "")
        .strip()
        .zfill(10)
    )

    metadata_records = _load_metadata(cik)

    if not metadata_records:
        raise FileNotFoundError(
            f"No document metadata found for CIK {cik}"
        )

    classified = 0
    skipped = 0
    failed = 0
    results = []

    for _, metadata in metadata_records:
        for _, row in metadata.iterrows():
            current_document_id = str(row["document_id"]).strip()
            if document_id and current_document_id != str(document_id).strip():
                continue
            sha256 = str(row["sha256"]).strip()

            try:
                if (
                    not force
                    and _already_classified(
                        cik,
                        current_document_id,
                        sha256,
                    )
                ):
                    skipped += 1

                    results.append(
                        {
                            "document_id": current_document_id,
                            "status": "SKIPPED",
                        }
                    )

                    continue

                classification, method, confidence = (
                    _classify_metadata(row)
                )

                text_used = False

                # Extension-only classification is the fallback.
                # Use extracted text only when metadata/filename
                # cannot establish a more specific SEC class.
                if classification in {
                    "PDF_DOCUMENT",
                    "TEXT_DOCUMENT",
                    "HTML_DOCUMENT",
                    "OTHER",
                }:
                    text = _load_document_text(
                        cik,
                        current_document_id,
                    )

                    text_class = _classify_text(text)

                    if text_class:
                        classification = text_class
                        method = "TEXT_RULE"
                        confidence = 0.85
                        text_used = True

                path = _write_classification(
                    row=row,
                    classification=classification,
                    method=method,
                    confidence=confidence,
                    text_used=text_used,
                )

                classified += 1

                results.append(
                    {
                        "document_id": current_document_id,
                        "document_class": classification,
                        "classification_method": method,
                        "status": "CLASSIFIED",
                        "path": str(path.resolve()),
                    }
                )

            except Exception as exc:
                failed += 1

                results.append(
                    {
                        "document_id": current_document_id,
                        "status": "FAILED",
                        "error": (
                            f"{type(exc).__name__}: {exc}"
                        ),
                    }
                )

    if failed:
        raise RuntimeError(
            f"Document classification failed for "
            f"{failed} document(s): {results}"
        )

    return {
        "cik": cik,
        "company_id": f"SEC_{cik}",
        "discovered": sum(
            len(frame)
            for _, frame in metadata_records
        ),
        "classified": classified,
        "skipped": skipped,
        "failed": failed,
        "results": results,
    }