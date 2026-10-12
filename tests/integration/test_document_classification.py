from __future__ import annotations

from pathlib import Path
import uuid

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]


def _test_cik() -> str:
    return f"{uuid.uuid4().int % 10**10:010d}"


def _write_extracted_document(
    *,
    cik: str,
    document_id: str,
    filename: str,
    extension: str,
    text: str,
    form: str = "",
):
    root = (
        REPO_ROOT
        / "data"
        / "silver"
        / "documents"
        / f"cik={cik}"
        / document_id
    )

    root.mkdir(
        parents=True,
        exist_ok=True,
    )

    source_path = (
        REPO_ROOT
        / "data"
        / "test"
        / "integration"
        / f"{document_id}{extension}"
    )

    source_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    source_path.write_text(
        text,
        encoding="utf-8",
    )

    text_frame = pd.DataFrame(
        [
            {
                "document_id": document_id,
                "company_id": f"SEC_{cik}",
                "cik": cik,
                "page_number": 1,
                "text": text,
                "source_path": str(source_path.resolve()),
                "sha256": "test-sha256",
                "extracted_at_utc": "2026-01-01T00:00:00+00:00",
                "extraction_method": "TEST",
                "extraction_status": "SUCCESS",
            }
        ]
    )

    text_frame.to_parquet(
        root / "document_text.parquet",
        index=False,
    )

    metadata = pd.DataFrame(
        [
            {
                "document_id": document_id,
                "company_id": f"SEC_{cik}",
                "cik": cik,
                "source_path": str(source_path.resolve()),
                "source_filename": filename,
                "source_extension": extension,
                "sha256": "test-sha256",
                "page_count": 1,
                "character_count": len(text),
                "extraction_method": "TEST",
                "extraction_status": "SUCCESS",
                "extracted_at_utc": "2026-01-01T00:00:00+00:00",
                "accession_number": "",
                "filing_date": "",
                "form": form,
            }
        ]
    )

    metadata.to_parquet(
        root / "document_metadata.parquet",
        index=False,
    )


def test_classifies_sec_form_d():
    from company_platform.document_classification import (
        classify_documents_for_company,
    )

    cik = _test_cik()

    _write_extracted_document(
        cik=cik,
        document_id="form-d-001",
        filename="filing.xml",
        extension=".xml",
        text="SEC Form D filing",
        form="D",
    )

    result = classify_documents_for_company(cik)

    assert result["failed"] == 0
    assert result["classified"] == 1

    output = (
        REPO_ROOT
        / "data"
        / "silver"
        / "documents"
        / f"cik={cik}"
        / "form-d-001"
        / "document_classification.parquet"
    )

    frame = pd.read_parquet(output)

    assert frame.iloc[0]["document_class"] == "SEC_FORM_D"
    assert (
        frame.iloc[0]["classification_method"]
        == "SEC_FORM_METADATA"
    )


def test_classifies_sec_10k_from_metadata():
    from company_platform.document_classification import (
        classify_documents_for_company,
    )

    cik = _test_cik()

    _write_extracted_document(
        cik=cik,
        document_id="10k-001",
        filename="annual.htm",
        extension=".html",
        text="Annual report",
        form="10-K",
    )

    result = classify_documents_for_company(cik)

    assert result["classified"] == 1

    output = (
        REPO_ROOT
        / "data"
        / "silver"
        / "documents"
        / f"cik={cik}"
        / "10k-001"
        / "document_classification.parquet"
    )

    frame = pd.read_parquet(output)

    assert frame.iloc[0]["document_class"] == "SEC_10_K"


def test_classifies_defa14a_as_sec_other():
    from company_platform.document_classification import (
        classify_documents_for_company,
    )

    cik = _test_cik()
    _write_extracted_document(
        cik=cik,
        document_id="defa14a-001",
        filename="defa14a.htm",
        extension=".html",
        text="Definitive additional proxy statement",
        form="DEFA14A",
    )

    result = classify_documents_for_company(cik)
    assert result["failed"] == 0

    output = (
        REPO_ROOT
        / "data"
        / "silver"
        / "documents"
        / f"cik={cik}"
        / "defa14a-001"
        / "document_classification.parquet"
    )
    frame = pd.read_parquet(output)
    assert frame.iloc[0]["document_class"] == "SEC_OTHER"


def test_classifies_amended_form_d_from_metadata():
    from company_platform.document_classification import (
        classify_documents_for_company,
    )

    cik = _test_cik()
    _write_extracted_document(
        cik=cik,
        document_id="form-d-amended-001",
        filename="form-d-amended.xml",
        extension=".xml",
        text="Amended Form D filing",
        form="D/A",
    )

    result = classify_documents_for_company(cik)
    assert result["failed"] == 0

    output = (
        REPO_ROOT
        / "data"
        / "silver"
        / "documents"
        / f"cik={cik}"
        / "form-d-amended-001"
        / "document_classification.parquet"
    )
    frame = pd.read_parquet(output)
    assert frame.iloc[0]["document_class"] == "SEC_FORM_D"


def test_classifies_pdf_by_extension():
    from company_platform.document_classification import (
        classify_documents_for_company,
    )

    cik = _test_cik()

    _write_extracted_document(
        cik=cik,
        document_id="pdf-001",
        filename="notice.pdf",
        extension=".pdf",
        text="Generic document text",
    )

    result = classify_documents_for_company(cik)

    assert result["classified"] == 1

    output = (
        REPO_ROOT
        / "data"
        / "silver"
        / "documents"
        / f"cik={cik}"
        / "pdf-001"
        / "document_classification.parquet"
    )

    frame = pd.read_parquet(output)

    assert frame.iloc[0]["document_class"] == "PDF_DOCUMENT"


def test_classification_is_idempotent():
    from company_platform.document_classification import (
        classify_documents_for_company,
    )

    cik = _test_cik()

    _write_extracted_document(
        cik=cik,
        document_id="idem-001",
        filename="notice.txt",
        extension=".txt",
        text="Generic text",
    )

    first = classify_documents_for_company(cik)

    assert first["classified"] == 1
    assert first["skipped"] == 0

    second = classify_documents_for_company(cik)

    assert second["classified"] == 0
    assert second["skipped"] == 1


def test_classification_company_isolation():
    from company_platform.document_classification import (
        classify_documents_for_company,
    )

    cik_a = _test_cik()
    cik_b = _test_cik()

    _write_extracted_document(
        cik=cik_a,
        document_id="company-a-001",
        filename="a.txt",
        extension=".txt",
        text="Company A",
    )

    _write_extracted_document(
        cik=cik_b,
        document_id="company-b-001",
        filename="b.txt",
        extension=".txt",
        text="Company B",
    )

    result = classify_documents_for_company(cik_a)

    assert result["classified"] == 1

    output_a = (
        REPO_ROOT
        / "data"
        / "silver"
        / "documents"
        / f"cik={cik_a}"
        / "company-a-001"
        / "document_classification.parquet"
    )

    output_b = (
        REPO_ROOT
    / "data"
    / "silver"
    / "documents"
    / f"cik={cik_b}"
    / "company-b-001"
    / "document_classification.parquet"
    )

    assert output_a.is_file()
    assert not output_b.exists()