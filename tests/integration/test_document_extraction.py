from __future__ import annotations

import hashlib
from pathlib import Path
import uuid

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]


def _test_cik() -> str:
    return f"{uuid.uuid4().int % 10**10:010d}"


def _write_manifest_document(
    *,
    cik: str,
    document_id: str,
    filename: str,
    content: bytes,
    source_id: str = "DOCUMENT",
) -> Path:
    root_name = "sec/documents" if source_id.upper() in {"SEC", "SEC_FORM_D"} else "documents"
    root = (
        REPO_ROOT
        / "data"
        / "bronze"
        / root_name
        / f"cik={cik}"
        / document_id
    )

    root.mkdir(parents=True, exist_ok=True)

    path = root / filename
    path.write_bytes(content)

    manifest = pd.DataFrame(
        [
            {
                "document_id": document_id,
                "company_id": f"SEC_{cik}",
                "cik": cik,
                "company_name": "TEST COMPANY",
                "local_path": str(path.resolve()),
                "sha256": hashlib.sha256(content).hexdigest(),
                "status": "DOWNLOADED",
            }
        ]
    )

    manifest.to_parquet(
        root / "document_manifest.parquet",
        index=False,
    )

    return path


def test_txt_document_extraction(tmp_path):
    from company_platform.document_extraction import (
        extract_documents_for_company,
    )

    cik = _test_cik()
    document_id = "test-txt-document"

    path = _write_manifest_document(
        cik=cik,
        document_id=document_id,
        filename="notice.txt",
        content=b"Hello financial world.\nPrincipal amount: 1000000.",
    )

    result = extract_documents_for_company(cik, source_id="DOCUMENT")

    assert result["failed"] == 0
    assert result["extracted"] == 1

    output_root = (
        REPO_ROOT
        / "data"
        / "silver"
        / "documents"
        / f"cik={cik}"
        / document_id
    )

    text = pd.read_parquet(
        output_root / "document_text.parquet"
    )

    assert len(text) == 1
    assert "Hello financial world." in text.iloc[0]["text"]
    assert text.iloc[0]["cik"] == cik

    metadata = pd.read_parquet(
        output_root / "document_metadata.parquet"
    )

    assert metadata.iloc[0]["extraction_status"] == "SUCCESS"
    assert metadata.iloc[0]["page_count"] == 1

    assert path.exists()


def test_html_document_extraction():
    from company_platform.document_extraction import (
        extract_documents_for_company,
    )

    cik = _test_cik()
    document_id = "test-html-document"

    _write_manifest_document(
        cik=cik,
        document_id=document_id,
        filename="notice.html",
        content=b"""
        <html>
          <head><title>Test</title></head>
          <body>
            <h1>Loan Notice</h1>
            <p>Principal amount: 5000000.</p>
            <script>ignore_this_script()</script>
          </body>
        </html>
        """,
    )

    result = extract_documents_for_company(cik, source_id="DOCUMENT")

    assert result["failed"] == 0
    assert result["extracted"] == 1

    output_root = (
        REPO_ROOT
        / "data"
        / "silver"
        / "documents"
        / f"cik={cik}"
        / document_id
    )

    text = pd.read_parquet(
        output_root / "document_text.parquet"
    )

    extracted = "\n".join(text["text"].astype(str))

    assert "Loan Notice" in extracted
    assert "Principal amount: 5000000." in extracted
    assert "ignore_this_script" not in extracted


def test_xml_document_extraction():
    from company_platform.document_extraction import (
        extract_documents_for_company,
    )

    cik = _test_cik()
    document_id = "test-xml-document"

    _write_manifest_document(
        cik=cik,
        document_id=document_id,
        filename="notice.xml",
        content=(
            b"<filing><issuer>Example Corp</issuer>"
            b"<amount>1000000</amount></filing>"
        ),
    )

    result = extract_documents_for_company(cik, source_id="DOCUMENT")

    assert result["failed"] == 0
    assert result["extracted"] == 1

    output_root = (
        REPO_ROOT
        / "data"
        / "silver"
        / "documents"
        / f"cik={cik}"
        / document_id
    )
    text = pd.read_parquet(output_root / "document_text.parquet")

    assert "Example Corp" in text.iloc[0]["text"]
    assert "1000000" in text.iloc[0]["text"]


def test_document_extraction_is_idempotent():
    from company_platform.document_extraction import (
        extract_documents_for_company,
    )

    cik = _test_cik()
    document_id = "test-idempotent-document"

    _write_manifest_document(
        cik=cik,
        document_id=document_id,
        filename="notice.txt",
        content=b"Idempotent document.",
    )

    first = extract_documents_for_company(cik, source_id="DOCUMENT")

    assert first["extracted"] == 1
    assert first["skipped"] == 0

    second = extract_documents_for_company(cik, source_id="DOCUMENT")

    assert second["extracted"] == 0
    assert second["skipped"] == 1
    assert second["failed"] == 0


def test_default_extraction_is_sec_scoped():
    from company_platform.document_extraction import (
        extract_documents_for_company,
    )

    cik = _test_cik()

    _write_manifest_document(
        cik=cik,
        document_id="sec-document",
        filename="sec.txt",
        content=b"SEC_ONLY",
        source_id="SEC",
    )

    # Simulate an uploaded-document artifact for the same company.
    document_root = (
        REPO_ROOT
        / "data"
        / "bronze"
        / "documents"
        / f"cik={cik}"
        / "uploaded-document"
    )
    document_root.mkdir(parents=True, exist_ok=True)
    uploaded = document_root / "uploaded.txt"
    uploaded.write_bytes(b"UPLOADED_ONLY")
    pd.DataFrame([
        {
            "document_id": "uploaded-document",
            "company_id": f"SEC_{cik}",
            "cik": cik,
            "local_path": str(uploaded.resolve()),
            "sha256": hashlib.sha256(uploaded.read_bytes()).hexdigest(),
            "status": "DOWNLOADED",
        }
    ]).to_parquet(document_root / "document_manifest.parquet", index=False)

    result = extract_documents_for_company(cik)

    assert result["failed"] == 0
    assert result["extracted"] == 1
    assert {row["document_id"] for row in result["results"]} == {"sec-document"}


def test_document_extraction_company_isolation():
    from company_platform.document_extraction import (
        extract_documents_for_company,
    )

    cik_a = "9999999994"
    cik_b = "9999999995"

    _write_manifest_document(
        cik=cik_a,
        document_id="company-a-document",
        filename="a.txt",
        content=b"COMPANY_A_SECRET",
    )

    _write_manifest_document(
        cik=cik_b,
        document_id="company-b-document",
        filename="b.txt",
        content=b"COMPANY_B_SECRET",
    )

    result = extract_documents_for_company(cik_a, source_id="DOCUMENT")

    assert result["failed"] == 0

    output_a = (
        REPO_ROOT
        / "data"
        / "silver"
        / "documents"
        / f"cik={cik_a}"
    )

    output_b = (
         REPO_ROOT
    / "data"
    / "silver"
    / "documents"
    / f"cik={cik_b}"
    )

    assert (
        output_a
        / "company-a-document"
        / "document_text.parquet"
    ).is_file()

    assert not (
        output_b
        / "company-b-document"
        / "document_text.parquet"
    ).exists()