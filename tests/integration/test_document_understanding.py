from pathlib import Path

import pandas as pd

from company_platform.document_understanding import (
    understand_documents_for_company,
)
from company_platform.document_understanding.understander import (
    _extract_form4_html,
)


APPLE_CIK = "0000320193"
MICROSOFT_CIK = "0000789019"


def _write_document(
    tmp_path: Path,
    *,
    cik: str,
    document_id: str,
    document_class: str,
    text: str,
    sha256: str = "test-sha-001",
):
    """
    Build a minimal Silver document fixture:

        document_text.parquet
        document_metadata.parquet
        document_classification.parquet
    """

    document_dir = (
        tmp_path
        / "silver"
        / "documents"
        / f"cik={cik}"
        / document_id
    )

    document_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    metadata = pd.DataFrame(
        [
            {
                "document_id": document_id,
                "company_id": f"SEC_{cik}",
                "cik": cik,
                "source_path": (
                    f"/bronze/{document_id}/document.pdf"
                ),
                "source_filename": "document.pdf",
                "source_extension": ".pdf",
                "extraction_status": "SUCCESS",
                "sha256": sha256,
            }
        ]
    )

    classification = pd.DataFrame(
        [
            {
                "document_id": document_id,
                "company_id": f"SEC_{cik}",
                "cik": cik,
                "document_class": document_class,
                "classification_method": "TEST",
                "classification_confidence": 1.0,
                "classification_status": "SUCCESS",
                "sha256": sha256,
            }
        ]
    )

    document_text = pd.DataFrame(
        [
            {
                "document_id": document_id,
                "company_id": f"SEC_{cik}",
                "cik": cik,
                "page_number": 1,
                "text": text,
                "sha256": sha256,
            }
        ]
    )

    metadata.to_parquet(
        document_dir / "document_metadata.parquet",
        index=False,
    )

    classification.to_parquet(
        document_dir / "document_classification.parquet",
        index=False,
    )

    document_text.to_parquet(
        document_dir / "document_text.parquet",
        index=False,
    )

    return document_dir


def test_understands_sec_form_d(tmp_path, monkeypatch):
    """
    SEC_FORM_D should produce a confirmed
    SEC_FORM_D_FILING structured event.
    """

    document_dir = _write_document(
        tmp_path,
        cik=APPLE_CIK,
        document_id="FORM-D-001",
        document_class="SEC_FORM_D",
        text="""
        FORM D

        ISSUER NAME: Example Technologies Inc.

        FILED DATE: 09/15/2026

        TOTAL OFFERING AMOUNT: $10,000,000

        TOTAL AMOUNT SOLD: $4,000,000
        """,
    )

    monkeypatch.setattr(
        "company_platform.document_understanding.understander.SILVER_ROOT",
        tmp_path / "silver" / "documents",
    )

    result = understand_documents_for_company(
        APPLE_CIK
    )

    assert result["discovered"] == 1
    assert result["understood"] == 1
    assert result["failed"] == 0

    output_path = (
        document_dir
        / "document_understanding.parquet"
    )

    assert output_path.is_file()

    output = pd.read_parquet(output_path)

    assert len(output) == 1

    row = output.iloc[0]

    assert row["document_class"] == "SEC_FORM_D"
    assert row["event_type"] == "SEC_FORM_D_FILING"
    assert row["event_status"] == "CONFIRMED"
    assert row["understanding_status"] == "SUCCESS"

    assert "Example Technologies Inc." in row["facts_json"]
    assert "10,000,000" in row["facts_json"]
    assert "4,000,000" in row["facts_json"]


def test_extracts_explicit_form_d_facts(tmp_path, monkeypatch):
    """
    Explicitly supported Form D facts should be extracted
    without inference.
    """

    document_dir = _write_document(
        tmp_path,
        cik=APPLE_CIK,
        document_id="FORM-D-002",
        document_class="SEC_FORM_D",
        text="""
        FORM D

        ISSUER NAME: Alpha Ventures Inc.

        FILED DATE: 08/20/2026

        TOTAL OFFERING AMOUNT: $25,000,000

        TOTAL AMOUNT SOLD: $7,500,000
        """,
        sha256="test-sha-002",
    )

    monkeypatch.setattr(
        "company_platform.document_understanding.understander.SILVER_ROOT",
        tmp_path / "silver" / "documents",
    )

    result = understand_documents_for_company(
        APPLE_CIK
    )

    assert result["failed"] == 0
    assert result["understood"] == 1

    row = result["results"][0]

    assert row["status"] == "SUCCESS"

    assert row["facts"]["issuer_name"] == (
        "Alpha Ventures Inc."
    )

    assert row["facts"]["filing_date"] == (
        "08/20/2026"
    )

    assert row["facts"]["total_offering_amount"] == (
        25000000.0
    )

    assert row["facts"]["total_amount_sold"] == (
        7500000.0
    )


def test_understands_sec_10k(tmp_path, monkeypatch):
    """
    A classified 10-K should produce a generic
    SEC_FILING event.
    """

    document_dir = _write_document(
        tmp_path,
        cik=APPLE_CIK,
        document_id="10K-001",
        document_class="SEC_10_K",
        text="""
        UNITED STATES SECURITIES AND EXCHANGE COMMISSION

        FORM 10-K

        FILED DATE: 02/15/2026

        Annual Report for the fiscal year ended
        December 31, 2025.
        """,
        sha256="test-sha-003",
    )

    monkeypatch.setattr(
        "company_platform.document_understanding.understander.SILVER_ROOT",
        tmp_path / "silver" / "documents",
    )

    result = understand_documents_for_company(
        APPLE_CIK
    )

    assert result["discovered"] == 1
    assert result["understood"] == 1
    assert result["failed"] == 0

    output = pd.read_parquet(
        document_dir
        / "document_understanding.parquet"
    )

    row = output.iloc[0]

    assert row["document_class"] == "SEC_10_K"
    assert row["event_type"] == "SEC_FILING"
    assert row["event_status"] == "CONFIRMED"
    assert row["understanding_status"] == "SUCCESS"


def test_unsupported_pdf_produces_no_supported_facts(
    tmp_path,
    monkeypatch,
):
    """
    A generic PDF with no supported deterministic
    facts must not produce a fabricated event.
    """

    document_dir = _write_document(
        tmp_path,
        cik=APPLE_CIK,
        document_id="GENERIC-001",
        document_class="PDF_DOCUMENT",
        text="""
        This is a generic business document.

        It contains operational information but
        no supported SEC filing facts.
        """,
        sha256="test-sha-004",
    )

    monkeypatch.setattr(
        "company_platform.document_understanding.understander.SILVER_ROOT",
        tmp_path / "silver" / "documents",
    )

    result = understand_documents_for_company(
        APPLE_CIK
    )

    assert result["discovered"] == 1
    assert result["understood"] == 1
    assert result["failed"] == 0

    output = pd.read_parquet(
        document_dir
        / "document_understanding.parquet"
    )

    row = output.iloc[0]

    assert row["event_type"] is None
    assert row["event_status"] is None
    assert row["understanding_status"] == (
        "NO_SUPPORTED_FACTS"
    )


def test_understanding_is_idempotent(
    tmp_path,
    monkeypatch,
):
    """
    Running understanding twice with the same SHA
    should skip the second run.
    """

    document_dir = _write_document(
        tmp_path,
        cik=APPLE_CIK,
        document_id="IDEMPOTENT-001",
        document_class="SEC_FORM_D",
        text="""
        FORM D

        ISSUER NAME: Idempotent Holdings Inc.

        FILED DATE: 09/01/2026
        """,
        sha256="test-sha-005",
    )

    monkeypatch.setattr(
        "company_platform.document_understanding.understander.SILVER_ROOT",
        tmp_path / "silver" / "documents",
    )

    first = understand_documents_for_company(
        APPLE_CIK
    )

    second = understand_documents_for_company(
        APPLE_CIK
    )

    assert first["understood"] == 1
    assert first["skipped"] == 0

    assert second["understood"] == 0
    assert second["skipped"] == 1
    assert second["failed"] == 0

    assert (
        document_dir
        / "document_understanding.parquet"
    ).is_file()


def test_understanding_is_company_isolated(
    tmp_path,
    monkeypatch,
):
    """
    A company's understanding run must not process
    another company's Silver documents.
    """

    apple_dir = _write_document(
        tmp_path,
        cik=APPLE_CIK,
        document_id="APPLE-DOC-001",
        document_class="SEC_FORM_D",
        text="""
        FORM D

        ISSUER NAME: Apple Test Issuer

        FILED DATE: 09/05/2026
        """,
        sha256="apple-sha",
    )

    microsoft_dir = _write_document(
        tmp_path,
        cik=MICROSOFT_CIK,
        document_id="MSFT-DOC-001",
        document_class="SEC_FORM_D",
        text="""
        FORM D

        ISSUER NAME: Microsoft Test Issuer

        FILED DATE: 09/06/2026
        """,
        sha256="microsoft-sha",
    )

    monkeypatch.setattr(
        "company_platform.document_understanding.understander.SILVER_ROOT",
        tmp_path / "silver" / "documents",
    )

    result = understand_documents_for_company(
        APPLE_CIK
    )

    assert result["discovered"] == 1
    assert result["understood"] == 1
    assert result["failed"] == 0

    assert (
        apple_dir
        / "document_understanding.parquet"
    ).is_file()

    assert not (
        microsoft_dir
        / "document_understanding.parquet"
    ).exists()


def test_form4_html_keeps_stated_transaction_fields():
    raw = """
    <html><body>
    <table><tr>
      <td>1. Name and Address of Reporting Person*
        <a href="/cgi-bin/browse-edgar?action=getcompany&amp;CIK=0001780525">Newstead Jennifer</a>
      </td>
      <td>5. Relationship of Reporting Person(s) to Issuer
        <table>
          <tr><td></td><td>Director</td><td></td><td>10% Owner</td></tr>
          <tr><td><span class="FormData">X</span></td><td>Officer (give title below)</td><td></td><td>Other (specify below)</td></tr>
          <tr><td></td><td>SVP, GC and Government Affairs</td><td></td><td></td></tr>
        </table>
      </td>
    </tr></table>
    <table>
      <thead>
        <tr><th>Table I - Non-Derivative Securities Acquired, Disposed of, or Beneficially Owned</th></tr>
        <tr><th>1. Title of Security</th><th>Transaction Date</th><th>Transaction Code</th></tr>
      </thead>
      <tbody>
        <tr>
          <td><span class="FormData">Common Stock</span></td>
          <td><span class="FormData">10/06/2026</span></td>
          <td></td>
          <td><span class="SmallFormData">S</span></td>
          <td></td>
          <td><span class="FormData">2,399</span></td>
          <td><span class="FormData">D</span></td>
          <td><span class="FormData">332.01</span></td>
          <td><span class="FormData">39,593</span></td>
          <td><span class="FormData">D</span></td>
          <td></td>
        </tr>
      </tbody>
    </table>
    </body></html>
    """
    facts = _extract_form4_html(raw)
    assert facts["reporting_owner_name"] == "Newstead Jennifer"
    assert facts["reporting_owner_relationship"] == "Officer"
    assert facts["reporting_owner_title"] == "SVP, GC and Government Affairs"
    assert "ownership_percentage" not in facts
    transaction = facts["non_derivative_transactions"][0]
    assert transaction["transaction_code"] == "S"
    assert transaction["acquired_or_disposed"] == "D"
    assert transaction["shares"] == 2399
    assert transaction["price"] == 332.01
    assert transaction["shares_owned_following"] == 39593
    assert "ownership_percentage" not in transaction