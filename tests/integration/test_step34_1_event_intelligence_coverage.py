import sqlite3

import pandas as pd

from company_platform.integration_v2 import components
from company_platform.multi_company import event_intelligence_coverage as coverage


CIK = "0000789019"
COMPANY_ID = f"SEC_{CIK}"
DOCUMENT_ID = "DOC-34-1"


def _database(path, table, columns, rows=()):
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as connection:
        connection.execute(
            f"CREATE TABLE {table} ({', '.join(columns)})"
        )
        if rows:
            placeholders = ", ".join("?" for _ in columns)
            connection.executemany(
                f"INSERT INTO {table} VALUES ({placeholders})",
                rows,
            )


def _configure_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(coverage, "FILINGS_ROOT", tmp_path / "filings")
    monkeypatch.setattr(
        coverage, "FUNDING_EVENTS_PATH", tmp_path / "funding_events.parquet"
    )
    monkeypatch.setattr(
        coverage, "BRONZE_DOCUMENTS_ROOT", tmp_path / "bronze" / "documents"
    )
    monkeypatch.setattr(coverage, "DOCUMENTS_ROOT", tmp_path / "silver" / "documents")
    monkeypatch.setattr(coverage, "CONTROL_DB", tmp_path / "platform.db")
    monkeypatch.setattr(
        coverage, "STRUCTURED_EVENTS_DB", tmp_path / "structured_events.db"
    )
    monkeypatch.setattr(coverage, "COMPANY_STATE_DB", tmp_path / "state.db")
    monkeypatch.setattr(
        coverage, "INVESTMENT_SIGNALS_DB", tmp_path / "signals.db"
    )
    monkeypatch.setattr(
        coverage, "DECISION_SIGNALS_DB", tmp_path / "decisions.db"
    )
    monkeypatch.setattr(
        coverage, "COMPANY_REGISTRY", tmp_path / "registry.parquet"
    )
    monkeypatch.setattr(
        coverage, "UNIFIED_OUTPUT_ROOT", tmp_path / "unified"
    )
    monkeypatch.setattr(
        coverage,
        "UNIFIED_VIEW_PATH",
        tmp_path / "unified" / "unified_company_view" / "unified_company_view.parquet",
    )


def _create_filing_fixture(tmp_path, monkeypatch, *, include_documents, persist_event):
    _configure_paths(tmp_path, monkeypatch)
    filing_dir = coverage.FILINGS_ROOT / f"cik={CIK}"
    filing_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "cik": CIK,
                "accession_number": "0000000000-26-000001",
                "filing_date": "2026-09-01",
            }
        ]
    ).to_parquet(filing_dir / "filings.parquet", index=False)
    pd.DataFrame([{"cik": CIK}]).to_parquet(
        coverage.COMPANY_REGISTRY,
        index=False,
    )
    coverage.UNIFIED_VIEW_PATH.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"cik": CIK}]).to_parquet(
        coverage.UNIFIED_VIEW_PATH,
        index=False,
    )

    local_path = tmp_path / "bronze" / "filing.htm"
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text("SEC filing", encoding="utf-8")
    manifest_dir = coverage.BRONZE_DOCUMENTS_ROOT / f"cik={CIK}"
    manifest_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "document_id": DOCUMENT_ID,
                "status": "DOWNLOADED",
                "local_path": str(local_path),
            }
        ]
    ).to_parquet(manifest_dir / "document_manifest.parquet", index=False)

    structured_rows = []
    if include_documents:
        doc_dir = coverage.DOCUMENTS_ROOT / f"cik={CIK}" / DOCUMENT_ID
        doc_dir.mkdir(parents=True)
        for artifact in coverage.DOCUMENT_ARTIFACTS:
            row = {
                "document_id": DOCUMENT_ID,
                "company_id": COMPANY_ID,
                "cik": CIK,
            }
            if artifact == "document_metadata.parquet":
                row["source_path"] = (
                    f"data/bronze/sec/documents/cik={CIK}/{DOCUMENT_ID}/filing.htm"
                )
            if artifact == "document_understanding.parquet":
                row["event_type"] = "SEC_FILING"
            pd.DataFrame([row]).to_parquet(doc_dir / artifact, index=False)
        if persist_event:
            structured_rows.append(
                (
                    "EV-1",
                    "SEC_FILING",
                    "PERSISTED",
                    COMPANY_ID,
                    DOCUMENT_ID,
                    "PRODUCTION",
                )
            )

    _database(
        coverage.STRUCTURED_EVENTS_DB,
        "structured_events",
        [
            "event_id TEXT",
            "event_type TEXT",
            "status TEXT",
            "company_id TEXT",
            "document_id TEXT",
            "source_environment TEXT",
        ],
        structured_rows,
    )
    _database(
        coverage.CONTROL_DB,
        "events",
        ["event_id TEXT", "company_id TEXT"],
    )
    _database(
        coverage.COMPANY_STATE_DB,
        "company_state",
        ["company_id TEXT", "status TEXT"],
    )
    _database(
        coverage.INVESTMENT_SIGNALS_DB,
        "investment_signals",
        ["company_id TEXT"],
    )


def test_coverage_flags_filings_without_document_artifacts(tmp_path, monkeypatch):
    _create_filing_fixture(
        tmp_path,
        monkeypatch,
        include_documents=False,
        persist_event=False,
    )

    row = coverage.build_coverage_matrix({CIK: "Microsoft"}).iloc[0]

    assert row["sec_filing_count"] == 1
    assert row["downloaded_document_count"] == 1
    assert row["missing_document_artifact_count"] == 1
    assert row["coverage_status"] == "DOCUMENTS_INCOMPLETE"


def test_coverage_requires_persisted_events_for_understood_documents(
    tmp_path, monkeypatch
):
    _create_filing_fixture(
        tmp_path,
        monkeypatch,
        include_documents=True,
        persist_event=False,
    )

    row = coverage.build_coverage_matrix({CIK: "Microsoft"}).iloc[0]

    assert row["expected_structured_event_document_count"] == 1
    assert row["missing_structured_event_document_count"] == 1
    assert row["coverage_status"] == "STRUCTURED_EVENTS_INCOMPLETE"


def test_complete_document_coverage_allows_no_form_d_state_or_signals(
    tmp_path, monkeypatch
):
    _create_filing_fixture(
        tmp_path,
        monkeypatch,
        include_documents=True,
        persist_event=True,
    )

    row = coverage.build_coverage_matrix({CIK: "Microsoft"}).iloc[0]

    assert row["coverage_status"] == "COMPLETE"
    assert row["company_state_status"] == "NO_DATA"
    assert row["funding_event_count"] == 0
    assert row["investment_signal_count"] == 0
    assert row["state_signal_status"] == "NO_SUPPORTED_FUNDING_EVENTS"


def test_coverage_counts_step17_form_d_observations_and_flags_missing_pipeline_rows(
    tmp_path, monkeypatch
):
    _create_filing_fixture(
        tmp_path,
        monkeypatch,
        include_documents=True,
        persist_event=True,
    )
    funding_rows = []
    for suffix in ("000001", "000002"):
        funding_rows.append(
            {
                "cik": CIK,
                "accession_number": f"0001209191-18-{suffix}",
                "filing_type": "D",
                "source_form": "D",
                "source_url": (
                    "https://www.sec.gov/Archives/edgar/data/789019/"
                    f"000120919118{suffix}/primary_doc.xml"
                ),
                "source_system": "SEC_FORM_D",
                "event_status": "CURRENT_LATEST_FILING",
                "issuer_name_source": "MICROSOFT CORP",
                "filing_date": pd.Timestamp("2018-11-09"),
                "first_sale_date": pd.Timestamp("2018-10-25"),
                "total_offering_amount_usd": 0,
                "total_amount_sold_usd": 0,
            }
        )
    funding_rows.append(
        {
            **funding_rows[0],
            "cik": "0001652044",
            "accession_number": "0000903423-16-000722",
        }
    )
    pd.DataFrame(funding_rows).to_parquet(
        coverage.FUNDING_EVENTS_PATH,
        index=False,
    )

    matrix = coverage.build_coverage_matrix({CIK: "Microsoft"})
    row = matrix.iloc[0]
    source_counts = coverage._step17_form_d_rows().groupby("cik").size().to_dict()

    assert source_counts == {CIK: 2, "0001652044": 1}
    assert row["step17_form_d_observation_count"] == 2
    assert row["form_d_filing_count"] == 2
    assert row["missing_step17_form_d_document_count"] == 2
    assert row["audit_failure_reason"] == "FORM_D_EVENTS_NOT_PERSISTED"


def test_no_supported_company_state_is_a_valid_pipeline_outcome(monkeypatch):
    sentinel = object()

    class NoStateIntelligence:
        def project_company(self, company_id):
            return {"company_id": company_id, "status": "NO_DATA"}

    monkeypatch.setattr(
        components, "UnifiedInvestmentIntelligence", NoStateIntelligence
    )
    monkeypatch.setattr(
        components, "_artifact_dq", lambda *args, **kwargs: sentinel
    )

    result = components._company_state_investment_intelligence(
        context=type("Context", (), {"company_id": COMPANY_ID})(),
        component_id="company_state_investment_intelligence",
        task_id="task-1",
    )

    assert result is sentinel


def test_coverage_refresh_preserves_backfill_run_history(tmp_path, monkeypatch):
    output = tmp_path / "coverage"
    output.mkdir()
    pd.DataFrame(
        [
            {
                "cik": CIK,
                "company_id": COMPANY_ID,
                "event_id": "EVENT-1",
                "run_id": "RUN-1",
                "run_status": "SUCCESS",
            }
        ]
    ).to_parquet(output / "backfill_runs.parquet", index=False)
    monkeypatch.setattr(coverage, "OUTPUT_ROOT", output)

    rows = coverage._load_backfill_runs()

    assert rows[0]["event_id"] == "EVENT-1"
    assert rows[0]["run_status"] == "SUCCESS"
