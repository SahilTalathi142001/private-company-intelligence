import pandas as pd

from company_platform.orchestrator import artifact_validator


def test_source_registry_is_validated_as_global_metadata(tmp_path, monkeypatch):
    source_registry = tmp_path / "source_registry.parquet"
    source_registry.touch()
    monkeypatch.setattr(
        artifact_validator,
        "_path",
        lambda *parts: source_registry,
    )
    monkeypatch.setattr(
        artifact_validator.pd,
        "read_parquet",
        lambda path: pd.DataFrame({"source_id": ["SEC"]}),
    )

    result = artifact_validator.validate_artifact(
        "SEC_0000320193",
        "source_registry",
        "1.0.0",
    )

    assert result == (True, "OK")


def test_form_d_event_table_may_have_no_row_for_company(tmp_path, monkeypatch):
    funding_events = tmp_path / "funding_events.parquet"
    funding_snapshot = tmp_path / "company_funding_snapshot.parquet"
    funding_events.touch()
    funding_snapshot.touch()
    monkeypatch.setitem(
        artifact_validator._SHARED,
        "form_d_ingestion",
        [funding_events, funding_snapshot],
    )
    frames = {
        funding_events: pd.DataFrame({"cik": ["0000789019"]}),
        funding_snapshot: pd.DataFrame({"cik": ["0000320193"]}),
    }
    monkeypatch.setattr(
        artifact_validator.pd,
        "read_parquet",
        lambda path: frames[path],
    )

    result = artifact_validator.validate_artifact(
        "SEC_0000320193",
        "form_d_ingestion",
        "1.0.0",
    )

    assert result == (True, "AVAILABLE")


def test_valuation_snapshot_may_have_no_row_for_company(
    tmp_path,
    monkeypatch,
):
    valuation_snapshot = tmp_path / "company_valuation_snapshot.parquet"
    valuation_snapshot.touch()
    monkeypatch.setitem(
        artifact_validator._SHARED,
        "valuation",
        [valuation_snapshot],
    )
    monkeypatch.setattr(
        artifact_validator.pd,
        "read_parquet",
        lambda path: pd.DataFrame({"cik": ["0000789019"]}),
    )

    result = artifact_validator.validate_artifact(
        "SEC_0000909832",
        "valuation",
        "1.0.0",
    )

    assert result == (True, "NO_DATA")
