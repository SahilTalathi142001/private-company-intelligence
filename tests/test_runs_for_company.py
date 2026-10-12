from company_platform.run_control.models import PipelineRun
from company_platform.run_control.sqlite_run_store import SQLiteRunStore


def test_get_runs_for_company_returns_only_matching_runs(tmp_path):
    store = SQLiteRunStore(db_path=tmp_path / "control.db")
    company_run = PipelineRun.create(
        "SEC_0000320193",
        "correlation-1",
    )
    other_run = PipelineRun.create(
        "SEC_0000789019",
        "correlation-2",
    )
    store.create_run(company_run)
    store.create_run(other_run)

    result = store.get_runs_for_company("SEC_0000320193")

    assert result["run_id"].tolist() == [company_run.run_id]


def test_get_runs_for_unknown_company_returns_empty_dataframe(tmp_path):
    store = SQLiteRunStore(db_path=tmp_path / "control.db")

    result = store.get_runs_for_company("unknown-company")

    assert result.empty
    assert "run_id" in result.columns
