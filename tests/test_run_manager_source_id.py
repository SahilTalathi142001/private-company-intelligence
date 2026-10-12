from company_platform.run_control.run_manager import RunManager
from company_platform.run_control.sqlite_run_store import SQLiteRunStore


def test_create_run_from_plan_preserves_source_id(tmp_path):
    store = SQLiteRunStore(db_path=tmp_path / "control.db")
    manager = RunManager(store=store)

    run = manager.create_run_from_plan(
        company_id="company-1",
        correlation_id="correlation-1",
        plan=["extract"],
        source_id="SEC_FORM_D",
    )

    assert run.source_id == "SEC_FORM_D"
    assert store.get_run(run.run_id)["source_id"] == "SEC_FORM_D"
    assert store.runs().loc[0, "source_id"] == "SEC_FORM_D"
