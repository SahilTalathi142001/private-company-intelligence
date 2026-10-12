import pytest
from company_platform.run_control.sqlite_run_store import SQLiteRunStore
from company_platform.run_control.models import PipelineRun, TaskRun


def make_store(tmp_path):
    store = SQLiteRunStore(db_path=tmp_path / "control.db")
    run = PipelineRun.create("co-1", "corr-1")
    task = TaskRun.create(run.run_id, "co-1", "extract", 1)
    store.create_run_with_tasks(run, [task])
    store.update_task(task.task_id, "RUNNING", attempt_number=1)
    return store, task


def test_reconcile_running_task_to_pending_is_audited(tmp_path):
    store, task = make_store(tmp_path)
    updated = store.reconcile_running_task(task.task_id, "PENDING", "Verified no output was committed")
    assert updated["status"] == "PENDING"
    assert updated["error_message"].startswith("Operator reconciliation:")
    import sqlite3
    with sqlite3.connect(store.db_path) as conn:
        row = conn.execute("SELECT from_status,to_status,reason FROM task_recovery_audit").fetchone()
    assert row == ("RUNNING", "PENDING", "Verified no output was committed")


def test_reconcile_requires_running_task_and_reason(tmp_path):
    store, task = make_store(tmp_path)
    with pytest.raises(ValueError, match="reason"):
        store.reconcile_running_task(task.task_id, "PENDING", " ")
    store.reconcile_running_task(task.task_id, "SUCCESS", "Checked committed artifact")
    with pytest.raises(ValueError, match="Only RUNNING"):
        store.reconcile_running_task(task.task_id, "PENDING", "Second attempt")
