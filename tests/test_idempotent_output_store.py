import threading
from company_platform.run_control.idempotent_output_store import IdempotentOutputStore


def test_commit_once_survives_retry(tmp_path):
    store = IdempotentOutputStore(tmp_path / "control.db")
    first = store.commit_once("run1/task1/output", "task1", {"rows": 12})
    retry = store.commit_once("run1/task1/output", "task1", {"rows": 99})
    assert first["created"] is True
    assert retry["created"] is False
    assert retry["payload"] == {"rows": 12}
    assert store.get("run1/task1/output")["payload"] == {"rows": 12}


def test_concurrent_commits_have_one_winner(tmp_path):
    store = IdempotentOutputStore(tmp_path / "control.db")
    barrier = threading.Barrier(8)
    results = []
    errors = []
    def worker(i):
        try:
            barrier.wait()
            results.append(store.commit_once("same-key", f"task-{i}", {"worker": i}))
        except Exception as exc:
            errors.append(exc)
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert not errors
    assert sum(r["created"] for r in results) == 1
    assert len({str(r["payload"]) for r in results}) == 1
