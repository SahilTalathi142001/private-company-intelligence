from importlib import import_module
from pathlib import Path
import sys

import pandas as pd


def _load_module():
    try:
        return import_module(
            "company_platform.scheduler.production_refresh"
        )
    except ModuleNotFoundError as exc:
        if exc.name != "company_platform":
            raise

    root = Path(__file__).resolve().parents[2]

    for candidate in (
        root,
        root / "src",
    ):
        candidate = str(candidate)

        if candidate not in sys.path:
            sys.path.insert(
                0,
                candidate,
            )

    return import_module(
        "company_platform.scheduler.production_refresh"
    )


scheduler = _load_module()


def test_version():
    assert scheduler.VERSION == "16.0.0"


def test_lock_acquire_and_release(tmp_path):
    lock_path = (
        tmp_path
        / "refresh.lock"
    )

    first = scheduler.RefreshLock(
        lock_path
    )

    second = scheduler.RefreshLock(
        lock_path
    )

    assert first.acquire() is True
    assert second.acquire() is False

    first.release()

    assert second.acquire() is True

    second.release()


def test_lock_context_manager(tmp_path):
    lock_path = (
        tmp_path
        / "refresh.lock"
    )

    with scheduler.RefreshLock(
        lock_path
    ):
        assert lock_path.exists()

    assert not lock_path.exists()


def test_run_once_delegates_to_step36(
    tmp_path,
    monkeypatch,
):
    audit_path = (
        tmp_path
        / "audit.parquet"
    )

    lock_path = (
        tmp_path
        / "refresh.lock"
    )

    monkeypatch.setattr(
        scheduler,
        "AUDIT_PATH",
        audit_path,
    )

    monkeypatch.setattr(
        scheduler,
        "LOCK_PATH",
        lock_path,
    )

    class FakeRefresh:
        def __init__(self, **kwargs):
            assert kwargs["user_agent"] == "test@example.com"

        def refresh_once(
            self,
            *,
            forms,
            sleep_seconds,
        ):
            assert forms == ["10-Q", "8-K"]
            assert sleep_seconds == 0

            return {
                "status": "SUCCESS",
                "refresh_id": "refresh-001",
                "checked": 20,
                "emitted": 2,
                "duplicates": 18,
                "filtered": 0,
                "failed": 0,
            }

    monkeypatch.setattr(
        scheduler,
        "SECLiveRefresh",
        FakeRefresh,
    )

    result = scheduler.run_once(
        forms=["10-Q", "8-K"],
        user_agent="test@example.com",
    )

    assert result["status"] == "SUCCESS"
    assert result["refresh_id"] == "refresh-001"
    assert result["checked"] == 20
    assert result["emitted"] == 2

    audit = pd.read_parquet(
        audit_path
    )

    assert len(audit) == 1
    assert (
        audit.iloc[0]["status"]
        == "SUCCESS"
    )


def test_overlapping_refresh_is_skipped(
    tmp_path,
    monkeypatch,
):
    audit_path = (
        tmp_path
        / "audit.parquet"
    )

    lock_path = (
        tmp_path
        / "refresh.lock"
    )

    monkeypatch.setattr(
        scheduler,
        "AUDIT_PATH",
        audit_path,
    )

    monkeypatch.setattr(
        scheduler,
        "LOCK_PATH",
        lock_path,
    )

    lock = scheduler.RefreshLock(
        lock_path
    )

    assert lock.acquire() is True

    try:
        result = scheduler.run_once(
            user_agent="test@example.com"
        )

        assert result["status"] == "SKIPPED"
        assert (
            result["reason"]
            == "REFRESH_ALREADY_RUNNING"
        )

    finally:
        lock.release()


def test_refresh_failure_is_audited(
    tmp_path,
    monkeypatch,
):
    audit_path = (
        tmp_path
        / "audit.parquet"
    )

    lock_path = (
        tmp_path
        / "refresh.lock"
    )

    monkeypatch.setattr(
        scheduler,
        "AUDIT_PATH",
        audit_path,
    )

    monkeypatch.setattr(
        scheduler,
        "LOCK_PATH",
        lock_path,
    )

    class BrokenRefresh:
        def __init__(self, **kwargs):
            pass

        def refresh_once(
            self,
            *,
            forms,
            sleep_seconds,
        ):
            raise RuntimeError(
                "SEC unavailable"
            )

    monkeypatch.setattr(
        scheduler,
        "SECLiveRefresh",
        BrokenRefresh,
    )

    result = scheduler.run_once(
        user_agent="test@example.com"
    )

    assert result["status"] == "FAILED"
    assert (
        "SEC unavailable"
        in result["reason"]
    )

    audit = pd.read_parquet(
        audit_path
    )

    assert len(audit) == 1
    assert (
        audit.iloc[0]["status"]
        == "FAILED"
    )


def test_failed_company_result_does_not_become_scheduler_exception(
    tmp_path,
    monkeypatch,
):
    audit_path = (
        tmp_path
        / "audit.parquet"
    )

    lock_path = (
        tmp_path
        / "refresh.lock"
    )

    monkeypatch.setattr(
        scheduler,
        "AUDIT_PATH",
        audit_path,
    )

    monkeypatch.setattr(
        scheduler,
        "LOCK_PATH",
        lock_path,
    )

    class PartialRefresh:
        def __init__(self, **kwargs):
            pass

        def refresh_once(
            self,
            *,
            forms,
            sleep_seconds,
        ):
            return {
                "status": "SUCCESS",
                "refresh_id": "refresh-002",
                "checked": 20,
                "emitted": 19,
                "duplicates": 0,
                "filtered": 0,
                "failed": 1,
            }

    monkeypatch.setattr(
        scheduler,
        "SECLiveRefresh",
        PartialRefresh,
    )

    result = scheduler.run_once(
        user_agent="test@example.com"
    )

    assert result["status"] == "SUCCESS"
    assert result["failed"] == 1
    assert (
        result["reason"]
        == "ONE_OR_MORE_COMPANIES_FAILED"
    )


def test_audit_is_append_only(
    tmp_path,
    monkeypatch,
):
    audit_path = (
        tmp_path
        / "audit.parquet"
    )

    monkeypatch.setattr(
        scheduler,
        "AUDIT_PATH",
        audit_path,
    )

    scheduler.append_audit(
        {
            "execution_id": "1",
            "status": "SUCCESS",
        }
    )

    scheduler.append_audit(
        {
            "execution_id": "2",
            "status": "SUCCESS",
        }
    )

    audit = pd.read_parquet(
        audit_path
    )

    assert len(audit) == 2

    assert set(
        audit["execution_id"]
    ) == {"1", "2"}