from importlib import import_module
from pathlib import Path
import json
import sys
import uuid
from typing import Any

from company_platform.events.event_store import EventStore


pd = import_module("pandas")


def _load_live_refresh_module():
    try:
        return import_module(
            "company_platform.sec_detection.live_refresh"
        )
    except ModuleNotFoundError as exc:
        if exc.name != "company_platform":
            raise

    repository_root = Path(__file__).resolve().parents[2]
    for candidate in (
        repository_root,
        repository_root / "src",
    ):
        candidate = str(candidate)
        if candidate not in sys.path:
            sys.path.insert(0, candidate)

    return import_module(
        "company_platform.sec_detection.live_refresh"
    )


_live_refresh_module = _load_live_refresh_module()
SECLiveRefresh = _live_refresh_module.SECLiveRefresh
VERSION = _live_refresh_module.VERSION


def _event_store(tmp_path: Path) -> EventStore:
    return EventStore(
        path=str(
            tmp_path
            / f"events_{uuid.uuid4().hex}.db"
        )
    )


def _companies() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "cik": "0000320193",
                "company_id": "SEC_0000320193",
                "status": "ACTIVE",
                "ingestion_enabled": True,
            },
            {
                "cik": "0000789019",
                "company_id": "SEC_0000789019",
                "status": "ACTIVE",
                "ingestion_enabled": True,
            },
        ]
    )


def _controller(tmp_path, monkeypatch):

    monkeypatch.setattr(
        _live_refresh_module,
        "list_companies",
        _companies,
    )

    return SECLiveRefresh(
        event_store=_event_store(tmp_path),
        state_path=tmp_path / "state.json",
        audit_path=tmp_path / "audit.parquet",
        coverage_path=tmp_path / "coverage.parquet",
        user_agent="test@example.com",
    )


def test_refresh_emits_new_filing_and_persists_state(
    tmp_path,
    monkeypatch,
):

    controller = _controller(
        tmp_path,
        monkeypatch,
    )

    responses = {
        "0000320193": [
            {
                "accessionNumber":
                    "0000320193-26-000001",
                "filingDate":
                    "2026-10-01",
                "reportDate":
                    "2026-09-30",
                "form":
                    "10-Q",
                "primaryDocument":
                    "aapl.htm",
                "primaryDocDescription":
                    "Quarterly Report",
            }
        ],
        "0000789019": [],
    }

    monkeypatch.setattr(
        controller,
        "_recent",
        lambda cik: responses[cik],
    )

    result = controller.refresh_once(
        sleep_seconds=0
    )

    assert result["version"] == VERSION
    assert result["status"] == "SUCCESS"
    assert result["checked"] == 2
    assert result["emitted"] == 1
    assert result["failed"] == 0

    state = json.loads(
        (
            tmp_path / "state.json"
        ).read_text()
    )

    assert (
        "0000320193"
        in state["companies"]
    )

    assert (
        "0000320193-26-000001"
        in state["companies"][
            "0000320193"
        ]["seen_accessions"]
    )

    audit = pd.read_parquet(
        tmp_path / "audit.parquet"
    )

    assert len(audit) == 1
    assert (
        audit.iloc[0]["action"]
        == "EMITTED"
    )


def test_refresh_is_idempotent_for_same_accession(
    tmp_path,
    monkeypatch,
):

    controller = _controller(
        tmp_path,
        monkeypatch,
    )

    filing = {
        "accessionNumber":
            "0000320193-26-000002",
        "filingDate":
            "2026-10-02",
        "reportDate":
            "2026-09-30",
        "form":
            "8-K",
        "primaryDocument":
            "aapl.htm",
        "primaryDocDescription":
            "Current Report",
    }

    monkeypatch.setattr(
        controller,
        "_recent",
        lambda cik: (
            [filing]
            if cik == "0000320193"
            else []
        ),
    )

    first = controller.refresh_once(
        sleep_seconds=0
    )

    second = controller.refresh_once(
        sleep_seconds=0
    )

    assert first["emitted"] == 1
    assert second["emitted"] == 0


def test_form_filter_does_not_mark_filing_seen(
    tmp_path,
    monkeypatch,
):

    monkeypatch.setattr(
        _live_refresh_module,
        "list_companies",
        lambda: _companies().iloc[:1],
    )

    controller = SECLiveRefresh(
        event_store=_event_store(tmp_path),
        state_path=tmp_path / "state.json",
        audit_path=tmp_path / "audit.parquet",
        coverage_path=tmp_path / "coverage.parquet",
        user_agent="test@example.com",
    )

    filing = {
        "accessionNumber":
            "0000320193-26-000003",
        "filingDate":
            "2026-10-03",
        "reportDate":
            "2026-09-30",
        "form":
            "4",
        "primaryDocument":
            "x.xml",
        "primaryDocDescription":
            "Ownership",
    }

    monkeypatch.setattr(
        controller,
        "_recent",
        lambda cik: [filing],
    )

    result = controller.refresh_once(
        forms=["10-Q"],
        sleep_seconds=0,
    )

    assert result["emitted"] == 0
    assert result["filtered"] == 1

    state = json.loads(
        (
            tmp_path / "state.json"
        ).read_text()
    )

    assert (
        state["companies"][
            "0000320193"
        ]["seen_accessions"]
        == []
    )


def test_failed_company_does_not_abort_other_companies(
    tmp_path,
    monkeypatch,
):

    controller = _controller(
        tmp_path,
        monkeypatch,
    )

    def recent(cik):

        if cik == "0000320193":
            raise RuntimeError(
                "SEC unavailable"
            )

        return [
            {
                "accessionNumber":
                    "0000789019-26-000001",
                "filingDate":
                    "2026-10-04",
                "reportDate":
                    "2026-09-30",
                "form":
                    "10-Q",
                "primaryDocument":
                    "msft.htm",
                "primaryDocDescription":
                    "Quarterly Report",
            }
        ]

    monkeypatch.setattr(
        controller,
        "_recent",
        recent,
    )

    result = controller.refresh_once(
        sleep_seconds=0
    )

    assert result["status"] == "PARTIAL"
    assert result["failed"] == 1
    assert result["emitted"] == 1

    coverage = pd.read_parquet(
        tmp_path / "coverage.parquet"
    )

    assert set(
        coverage["status"]
    ) == {
        "FAILED",
        "SUCCESS",
    }