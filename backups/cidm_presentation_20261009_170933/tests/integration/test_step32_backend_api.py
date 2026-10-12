import json
import sqlite3

import pandas as pd

from company_platform.api import service


TARGET = "0000320193"


def parquet(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def control_db(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE events (
                event_id TEXT PRIMARY KEY,
                event_type TEXT,
                company_id TEXT,
                status TEXT,
                payload TEXT
            );
            CREATE TABLE pipeline_runs (
                run_id TEXT PRIMARY KEY,
                status TEXT
            );
            CREATE TABLE task_runs (
                task_id TEXT PRIMARY KEY,
                status TEXT
            );
            """
        )
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?, ?)",
            (
                "E1",
                "NEW_SEC_FILING",
                "SEC_0000320193",
                "COMPLETED",
                json.dumps({"accession_number": "A1"}),
            ),
        )
        conn.execute(
            "INSERT INTO pipeline_runs VALUES (?, ?)",
            ("R1", "SUCCESS"),
        )
        conn.execute(
            "INSERT INTO task_runs VALUES (?, ?)",
            ("T1", "SUCCESS"),
        )


def structured_db(path, rows=()):
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE structured_events (
                event_id TEXT PRIMARY KEY,
                event_type TEXT,
                cik TEXT,
                document_id TEXT,
                status TEXT,
                facts_json TEXT
            )
            """
        )
        conn.executemany(
            "INSERT INTO structured_events VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )


def patch_paths(monkeypatch, tmp_path):
    data = tmp_path / "data"
    monkeypatch.setattr(service, "DATA", data)
    monkeypatch.setattr(
        service,
        "CONTROL_DB",
        data / "silver" / "platform" / "control.db",
    )
    monkeypatch.setattr(
        service,
        "STRUCTURED_DB",
        data / "silver" / "platform" / "structured_events.db",
    )
    monkeypatch.setattr(
        service,
        "REGISTRY",
        data / "silver" / "platform" / "company_registry.parquet",
    )
    monkeypatch.setattr(
        service,
        "UNIFIED",
        data
        / "gold"
        / "unified_company_intelligence"
        / "unified_company_view"
        / "unified_company_view.parquet",
    )


def registry_rows():
    return [
        {
            "cik": TARGET,
            "company_id": f"SEC_{TARGET}",
            "company_name": "Apple",
            "status": "ACTIVE",
        },
        {
            "cik": "0000789019",
            "company_id": "SEC_0000789019",
            "company_name": "Microsoft",
            "status": "ACTIVE",
        },
    ]


def test_health_reports_current_api_version():
    result = service.get_health()
    assert result == {
        "status": "ok",
        "api_version": service.API_VERSION,
    }


def test_readiness_delegates_to_production_readiness(monkeypatch):
    from company_platform.production_readiness import readiness

    expected = {"status": "READY", "target_company_count": 5}
    monkeypatch.setattr(
        readiness,
        "evaluate_readiness",
        lambda: (pd.DataFrame([expected]), pd.DataFrame()),
    )
    assert service.get_readiness() == expected


def test_company_list_has_deterministic_identity(monkeypatch, tmp_path):
    patch_paths(monkeypatch, tmp_path)
    parquet(service.REGISTRY, registry_rows())
    parquet(
        service.UNIFIED,
        [
            {"cik": TARGET, "company_name": "Apple"},
            {"cik": "0000789019", "company_name": "Microsoft"},
        ],
    )
    structured_db(service.STRUCTURED_DB)

    result = service.list_companies()

    assert [company["cik"] for company in result] == [
        TARGET,
        "0000789019",
    ]
    assert result[0]["company_id"] == f"SEC_{TARGET}"
    assert result[0]["intelligence"]["company_name"] == "Apple"


def test_company_detail_is_company_scoped(monkeypatch, tmp_path):
    patch_paths(monkeypatch, tmp_path)
    parquet(service.REGISTRY, registry_rows())
    parquet(service.UNIFIED, [{"cik": TARGET, "company_name": "Apple"}])
    structured_db(
        service.STRUCTURED_DB,
        [
            (
                "E1",
                "SEC_FORM_D_FILING",
                TARGET,
                "doc-1",
                "COMPLETED",
                json.dumps({"amount": 100}),
            ),
            (
                "E2",
                "SEC_FORM_D_FILING",
                "0000789019",
                "doc-2",
                "COMPLETED",
                json.dumps({"amount": 200}),
            ),
        ],
    )

    result = service.get_company(TARGET)

    assert result["cik"] == TARGET
    assert result["company_id"] == f"SEC_{TARGET}"
    assert result["event_count"] == 1
    assert result["events"][0]["event_id"] == "E1"
    assert result["events"][0]["facts"] == {"amount": 100}


def test_company_lookup_accepts_prefixed_cik(monkeypatch, tmp_path):
    patch_paths(monkeypatch, tmp_path)
    parquet(service.REGISTRY, registry_rows())
    parquet(service.UNIFIED, [{"cik": TARGET, "company_name": "Apple"}])
    structured_db(service.STRUCTURED_DB)

    assert service.get_company(f"SEC_{TARGET}")["cik"] == TARGET


def test_unknown_company_returns_none(monkeypatch, tmp_path):
    patch_paths(monkeypatch, tmp_path)
    parquet(service.REGISTRY, registry_rows())

    assert service.get_company("0000999999") is None


def test_event_and_run_lookup(monkeypatch, tmp_path):
    patch_paths(monkeypatch, tmp_path)
    control_db(service.CONTROL_DB)
    structured_db(service.STRUCTURED_DB)

    assert service.get_event("E1")["company_id"] == "SEC_0000320193"
    assert service.get_run("R1")["status"] == "SUCCESS"


def test_operational_summary(monkeypatch, tmp_path):
    patch_paths(monkeypatch, tmp_path)
    control_db(service.CONTROL_DB)
    parquet(service.REGISTRY, registry_rows())
    structured_db(service.STRUCTURED_DB)

    result = service.get_operations_summary()

    assert result["events"]["total"] == 1
    assert result["events"]["pending"] == 0
    assert result["runs"]["total"] == 1
    assert result["tasks"]["total"] == 1


def test_coverage_excludes_test_structured_events(monkeypatch, tmp_path):
    patch_paths(monkeypatch, tmp_path)
    parquet(service.REGISTRY, registry_rows())
    parquet(service.UNIFIED, [{"cik": TARGET, "company_name": "Apple"}])
    structured_db(
        service.STRUCTURED_DB,
        [
            (
                "E1",
                "SEC_FORM_D_FILING",
                TARGET,
                "production-doc",
                "COMPLETED",
                "{}",
            ),
            (
                "E2",
                "SEC_FORM_D_FILING",
                TARGET,
                "TEST-E2E-doc",
                "COMPLETED",
                "{}",
            ),
        ],
    )

    result = service.get_company(TARGET)

    assert result["coverage"]["production_structured_events"] == 1
    assert result["coverage"]["form_d_observations"] == 1
    assert result["coverage"]["test_structured_events_excluded"] == 1


def test_api_import_contract():
    from company_platform.api.app import app

    routes = {route.path for route in app.routes}
    assert "/health" in routes
    assert "/readiness" in routes
    assert "/companies" in routes
    assert "/companies/{cik}" in routes
    assert "/events/{event_id}" in routes
    assert "/runs/{run_id}" in routes
    assert "/api/operations/summary" in routes
