import json
import sqlite3
from pathlib import Path

import pandas as pd

import company_platform.multi_company.hardening as h


TARGET = [
    "0000320193","0000789019","0001018724","0001045810","0001652044"
]


def write_df(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def make_control(path, companies):
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE events (event_id TEXT PRIMARY KEY, data TEXT)")
    c.execute("CREATE TABLE pipeline_runs (run_id TEXT PRIMARY KEY, trigger_event_id TEXT, data TEXT)")
    c.execute("CREATE TABLE task_runs (task_id TEXT PRIMARY KEY, run_id TEXT, component_id TEXT, data TEXT)")
    for i, cik in enumerate(companies):
        cid = "SEC_" + cik
        e = {"event_id":f"E{i}","company_id":cid,"status":"COMPLETED"}
        r = {"run_id":f"R{i}","company_id":cid,"trigger_event_id":f"E{i}","status":"SUCCESS"}
        t = {"task_id":f"T{i}","run_id":f"R{i}","company_id":cid,"status":"SUCCESS"}
        c.execute("INSERT INTO events VALUES (?,?)",(f"E{i}",json.dumps(e)))
        c.execute("INSERT INTO pipeline_runs VALUES (?,?,?)",(f"R{i}",f"E{i}",json.dumps(r)))
        c.execute("INSERT INTO task_runs VALUES (?,?,?,?)",(f"T{i}",f"R{i}","x",json.dumps(t)))
    c.commit(); c.close()


def test_target_scope():
    active_targets = h.active_target_companies()
    assert len(active_targets) > 0
    assert set(TARGET).issubset(active_targets)
    assert set(active_targets) == set(h.active_target_companies())


def test_full_target_universe(tmp_path, monkeypatch):
    monkeypatch.setattr(h, "COMPANY_MASTER", tmp_path/"master.parquet")
    monkeypatch.setattr(h, "STEP21_SEARCH", tmp_path/"s21.parquet")
    monkeypatch.setattr(h, "STEP16_PROFILE", tmp_path/"s16.parquet")
    monkeypatch.setattr(h, "STEP20_PROFILE", tmp_path/"s20.parquet")
    monkeypatch.setattr(h, "RECON_SUMMARY", tmp_path/"recon.parquet")
    monkeypatch.setattr(h, "CONTROL_DB", tmp_path/"control.db")
    write_df(h.COMPANY_MASTER, [{"cik":c,"company_name":h.active_target_companies().get(c, c)} for c in TARGET])
    for p in [h.STEP21_SEARCH,h.STEP16_PROFILE,h.STEP20_PROFILE]:
        write_df(p, [{"cik":c} for c in TARGET])
    write_df(h.RECON_SUMMARY, [{"reconciliation_status":"READY","failed_checks":0}])
    make_control(h.CONTROL_DB, TARGET)
    execution_scope = {
        c: h.active_target_companies().get(c, c)
        for c in TARGET
    }
    s, checks, companies = h.reconcile(execution_scope=execution_scope)
    assert s.iloc[0]["status"] == "READY"
    assert len(companies) == 5
    assert checks["passed"].all()


def test_company_master_allows_multiple_ticker_listings_per_cik(
    tmp_path, monkeypatch
):
    master_path = tmp_path / "master.parquet"
    write_df(master_path, [
        {"cik": "0001652044", "ticker": ticker, "company_name": "Alphabet Inc."}
        for ticker in ["GOOGL", "GOOG", "GOOGM", "GOOGN"]
    ])
    monkeypatch.setattr(h, "COMPANY_MASTER", master_path)
    monkeypatch.setattr(h, "STEP21_SEARCH", tmp_path / "missing-step21.parquet")
    monkeypatch.setattr(h, "STEP16_PROFILE", tmp_path / "missing-step16.parquet")
    monkeypatch.setattr(h, "STEP20_PROFILE", tmp_path / "missing-step20.parquet")

    checks = h.audit_company_universe()
    identity_check = next(
        check for check in checks
        if check["check"] == "COMPANY_MASTER_CIK_IDENTITY_CONSISTENT"
    )
    assert identity_check["passed"] is True


def test_company_master_rejects_conflicting_names_for_cik(
    tmp_path, monkeypatch
):
    master_path = tmp_path / "master.parquet"
    write_df(master_path, [
        {"cik": "0001652044", "ticker": "GOOGL", "company_name": "Alphabet Inc."},
        {"cik": "0001652044", "ticker": "GOOG", "company_name": "Different Issuer"},
    ])
    monkeypatch.setattr(h, "COMPANY_MASTER", master_path)
    monkeypatch.setattr(h, "STEP21_SEARCH", tmp_path / "missing-step21.parquet")
    monkeypatch.setattr(h, "STEP16_PROFILE", tmp_path / "missing-step16.parquet")
    monkeypatch.setattr(h, "STEP20_PROFILE", tmp_path / "missing-step20.parquet")

    checks = h.audit_company_universe()
    identity_check = next(
        check for check in checks
        if check["check"] == "COMPANY_MASTER_CIK_IDENTITY_CONSISTENT"
    )
    assert identity_check["passed"] is False


def test_missing_company_is_fatal(tmp_path, monkeypatch):
    monkeypatch.setattr(h, "COMPANY_MASTER", tmp_path/"master.parquet")
    monkeypatch.setattr(h, "STEP21_SEARCH", tmp_path/"s21.parquet")
    monkeypatch.setattr(h, "STEP16_PROFILE", tmp_path/"s16.parquet")
    monkeypatch.setattr(h, "STEP20_PROFILE", tmp_path/"s20.parquet")
    monkeypatch.setattr(h, "RECON_SUMMARY", tmp_path/"recon.parquet")
    monkeypatch.setattr(h, "CONTROL_DB", tmp_path/"control.db")
    subset = TARGET[:-1]
    for p in [h.COMPANY_MASTER,h.STEP21_SEARCH,h.STEP16_PROFILE,h.STEP20_PROFILE]:
        write_df(p, [{"cik":c} for c in subset])
    write_df(h.RECON_SUMMARY, [{"reconciliation_status":"READY","failed_checks":0}])
    make_control(h.CONTROL_DB, subset)
    s, checks, _ = h.reconcile()
    assert s.iloc[0]["status"] == "NOT_READY"


def test_cross_company_task_contamination(tmp_path, monkeypatch):
    db = tmp_path/"control.db"
    make_control(db, TARGET)
    c = sqlite3.connect(db)
    bad = {"task_id":"BAD","run_id":"R0","company_id":"SEC_0000789019","status":"SUCCESS"}
    c.execute("INSERT INTO task_runs VALUES (?,?,?,?)",("BAD","R0","x",json.dumps(bad)))
    c.commit(); c.close()
    monkeypatch.setattr(h, "CONTROL_DB", db)
    checks = h.audit_control_plane_isolation()
    assert not all(x["passed"] for x in checks if x["check"] == "TASK_RUN_COMPANY_ISOLATION")


def test_cross_company_run_event_contamination(tmp_path, monkeypatch):
    db = tmp_path/"control.db"
    make_control(db, TARGET)
    c = sqlite3.connect(db)
    bad = {"run_id":"BAD","company_id":"SEC_0000789019","trigger_event_id":"E0","status":"SUCCESS"}
    c.execute("INSERT INTO pipeline_runs VALUES (?,?,?)",("BAD","E0",json.dumps(bad)))
    c.commit(); c.close()
    monkeypatch.setattr(h, "CONTROL_DB", db)
    checks = h.audit_control_plane_isolation()
    assert not all(x["passed"] for x in checks if x["check"] == "RUN_EVENT_COMPANY_ISOLATION")


def test_duplicate_trigger_detection(tmp_path, monkeypatch):
    db = tmp_path/"control.db"
    make_control(db, TARGET)
    c = sqlite3.connect(db)
    bad = {"run_id":"DUP","company_id":"SEC_0000320193","trigger_event_id":"E0","status":"SUCCESS"}
    c.execute("INSERT INTO pipeline_runs VALUES (?,?,?)",("DUP","E0",json.dumps(bad)))
    c.commit(); c.close()
    monkeypatch.setattr(h, "CONTROL_DB", db)
    checks = h.audit_control_plane_isolation()
    assert not all(x["passed"] for x in checks if x["check"] == "UNIQUE_TRIGGER_EVENT_RUN")


def test_artifact_duplicate_cik_detected(tmp_path, monkeypatch):
    p = tmp_path/"s21.parquet"
    write_df(p, [{"cik":TARGET[0]},{"cik":TARGET[0]}])
    monkeypatch.setattr(h, "STEP21_SEARCH", p)
    checks = h.audit_artifact_isolation()
    assert not all(x["passed"] for x in checks if x["check"] == "STEP21_ONE_ROW_PER_CIK")


def test_no_cross_scope_cik():
    assert all(len(c) == 10 and c.isdigit() for c in h.TARGET_COMPANIES)


def test_summary_schema(tmp_path, monkeypatch):
    db = tmp_path/"control.db"
    make_control(db, TARGET)
    monkeypatch.setattr(h, "CONTROL_DB", db)
    # summary test only needs the control-plane isolation layer
    checks = h.audit_control_plane_isolation()
    assert checks
    assert {"check","passed","severity"}.issubset(checks[0])


def test_isolation_checks_are_read_only(tmp_path, monkeypatch):
    db = tmp_path/"control.db"
    make_control(db, TARGET)
    before = db.read_bytes()
    monkeypatch.setattr(h, "CONTROL_DB", db)
    h.audit_control_plane_isolation()
    assert before == db.read_bytes()
