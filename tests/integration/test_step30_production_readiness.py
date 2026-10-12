import pandas as pd
from company_platform.production_readiness.readiness import READINESS_VERSION, evaluate_readiness


def base():
    return dict(
        control={
            "pending_events":0,"processing_events":0,"stale_events":0,
            "historical_failed_events":4,"historical_failed_runs":38,
            "historical_failed_tasks":32,"historical_blocked_tasks":38,
        },
        reconciliation={"failed_checks":0,"reconciliation_status":"READY"},
        failure_analysis={"unresolved_failures":0,"retry_exhausted":0,"recovery_readiness":"READY"},
        recovery_verification={"recovery_failed":0,"recovery_incomplete":0,"verification_status":"NO_RECOVERIES"},
    )


def test_version():
    assert READINESS_VERSION == "30.1.0"


def test_historical_failures_do_not_block_readiness():
    s, g = evaluate_readiness(**base())
    assert s.iloc[0]["status"] == "READY"
    assert int(s.iloc[0]["historical_failed_events"]) == 4
    assert int(s.iloc[0]["historical_failed_runs"]) == 38
    assert g["passed"].all()


def test_unresolved_failure_blocks_readiness():
    x = base()
    x["failure_analysis"]["unresolved_failures"] = 1
    x["failure_analysis"]["recovery_readiness"] = "NOT_READY"
    s, g = evaluate_readiness(**x)
    assert s.iloc[0]["status"] == "NOT_READY"


def test_retry_exhausted_blocks_readiness():
    x = base()
    x["failure_analysis"]["retry_exhausted"] = 1
    s, _ = evaluate_readiness(**x)
    assert s.iloc[0]["status"] == "NOT_READY"


def test_pending_blocks_readiness():
    x = base()
    x["control"]["pending_events"] = 1
    s, _ = evaluate_readiness(**x)
    assert s.iloc[0]["status"] == "NOT_READY"


def test_processing_blocks_readiness():
    x = base()
    x["control"]["processing_events"] = 1
    s, _ = evaluate_readiness(**x)
    assert s.iloc[0]["status"] == "NOT_READY"


def test_reconciliation_failure_blocks_readiness():
    x = base()
    x["reconciliation"]["failed_checks"] = 1
    x["reconciliation"]["reconciliation_status"] = "NOT_READY"
    s, _ = evaluate_readiness(**x)
    assert s.iloc[0]["status"] == "NOT_READY"


def test_recovery_failure_blocks_readiness():
    x = base()
    x["recovery_verification"]["recovery_failed"] = 1
    x["recovery_verification"]["verification_status"] = "RECOVERY_FAILED"
    s, _ = evaluate_readiness(**x)
    assert s.iloc[0]["status"] == "NOT_READY"


def test_summary_retains_historical_metrics():
    s, _ = evaluate_readiness(**base())
    row = s.iloc[0]
    assert set(["historical_failed_events","historical_failed_runs",
                "historical_failed_tasks","historical_blocked_tasks",
                "unresolved_failures"]).issubset(s.columns)
