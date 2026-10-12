from __future__ import annotations

from pathlib import Path

import pandas as pd

from company_platform.dq_framework.quality_engine import (
    DQReport,
    audit_parquet_artifact,
    check_company_identity,
    check_reconciliation,
    check_unique_grain,
    check_value_set,
    write_report,
)



def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "cik": "0000320193",
                "company_id": "SEC_0000320193",
                "company_name": "Apple",
                "financial_decision": "POSITIVE",
                "event_decision_posture": "POSITIVE",
            },
            {
                "cik": "0000789019",
                "company_id": "SEC_0000789019",
                "company_name": "Microsoft",
                "financial_decision": "POSITIVE",
                "event_decision_posture": "MONITOR",
            },
        ]
    )


def test_step23_company_identity_check():
    report = DQReport()
    frame = _frame()

    assert check_company_identity(report, frame, artifact="memory")
    assert not report.fatal_failures


def test_step23_duplicate_grain_is_fatal():
    report = DQReport()
    frame = pd.concat([_frame(), _frame().iloc[[0]]], ignore_index=True)

    assert not check_unique_grain(report, frame, ["cik"], artifact="memory")
    assert len(report.fatal_failures) == 1
    assert report.promotion_decision == "BLOCKED"


def test_step23_allowed_values_are_enforced():
    report = DQReport()
    frame = _frame()
    frame.loc[0, "event_decision_posture"] = "BUY"

    assert not check_value_set(
        report,
        frame,
        "event_decision_posture",
        {"POSITIVE", "MONITOR", "INSUFFICIENT_DATA"},
        artifact="memory",
    )
    assert report.promotion_decision == "BLOCKED"


def test_step23_cross_layer_reconciliation():
    report = DQReport()
    left = _frame()
    right = _frame().copy()

    assert check_reconciliation(
        report,
        left,
        right,
        key="cik",
        columns=["company_id", "company_name", "financial_decision"],
        artifact="left",
        right_artifact="right",
    )


def test_step23_cross_layer_mismatch_blocks():
    report = DQReport()
    left = _frame()
    right = _frame().copy()
    right.loc[1, "financial_decision"] = "CAUTION"

    assert not check_reconciliation(
        report,
        left,
        right,
        key="cik",
        columns=["company_id", "company_name", "financial_decision"],
        artifact="left",
        right_artifact="right",
    )
    assert report.promotion_decision == "BLOCKED"


def test_step23_artifact_audit_and_promotion_outputs(tmp_path: Path):
    artifact = tmp_path / "sample.parquet"
    _frame().to_parquet(artifact, index=False)

    report = DQReport()
    loaded = audit_parquet_artifact(
        report,
        artifact,
        required_columns=[
            "cik",
            "company_id",
            "company_name",
            "financial_decision",
        ],
        grain_keys=["cik"],
        identity_check=True,
    )

    assert loaded is not None
    assert report.promotion_decision == "PROMOTE"

    paths = write_report(report, tmp_path / "dq")
    assert all(path.is_file() for path in paths.values())

    summary = pd.read_parquet(paths["summary"]).iloc[0]
    assert summary["promotion_decision"] == "PROMOTE"


def test_step23_missing_artifact_blocks():
    report = DQReport()
    loaded = audit_parquet_artifact(
        report,
        Path("definitely_missing_step23_artifact.parquet"),
        required_columns=["cik"],
        grain_keys=["cik"],
    )

    assert loaded is None
    assert report.promotion_decision == "BLOCKED"
