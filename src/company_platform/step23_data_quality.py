from __future__ import annotations

import logging
from pathlib import Path

from company_platform.dq_framework.quality_engine import (
    DQReport,
    audit_parquet_artifact,
    check_provenance_consistency,
    check_reconciliation,
    check_value_set,
    write_report,
)


ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"

OUTPUT = DATA / "gold" / "platform_dq"

UNIFIED_VIEW = (
    DATA
    / "gold"
    / "unified_company_intelligence"
    / "unified_company_view"
    / "unified_company_view.parquet"
)

SEARCH = (
    DATA
    / "gold"
    / "company_intelligence"
    / "company_search"
    / "company_search.parquet"
)

SCREENING = (
    DATA
    / "gold"
    / "company_intelligence"
    / "company_screening"
    / "company_screening.parquet"
)

logger = logging.getLogger("step23_dq")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

UNIFIED_REQUIRED = [
    "cik",
    "company_id",
    "company_name",
    "financial_decision",
    "financial_score",
    "company_state_status",
    "company_state_version",
    "investment_signal_count",
    "event_decision_posture",
    "event_decision_confidence",
    "event_decision_id",
    "source_event_id",
    "source_run_id",
    "source_document_id",
    "source_document_sha256",
    "unified_company_view_version",
]

SCREENING_REQUIRED = [
    "cik",
    "company_id",
    "company_name",
    "financial_decision",
    "financial_score",
    "screening_eligibility",
    "unified_posture",
    "company_state_version",
    "investment_signal_count",
    "event_decision_posture",
    "event_decision_confidence",
    "event_decision_id",
    "source_event_id",
    "source_run_id",
    "source_document_id",
    "source_document_sha256",
    "unified_company_view_version",
]

PROVENANCE = [
    "source_event_id",
    "source_run_id",
    "source_document_id",
    "source_document_sha256",
]


def audit_provenance_consistency(
    report: DQReport,
    dataframe,
    artifact: Path,
) -> None:
    """
    Add the explicit row-level provenance consistency check.

    A row is valid when:
      - all four provenance fields are populated, or
      - all four provenance fields are absent.

    Partial provenance remains FATAL.

    The completeness check performed by audit_parquet_artifact() separately
    records all-BATCH/LEGACY lineage as a controlled WARNING.
    """
    check_provenance_consistency(
        report,
        dataframe,
        artifact=str(artifact),
    )


def main() -> int:
    report = DQReport()

    unified = audit_parquet_artifact(
        report,
        UNIFIED_VIEW,
        required_columns=UNIFIED_REQUIRED,
        grain_keys=["cik"],
        identity_check=True,
        provenance_columns=PROVENANCE,
    )

    screening = audit_parquet_artifact(
        report,
        SCREENING,
        required_columns=SCREENING_REQUIRED,
        grain_keys=["cik"],
        identity_check=True,
        provenance_columns=PROVENANCE,
    )

    search = audit_parquet_artifact(
        report,
        SEARCH,
        required_columns=[
            "cik",
            "company_id",
            "company_name",
            "financial_decision",
        ],
        grain_keys=["cik"],
        identity_check=True,
    )

    if unified is not None:
        check_value_set(
            report,
            unified,
            "event_decision_posture",
            {"POSITIVE", "MONITOR", "INSUFFICIENT_DATA"},
            artifact=str(UNIFIED_VIEW),
        )

        audit_provenance_consistency(
            report,
            unified,
            UNIFIED_VIEW,
        )

    if screening is not None:
        check_value_set(
            report,
            screening,
            "event_decision_posture",
            {"POSITIVE", "MONITOR", "INSUFFICIENT_DATA"},
            artifact=str(SCREENING),
        )

        audit_provenance_consistency(
            report,
            screening,
            SCREENING,
        )

    if search is not None and screening is not None:
        check_reconciliation(
            report,
            search,
            screening,
            key="cik",
            columns=[
                "company_id",
                "company_name",
                "financial_decision",
            ],
            artifact=str(SEARCH),
            right_artifact=str(SCREENING),
        )

    if unified is not None and screening is not None:
        check_reconciliation(
            report,
            unified,
            screening,
            key="cik",
            columns=[
                "company_id",
                "company_name",
                "financial_decision",
                "financial_score",
                "company_state_version",
                "investment_signal_count",
                "event_decision_posture",
                "event_decision_confidence",
                "event_decision_id",
                "source_event_id",
                "source_run_id",
                "source_document_id",
                "source_document_sha256",
                "unified_company_view_version",
            ],
            artifact=str(UNIFIED_VIEW),
            right_artifact=str(SCREENING),
        )

    write_report(report, OUTPUT)
    summary = report.summary()

    logger.info("=" * 80)
    logger.info("STEP 23 - PLATFORM DATA QUALITY / RECONCILIATION")
    logger.info("DQ version          : %s", summary["dq_version"])
    logger.info("Validation checks   : %d", summary["validation_checks"])
    logger.info("Passed checks       : %d", summary["passed_checks"])
    logger.info("Failed checks       : %d", summary["failed_checks"])
    logger.info("Fatal failures      : %d", summary["fatal_failures"])
    logger.info("Controlled warnings : %d", summary["controlled_warnings"])
    logger.info("Promotion decision   : %s", summary["promotion_decision"])
    logger.info("DQ output            : %s", OUTPUT)
    logger.info("=" * 80)

    return 0 if summary["promotion_decision"] == "PROMOTE" else 1


if __name__ == "__main__":
    raise SystemExit(main())
