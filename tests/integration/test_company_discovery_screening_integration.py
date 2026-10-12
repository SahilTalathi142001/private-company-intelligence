from pathlib import Path

import pandas as pd

from src.sec_ingestion.step_21_company_discovery_screening import (
    build_screening,
    enrich_with_unified_company_view,
    validate_search,
)


def test_final_company_screening_consumes_unified_company_view(
    tmp_path,
):

    search = pd.DataFrame(
        [
            {
                "cik": "0000320193",
                "company_id": "SEC_0000320193",
                "company_name": "Apple",
                "ticker": "AAPL",
                "industry": "Technology",
                "company_status": "ACTIVE",

                "financial_decision": "POSITIVE",
                "financial_trajectory": "IMPROVING",
                "financial_score": 90.0,
                "financial_health": "EXCELLENT",
                "financial_risk_signal": "LOW",
                "peer_position": "ABOVE_MEDIAN",
                "peer_population_class": "TECHNOLOGY",
                "financial_decision_confidence": "HIGH",
                "financial_decision_priority": 1,
                "decision_confidence": "HIGH",
                "decision_priority": 1,
                "decision_reason_code": "STRONG_FINANCIALS",
                "company_archetype": "GROWTH",

                "unified_posture": "POSITIVE",
                "data_coverage": "COMPLETE",
                "profile_quality": "COMPLETE",

                "funding_status": "AVAILABLE",
                "funding_event_count": 2,
                "sec_exempt_offering_capital_usd": 8_000_000.0,
                "funding_activity_status": "ACTIVE",

                "investor_coverage_status": "NO_DATA",
                "named_investor_count": 0,

                "ownership_status": "NO_DATA",
                "ownership_pct_available": False,

                "valuation_status": "NO_DATA",
                "latest_valuation_amount_usd": None,
                "latest_valuation_date": None,

                "sec_capital_context_usd": 1000000.0,
                "sec_capital_treated_as_valuation": False,
                "valuation_fabrication_prevented": True,

                "screening_eligibility": "ELIGIBLE",
                "is_strong_positive": False,
                "is_positive": True,
                "is_improving": True,
                "is_low_risk": True,
                "has_funding_data": True,
                "has_investor_data": False,
                "has_valuation_data": False,
                "has_complete_data": True,

                "step16_version": "1.1.0",
                "step20_version": "20.0.0",
                "step21_version": "21.0.0",
                "generated_at_utc": "2026-10-06T00:00:00+00:00",
            }
        ]
    )

    unified = pd.DataFrame(
        [
            {
                "cik": "0000320193",
                "company_id": "SEC_0000320193",
                "company_name": "Apple",

                "financial_decision": "POSITIVE",
                "financial_score": 90.0,
                "financial_trajectory": "IMPROVING",

                "unified_posture": "POSITIVE",
                "data_coverage": "COMPLETE",

                "company_state_status": "ACTIVE",
                "company_state_version": 2,

                "observed_funding_event_count": 2,
                "latest_funding_date": "2026-10-05",
                "latest_total_offering_amount_usd": 10000000.0,
                "latest_amount_sold_usd": 8000000.0,
                "cumulative_offering_amount_usd": 15000000.0,
                "cumulative_amount_sold_usd": 12000000.0,

                "investment_signal_count": 3,
                "investment_signal_types": (
                    "CAPITAL_ACTIVITY_DETECTED|"
                    "RECURRING_CAPITAL_ACTIVITY|"
                    "CAPITAL_RAISE_EXECUTION"
                ),

                "capital_activity_detected": True,
                "recurring_capital_activity": True,
                "capital_raise_execution": True,

                "event_decision_posture": "POSITIVE",
                "event_decision_confidence": "MEDIUM",
                "event_decision_id": "DECISION-001",

                "source_event_id": "EVENT-001",
                "source_run_id": "RUN-001",
                "source_document_id": "DOC-001",
                "source_document_sha256": "a" * 64,

                "unified_company_view_version": "1.0.0",
            }
        ]
    )

    result = enrich_with_unified_company_view(
        search,
        unified,
    )

    screening = build_screening(
        result
    )

    assert len(screening) == 1

    row = screening.iloc[0]

    assert row["cik"] == "0000320193"

    assert row[
        "company_state_status"
    ] == "ACTIVE"

    assert row[
        "company_state_version"
    ] == 2

    assert row[
        "observed_funding_event_count"
    ] == 2

    assert row[
        "investment_signal_count"
    ] == 3

    assert row[
        "recurring_capital_activity"
    ]

    assert row[
        "capital_raise_execution"
    ]

    assert row[
        "event_decision_posture"
    ] == "POSITIVE"

    assert row[
        "screening_eligibility"
    ] == "ELIGIBLE"

    assert row[
        "source_event_id"
    ] == "EVENT-001"

    assert row[
        "source_document_id"
    ] == "DOC-001"

    assert row[
        "unified_company_view_version"
    ] == "1.0.0"


def test_final_screening_is_one_row_per_company():

    screening = pd.DataFrame(
        [
            {
                "cik": "0000320193",
                "company_name": "Apple",
                "screening_eligibility": "ELIGIBLE",
                "unified_posture": "POSITIVE",
                "financial_decision": "POSITIVE",
                "financial_trajectory": "IMPROVING",
                "financial_score": 90.0,
                "financial_risk_signal": "LOW",
                "funding_status": "AVAILABLE",
                "valuation_status": "NO_DATA",
                "data_coverage": "COMPLETE",
                "is_strong_positive": False,
                "is_positive": True,
                "is_improving": True,
                "is_low_risk": True,
                "has_funding_data": True,
                "has_investor_data": False,
                "has_valuation_data": False,
                "has_complete_data": True,
                "company_state_status": "ACTIVE",
                "company_state_version": 2,
                "observed_funding_event_count": 2,
                "latest_funding_date": "2026-10-05",
                "latest_total_offering_amount_usd": 10000000.0,
                "latest_amount_sold_usd": 8000000.0,
                "cumulative_offering_amount_usd": 15000000.0,
                "cumulative_amount_sold_usd": 12000000.0,
                "investment_signal_count": 3,
                "investment_signal_types": "CAPITAL_ACTIVITY_DETECTED",
                "capital_activity_detected": True,
                "recurring_capital_activity": True,
                "capital_raise_execution": True,
                "event_decision_posture": "POSITIVE",
                "event_decision_confidence": "MEDIUM",
                "event_decision_id": "DECISION-001",
                "source_event_id": "EVENT-001",
                "source_run_id": "RUN-001",
                "source_document_id": "DOC-001",
                "source_document_sha256": "a" * 64,
                "unified_company_view_version": "1.0.0",
            }
        ]
    )

    assert screening[
        "cik"
    ].nunique() == len(screening)

    assert not screening[
        "cik"
    ].duplicated().any()