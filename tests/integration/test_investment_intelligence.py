from __future__ import annotations

from company_platform.company_intelligence.company_state_store import (
    CompanyStateStore,
)

from company_platform.events.structured_event_store import (
    StructuredEventStore,
)

from company_platform.investment_intelligence import (
    DecisionSignalEngine,
    DecisionSignalStore,
    InvestmentSignalEngine,
    InvestmentSignalStore,
    UnifiedInvestmentIntelligence,
)


COMPANY_ID = "SEC_0000320193"
CIK = "0000320193"


def build_environment(tmp_path):
    event_store = StructuredEventStore(
        path=tmp_path / "structured_events.db"
    )

    state_store = CompanyStateStore(
        path=tmp_path / "company_state.db"
    )

    signal_store = InvestmentSignalStore(
        path=tmp_path / "investment_signals.db"
    )

    decision_store = DecisionSignalStore(
        path=tmp_path / "decision_signals.db"
    )

    return (
        event_store,
        state_store,
        signal_store,
        decision_store,
    )


def create_event(
    event_store,
    *,
    document_id,
    document_sha,
    filing_date,
    offering,
    sold,
    source_event_id,
    source_run_id,
):
    facts = {
        "filing_date": filing_date,
        "issuer_name": "Example Technologies Inc.",
        "total_offering_amount": str(
            offering
        ),
        "total_amount_sold": str(
            sold
        ),
    }

    return event_store.persist(
        company_id=COMPANY_ID,
        cik=CIK,
        document_id=document_id,
        document_class="SEC_FORM_D",
        event_type="SEC_FORM_D_FILING",
        source_document_path=(
            f"data/bronze/documents/"
            f"cik={CIK}/"
            f"{document_id}/filing.pdf"
        ),
        source_document_sha256=document_sha,
        source_environment="PRODUCTION",
        source_event_id=source_event_id,
        source_run_id=source_run_id,
        event_confidence=1.0,
        facts=facts,
    )


def build_company_state(
    event_store,
    state_store,
):
    from company_platform.company_intelligence.state_projector import (
        CompanyStateProjector,
    )

    projector = CompanyStateProjector(
        event_store=event_store,
        state_store=state_store,
    )

    return projector


def test_company_state_to_investment_signals(
    tmp_path,
):
    (
        event_store,
        state_store,
        signal_store,
        _,
    ) = build_environment(tmp_path)

    create_event(
        event_store,
        document_id="DOC-001",
        document_sha="a" * 64,
        filing_date="2026-10-05",
        offering=10_000_000,
        sold=4_000_000,
        source_event_id="EVENT-001",
        source_run_id="RUN-001",
    )

    projector = build_company_state(
        event_store,
        state_store,
    )

    projector.project_company(
        COMPANY_ID
    )

    engine = InvestmentSignalEngine(
        state_store=state_store,
        signal_store=signal_store,
    )

    result = engine.generate(
        COMPANY_ID
    )

    assert result["status"] == "ACTIVE"
    assert result["signals_created"] == 2

    signals = signal_store.list_for_company(
        COMPANY_ID
    )

    signal_types = {
        signal["signal_type"]
        for signal in signals
    }

    assert "CAPITAL_ACTIVITY_DETECTED" in (
        signal_types
    )

    assert "CAPITAL_RAISE_EXECUTION" in (
        signal_types
    )


def test_multiple_events_generate_recurring_signal(
    tmp_path,
):
    (
        event_store,
        state_store,
        signal_store,
        _,
    ) = build_environment(tmp_path)

    create_event(
        event_store,
        document_id="DOC-001",
        document_sha="a" * 64,
        filing_date="2026-09-01",
        offering=10_000_000,
        sold=4_000_000,
        source_event_id="EVENT-001",
        source_run_id="RUN-001",
    )

    create_event(
        event_store,
        document_id="DOC-002",
        document_sha="b" * 64,
        filing_date="2026-10-05",
        offering=20_000_000,
        sold=8_000_000,
        source_event_id="EVENT-002",
        source_run_id="RUN-002",
    )

    projector = build_company_state(
        event_store,
        state_store,
    )

    projector.project_company(
        COMPANY_ID
    )

    engine = InvestmentSignalEngine(
        state_store=state_store,
        signal_store=signal_store,
    )

    result = engine.generate(
        COMPANY_ID
    )

    assert result["status"] == "ACTIVE"

    signals = signal_store.list_for_company(
        COMPANY_ID
    )

    signal_types = {
        signal["signal_type"]
        for signal in signals
    }

    assert "CAPITAL_ACTIVITY_DETECTED" in (
        signal_types
    )

    assert "RECURRING_CAPITAL_ACTIVITY" in (
        signal_types
    )


def test_decision_engine_produces_positive_posture(
    tmp_path,
):
    (
        event_store,
        state_store,
        signal_store,
        decision_store,
    ) = build_environment(tmp_path)

    create_event(
        event_store,
        document_id="DOC-001",
        document_sha="a" * 64,
        filing_date="2026-09-01",
        offering=10_000_000,
        sold=4_000_000,
        source_event_id="EVENT-001",
        source_run_id="RUN-001",
    )

    create_event(
        event_store,
        document_id="DOC-002",
        document_sha="b" * 64,
        filing_date="2026-10-05",
        offering=20_000_000,
        sold=8_000_000,
        source_event_id="EVENT-002",
        source_run_id="RUN-002",
    )

    projector = build_company_state(
        event_store,
        state_store,
    )

    projector.project_company(
        COMPANY_ID
    )

    signal_engine = InvestmentSignalEngine(
        state_store=state_store,
        signal_store=signal_store,
    )

    signal_engine.generate(
        COMPANY_ID
    )

    decision_engine = DecisionSignalEngine(
        state_store=state_store,
        signal_store=signal_store,
        decision_store=decision_store,
    )

    result = decision_engine.evaluate(
        COMPANY_ID
    )

    assert result["status"] == "ACTIVE"

    assert result["decision"] == "POSITIVE"

    assert result["confidence"] == "MEDIUM"

    decisions = decision_store.list_for_company(
        COMPANY_ID
    )

    assert len(decisions) == 1

    assert decisions[0]["decision_type"] == (
        "INVESTMENT_POSTURE"
    )

    assert decisions[0]["decision_value"] == (
        "POSITIVE"
    )


def test_unified_investment_intelligence(
    tmp_path,
):
    (
        event_store,
        state_store,
        signal_store,
        decision_store,
    ) = build_environment(tmp_path)

    create_event(
        event_store,
        document_id="DOC-001",
        document_sha="a" * 64,
        filing_date="2026-09-01",
        offering=10_000_000,
        sold=4_000_000,
        source_event_id="EVENT-001",
        source_run_id="RUN-001",
    )

    create_event(
        event_store,
        document_id="DOC-002",
        document_sha="b" * 64,
        filing_date="2026-10-05",
        offering=20_000_000,
        sold=8_000_000,
        source_event_id="EVENT-002",
        source_run_id="RUN-002",
    )

    projector = build_company_state(
        event_store,
        state_store,
    )

    projector.project_company(
        COMPANY_ID
    )

    intelligence = UnifiedInvestmentIntelligence(
        state_store=state_store,
        signal_store=signal_store,
        decision_store=decision_store,
    )

    result = intelligence.project_company(
        COMPANY_ID
    )

    assert result["company_id"] == (
        COMPANY_ID
    )

    assert result["cik"] == CIK

    assert result["funding_event_count"] == 2

    assert result[
        "cumulative_offering_amount"
    ] == 30_000_000.0

    assert result[
        "cumulative_amount_sold"
    ] == 12_000_000.0

    assert result[
        "investment_signal_count"
    ] >= 2

    assert result[
        "investment_posture"
    ] == "POSITIVE"

    assert result[
        "investment_posture_confidence"
    ] == "MEDIUM"


def test_investment_signal_generation_is_idempotent(
    tmp_path,
):
    (
        event_store,
        state_store,
        signal_store,
        _,
    ) = build_environment(tmp_path)

    create_event(
        event_store,
        document_id="DOC-001",
        document_sha="a" * 64,
        filing_date="2026-10-05",
        offering=10_000_000,
        sold=4_000_000,
        source_event_id="EVENT-001",
        source_run_id="RUN-001",
    )

    projector = build_company_state(
        event_store,
        state_store,
    )

    projector.project_company(
        COMPANY_ID
    )

    engine = InvestmentSignalEngine(
        state_store=state_store,
        signal_store=signal_store,
    )

    first = engine.generate(
        COMPANY_ID
    )

    second = engine.generate(
        COMPANY_ID
    )

    assert first["signals_created"] == 2
    assert second["signals_created"] == 2

    signals = signal_store.list_for_company(
        COMPANY_ID
    )

    assert len(signals) == 2

    signal_ids = [
        signal["signal_id"]
        for signal in signals
    ]

    assert len(signal_ids) == len(
        set(signal_ids)
    )