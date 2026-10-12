from __future__ import annotations

from pathlib import Path

import pytest

from company_platform.company_intelligence.company_state_store import (
    CompanyStateStore,
)
from company_platform.company_intelligence.state_projector import (
    CompanyStateProjector,
)
from company_platform.events.structured_event_store import (
    StructuredEventStore,
)


# =============================================================================
# TEST CONSTANTS
# =============================================================================

COMPANY_ID = "SEC_0000320193"
CIK = "0000320193"

SOURCE_EVENT_ID = "SOURCE-EVENT-001"
SOURCE_RUN_ID = "RUN-001"

DOCUMENT_ID_1 = "DOCUMENT-001"
DOCUMENT_ID_2 = "DOCUMENT-002"

DOCUMENT_SHA_1 = "a" * 64
DOCUMENT_SHA_2 = "b" * 64


# =============================================================================
# HELPERS
# =============================================================================

def create_structured_event(
    store: StructuredEventStore,
    *,
    document_id: str,
    document_sha256: str,
    source_event_id: str,
    source_run_id: str,
    filing_date: str,
    issuer_name: str,
    total_offering_amount: str,
    total_amount_sold: str,
):
    """
    Persist one SEC_FORM_D_FILING structured event.
    """

    facts = {
        "filing_date": filing_date,
        "issuer_name": issuer_name,
        "total_offering_amount": total_offering_amount,
        "total_amount_sold": total_amount_sold,
    }

    return store.persist(
        company_id=COMPANY_ID,
        cik=CIK,
        document_id=document_id,
        document_class="SEC_FORM_D",
        event_type="SEC_FORM_D_FILING",
        source_document_path=(
            f"data/bronze/documents/"
            f"cik={CIK}/"
            f"{document_id}/"
            f"filing.pdf"
        ),
        source_document_sha256=document_sha256,
        source_environment="PRODUCTION",
        source_event_id=source_event_id,
        source_run_id=source_run_id,
        event_confidence=1.0,
        facts=facts,
    )


def build_projector(tmp_path: Path):
    """
    Build isolated event + company-state stores for the test.
    """

    structured_event_db = (
        tmp_path / "structured_events.db"
    )

    company_state_db = (
        tmp_path / "company_intelligence_state.db"
    )

    event_store = StructuredEventStore(
        path=structured_event_db
    )

    state_store = CompanyStateStore(
        path=company_state_db
    )

    projector = CompanyStateProjector(
        event_store=event_store,
        state_store=state_store,
    )

    return (
        event_store,
        state_store,
        projector,
    )


# =============================================================================
# TEST 1
# =============================================================================

def test_form_d_event_creates_company_state(tmp_path):
    """
    One SEC_FORM_D_FILING event should produce an ACTIVE
    company intelligence state.
    """

    (
        event_store,
        state_store,
        projector,
    ) = build_projector(tmp_path)

    event = create_structured_event(
        event_store,
        document_id=DOCUMENT_ID_1,
        document_sha256=DOCUMENT_SHA_1,
        source_event_id=SOURCE_EVENT_ID,
        source_run_id=SOURCE_RUN_ID,
        filing_date="2026-10-05",
        issuer_name="Example Technologies Inc.",
        total_offering_amount="10000000.0",
        total_amount_sold="4000000.0",
    )

    result = projector.project_company(
        COMPANY_ID
    )

    # -------------------------------------------------------------------------
    # Projection result
    # -------------------------------------------------------------------------

    assert result["projected"] is True
    assert result["status"] == "ACTIVE"
    assert result["company_id"] == COMPANY_ID
    assert result["cik"] == CIK

    assert result["funding_event_count"] == 1

    assert result["latest_funding_event_id"] == (
        event["event_id"]
    )

    assert result["latest_funding_date"] == (
        "2026-10-05"
    )

    assert result["cumulative_offering_amount"] == (
        10_000_000.0
    )

    assert result["cumulative_amount_sold"] == (
        4_000_000.0
    )

    # -------------------------------------------------------------------------
    # Persisted state
    # -------------------------------------------------------------------------

    state = state_store.get(
        COMPANY_ID
    )

    assert state is not None

    assert state["company_id"] == COMPANY_ID
    assert state["cik"] == CIK
    assert state["status"] == "ACTIVE"

    assert state["state_version"] == 1

    assert state["funding_event_count"] == 1

    assert state["latest_funding_event_id"] == (
        event["event_id"]
    )

    assert state["latest_funding_event_type"] == (
        "SEC_FORM_D_FILING"
    )

    assert state["latest_funding_date"] == (
        "2026-10-05"
    )

    assert state["latest_issuer_name"] == (
        "Example Technologies Inc."
    )

    assert float(
        state["latest_total_offering_amount"]
    ) == 10_000_000.0

    assert float(
        state["latest_total_amount_sold"]
    ) == 4_000_000.0

    assert float(
        state["cumulative_offering_amount"]
    ) == 10_000_000.0

    assert float(
        state["cumulative_amount_sold"]
    ) == 4_000_000.0

    # -------------------------------------------------------------------------
    # Provenance
    # -------------------------------------------------------------------------

    assert state["last_source_event_id"] == (
        SOURCE_EVENT_ID
    )

    assert state["last_source_run_id"] == (
        SOURCE_RUN_ID
    )

    assert state["last_source_document_id"] == (
        DOCUMENT_ID_1
    )

    assert state["last_source_document_sha256"] == (
        DOCUMENT_SHA_1
    )


# =============================================================================
# TEST 2
# =============================================================================

def test_duplicate_event_does_not_double_count(tmp_path):
    """
    Persisting the same business event twice must not cause
    the company state to double count the funding event.
    """

    (
        event_store,
        state_store,
        projector,
    ) = build_projector(tmp_path)

    first = create_structured_event(
        event_store,
        document_id=DOCUMENT_ID_1,
        document_sha256=DOCUMENT_SHA_1,
        source_event_id=SOURCE_EVENT_ID,
        source_run_id=SOURCE_RUN_ID,
        filing_date="2026-10-05",
        issuer_name="Example Technologies Inc.",
        total_offering_amount="10000000.0",
        total_amount_sold="4000000.0",
    )

    duplicate = create_structured_event(
        event_store,
        document_id=DOCUMENT_ID_1,
        document_sha256=DOCUMENT_SHA_1,
        source_event_id=SOURCE_EVENT_ID,
        source_run_id=SOURCE_RUN_ID,
        filing_date="2026-10-05",
        issuer_name="Example Technologies Inc.",
        total_offering_amount="10000000.0",
        total_amount_sold="4000000.0",
    )

    # The StructuredEventStore should return the same event identity.
    assert duplicate["event_id"] == first["event_id"]

    result = projector.project_company(
        COMPANY_ID
    )

    assert result["projected"] is True

    assert result["funding_event_count"] == 1

    assert result["cumulative_offering_amount"] == (
        10_000_000.0
    )

    assert result["cumulative_amount_sold"] == (
        4_000_000.0
    )

    state = state_store.get(
        COMPANY_ID
    )

    assert state is not None

    assert state["funding_event_count"] == 1

    assert float(
        state["cumulative_offering_amount"]
    ) == 10_000_000.0

    assert float(
        state["cumulative_amount_sold"]
    ) == 4_000_000.0


# =============================================================================
# TEST 3
# =============================================================================

def test_multiple_events_build_cumulative_state(tmp_path):
    """
    Multiple distinct funding events should produce:

        funding_event_count = 2
        cumulative offering = 30M
        cumulative sold = 12M

    while latest_* fields represent only the most recent event.
    """

    (
        event_store,
        state_store,
        projector,
    ) = build_projector(tmp_path)

    event_1 = create_structured_event(
        event_store,
        document_id=DOCUMENT_ID_1,
        document_sha256=DOCUMENT_SHA_1,
        source_event_id="SOURCE-EVENT-001",
        source_run_id="RUN-001",
        filing_date="2026-09-01",
        issuer_name="Example Technologies Inc.",
        total_offering_amount="10000000.0",
        total_amount_sold="4000000.0",
    )

    event_2 = create_structured_event(
        event_store,
        document_id=DOCUMENT_ID_2,
        document_sha256=DOCUMENT_SHA_2,
        source_event_id="SOURCE-EVENT-002",
        source_run_id="RUN-002",
        filing_date="2026-10-05",
        issuer_name="Example Technologies Inc.",
        total_offering_amount="20000000.0",
        total_amount_sold="8000000.0",
    )

    result = projector.project_company(
        COMPANY_ID
    )

    # -------------------------------------------------------------------------
    # Aggregate state
    # -------------------------------------------------------------------------

    assert result["funding_event_count"] == 2

    assert result["cumulative_offering_amount"] == (
        30_000_000.0
    )

    assert result["cumulative_amount_sold"] == (
        12_000_000.0
    )

    # -------------------------------------------------------------------------
    # Latest event state
    # -------------------------------------------------------------------------

    assert result["latest_funding_event_id"] == (
        event_2["event_id"]
    )

    assert result["latest_funding_date"] == (
        "2026-10-05"
    )

    state = state_store.get(
        COMPANY_ID
    )

    assert state is not None

    assert state["funding_event_count"] == 2

    assert state["latest_funding_event_id"] == (
        event_2["event_id"]
    )

    assert state["latest_funding_event_type"] == (
        "SEC_FORM_D_FILING"
    )

    assert float(
        state["latest_total_offering_amount"]
    ) == 20_000_000.0

    assert float(
        state["latest_total_amount_sold"]
    ) == 8_000_000.0

    assert float(
        state["cumulative_offering_amount"]
    ) == 30_000_000.0

    assert float(
        state["cumulative_amount_sold"]
    ) == 12_000_000.0

    # -------------------------------------------------------------------------
    # Latest-event provenance
    # -------------------------------------------------------------------------

    assert state["last_source_event_id"] == (
        "SOURCE-EVENT-002"
    )

    assert state["last_source_run_id"] == (
        "RUN-002"
    )

    assert state["last_source_document_id"] == (
        DOCUMENT_ID_2
    )

    assert state["last_source_document_sha256"] == (
        DOCUMENT_SHA_2
    )


# =============================================================================
# TEST 4
# =============================================================================

def test_company_isolation(tmp_path):
    """
    Events belonging to one company must never appear in another
    company's state.
    """

    (
        event_store,
        state_store,
        projector,
    ) = build_projector(tmp_path)

    microsoft_company_id = "SEC_0000789019"
    microsoft_cik = "0000789019"

    # -------------------------------------------------------------------------
    # Apple event
    # -------------------------------------------------------------------------

    create_structured_event(
        event_store,
        document_id=DOCUMENT_ID_1,
        document_sha256=DOCUMENT_SHA_1,
        source_event_id="APPLE-SOURCE-001",
        source_run_id="APPLE-RUN-001",
        filing_date="2026-09-01",
        issuer_name="Apple Example Entity",
        total_offering_amount="10000000.0",
        total_amount_sold="4000000.0",
    )

    # -------------------------------------------------------------------------
    # Microsoft event
    #
    # The helper uses the Apple constants, so insert this one directly
    # against the StructuredEventStore with Microsoft's identity.
    # -------------------------------------------------------------------------

    microsoft_facts = {
        "filing_date": "2026-10-01",
        "issuer_name": "Microsoft Example Entity",
        "total_offering_amount": "5000000.0",
        "total_amount_sold": "2000000.0",
    }

    microsoft_event = event_store.persist(
        company_id=microsoft_company_id,
        cik=microsoft_cik,
        document_id="MICROSOFT-DOCUMENT-001",
        document_class="SEC_FORM_D",
        event_type="SEC_FORM_D_FILING",
        source_document_path=(
            "data/bronze/documents/"
            f"cik={microsoft_cik}/"
            "MICROSOFT-DOCUMENT-001/"
            "filing.pdf"
        ),
        source_document_sha256="c" * 64,
        source_environment="PRODUCTION",
        source_event_id="MICROSOFT-SOURCE-001",
        source_run_id="MICROSOFT-RUN-001",
        event_confidence=1.0,
        facts=microsoft_facts,
    )

    # -------------------------------------------------------------------------
    # Project Apple only
    # -------------------------------------------------------------------------

    apple_result = projector.project_company(
        COMPANY_ID
    )

    assert apple_result["projected"] is True
    assert apple_result["funding_event_count"] == 1

    # -------------------------------------------------------------------------
    # Microsoft state must not exist yet
    # -------------------------------------------------------------------------

    microsoft_state_before = state_store.get(
        microsoft_company_id
    )

    assert microsoft_state_before is None

    # -------------------------------------------------------------------------
    # Project Microsoft
    # -------------------------------------------------------------------------

    microsoft_result = projector.project_company(
        microsoft_company_id
    )

    assert microsoft_result["projected"] is True

    assert microsoft_result["funding_event_count"] == 1

    assert microsoft_result["latest_funding_event_id"] == (
        microsoft_event["event_id"]
    )

    assert microsoft_result[
        "cumulative_offering_amount"
    ] == 5_000_000.0

    assert microsoft_result[
        "cumulative_amount_sold"
    ] == 2_000_000.0

    # -------------------------------------------------------------------------
    # Verify both states independently
    # -------------------------------------------------------------------------

    apple_state = state_store.get(
        COMPANY_ID
    )

    microsoft_state = state_store.get(
        microsoft_company_id
    )

    assert apple_state is not None
    assert microsoft_state is not None

    assert apple_state["cik"] == CIK
    assert microsoft_state["cik"] == microsoft_cik

    assert float(
        apple_state["cumulative_offering_amount"]
    ) == 10_000_000.0

    assert float(
        microsoft_state["cumulative_offering_amount"]
    ) == 5_000_000.0

    assert apple_state["last_source_event_id"] == (
        "APPLE-SOURCE-001"
    )

    assert microsoft_state["last_source_event_id"] == (
        "MICROSOFT-SOURCE-001"
    )


# =============================================================================
# TEST 5
# =============================================================================

def test_projection_replay_produces_same_business_state(tmp_path):
    """
    Re-projecting the same immutable structured-event history must
    reproduce the same business values.
    """

    (
        event_store,
        state_store,
        projector,
    ) = build_projector(tmp_path)

    create_structured_event(
        event_store,
        document_id=DOCUMENT_ID_1,
        document_sha256=DOCUMENT_SHA_1,
        source_event_id="SOURCE-EVENT-001",
        source_run_id="RUN-001",
        filing_date="2026-09-01",
        issuer_name="Example Technologies Inc.",
        total_offering_amount="10000000.0",
        total_amount_sold="4000000.0",
    )

    create_structured_event(
        event_store,
        document_id=DOCUMENT_ID_2,
        document_sha256=DOCUMENT_SHA_2,
        source_event_id="SOURCE-EVENT-002",
        source_run_id="RUN-002",
        filing_date="2026-10-05",
        issuer_name="Example Technologies Inc.",
        total_offering_amount="20000000.0",
        total_amount_sold="8000000.0",
    )

    # -------------------------------------------------------------------------
    # First projection
    # -------------------------------------------------------------------------

    first_result = projector.project_company(
        COMPANY_ID
    )

    first_state = state_store.get(
        COMPANY_ID
    )

    assert first_state is not None

    first_business_values = {
        "status": first_state["status"],
        "funding_event_count": first_state[
            "funding_event_count"
        ],
        "latest_funding_event_id": first_state[
            "latest_funding_event_id"
        ],
        "latest_funding_event_type": first_state[
            "latest_funding_event_type"
        ],
        "latest_funding_date": first_state[
            "latest_funding_date"
        ],
        "latest_issuer_name": first_state[
            "latest_issuer_name"
        ],
        "latest_total_offering_amount": float(
            first_state[
                "latest_total_offering_amount"
            ]
        ),
        "latest_total_amount_sold": float(
            first_state[
                "latest_total_amount_sold"
            ]
        ),
        "cumulative_offering_amount": float(
            first_state[
                "cumulative_offering_amount"
            ]
        ),
        "cumulative_amount_sold": float(
            first_state[
                "cumulative_amount_sold"
            ]
        ),
        "last_source_event_id": first_state[
            "last_source_event_id"
        ],
        "last_source_run_id": first_state[
            "last_source_run_id"
        ],
        "last_source_document_id": first_state[
            "last_source_document_id"
        ],
        "last_source_document_sha256": first_state[
            "last_source_document_sha256"
        ],
    }

    # -------------------------------------------------------------------------
    # Second projection
    # -------------------------------------------------------------------------

    second_result = projector.project_company(
        COMPANY_ID
    )

    second_state = state_store.get(
        COMPANY_ID
    )

    assert second_state is not None

    second_business_values = {
        "status": second_state["status"],
        "funding_event_count": second_state[
            "funding_event_count"
        ],
        "latest_funding_event_id": second_state[
            "latest_funding_event_id"
        ],
        "latest_funding_event_type": second_state[
            "latest_funding_event_type"
        ],
        "latest_funding_date": second_state[
            "latest_funding_date"
        ],
        "latest_issuer_name": second_state[
            "latest_issuer_name"
        ],
        "latest_total_offering_amount": float(
            second_state[
                "latest_total_offering_amount"
            ]
        ),
        "latest_total_amount_sold": float(
            second_state[
                "latest_total_amount_sold"
            ]
        ),
        "cumulative_offering_amount": float(
            second_state[
                "cumulative_offering_amount"
            ]
        ),
        "cumulative_amount_sold": float(
            second_state[
                "cumulative_amount_sold"
            ]
        ),
        "last_source_event_id": second_state[
            "last_source_event_id"
        ],
        "last_source_run_id": second_state[
            "last_source_run_id"
        ],
        "last_source_document_id": second_state[
            "last_source_document_id"
        ],
        "last_source_document_sha256": second_state[
            "last_source_document_sha256"
        ],
    }

    assert second_business_values == (
        first_business_values
    )

    # -------------------------------------------------------------------------
    # Projection result should also contain the same business values
    # -------------------------------------------------------------------------

    assert second_result["projected"] is True

    assert second_result["funding_event_count"] == (
        first_result["funding_event_count"]
    )

    assert second_result[
        "latest_funding_event_id"
    ] == first_result[
        "latest_funding_event_id"
    ]

    assert second_result[
        "cumulative_offering_amount"
    ] == first_result[
        "cumulative_offering_amount"
    ]

    assert second_result[
        "cumulative_amount_sold"
    ] == first_result[
        "cumulative_amount_sold"
    ]


# =============================================================================
# TEST 6
# =============================================================================

def test_no_structured_events_does_not_create_state(tmp_path):
    """
    A company with no structured events should remain NO_DATA and
    should not receive a fabricated company state.
    """

    (
        event_store,
        state_store,
        projector,
    ) = build_projector(tmp_path)

    result = projector.project_company(
        COMPANY_ID
    )

    assert result["projected"] is False
    assert result["status"] == "NO_DATA"

    assert result["reason"] == (
        "NO_STRUCTURED_EVENTS"
    )

    state = state_store.get(
        COMPANY_ID
    )

    assert state is None