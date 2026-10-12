from __future__ import annotations

import json
from datetime import datetime, timezone

from company_platform.company_intelligence.company_state_store import (
    CompanyStateStore,
)
from company_platform.events.structured_event_store import (
    StructuredEventStore,
)


SUPPORTED_EVENTS = {
    "SEC_FORM_D_FILING",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _number(value):
    if value is None:
        return 0.0

    try:
        return float(str(value).replace(",", "").replace("$", ""))
    except (TypeError, ValueError):
        return 0.0


def _latest_production_filing(events):
    """Return the latest persisted production filing, without treating it as funding."""
    if events is None or events.empty or "event_type" not in events.columns:
        return None
    filings = events[
        events["event_type"].astype(str).str.upper().eq("SEC_FILING")
        & events["document_id"].notna()
        & events["document_id"].astype(str).str.strip().ne("")
    ]
    if filings.empty:
        return None
    sort_columns = [
        column
        for column in ("updated_at_utc", "created_at_utc", "event_id")
        if column in filings.columns
    ]
    ordered = filings.sort_values(sort_columns, kind="mergesort")
    return ordered.iloc[-1]


def _event_date(facts: dict) -> str | None:
    value = facts.get("filing_date")

    if value is None:
        return None

    return str(value)


class CompanyStateProjector:
    """
    Projects structured business events into current company state.

    The projection is deterministic and replayable.
    """

    def __init__(
        self,
        *,
        event_store=None,
        state_store=None,
    ):
        self.event_store = event_store or StructuredEventStore()
        self.state_store = state_store or CompanyStateStore()

    def project_company(self, company_id: str) -> dict:
        all_events = self.event_store.list_events(
            company_id=company_id,
        )
        events = self.event_store.list_events(
            company_id=company_id,
            source_environment="PRODUCTION",
        )

        if events.empty:
            cik = (
                str(all_events.iloc[0]["cik"]).zfill(10)
                if not all_events.empty and "cik" in all_events.columns
                else company_id.removeprefix("SEC_").zfill(10)
            )
            state = self.state_store.mark_no_data(company_id, cik)
            return {
                "company_id": company_id,
                "status": "NO_DATA",
                "projected": False,
                "reason": (
                    "NO_PRODUCTION_STRUCTURED_EVENTS"
                    if not all_events.empty
                    else "NO_STRUCTURED_EVENTS"
                ),
                "state_version": state.get("state_version"),
            }

        events = events[
            events["status"].isin(
                {
                    "PERSISTED",
                    "PROCESSED",
                }
            )
        ]

        production_events = events
        events = production_events[
            production_events["event_type"].isin(SUPPORTED_EVENTS)
        ]

        if events.empty:
            cik = (
                str(all_events.iloc[0]["cik"]).zfill(10)
                if not all_events.empty and "cik" in all_events.columns
                else company_id.removeprefix("SEC_").zfill(10)
            )
            filing = _latest_production_filing(production_events)
            if filing is not None:
                state = self.state_store.record_filing_evidence(
                    company_id=company_id,
                    cik=cik,
                    source_event_id=filing.get("source_event_id"),
                    source_run_id=filing.get("source_run_id"),
                    source_document_id=filing.get("document_id"),
                    source_document_sha256=filing.get("source_document_sha256"),
                )
                return {
                    "company_id": company_id,
                    "status": "NO_DATA",
                    "projected": False,
                    "reason": "NO_SUPPORTED_FUNDING_EVENTS",
                    "source_document_id": filing.get("document_id"),
                    "state_version": state.get("state_version"),
                }
            state = self.state_store.mark_no_data(company_id, cik)
            return {
                "company_id": company_id,
                "status": "NO_DATA",
                "projected": False,
                "reason": "NO_SUPPORTED_STRUCTURED_EVENTS",
                "state_version": state.get("state_version"),
            }

        rows = []

        for _, event in events.iterrows():
            facts = json.loads(
                event["facts_json"]
            )

            rows.append(
                {
                    "event_id": event["event_id"],
                    "event_type": event["event_type"],
                    "document_id": event["document_id"],
                    "event_date": _event_date(facts),
                    "issuer_name": facts.get("issuer_name"),
                    "total_offering_amount": _number(
                        facts.get("total_offering_amount")
                    ),
                    "total_amount_sold": _number(
                        facts.get("total_amount_sold")
                    ),
                    "source_event_id": event["source_event_id"],
                    "source_run_id": event["source_run_id"],
                    "source_document_sha256":
                        event["source_document_sha256"],
                }
            )

        rows.sort(
            key=lambda row: (
                row["event_date"] or "",
                row["event_id"],
            )
        )

        latest = rows[-1]

        cumulative_offering = sum(
            row["total_offering_amount"]
            for row in rows
        )

        cumulative_sold = sum(
            row["total_amount_sold"]
            for row in rows
        )

        company = events.iloc[0]["company_id"]
        cik = str(events.iloc[0]["cik"]).zfill(10)

        previous = self.state_store.get(company)

        state_version = (
            int(previous["state_version"]) + 1
            if previous
            else 1
        )

        state = self.state_store.upsert(
            company_id=company,
            cik=cik,
            state_version=state_version,
            status="ACTIVE",
            funding_event_count=len(rows),
            latest_funding_event_id=latest["event_id"],
            latest_funding_event_type=latest["event_type"],
            latest_funding_date=latest["event_date"],
            latest_issuer_name=latest["issuer_name"],
            latest_total_offering_amount=(
                latest["total_offering_amount"]
            ),
            latest_total_amount_sold=(
                latest["total_amount_sold"]
            ),
            cumulative_offering_amount=cumulative_offering,
            cumulative_amount_sold=cumulative_sold,
            last_source_event_id=latest["source_event_id"],
            last_source_run_id=latest["source_run_id"],
            last_source_document_id=latest["document_id"],
            last_source_document_sha256=(
                latest["source_document_sha256"]
            ),
        )

        return {
            "company_id": company,
            "cik": cik,
            "status": "ACTIVE",
            "projected": True,
            "state_version": state["state_version"],
            "funding_event_count": len(rows),
            "latest_funding_event_id": latest["event_id"],
            "latest_funding_date": latest["event_date"],
            "cumulative_offering_amount": cumulative_offering,
            "cumulative_amount_sold": cumulative_sold,
        }