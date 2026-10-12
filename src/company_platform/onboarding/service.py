from __future__ import annotations

from company_platform.events.models import PlatformEvent
from company_platform.events.event_store import EventStore

from company_platform.registry.company_registry import (
    register_company,
)

from company_platform.registry.source_registry import (
    enable_company_sources,
)


class CompanyOnboardingService:

    def __init__(self, event_store=None):
        self.event_store = event_store or EventStore()

    def onboard(self, cik: str) -> PlatformEvent:

        company = register_company(
            cik=cik,
            source="USER",
        )

        company_id = company["company_id"]

        enable_company_sources(
            company_id=company_id,
        )

        event = PlatformEvent(
            event_type="COMPANY_ONBOARDED",
            company_id=company_id,
            source_id="SEC",
            payload={
                "cik": company["cik"],
                "company_name": company["company_name"],
                "ticker": company["ticker"],
            },
        )

        self.event_store.append(event)

        return event