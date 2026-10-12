from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


CAPABILITY_STATES = {
    "IMPLEMENTED",
    "NOT_READY",
    "DISABLED",
}


@dataclass(frozen=True)
class ComponentCapability:
    component_id: str
    state: str
    reason: str = ""

    def validate(self) -> None:
        if not self.component_id:
            raise ValueError("component_id is required")

        if self.state not in CAPABILITY_STATES:
            raise ValueError(
                f"Invalid capability state: {self.state}"
            )


COMPONENT_CAPABILITIES: Dict[str, ComponentCapability] = {
    "company_registry": ComponentCapability(
        "company_registry",
        "IMPLEMENTED",
        "Company lifecycle registry",
    ),

    "source_registry": ComponentCapability(
        "source_registry",
        "IMPLEMENTED",
        "Company source mapping",
    ),

    "sec_ingestion": ComponentCapability(
        "sec_ingestion",
        "IMPLEMENTED",
        "SEC submissions and company facts ingestion",
    ),

    "financial_normalization": ComponentCapability(
        "financial_normalization",
        "IMPLEMENTED",
        "Financial Silver normalization",
    ),

    "quarter_derivation": ComponentCapability(
        "quarter_derivation",
        "IMPLEMENTED",
        "Quarterly derivation and Step 9/10 promotion",
    ),

    "financial_features": ComponentCapability(
        "financial_features",
        "IMPLEMENTED",
        "Financial feature generation",
    ),

    "financial_decision": ComponentCapability(
        "financial_decision",
        "IMPLEMENTED",
        "Financial decision intelligence",
    ),

    "form_d_ingestion": ComponentCapability(
        "form_d_ingestion",
        "IMPLEMENTED",
        "Step 17 Form D processing and Step 17.1 DQ",
    ),

    "investor_ownership": ComponentCapability(
        "investor_ownership",
        "IMPLEMENTED",
        "Step 18 ownership layer and Step 18.1 DQ",
    ),

    "valuation": ComponentCapability(
        "valuation",
        "IMPLEMENTED",
        "Step 19 valuation layer and Step 19.1 DQ",
    ),

    "company_profile": ComponentCapability(
        "company_profile",
        "IMPLEMENTED",
        "Step 16 profile generation and Step 16.1 quality audit",
    ),

    "investment_intelligence": ComponentCapability(
        "investment_intelligence",
        "IMPLEMENTED",
        "Step 20 integration and Step 20.1 DQ",
    ),

    "unified_company_view": ComponentCapability(
        "unified_company_view",
        "IMPLEMENTED",
        "Company-scoped unified investment and event intelligence view",
    ),

    "company_discovery": ComponentCapability(
        "company_discovery",
        "IMPLEMENTED",
        "Step 21 screening and Step 21.1 quality audit",
    ),

    "document_ingestion": ComponentCapability(
        "document_ingestion",
        "IMPLEMENTED",
        "Document ingestion orchestration adapter implemented",
    ),

    "document_extraction": ComponentCapability(
        "document_extraction",
        "IMPLEMENTED",
        "Deterministic document text extraction adapter implemented",
    ),

    "document_classification": ComponentCapability(
        "document_classification",
        "IMPLEMENTED",
        "Deterministic document classification adapter implemented",
    ),

    "document_understanding": ComponentCapability(
        "document_understanding",
        "IMPLEMENTED",
        "Deterministic document understanding adapter implemented",
    ),

    "structured_event_persistence": ComponentCapability(
        "structured_event_persistence",
        "IMPLEMENTED",
        "Durable persistence for deterministic structured document events",
    ),

    "company_state_projection": ComponentCapability(
        "company_state_projection",
        "IMPLEMENTED",
        "Projects persisted structured events into company intelligence state",
    ),

    "company_state_investment_intelligence": ComponentCapability(
        "company_state_investment_intelligence",
        "IMPLEMENTED",
        "Derives investment signals and posture from projected company state",
    ),

}


def get_capability(component_id: str) -> ComponentCapability:
    capability = COMPONENT_CAPABILITIES.get(component_id)

    if capability is None:
        raise KeyError(
            f"No capability registered for component: {component_id}"
        )

    capability.validate()
    return capability


def is_executable(component_id: str) -> bool:
    return get_capability(component_id).state == "IMPLEMENTED"