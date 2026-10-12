from typing import Dict

class EventRouter:
    """Maps A3 event types to orchestration change types."""

    EVENT_TO_CHANGE = {
        "COMPANY_ONBOARDED": "NEW_COMPANY",
        "NEW_SEC_FILING": "NEW_SEC_FILING",
        "SOURCE_DATA_AVAILABLE": "SOURCE_DATA_CHANGED",
        "DOCUMENT_RECEIVED": "DOCUMENT_RECEIVED",
        "PIPELINE_REQUESTED": "MANUAL_REPROCESS",
    }

    def route(self, event):
        event.validate()
        if event.event_type not in self.EVENT_TO_CHANGE:
            return None
        return {
            "change_type": self.EVENT_TO_CHANGE[event.event_type],
            "company_id": event.company_id,
            "source_id": event.source_id,
            "correlation_id": event.correlation_id,
            "trigger_event_id": event.event_id,
        }
