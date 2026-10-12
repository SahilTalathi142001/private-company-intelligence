from dataclasses import dataclass
from typing import List, Optional

CHANGE_TYPES = {"NEW_COMPANY","NEW_SEC_FILING","SOURCE_DATA_CHANGED","DOCUMENT_RECEIVED","MANUAL_REPROCESS","RETRY"}
CHANGE_SCOPES = {"COMPANY","SOURCE","DATASET","DOCUMENT"}
PROCESSING_MODES = {"FULL","INCREMENTAL"}

@dataclass(frozen=True)
class ChangeEvent:
    change_id: str
    change_type: str
    company_id: str
    source_id: Optional[str]
    dataset_id: Optional[str]
    occurred_at_utc: str
    scope: str
    reference: Optional[str] = None

    def validate(self):
        if not self.change_id or not self.company_id:
            raise ValueError("change_id and company_id are required")
        if self.change_type not in CHANGE_TYPES:
            raise ValueError(f"Invalid change_type: {self.change_type}")
        if self.scope not in CHANGE_SCOPES:
            raise ValueError(f"Invalid scope: {self.scope}")

@dataclass(frozen=True)
class IncrementalDecision:
    change_id: str
    company_id: str
    processing_mode: str
    root_component_id: str
    affected_components: List[str]
    reason_code: str
    dependency_version: str

    def validate(self):
        if self.processing_mode not in PROCESSING_MODES:
            raise ValueError(f"Invalid processing_mode: {self.processing_mode}")
        if not self.root_component_id or not self.affected_components:
            raise ValueError("root_component_id and affected_components are required")
        if not self.reason_code:
            raise ValueError("reason_code is required")
