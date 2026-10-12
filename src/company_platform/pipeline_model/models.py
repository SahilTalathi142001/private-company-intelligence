from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

VALID_COMPONENT_TYPES={"CONTROL","INGESTION","TRANSFORMATION","ANALYTICS","ENRICHMENT","GOLD","SERVING_GOLD","PERSISTENCE"}
VALID_EXECUTION_MODES={"EVENT_DRIVEN","INCREMENTAL","FULL"}
@dataclass(frozen=True)
class RetryPolicy:
    max_attempts:int
    backoff_seconds:int
    def validate(self):
        if self.max_attempts<1: raise ValueError("max_attempts must be >= 1")
        if self.backoff_seconds<0: raise ValueError("backoff_seconds cannot be negative")
@dataclass(frozen=True)
class PipelineComponent:
    component_id:str; component_type:str; description:str; depends_on:List[str]; trigger_events:List[str]; execution_mode:str; retry_policy:RetryPolicy; dq_gate:Optional[str]; outputs:List[str]; downstream:List[str]
    def validate(self):
        if not self.component_id.strip(): raise ValueError("component_id must be nonblank")
        if self.component_type not in VALID_COMPONENT_TYPES: raise ValueError(f"Invalid component_type: {self.component_type}")
        if self.execution_mode not in VALID_EXECUTION_MODES: raise ValueError(f"Invalid execution_mode: {self.execution_mode}")
        self.retry_policy.validate()
        if not self.outputs: raise ValueError(f"{self.component_id}: outputs cannot be empty")
    def to_dict(self)->Dict[str,Any]:
        self.validate(); d=asdict(self); d["retry_policy"]=asdict(self.retry_policy); return d
