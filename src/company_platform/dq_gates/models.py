from dataclasses import dataclass
from typing import Optional

GATE_STATUSES = {"PENDING", "PASS", "FAIL", "QUARANTINED", "BYPASSED"}
SEVERITIES = {"INFO", "WARNING", "FATAL"}
PROMOTION_STATUSES = {"PENDING", "PROMOTE", "BLOCK", "QUARANTINE"}

@dataclass(frozen=True)
class DQResult:
    gate_id: str
    run_id: str
    task_id: Optional[str]
    component_id: str
    status: str
    severity: str
    check_count: int
    failed_count: int
    fatal_count: int
    warning_count: int
    message: Optional[str] = None

    def validate(self):
        if self.status not in GATE_STATUSES:
            raise ValueError(f"Invalid DQ status: {self.status}")
        if self.severity not in SEVERITIES:
            raise ValueError(f"Invalid severity: {self.severity}")
        if min(self.check_count, self.failed_count, self.fatal_count, self.warning_count) < 0:
            raise ValueError("DQ counts cannot be negative")
        if self.failed_count > self.check_count:
            raise ValueError("failed_count cannot exceed check_count")
        if self.fatal_count > self.failed_count:
            raise ValueError("fatal_count cannot exceed failed_count")
        if self.warning_count > self.failed_count:
            raise ValueError("warning_count cannot exceed failed_count")

@dataclass(frozen=True)
class PromotionDecision:
    promotion_id: str
    run_id: str
    component_id: str
    dq_status: str
    decision: str
    reason_code: str
    decided_at_utc: str

    def validate(self):
        if self.dq_status not in GATE_STATUSES:
            raise ValueError(f"Invalid dq_status: {self.dq_status}")
        if self.decision not in PROMOTION_STATUSES:
            raise ValueError(f"Invalid promotion decision: {self.decision}")
        if not self.reason_code:
            raise ValueError("reason_code is required")
