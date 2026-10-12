from datetime import datetime, timezone
import uuid
from .models import DQResult, PromotionDecision

def utc_now():
    return datetime.now(timezone.utc).isoformat()

class DQGateEngine:
    """
    A7 converts component DQ outcomes into an explicit promotion decision.
    It does not run analytical DQ code itself; it consumes the authoritative
    result produced by a component's DQ check.
    """

    def evaluate(self, run_id, component_id, check_count, failed_count,
                 fatal_count=0, warning_count=0, task_id=None, message=None):
        if check_count < 0 or failed_count < 0 or fatal_count < 0 or warning_count < 0:
            raise ValueError("DQ counts cannot be negative")

        if fatal_count > 0:
            status, severity = "FAIL", "FATAL"
        elif failed_count > 0:
            status, severity = "FAIL", "WARNING"
        else:
            status, severity = "PASS", "INFO"

        result = DQResult(
            gate_id=str(uuid.uuid4()),
            run_id=run_id,
            task_id=task_id,
            component_id=component_id,
            status=status,
            severity=severity,
            check_count=check_count,
            failed_count=failed_count,
            fatal_count=fatal_count,
            warning_count=warning_count,
            message=message,
        )
        result.validate()
        return result

    def promote(self, dq_result):
        dq_result.validate()
        if dq_result.fatal_count > 0:
            decision, reason = "BLOCK", "FATAL_DQ_FAILURE"
        elif dq_result.failed_count > 0:
            decision, reason = "QUARANTINE", "NON_FATAL_DQ_FAILURE"
        else:
            decision, reason = "PROMOTE", "ALL_DQ_CHECKS_PASSED"

        result = PromotionDecision(
            promotion_id=str(uuid.uuid4()),
            run_id=dq_result.run_id,
            component_id=dq_result.component_id,
            dq_status=dq_result.status,
            decision=decision,
            reason_code=reason,
            decided_at_utc=utc_now(),
        )
        result.validate()
        return result
