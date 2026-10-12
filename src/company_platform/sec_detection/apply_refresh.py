"""Apply newly detected SEC filings for selected companies.

Detection stays in SECLiveRefresh. Execution stays in EventConsumer, which
plans a NEW_SEC_FILING incremental closure and runs it for that company.
A failed company is recorded and does not delete another company's gold.
"""

from __future__ import annotations

import argparse
import json

from company_platform.incremental.planner import IncrementalPlanner
from company_platform.pipeline_model.pipeline_registry import PipelineRegistry
from company_platform.sec_detection.live_refresh import SECLiveRefresh


def filing_plan() -> dict:
    plan = IncrementalPlanner(PipelineRegistry()).plan_for_change(
        "NEW_SEC_FILING"
    )
    if plan.get("processing_mode") != "INCREMENTAL":
        raise RuntimeError(
            f"NEW_SEC_FILING must be incremental, got {plan}"
        )
    affected = plan.get("affected_components") or []
    if "company_discovery" not in affected:
        raise RuntimeError(
            "NEW_SEC_FILING plan does not keep company discovery "
            "as the publication gate."
        )
    return plan


def apply_refresh(
    ciks: list[str],
    *,
    max_filings_per_company: int = 1,
    execute: bool = True,
) -> dict:
    """Detect and, when requested, process filings for the given CIKs only."""
    if not ciks:
        raise ValueError(
            "At least one CIK is required. Refresh does not rerun the universe."
        )
    normalized = []
    for cik in ciks:
        text = str(cik).strip()
        if not text.isdigit() or len(text) > 10:
            raise ValueError(f"Invalid CIK: {cik}")
        normalized.append(text.zfill(10))

    plan = filing_plan()
    if not execute:
        return {
            "mode": "PLAN_ONLY",
            "ciks": normalized,
            "plan": plan,
            "runs": [],
        }

    refresh = SECLiveRefresh().refresh_once(
        ciks=normalized,
        max_filings_per_company=max_filings_per_company,
    )
    from company_platform.orchestrator.event_consumer import EventConsumer

    consumer = EventConsumer()
    runs = []
    for event_id in refresh.get("events") or []:
        try:
            result = consumer.consume_event(event_id)
        except Exception as exc:
            result = {
                "event_id": event_id,
                "run_status": "FAILED",
                "error": str(exc),
            }
        runs.append(result)
    failed = [
        run for run in runs
        if run.get("run_status") not in {"SUCCESS", "PARTIAL", None}
        and run.get("action") not in {
            "NO_OP_ALREADY_COMPLETED",
            "NO_OP_ALREADY_CLAIMED",
        }
    ]
    return {
        "mode": "EXECUTE",
        "ciks": normalized,
        "plan": {
            "processing_mode": plan["processing_mode"],
            "root_component_id": plan["root_component_id"],
            "reason_code": plan["reason_code"],
            "affected_components": plan["affected_components"],
        },
        "refresh_status": refresh.get("status"),
        "emitted": refresh.get("emitted", 0),
        "refresh_failed": refresh.get("failed", 0),
        "runs": runs,
        "company_failures": len(failed),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Detect new SEC filings for selected companies and process "
            "each company through the incremental plan."
        )
    )
    parser.add_argument(
        "--cik",
        action="append",
        required=True,
        help="10-digit CIK. Repeat for more than one company.",
    )
    parser.add_argument(
        "--max-filings",
        type=int,
        default=1,
        help="Maximum new filings to emit per company.",
    )
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="Print the incremental plan without contacting SEC or executing.",
    )
    args = parser.parse_args()
    result = apply_refresh(
        args.cik,
        max_filings_per_company=args.max_filings,
        execute=not args.plan_only,
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
