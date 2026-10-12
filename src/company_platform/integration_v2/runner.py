


from __future__ import annotations

import argparse
import uuid

from company_platform.pipeline_model.pipeline_registry import PipelineRegistry
from company_platform.run_control.run_manager import RunManager
from company_platform.incremental.planner import IncrementalPlanner
from company_platform.dq_gates.gate_engine import DQGateEngine
from company_platform.orchestrator.engine import Orchestrator
from .components import HANDLERS

PIPELINE_VERSION = "integration-v2.6.0"
PIPELINE_TARGETS = ["company_discovery"]


def build_orchestrator() -> Orchestrator:
    registry = PipelineRegistry()
    manager = RunManager()
    planner = IncrementalPlanner(registry)
    return Orchestrator(
        registry, manager, planner, DQGateEngine(), HANDLERS
    )


def build_execution_plan(registry: PipelineRegistry) -> list[str]:
    """Resolve the complete dependency chain for the serving target."""
    plan = registry.execution_plan(PIPELINE_TARGETS)

    # Support registries that return component objects as well as IDs.
    component_ids = [
        item if isinstance(item, str) else item.component_id
        for item in plan
    ]

    if not component_ids:
        raise RuntimeError("Pipeline planner returned an empty execution plan")

    required = set(PIPELINE_TARGETS)
    if not required.issubset(set(component_ids)):
        raise RuntimeError(
            f"Execution plan does not include targets: "
            f"{sorted(required - set(component_ids))}"
        )

    return component_ids


def normalize_ciks(raw_ciks: list[str], parser) -> list[str]:
    ciks = [value.strip() for value in raw_ciks if value.strip()]
    if not ciks or any(not cik.isdigit() or len(cik) > 10 for cik in ciks):
        parser.error("CIKs must be non-empty numeric values of at most 10 digits")

    normalized = [cik.zfill(10) for cik in ciks]
    if len(set(normalized)) != len(normalized):
        parser.error("Duplicate CIKs are not allowed")

    return normalized


def run(
    cik: str,
    plan: list[str],
    trigger_event_id: str | None = None,
) -> tuple[str, str]:
    company_id = f"SEC_{cik}"
    correlation_id = str(uuid.uuid4())
    orch = build_orchestrator()

    run_record = orch.run_manager.create_run_from_plan(
        company_id=company_id,
        correlation_id=correlation_id,
        plan=plan,
        pipeline_version=PIPELINE_VERSION,
        trigger_type="EVENT" if trigger_event_id else "MANUAL",
        trigger_event_id=trigger_event_id,
    )

    status = orch.execute_run(run_record.run_id)
    return run_record.run_id, status


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Integration v2 planner-driven pipeline"
    )

    parser.add_argument("--cik", help="Run one SEC CIK")
    parser.add_argument(
        "--ciks",
        help="Comma-separated SEC CIKs; executed sequentially for isolation",
    )
    parser.add_argument("--trigger-event-id")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--resume-run-id",
        help="Resume an existing run after reconciling stale RUNNING tasks",
    )

    args = parser.parse_args()

    orch = build_orchestrator()

    # Resume is a standalone operation.
    if args.resume_run_id:
        if args.cik or args.ciks:
            parser.error(
                "--resume-run-id cannot be combined with --cik or --ciks"
            )

        status = orch.execute_run(args.resume_run_id)

        print(f"RUN_ID: {args.resume_run_id}")
        print(f"RUN_STATUS: {status}")
        return

    # New execution requires exactly one CIK input mode.
    if bool(args.cik) == bool(args.ciks):
        parser.error(
            "exactly one of --cik or --ciks is required "
            "when --resume-run-id is not supplied"
        )

    raw_ciks = args.ciks.split(",") if args.ciks else [args.cik]
    normalized = normalize_ciks(raw_ciks, parser)

    plan = build_execution_plan(orch.registry)

    if args.dry_run:
        print("PLANNER-GENERATED EXECUTION PLAN")
        print(" -> ".join(plan))

        for cik in normalized:
            print(f"CIK: {cik}")

        print(f"COMPANIES: {len(normalized)}")
        print("EXECUTION: sequential")
        print("RUN RECORD: not created")
        return

    results = []

    for cik in normalized:
        run_id, status = run(
            cik,
            plan,
            args.trigger_event_id,
        )

        results.append((cik, run_id, status))

        print(
            f"CIK: {cik} | "
            f"RUN_ID: {run_id} | "
            f"RUN_STATUS: {status}",
            flush=True,
        )

    failed = [
        item for item in results
        if item[2] != "SUCCESS"
    ]

    print(f"COMPANIES: {len(results)}")
    print(f"SUCCEEDED: {len(results) - len(failed)}")
    print(f"FAILED: {len(failed)}")

    if failed:
        raise SystemExit(1)
if __name__ == "__main__":
    main()
