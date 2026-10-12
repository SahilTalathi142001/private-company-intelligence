from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from company_platform.pipeline_model.pipeline_registry import PipelineRegistry
from company_platform.incremental.planner import IncrementalPlanner
from company_platform.run_control.run_manager import RunManager
from company_platform.dq_gates.gate_engine import DQGateEngine
from company_platform.orchestrator.engine import Orchestrator, ExecutionOutcome

def main():
    registry = PipelineRegistry(ROOT / "config/pipeline_definition.json")
    checks = []

    def check(name, condition):
        checks.append((name, bool(condition)))

    # 1. Event -> change -> incremental plan
    orch = Orchestrator(registry, RunManager(), IncrementalPlanner(registry), DQGateEngine())
    decision = orch.build_plan("NEW_SEC_FILING", "SEC_0000320193")
    check("SEC_EVENT_PRODUCES_INCREMENTAL_PLAN", decision["processing_mode"] == "INCREMENTAL")
    check("SEC_ROOT_IS_SEC_INGESTION", decision["root_component_id"] == "sec_ingestion")
    check("SEC_PLAN_REACHES_DISCOVERY", "company_discovery" in decision["affected_components"])

    # 2. A7 policy is respected by orchestration outcome.
    gate = DQGateEngine()
    clean = gate.promote(gate.evaluate("r1","financial_features",10,0))
    warning = gate.promote(gate.evaluate("r2","funding",10,1,warning_count=1))
    fatal = gate.promote(gate.evaluate("r3","company_profile",10,1,fatal_count=1))
    check("A7_PASS_PROMOTES", clean.decision == "PROMOTE")
    check("A7_WARNING_QUARANTINES", warning.decision == "QUARANTINE")
    check("A7_FATAL_BLOCKS", fatal.decision == "BLOCK")

    # 3. Handler failure blocks dependents.
    called = []
    def fail_handler(**kwargs):
        called.append(kwargs["component_id"])
        return ExecutionOutcome("FAILED", "simulated failure")

    orch2 = Orchestrator(
        registry, RunManager(), IncrementalPlanner(registry), DQGateEngine(),
        component_handlers={"sec_ingestion": fail_handler},
        dependency_artifact_validator=lambda *_: True,
    )
    run, _ = orch2.create_run("SEC_0000320193", "a8-failure-test", "NEW_SEC_FILING")
    status = orch2.execute_run(run.run_id)
    tasks = orch2.run_manager.store.get_tasks_for_run(run.run_id)
    sec = tasks[tasks["component_id"] == "sec_ingestion"].iloc[0]
    downstream = tasks[tasks["component_id"] == "financial_normalization"].iloc[0]
    check("FAILED_HANDLER_RECORDS_FAILED", sec["status"] == "FAILED")
    check("FAILED_UPSTREAM_BLOCKS_DEPENDENT", downstream["status"] == "BLOCKED")
    check("RUN_NOT_SUCCESS_AFTER_FAILURE", status != "SUCCESS")

    failed = [n for n, ok in checks if not ok]
    print("========================================")
    print("A8 - ORCHESTRATOR DQ")
    print("========================================")
    print(f"Validation checks    : {len(checks)}")
    print(f"Failed checks        : {len(failed)}")
    print(f"Promotion            : {'PROMOTE' if not failed else 'QUARANTINE'}")
    if failed:
        for x in failed:
            print("-", x)
        raise SystemExit(1)
    print("\nA8 DQ PASS")

if __name__ == "__main__":
    main()
