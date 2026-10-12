from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[4]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from company_platform.pipeline_model.pipeline_registry import PipelineRegistry
from company_platform.incremental.planner import IncrementalPlanner
from company_platform.run_control.run_manager import RunManager
from company_platform.dq_gates.gate_engine import DQGateEngine
from company_platform.orchestrator.engine import Orchestrator

def main():
    registry = PipelineRegistry(ROOT / "config/pipeline_definition.json")
    orch = Orchestrator(
        registry,
        RunManager(),
        IncrementalPlanner(registry),
        DQGateEngine()
    )

    run, decision = orch.create_run(
        company_id="SEC_0000320193",
        correlation_id="a8-dry-run",
        change_type="NEW_SEC_FILING",
        trigger_type="EVENT"
    )

    print("A8 DRY RUN")
    print("run_id:", run.run_id)
    print("mode:", decision["processing_mode"])
    print("root:", decision["root_component_id"])
    print("affected components:")
    for i, component in enumerate(decision["affected_components"], 1):
        print(f"{i:02d}. {component}")
    print("\nNo pipeline component was executed.")

if __name__ == "__main__":
    main()
