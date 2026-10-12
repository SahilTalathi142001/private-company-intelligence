import argparse, json, uuid
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from company_platform.pipeline_model.pipeline_registry import PipelineRegistry
from company_platform.incremental.planner import IncrementalPlanner
from company_platform.incremental.models import IncrementalDecision
from company_platform.incremental.decision_store import IncrementalDecisionStore

def main():
    parser = argparse.ArgumentParser(description="A6 Incremental Processing")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--change-type", required=True)
    p.add_argument("--company-id", required=True)
    p.add_argument("--source-id")
    p.add_argument("--persist", action="store_true")
    sub.add_parser("decisions")
    args = parser.parse_args()

    registry = PipelineRegistry(ROOT / "config" / "pipeline_definition.json")
    planner = IncrementalPlanner(registry)

    if args.command == "plan":
        result = planner.plan_for_change(args.change_type, source_id=args.source_id)
        change_id = str(uuid.uuid4())
        decision = IncrementalDecision(
            change_id, args.company_id, result["processing_mode"],
            result["root_component_id"], result["affected_components"],
            result["reason_code"], "1.0.0"
        )
        print("INCREMENTAL PROCESSING PLAN")
        print(json.dumps({"change_id": change_id, "company_id": args.company_id, **result,
                          "dependency_version": "1.0.0"}, indent=2))
        if args.persist:
            IncrementalDecisionStore(ROOT / "data/silver/platform/incremental/processing_decisions.parquet").append(decision)
            print("\nDecision persisted.")
    else:
        df = IncrementalDecisionStore(ROOT / "data/silver/platform/incremental/processing_decisions.parquet").read()
        print(df.to_string(index=False) if not df.empty else "No decisions.")

if __name__ == "__main__":
    main()
