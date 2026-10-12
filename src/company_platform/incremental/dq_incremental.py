from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
from company_platform.pipeline_model.pipeline_registry import PipelineRegistry
from company_platform.incremental.planner import IncrementalPlanner

def main():
    r = PipelineRegistry(ROOT / "config/pipeline_definition.json")
    p = IncrementalPlanner(r)
    checks = []
    def c(name, condition):
        checks.append((name, bool(condition)))
    sec = p.plan_for_change("NEW_SEC_FILING")
    formd = p.plan_for_change("SOURCE_DATA_CHANGED", source_id="SEC_FORM_D")
    doc = p.plan_for_change("DOCUMENT_RECEIVED")
    new = p.plan_for_change("NEW_COMPANY")
    c("A4_GRAPH_TOPOLOGICAL_ORDER", len(r.execution_plan()) == len(r.all()))
    c("SEC_IS_INCREMENTAL", sec["processing_mode"] == "INCREMENTAL")
    c("SEC_ROOT_CORRECT", sec["root_component_id"] == "sec_ingestion")
    c("SEC_REACHES_FINANCIAL_DECISION", "financial_decision" in sec["affected_components"])
    c("SEC_REACHES_DISCOVERY", "company_discovery" in sec["affected_components"])
    c("SEC_REACHES_DOCUMENT_INGESTION", "document_ingestion" in sec["affected_components"])
    c("FORM_D_IS_INCREMENTAL", formd["processing_mode"] == "INCREMENTAL")
    c("FORM_D_ROOT_CORRECT", formd["root_component_id"] == "form_d_ingestion")
    c("FORM_D_REACHES_INVESTMENT", "investment_intelligence" in formd["affected_components"])
    c("DOCUMENT_ROOT_CORRECT", doc["root_component_id"] == "document_ingestion")
    c("NEW_COMPANY_IS_FULL", new["processing_mode"] == "FULL")
    c("NEW_COMPANY_STARTS_AT_REGISTRY", new["root_component_id"] == "company_registry")
    failed = [x for x, ok in checks if not ok]
    print("========================================")
    print("A6 - INCREMENTAL PROCESSING DQ")
    print("========================================")
    print(f"Validation checks    : {len(checks)}")
    print(f"Failed checks        : {len(failed)}")
    print(f"Promotion            : {'PROMOTE' if not failed else 'QUARANTINE'}")
    if failed:
        for x in failed: print("-", x)
        raise SystemExit(1)
    print("\nA6 DQ PASS")
if __name__ == "__main__":
    main()
