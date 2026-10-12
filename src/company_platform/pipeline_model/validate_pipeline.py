from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[3]; sys.path.insert(0,str(ROOT/"src"))
from company_platform.pipeline_model.pipeline_registry import PipelineRegistry
def main():
 r=PipelineRegistry(ROOT/"config/pipeline_definition.json"); cs=r.all(); ids={c.component_id for c in cs}; failures=[]
 if len(ids)!=len(cs): failures.append("COMPONENT_ID_UNIQUENESS")
 for c in cs:
  if any(d not in ids for d in c.depends_on):failures.append(f"DEPENDENCY_EXISTS:{c.component_id}")
  if any(x not in ids for x in c.downstream):failures.append(f"DOWNSTREAM_EXISTS:{c.component_id}")
  for d in c.depends_on:
   if c.component_id not in r.get(d).downstream: failures.append(f"BIDIRECTIONAL_DEPENDENCY:{c.component_id}->{d}")
 try: plan=r.execution_plan()
 except Exception: plan=[]; failures.append("NO_DEPENDENCY_CYCLE")
 expected={"financial_normalization":{"sec_ingestion"},"quarter_derivation":{"financial_normalization"},"financial_features":{"quarter_derivation"},"financial_decision":{"financial_features"},"company_profile":{"company_registry","financial_features","financial_decision"},"investment_intelligence":{"company_profile","financial_decision","form_d_ingestion","investor_ownership","valuation"},"company_discovery":{"company_profile","investment_intelligence"}}
 for cid,deps in expected.items():
  if not deps.issubset(set(r.get(cid).depends_on)):failures.append(f"CORE_DEPENDENCIES:{cid}")
 print("========================================\nA4 - PIPELINE & DEPENDENCY MODEL DQ\n========================================")
 print(f"Pipeline components  : {len(cs)}\nValidation checks    : 11\nFailed checks        : {len(failures)}\nPromotion            : {'PROMOTE' if not failures else 'QUARANTINE'}")
 if failures:
  print("\nFAILED CHECKS"); [print("-",x) for x in failures]; raise SystemExit(1)
 print("\nA4 DQ PASS\n\nFULL EXECUTION PLAN"); print(" -> ".join(plan))
if __name__=="__main__":main()
