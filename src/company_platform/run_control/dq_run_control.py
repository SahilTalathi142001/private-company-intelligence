from pathlib import Path
import sys, pandas as pd
ROOT=Path(__file__).resolve().parents[4]; SRC=ROOT/"src"
if str(SRC) not in sys.path: sys.path.insert(0,str(SRC))
from company_platform.run_control.run_store import RunStore, RUN_COLUMNS, TASK_COLUMNS
from company_platform.run_control.models import RUN_STATUSES,TASK_STATUSES,TRIGGER_TYPES

def main():
    store=RunStore(ROOT/"data/silver/platform/run_control"); runs=store.runs(); tasks=store.tasks(); checks=[]
    def add(n,c): checks.append((n,bool(c)))
    add("RUN_ARTIFACT_EXISTS",store.runs_path.exists()); add("TASK_ARTIFACT_EXISTS",store.tasks_path.exists())
    add("RUN_SCHEMA",list(runs.columns)==RUN_COLUMNS); add("TASK_SCHEMA",list(tasks.columns)==TASK_COLUMNS)
    add("RUN_ID_UNIQUENESS",runs.run_id.is_unique); add("RUN_STATUS_VOCABULARY",runs.status.isin(RUN_STATUSES).all()); add("TRIGGER_TYPE_VOCABULARY",runs.trigger_type.isin(TRIGGER_TYPES).all())
    add("RUN_COMPANY_NONBLANK",runs.company_id.fillna("").astype(str).str.strip().ne("").all()); add("RUN_CORRELATION_NONBLANK",runs.correlation_id.fillna("").astype(str).str.strip().ne("").all())
    add("TASK_ID_UNIQUENESS",tasks.task_id.is_unique); add("RUN_COMPONENT_GRAIN",(tasks.run_id.astype(str)+"|"+tasks.component_id.astype(str)).is_unique)
    add("TASK_STATUS_VOCABULARY",tasks.status.isin(TASK_STATUSES).all()); add("ATTEMPT_NONNEGATIVE",pd.to_numeric(tasks.attempt_number,errors="coerce").ge(0).all()); add("ATTEMPT_WITHIN_MAX",pd.to_numeric(tasks.attempt_number,errors="coerce").le(pd.to_numeric(tasks.max_attempts,errors="coerce")).all())
    add("TASK_RUN_REFERENCES",tasks.run_id.astype(str).isin(set(runs.run_id.astype(str))).all())
    failed=[n for n,p in checks if not p]
    print("========================================"); print("A5 - RUN / TASK CONTROL DQ"); print("========================================")
    print(f"Pipeline run rows   : {len(runs)}"); print(f"Task run rows       : {len(tasks)}"); print(f"Validation checks   : {len(checks)}"); print(f"Failed checks       : {len(failed)}"); print(f"Promotion           : {'PROMOTE' if not failed else 'QUARANTINE'}")
    if failed:
        print("\nFAILED CHECKS"); [print("-",x) for x in failed]; raise SystemExit(1)
    print("\nA5 DQ PASS")
if __name__=="__main__": main()
