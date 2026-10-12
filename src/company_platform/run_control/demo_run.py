from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[4]; SRC=ROOT/"src"
if str(SRC) not in sys.path: sys.path.insert(0,str(SRC))
from company_platform.run_control.run_manager import RunManager

def main():
    plan=["company_registry","source_registry","sec_ingestion","financial_normalization","quarter_derivation","financial_features","financial_decision","company_profile","investment_intelligence","company_discovery"]
    m=RunManager(); r=m.create_run_from_plan("SEC_0000320193","a5-demo-correlation",plan,trigger_type="MANUAL"); m.start_run(r.run_id)
    t=m.store.get_tasks_for_run(r.run_id); first=t.iloc[0].task_id; m.start_task(first); m.succeed_task(first,"PASS")
    print("A5 DEMO RUN"); print("run_id:",r.run_id); print("tasks created:",len(t)); print("first task:",t.iloc[0].component_id,"-> SUCCESS"); print("remaining tasks stay PENDING.")
if __name__=="__main__": main()
