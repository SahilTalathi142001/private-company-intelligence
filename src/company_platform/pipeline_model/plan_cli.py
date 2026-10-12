import argparse,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3]; sys.path.insert(0,str(ROOT/"src"))
from company_platform.pipeline_model.pipeline_registry import PipelineRegistry
def main():
 p=argparse.ArgumentParser(); s=p.add_subparsers(dest="command",required=True); s.add_parser("list"); q=s.add_parser("plan"); q.add_argument("--target",action="append"); d=s.add_parser("downstream"); d.add_argument("component_id"); a=p.parse_args(); r=PipelineRegistry(ROOT/"config/pipeline_definition.json")
 if a.command=="list":
  for c in r.all():print(f"{c.component_id:28} | {c.component_type:14} | deps={','.join(c.depends_on) or '-'} | mode={c.execution_mode} | dq={c.dq_gate or '-'}")
 elif a.command=="plan":
  for i,c in enumerate(r.execution_plan(a.target),1):print(f"{i:02d}. {c}")
 else:
  for c in r.affected_downstream(a.component_id):print(c)
if __name__=="__main__":main()
